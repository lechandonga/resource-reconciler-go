package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/lechandonga/resource-reconciler-go/pkg/actual"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
)

// Failure classification used by the fake actuator.
type FailureKind int

const (
	FailNone FailureKind = iota
	FailTransient
	FailDeterministic
)

func (k FailureKind) String() string {
	switch k {
	case FailTransient:
		return "Transient"
	case FailDeterministic:
		return "Deterministic"
	default:
		return "None"
	}
}

var (
	// ErrTransient simulates a retryable backend failure (timeout, 5xx).
	ErrTransient = errors.New("simulated transient backend failure")
	// ErrDeterministic simulates a non-retryable failure (4xx, bad config).
	ErrDeterministic = errors.New("simulated deterministic backend failure")
)

// FailureRule tells the fake actuator how to fail a named object.
type FailureRule struct {
	Kind  FailureKind
	Times int // transient: fail this many calls, then succeed;
	// deterministic: fail every call while the rule is present
}

// Op is one recorded side effect, for audit/idempotency assertions.
type Op struct {
	Key    string
	Method string // "Ensure" / "Remove"
	Action Action
	Failed bool
}

// Action describes what an actuator call effectively did.
type Action string

const (
	ActionCreated   Action = "Created"
	ActionUpdated   Action = "Updated"
	ActionUnchanged Action = "Unchanged"
	ActionDeleted   Action = "Deleted"
)

// Crash points identify the windows in which the process may be killed
// after the durable side effect took effect but before the control plane
// recorded the result.
const (
	// CrashAfterCreate fires once the resource has been durably created,
	// before Ensure returns and before status.actual is persisted.
	CrashAfterCreate = "after-create"
	// CrashAfterUpdate fires after a durably persisted update, before the
	// new actual state is recorded in the control plane.
	CrashAfterUpdate = "after-update"
	// CrashAfterRemove fires after the resource was durably deleted,
	// before the deletion phase is recorded (actual=nil).
	CrashAfterRemove = "after-remove"
)

// CrashHook is invoked immediately after a durable mutation. A hook that
// kills the process simulates a crash exactly between "the action took
// effect" and "the result was accounted for". Hooks must be concurrency
// safe. point is one of the CrashAfter* constants.
type CrashHook func(key, point string)

// Actuator applies desired state to the simulated world. Calls are
// idempotent: when actual already matches desired, Ensure must report
// Unchanged and perform no side effect.
type Actuator interface {
	Ensure(ctx context.Context, key string, desired *apis.WidgetSpec, actual *apis.ActualState) (*apis.ActualState, Action, error)
	Remove(ctx context.Context, key string, actual *apis.ActualState) (bool, error)
}

// FakeActuator is the controllable simulated backend. The simulated
// real-world resource lives in an actual.Store, so with a directory
// backend the resource (including its stable identity) survives process
// restarts. Its in-memory operation log makes side effects observable.
type FakeActuator struct {
	mu       sync.Mutex
	backend  actual.Store
	rules    map[string]FailureRule
	calls    map[string]int
	ops      []Op
	id       int64
	inflight map[string]int
	maxFor   map[string]int
	conc     int64
	crash    atomic.Pointer[CrashHook]
}

// NewFakeActuator constructs an empty fake backend.
func NewFakeActuator() *FakeActuator {
	return NewFakeActuatorWithStore(actual.NewMemStore())
}

// NewFakeActuatorWithStore constructs a fake backend backed by an
// arbitrary actual store (e.g. an actual.DirStore that survives
// restarts). A nil store is replaced by an in-memory one.
func NewFakeActuatorWithStore(store actual.Store) *FakeActuator {
	if store == nil {
		store = actual.NewMemStore()
	}
	return &FakeActuator{
		backend:  store,
		rules:    map[string]FailureRule{},
		calls:    map[string]int{},
		inflight: map[string]int{},
		maxFor:   map[string]int{},
	}
}

// Actual exposes the durable actual-resource backend. The controller uses
// it for startup and periodic ownership reconciliation.
func (a *FakeActuator) Actual() actual.Store { return a.backend }

// SetCrashHook installs (nil clears) a crash injection hook. The hook
// runs right after a durable mutation and before the result is returned.
func (a *FakeActuator) SetCrashHook(h CrashHook) {
	if h == nil {
		a.crash.Store(nil)
	} else {
		a.crash.Store(&h)
	}
}

// SetRule installs (or replaces with FailNone to clear) a failure rule.
func (a *FakeActuator) SetRule(key string, rule FailureRule) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if rule.Kind == FailNone {
		delete(a.rules, key)
	} else {
		a.rules[key] = rule
	}
}

// Ops returns a detached copy of the recorded side effects.
func (a *FakeActuator) Ops() []Op {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Op(nil), a.ops...)
}

// SideEffectCount returns how many mutating calls (Created/Updated/Deleted)
// actually happened. Repeating the same desired state must not grow it.
func (a *FakeActuator) SideEffectCount(key string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, op := range a.ops {
		if op.Key == key && !op.Failed && (op.Action == ActionCreated || op.Action == ActionUpdated || op.Action == ActionDeleted) {
			n++
		}
	}
	return n
}

// MaxConcurrent returns the peak number of Ensure/Remove calls in flight
// at the same time for the same key (must stay at most 1).
func (a *FakeActuator) MaxConcurrent(key string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.maxFor[key]
}

// SnapshotActual returns the current simulated resource, or nil.
func (a *FakeActuator) SnapshotActual(key string) *apis.ActualState {
	if x, err := a.backend.Load(context.Background(), key); err == nil {
		return x
	}
	return nil
}

// crashHook returns the current hook under lock.
func (a *FakeActuator) crashHook() CrashHook {
	if hp := a.crash.Load(); hp != nil {
		return *hp
	}
	return nil
}

// fireCrash invokes the hook (if any) after a durable mutation.
func (a *FakeActuator) fireCrash(key, point string) {
	if h := a.crashHook(); h != nil {
		h(key, point)
	}
}

func (a *FakeActuator) enter(key string) {
	a.mu.Lock()
	c := a.inflight[key] + 1
	a.inflight[key] = c
	if c > a.maxFor[key] {
		a.maxFor[key] = c
	}
	a.mu.Unlock()
	atomic.AddInt64(&a.conc, 1)
}

func (a *FakeActuator) exit(key string) {
	a.mu.Lock()
	a.inflight[key]--
	a.mu.Unlock()
	atomic.AddInt64(&a.conc, -1)
}

// maybeFail consumes one failure rule decision. Caller holds no lock.
func (a *FakeActuator) maybeFail(key, method string) (error, bool) {
	a.mu.Lock()
	rule, hasRule := a.rules[key]
	a.mu.Unlock()
	if !hasRule || rule.Kind == FailNone {
		return nil, false
	}
	a.mu.Lock()
	a.calls[key]++
	n := a.calls[key]
	a.mu.Unlock()
	switch rule.Kind {
	case FailDeterministic:
		a.record(Op{Key: key, Method: method, Failed: true})
		return fmt.Errorf("%w: %s", ErrDeterministic, key), true
	case FailTransient:
		if n <= rule.Times {
			a.record(Op{Key: key, Method: method, Failed: true})
			return fmt.Errorf("%w (attempt %d/%d) for %s", ErrTransient, n, rule.Times, key), true
		}
		// budget of failures exhausted: succeed forever afterwards
		a.mu.Lock()
		delete(a.rules, key)
		a.mu.Unlock()
		return nil, false
	}
	return nil, false
}

func (a *FakeActuator) record(op Op) {
	a.ops = append(a.ops, op)
}

// Ensure converges the simulated resource to desired. It never mutates
// anything when actual already matches: the response is Unchanged.
func (a *FakeActuator) Ensure(ctx context.Context, key string, desired *apis.WidgetSpec, hint *apis.ActualState) (*apis.ActualState, Action, error) {
	if err := ctx.Err(); err != nil {
		return nil, ActionUnchanged, err
	}
	a.enter(key)
	defer a.exit(key)
	if err, failed := a.maybeFail(key, "Ensure"); failed {
		return nil, ActionUnchanged, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	want := apis.ActualState{
		Replicas:     desired.Replicas,
		Mode:         desired.Mode,
		GraceSeconds: desired.GraceSeconds,
		Ready:        true,
	}
	existing, lerr := a.backend.Load(ctx, key)
	if errors.Is(lerr, actual.ErrNotFound) || existing == nil {
		// Identity is deterministic in the owning key so a re-create after
		// a backend loss keeps the same identity. A hint carried by the
		// control plane (legacy data) is adopted verbatim.
		if hint != nil && hint.ResourceID != "" {
			want.ResourceID = hint.ResourceID
		} else {
			want.ResourceID = stableResourceID(key)
		}
		cp := want
		if err := a.backend.Save(ctx, key, &cp); err != nil {
			return nil, ActionUnchanged, err
		}
		a.fireCrash(key, CrashAfterCreate)
		a.record(Op{Key: key, Method: "Ensure", Action: ActionCreated})
		got := cp
		return &got, ActionCreated, nil
	}
	if lerr != nil {
		return nil, ActionUnchanged, lerr
	}
	if existing.Replicas == want.Replicas && existing.Mode == want.Mode &&
		existing.GraceSeconds == want.GraceSeconds {
		// already at desired state: no side effect, no rewrite
		got := *existing
		return &got, ActionUnchanged, nil
	}
	// keep stable resource identity across updates
	want.ResourceID = existing.ResourceID
	cp := want
	if err := a.backend.Save(ctx, key, &cp); err != nil {
		return nil, ActionUnchanged, err
	}
	a.fireCrash(key, CrashAfterUpdate)
	a.record(Op{Key: key, Method: "Ensure", Action: ActionUpdated})
	got := cp
	return &got, ActionUpdated, nil
}

// Remove deletes the simulated resource. It is idempotent: removing a
// resource that is already gone returns (false, nil) and records
// nothing, so retried deletions after a restart produce no duplicate
// side effect.
func (a *FakeActuator) Remove(ctx context.Context, key string, actual *apis.ActualState) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	a.enter(key)
	defer a.exit(key)
	if err, failed := a.maybeFail(key, "Remove"); failed {
		return false, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if existing, err := a.backend.Load(ctx, key); err != nil || existing == nil {
		return false, nil
	}
	if err := a.backend.Remove(ctx, key); err != nil {
		return false, err
	}
	a.fireCrash(key, CrashAfterRemove)
	a.record(Op{Key: key, Method: "Remove", Action: ActionDeleted})
	return true, nil
}

// stableResourceID derives a stable, restart-independent identity from
// the owning object key.
func stableResourceID(key string) string {
	sum := sha256Sum(key)
	return "res-" + sum
}
