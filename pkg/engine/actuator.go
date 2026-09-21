package engine

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

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

// Actuator applies desired state to the simulated world. Calls are
// idempotent: when actual already matches desired, Ensure must report
// Unchanged and perform no side effect.
type Actuator interface {
	Ensure(ctx context.Context, key string, desired *apis.WidgetSpec, actual *apis.ActualState) (*apis.ActualState, Action, error)
	Remove(ctx context.Context, key string, actual *apis.ActualState) (bool, error)
}

// FakeActuator is the controllable simulated backend. It persists the
// simulated real-world resource in memory keyed by object name; its
// operation log makes side effects observable to tests.
type FakeActuator struct {
	mu       sync.Mutex
	state    map[string]*apis.ActualState
	rules    map[string]FailureRule
	calls    map[string]int
	ops      []Op
	id       int64
	inflight map[string]int
	maxFor   map[string]int
	conc     int64
}

// NewFakeActuator constructs an empty fake backend.
func NewFakeActuator() *FakeActuator {
	return &FakeActuator{
		state:    map[string]*apis.ActualState{},
		rules:    map[string]FailureRule{},
		calls:    map[string]int{},
		inflight: map[string]int{},
		maxFor:   map[string]int{},
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
	a.mu.Lock()
	defer a.mu.Unlock()
	if x, ok := a.state[key]; ok {
		cp := *x
		return &cp
	}
	return nil
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
func (a *FakeActuator) Ensure(ctx context.Context, key string, desired *apis.WidgetSpec, actual *apis.ActualState) (*apis.ActualState, Action, error) {
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
	existing, ok := a.state[key]
	want := apis.ActualState{
		Replicas:     desired.Replicas,
		Mode:         desired.Mode,
		GraceSeconds: desired.GraceSeconds,
		Ready:        true,
	}
	if !ok || existing == nil {
		a.id++
		want.ResourceID = "res-" + strconv.FormatInt(a.id, 10)
		cp := want
		a.state[key] = &cp
		a.record(Op{Key: key, Method: "Ensure", Action: ActionCreated})
		return a.copyLocked(key), ActionCreated, nil
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
	a.state[key] = &cp
	a.record(Op{Key: key, Method: "Ensure", Action: ActionUpdated})
	return a.copyLocked(key), ActionUpdated, nil
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
	if _, ok := a.state[key]; !ok {
		return false, nil
	}
	delete(a.state, key)
	a.record(Op{Key: key, Method: "Remove", Action: ActionDeleted})
	return true, nil
}

func (a *FakeActuator) copyLocked(key string) *apis.ActualState {
	cp := *a.state[key]
	return &cp
}
