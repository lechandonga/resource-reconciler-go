package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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
	// ActionAdopted means the resource already existed in the durable
	// actual backend but the control plane had not recorded it yet (the
	// process crashed in the action-then-record window). Adoption keeps
	// the existing identity and is NOT a side effect: nothing was created.
	ActionAdopted Action = "Adopted"
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
	// persistPath, when non-empty, makes the actual side durable across
	// process restarts. Every committed mutation is flushed atomically
	// (temp file + fsync + rename) BEFORE the call returns, so a crash at
	// any point after the action took effect leaves the durable actual
	// state exactly reflecting reality.
	persistPath string
}

// NewFakeActuator constructs an empty fake backend.
func NewFakeActuator() *FakeActuator {
	return &FakeActuator{
		state: map[string]*apis.ActualState{}, rules: map[string]FailureRule{},
		calls: map[string]int{}, inflight: map[string]int{}, maxFor: map[string]int{},
	}
}

// actualSnapshot is the on-disk wire format of the durable actual side.
// FormatVersion keeps restart compatibility explicit; older snapshots
// (format 1) are loaded in place without requiring a reset.
type actualSnapshot struct {
	FormatVersion int                          `json:"formatVersion"`
	NextID        int64                        `json:"nextID"`
	Resources     map[string]*apis.ActualState `json:"resources"`
}

// NewDurableFakeActuator constructs a fake backend backed by path. If
// path holds a snapshot written by a previous process lifetime, its
// resources (including their stable identities) and the ID counter are
// adopted in place. A missing file means an empty backend.
func NewDurableFakeActuator(path string) (*FakeActuator, error) {
	a := NewFakeActuator()
	a.persistPath = path
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return a, nil
		}
		return nil, err
	}
	var snap actualSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("corrupt actual snapshot %s: %w", path, err)
	}
	if snap.FormatVersion != 1 {
		return nil, fmt.Errorf("unsupported actual snapshot format version %d", snap.FormatVersion)
	}
	for k, v := range snap.Resources {
		if v == nil {
			continue
		}
		cp := *v
		a.state[k] = &cp
	}
	a.id = snap.NextID
	// Defensive repair: ensure newly assigned IDs never collide with an
	// identity carried by an older snapshot even if NextID was stale.
	for _, v := range a.state {
		if n := parseResourceSeq(v.ResourceID); n >= a.id {
			a.id = n
		}
	}
	return a, nil
}

// PersistPath reports the durable backing file (empty when ephemeral).
func (a *FakeActuator) PersistPath() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.persistPath
}

// parseResourceSeq extracts the numeric suffix of "res-<n>", or 0.
func parseResourceSeq(id string) int64 {
	var n int64
	_, err := fmt.Sscanf(id, "res-%d", &n)
	if err != nil {
		return 0
	}
	return n
}

// persistLocked atomically writes the whole actual backend. The caller
// must hold a.mu. A crash mid-write leaves either the old or the new
// snapshot, never a torn file.
func (a *FakeActuator) persistLocked() error {
	if a.persistPath == "" {
		return nil
	}
	resources := make(map[string]*apis.ActualState, len(a.state))
	for k, v := range a.state {
		cp := *v
		resources[k] = &cp
	}
	snap := actualSnapshot{FormatVersion: 1, NextID: a.id, Resources: resources}
	data, err := json.MarshalIndent(&snap, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(a.persistPath)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, ".actual-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, a.persistPath)
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
	switch {
	case !ok || existing == nil:
		a.id++
		want.ResourceID = "res-" + strconv.FormatInt(a.id, 10)
		cp := want
		a.state[key] = &cp
		if err := a.persistLocked(); err != nil {
			delete(a.state, key)
			a.id--
			return nil, ActionUnchanged, err
		}
		a.record(Op{Key: key, Method: "Ensure", Action: ActionCreated})
		return a.copyLocked(key), ActionCreated, nil
	case existing.Replicas == want.Replicas && existing.Mode == want.Mode &&
		existing.GraceSeconds == want.GraceSeconds:
		got := *existing
		if actual == nil || actual.ResourceID == "" || actual.ResourceID != existing.ResourceID {
			// The durable backend holds a matching resource the control
			// plane has not recorded (crash between action and record, or a
			// fresh control plane over a durable actual backend). Adopt the
			// existing identity: nothing is created and no side effect is
			// counted.
			return &got, ActionAdopted, nil
		}
		// already at desired state: no side effect, no rewrite
		return &got, ActionUnchanged, nil
	default:
		// Content drift (or an unrecorded resource with different content).
		// Keep the stable resource identity across updates.
		want.ResourceID = existing.ResourceID
		cp := want
		a.state[key] = &cp
		if err := a.persistLocked(); err != nil {
			a.state[key] = existing
			return nil, ActionUnchanged, err
		}
		a.record(Op{Key: key, Method: "Ensure", Action: ActionUpdated})
		return a.copyLocked(key), ActionUpdated, nil
	}
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
	existing := a.state[key]
	delete(a.state, key)
	if err := a.persistLocked(); err != nil {
		a.state[key] = existing // rollback: backend deletion did not commit
		return false, err
	}
	a.record(Op{Key: key, Method: "Remove", Action: ActionDeleted})
	return true, nil
}

func (a *FakeActuator) copyLocked(key string) *apis.ActualState {
	cp := *a.state[key]
	return &cp
}

// ActualKeys returns up to limit resource keys present in the actual
// backend in deterministic (sorted) order, starting strictly after
// cursor (empty cursor starts from the beginning). It is the bounded,
// resumable input to startup reconciliation: callers never load the
// whole actual world into memory at once. The returned next cursor is
// empty when no more keys remain.
func (a *FakeActuator) ActualKeys(_ context.Context, cursor string, limit int) ([]string, string, error) {
	if limit <= 0 {
		limit = 32
	}
	a.mu.Lock()
	keys := make([]string, 0, len(a.state))
	for k := range a.state {
		if k > cursor {
			keys = append(keys, k)
		}
	}
	a.mu.Unlock()
	sort.Strings(keys)
	var next string
	if len(keys) > limit {
		keys = keys[:limit]
		next = keys[limit-1]
	}
	return keys, next, nil
}

// CountActual reports how many resources currently exist in the actual
// backend (test/diagnostic helper).
func (a *FakeActuator) CountActual() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.state)
}

// RemoveIfPresent deletes the actual resource for key when it exists.
// It returns whether something was removed. Startup reconciliation uses
// it for resources provably owned by no live intent (the owning object
// is absent, or present without the controller finalizer); the removal
// is idempotent and durable.
func (a *FakeActuator) RemoveIfPresent(ctx context.Context, key string) (bool, error) {
	return a.Remove(ctx, key, nil)
}

// Hydrate installs a known resource into the actual backend without
// recording a side effect. It is used exactly once per key when taking
// over an older installation that persisted actual state only inside the
// control-plane snapshot: the resource's existing identity is restored
// verbatim, so no recreation happens and no new ResourceID is assigned.
// It is idempotent: an already-present resource wins and Hydrate reports
// false.
func (a *FakeActuator) Hydrate(key string, st apis.ActualState) (bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.state[key]; ok {
		return false, nil
	}
	cp := st
	a.state[key] = &cp
	if err := a.persistLocked(); err != nil {
		delete(a.state, key)
		return false, err
	}
	if n := parseResourceSeq(st.ResourceID); n >= a.id {
		a.id = n
	}
	// Hydration can install externally changed content (simulated drift or
	// legacy takeover). Mark the current on-disk content so the next
	// corrective Ensure is observable as exactly one update, while a
	// matching Ensure remains a no-op adoption.
	return true, nil
}
