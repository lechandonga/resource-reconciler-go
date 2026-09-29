package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

// errContinue is an internal signal that pass finished one durable step
// (e.g. finalizer added, actual resource removed) and the reconciler must
// re-read and run the next step immediately. Unlike storage.ErrConflict
// it is an expected multi-step transition, not a failed write.
var errContinue = errors.New("continue reconciliation")

// CrashPoint identifies a precise moment in the action/record sequence
// that tests can crash the process at, to exercise recovery windows.
type CrashPoint string

const (
	// CrashAfterEnsure fires after the actuator has committed an Ensure
	// (create/update/adopt, durably persisted in the actual backend) but
	// before any result is written to the control-plane store.
	CrashAfterEnsure CrashPoint = "AfterEnsure"
	// CrashAfterRemove fires after the actuator has durably removed the
	// actual resource but before st.Actual=nil is recorded.
	CrashAfterRemove CrashPoint = "AfterRemove"
)

// ErrInjectedCrash is returned to the caller when a crash hook fired. It
// simulates an abrupt process termination at the hook point: the current
// reconcile pass stops immediately with no further store write.
var ErrInjectedCrash = errors.New("injected crash at reconciliation barrier")

// CrashHook is invoked at a CrashPoint. Returning true aborts the pass
// with ErrInjectedCrash right there, modelling SIGKILL at that instant.
type CrashHook func(key string, at CrashPoint) bool

// Result tells the controller how to schedule the next attempt.
type Result struct {
	RequeueAfter time.Duration
	Reason       string
}

// ObjectStore is the persistence contract the reconciler needs.
type ObjectStore interface {
	Get(ctx context.Context, name string) (*apis.RawObject, int64, error)
	List(ctx context.Context) ([]*apis.RawObject, error)
	Update(ctx context.Context, obj *apis.RawObject, expectedRV int64) (*apis.RawObject, int64, error)
	Delete(ctx context.Context, name string, expectedRV int64) error
}

// DependencyEnqueuer re-enqueues blocked dependents when the graph
// changes (creation readiness or final removal).
type DependencyEnqueuer interface {
	Add(key string) error
}

// Reconciler converges one object at a time. Every state transition is
// persisted through compare-and-swap: a write based on stale state fails
// with a conflict and the pass is retried from fresh reads, so a spec
// updated mid-convergence always converges to the newest version.
type Reconciler struct {
	store   ObjectStore
	act     Actuator
	clock   Clock
	backoff BackoffPolicy
	sink    DecisionSink
	jitter  func() float64
	deps    DependencyEnqueuer
	crash   CrashHook

	mu sync.Mutex
}

// NewReconciler wires a reconciler.
func NewReconciler(store ObjectStore, act Actuator, clock Clock, bo BackoffPolicy, sink DecisionSink) *Reconciler {
	if sink == nil {
		sink = NopSink{}
	}
	return &Reconciler{store: store, act: act, clock: clock, backoff: bo, sink: sink}
}

// WithDependencyQueue enables dependent re-enqueue after ordering changes.
func (r *Reconciler) WithDependencyQueue(q DependencyEnqueuer) *Reconciler {
	r.deps = q
	return r
}

// WithJitter installs the randomness source (nil disables jitter).
func (r *Reconciler) WithJitter(j func() float64) *Reconciler {
	r.jitter = j
	return r
}

// WithCrashHook installs a barrier that aborts reconciliation at the
// named point (test support for crash-window recovery).
func (r *Reconciler) WithCrashHook(h CrashHook) *Reconciler {
	r.crash = h
	return r
}

func (r *Reconciler) crashBarrier(key string, at CrashPoint) error {
	if r.crash != nil && r.crash(key, at) {
		r.log(Decision{Key: key, Action: "crash", Reason: "InjectedCrash",
			Message: "simulated process kill " + string(at)})
		return ErrInjectedCrash
	}
	return nil
}

// Reconcile performs one bounded convergence pass for key. Store
// conflicts are retried internally a bounded number of times; transient
// actuator failures surface a backoff Result for delayed requeue;
// deterministic failures persist an observable condition and do not spin.
func (r *Reconciler) Reconcile(ctx context.Context, key string) (Result, error) {
	const maxCASHops = 4
	var conflictCount int
	for {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		obj, rv, err := r.store.Get(ctx, key)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return Result{}, nil // already gone: nothing to do
			}
			return Result{}, err
		}
		res, nrv, perr := r.pass(ctx, obj, rv)
		if perr == nil {
			if nrv > 0 {
				r.log(Decision{Key: key, Action: "persisted", Reason: res.Reason, ResultRV: nrv, Input: inputSummary(obj)})
			} else {
				r.log(Decision{Key: key, Action: "noop", Reason: res.Reason, Input: inputSummary(obj)})
			}
			return res, nil
		}
		if errors.Is(perr, errContinue) {
			// expected multi-step transition: bounded by maxSteps, re-read now
			conflictCount++
			if conflictCount >= maxCASHops {
				return Result{Reason: apis.ReasonConflict, RequeueAfter: r.backoff.Delay(2, r.jitter)}, nil
			}
			continue
		}
		if errors.Is(perr, storage.ErrConflict) {
			conflictCount++
			r.log(Decision{Key: key, Action: "conflict", Reason: apis.ReasonConflict,
				Message: fmt.Sprintf("cas rejected on rv %d (hop %d)", rv, conflictCount), Input: inputSummary(obj)})
			if conflictCount >= maxCASHops {
				return Result{Reason: apis.ReasonConflict, RequeueAfter: r.backoff.Delay(2, r.jitter)}, nil
			}
			continue
		}
		return Result{}, perr
	}
}

// pass performs convergence against one read snapshot. It either
// persists the transition (returning the new rv) or returns
// storage.ErrConflict to force a fresh read.
func (r *Reconciler) pass(ctx context.Context, obj *apis.RawObject, rv int64) (Result, int64, error) {
	now := r.clock.Now()

	// ---------- deletion branch: ordered teardown via finalizer ----------
	if obj.Metadata.DeletionTimestamp != nil {
		return r.passDelete(ctx, obj, rv, now)
	}

	// ---------- admission / defaulting ----------
	spec, defaults, derr := apis.DecodeSpec(obj)
	clearAdmission := false
	if derr != nil {
		var ae *apis.AdmissionError
		if errors.As(derr, &ae) {
			return r.rejectAdmission(ctx, obj, rv, now, ae)
		}
		return Result{}, 0, derr
	}

	// cross-object graph validation (unknown dependency, cycles)
	graph, gerr := r.buildGraph(ctx)
	if gerr != nil {
		return Result{}, 0, gerr
	}
	if verr := apis.Validate(obj.Metadata.Name, spec, graph); verr != nil {
		var ae *apis.AdmissionError
		if errors.As(verr, &ae) {
			return r.rejectAdmission(ctx, obj, rv, now, ae)
		}
		return Result{}, 0, verr
	}

	// ---------- ordering: do not create until dependencies are ready ----------
	if blocking := r.dependenciesNotReady(graph, obj.Metadata.Name, spec); len(blocking) > 0 {
		st := ensureStatus(obj)
		gen := obj.Metadata.Generation
		// keep previously recorded admission/ready facts out of the way:
		// only surface the ordering block.
		setCondition(obj, now, apis.CondDependents, "True", apis.ReasonDependencyPending,
			"waiting for dependencies: "+strings.Join(blocking, ","), gen)
		setCondition(obj, now, apis.CondReady, "False", apis.ReasonDependencyPending,
			"blocked by dependencies: "+strings.Join(blocking, ","), gen)
		st.ObservedGeneration = gen
		st.ObservedVersion = obj.APIVersion
		updated, nrv, err := r.persist(ctx, obj, rv)
		if err != nil {
			return Result{}, 0, err
		}
		_ = updated
		r.log(Decision{Key: obj.Metadata.Name, Action: "blocked", Reason: apis.ReasonDependencyPending,
			Message: "deps=" + strings.Join(blocking, ","), ResultRV: nrv, Input: inputSummary(updated)})
		return Result{}, nrv, nil
	}
	if getCond(obj, apis.CondDependents) != nil &&
		getCond(obj, apis.CondDependents).Reason == apis.ReasonDependencyPending {
		clearAdmission = true
	}

	// ---------- durable finalizer before any side effect ----------
	if !hasFinalizer(obj) {
		addFinalizer(obj)
		// Persist defaults and the observed generation in the same write
		// as the finalizer, so a re-read finds nothing left to fill in.
		fst := ensureStatus(obj)
		fst.ObservedGeneration = obj.Metadata.Generation
		fst.ObservedVersion = obj.APIVersion
		fst.Defaults = defaults
		setCondition(obj, now, apis.CondAdmission, "True", apis.ReasonAccepted, "admitted", obj.Metadata.Generation)
		updated, nrv, err := r.persist(ctx, obj, rv)
		if err != nil {
			return Result{}, 0, err
		}
		r.log(Decision{Key: obj.Metadata.Name, Action: "finalizer", Reason: apis.ReasonAccepted, ResultRV: nrv, Input: inputSummary(updated)})
		return Result{}, nrv, errContinue // re-read: spec may have changed
	}

	st := ensureStatus(obj)
	gen := obj.Metadata.Generation
	hash := spec.SpecHash()

	// The desired generation monotonically identifies an input change.
	// Re-reading the same generation after a failure is not a change.
	specChanged := st.ObservedGeneration != 0 && st.ObservedGeneration != gen
	if specChanged {
		st.Attempts = 0
		st.FatalReason = ""
		st.FatalMessage = ""
	}
	st.ObservedGeneration = gen
	st.ObservedVersion = obj.APIVersion
	st.Defaults = defaults

	// A previously observed deterministic failure on the same input is a
	// stable terminal state: do not keep calling the backend and do not
	// rewrite an object that already records it.
	if !specChanged && st.FatalReason != "" {
		c1 := getCond(obj, apis.CondReady)
		c2 := getCond(obj, apis.CondProgressing)
		already := c1 != nil && c1.Status == "False" && c1.Reason == st.FatalReason &&
			c2 != nil && c2.Status == "False"
		if already {
			r.log(Decision{Key: obj.Metadata.Name, Action: "terminal", Reason: st.FatalReason,
				Message: "terminal failure already recorded, no write", Input: inputSummary(obj)})
			return Result{}, 0, nil
		}
		setCondition(obj, now, apis.CondReady, "False", st.FatalReason, st.FatalMessage, gen)
		setCondition(obj, now, apis.CondProgressing, "False", apis.ReasonDeterministicError, st.FatalMessage, gen)
		updated, nrv, err := r.persist(ctx, obj, rv)
		if err != nil {
			return Result{}, 0, err
		}
		_ = updated
		r.log(Decision{Key: obj.Metadata.Name, Action: "terminal", Reason: st.FatalReason,
			Message: st.FatalMessage, ResultRV: nrv, Input: inputSummary(obj)})
		return Result{}, nrv, nil
	}

	// ---------- idempotent apply ----------
	actual := (*apis.ActualState)(nil)
	if st.Actual != nil {
		cp := *st.Actual
		actual = &cp
	}
	newActual, action, aerr := r.act.Ensure(ctx, obj.Metadata.Name, spec, actual)

	if aerr != nil {
		st.Attempts++
		res, nrv, err := r.recordActuatorError(ctx, obj, rv, now, gen, aerr)
		return res, nrv, err
	}
	// The action has taken effect in the (durable) actual backend. If the
	// process is killed exactly here, the control-plane record still
	// shows the old/empty actual; restart reconciliation adopts it.
	if cerr := r.crashBarrier(obj.Metadata.Name, CrashAfterEnsure); cerr != nil {
		return Result{}, 0, cerr
	}

	// success path. Persist only when something semantically changed:
	// an already-converged object must not be rewritten on every pass.
	dirty := false
	if st.Attempts != 0 {
		st.Attempts = 0
		dirty = true
	}
	if st.FatalReason != "" || st.FatalMessage != "" {
		st.FatalReason = ""
		st.FatalMessage = ""
		dirty = true
	}
	if st.SpecHash != hash {
		st.SpecHash = hash
		dirty = true
	}
	if !actualEqual(st.Actual, newActual) {
		st.Actual = newActual
		dirty = true
	}
	if defaults != nil && !defaultsEqual(st.Defaults, defaults) {
		st.Defaults = defaults
		dirty = true
	}
	reason := apis.ReasonUnchanged
	switch action {
	case ActionCreated:
		reason = apis.ReasonCreated
	case ActionUpdated:
		reason = apis.ReasonUpdated
	case ActionAdopted:
		// The resource already existed in the durable backend but the
		// control plane had not recorded it (crash between action and
		// record). Adopt the existing identity: content and identity stay
		// exactly as they are, no side effect is counted.
		reason = "Adopted"
	case ActionUnchanged:
		reason = apis.ReasonUnchanged
	}
	if setCondition(obj, now, apis.CondAdmission, "True", apis.ReasonAccepted, "admitted", gen) {
		dirty = true
	}
	if clearAdmission {
		if setCondition(obj, now, apis.CondDependents, "False", apis.ReasonDependenciesReady, "dependencies ready", gen) {
			dirty = true
		}
	}
	// Steady-state conditions use a stable reason AND stable message so
	// that repeating the same desired state flips nothing. The concrete
	// action (Created/Updated/Unchanged) goes only into the decision log.
	if setCondition(obj, now, apis.CondProgressing, "False", apis.ReasonReady, "at desired state", gen) {
		dirty = true
	}
	if setCondition(obj, now, apis.CondReady, "True", apis.ReasonReady, "at desired state", gen) {
		dirty = true
	}

	_ = reason
	if !dirty {
		r.log(Decision{Key: obj.Metadata.Name, Action: string(action), Reason: apis.ReasonUnchanged,
			Message: "already at desired state, no write", Input: inputSummary(obj)})
		return Result{}, 0, nil
	}
	updated, nrv, err := r.persist(ctx, obj, rv)
	if err != nil {
		return Result{}, 0, err
	}
	r.log(Decision{Key: obj.Metadata.Name, Action: string(action), Reason: reason,
		Message:  fmt.Sprintf("hash=%s replicas=%d mode=%s", hash, spec.Replicas, spec.Mode),
		ResultRV: nrv, Input: inputSummary(updated)})

	if action != ActionUnchanged {
		r.enqueueDependents(ctx, obj.Metadata.Name)
	}
	return Result{}, nrv, nil
}

// passDelete performs ordered teardown.
func (r *Reconciler) passDelete(ctx context.Context, obj *apis.RawObject, rv int64, now time.Time) (Result, int64, error) {
	gen := obj.Metadata.Generation

	// hard guard: never remove a dependency still needed by a live child
	if blocking := r.liveDependents(ctx, obj.Metadata.Name); len(blocking) > 0 {
		dirty := false
		if setCondition(obj, now, apis.CondDependents, "True", apis.ReasonDependencyPending,
			"cannot delete while dependents exist: "+strings.Join(blocking, ","), gen) {
			dirty = true
		}
		if setCondition(obj, now, apis.CondReady, "False", apis.ReasonDependencyPending,
			"deletion blocked by: "+strings.Join(blocking, ","), gen) {
			dirty = true
		}
		if dirty {
			updated, nrv, err := r.persist(ctx, obj, rv)
			if err != nil {
				return Result{}, 0, err
			}
			r.log(Decision{Key: obj.Metadata.Name, Action: "delete-blocked", Reason: apis.ReasonDependencyPending,
				Message: "dependents=" + strings.Join(blocking, ","), ResultRV: nrv, Input: inputSummary(updated)})
			return Result{}, nrv, nil
		}
		return Result{}, 0, nil
	}

	st := ensureStatus(obj)

	// Phase 1: the simulated resource must be gone before the finalizer is
	// released. st.Actual==nil is the durable record that this already
	// happened, so a restarted/repeated pass never calls Remove twice.
	if hasFinalizer(obj) && st.Actual != nil {
		cp := *st.Actual
		deleted, aerr := r.act.Remove(ctx, obj.Metadata.Name, &cp)
		if aerr != nil {
			setCondition(obj, now, apis.CondProgressing, "True", apis.ReasonDeleting,
				"deletion in progress: "+aerr.Error(), gen)
			return r.recordActuatorError(ctx, obj, rv, now, gen, aerr)
		}
		// The backend removal is durably committed. A crash here leaves an
		// actual-state record that points at something already gone; the
		// next pass observes Remove no-op and finishes the phases.
		if cerr := r.crashBarrier(obj.Metadata.Name, CrashAfterRemove); cerr != nil {
			return Result{}, 0, cerr
		}
		// Remove is idempotent: deleted==false means it was already gone
		// in the backend but our status still thought it existed. Either
		// way the canonical conclusion is: actual == nil.
		_ = deleted
		st.Attempts = 0
		st.FatalReason = ""
		st.FatalMessage = ""
		st.Actual = nil
		setCondition(obj, now, apis.CondProgressing, "False", apis.ReasonDeleted, "actual resource removed", gen)
		updated, nrv, err := r.persist(ctx, obj, rv)
		if err != nil {
			return Result{}, 0, err
		}
		r.log(Decision{Key: obj.Metadata.Name, Action: string(ActionDeleted), Reason: apis.ReasonDeleted,
			ResultRV: nrv, Input: inputSummary(updated)})
		// re-read and proceed to phase 2 in the same Reconcile call
		return Result{}, nrv, errContinue
	}

	// Phase 2: backend gone; release our finalizer.
	if hasFinalizer(obj) {
		removeFinalizer(obj)
		setCondition(obj, now, apis.CondProgressing, "False", apis.ReasonDeleted, "finalizer released", gen)
		updated, nrv, err := r.persist(ctx, obj, rv)
		if err != nil {
			return Result{}, 0, err
		}
		r.log(Decision{Key: obj.Metadata.Name, Action: "finalizer-released", Reason: apis.ReasonDeleted,
			ResultRV: nrv, Input: inputSummary(updated)})
		return Result{}, nrv, errContinue
	}

	// Phase 3: no finalizer and no dependents: remove from the store.
	if err := r.store.Delete(ctx, obj.Metadata.Name, rv); err != nil {
		return Result{}, 0, err
	}
	r.log(Decision{Key: obj.Metadata.Name, Action: "deleted-object", Reason: apis.ReasonDeleted,
		ResultRV: 0, Input: inputSummary(obj)})
	// anything that waited on this object should be re-evaluated
	r.enqueueDependents(ctx, obj.Metadata.Name)
	return Result{}, 0, nil
}

// recordActuatorError classifies and persists an actuator failure.
func (r *Reconciler) recordActuatorError(ctx context.Context, obj *apis.RawObject, rv int64, now time.Time, gen int64, aerr error) (Result, int64, error) {
	st := ensureStatus(obj)
	msg := aerr.Error()
	switch {
	case errors.Is(aerr, ErrDeterministic):
		st.FatalReason = apis.ReasonDeterministicError
		st.FatalMessage = msg
		setCondition(obj, now, apis.CondProgressing, "False", apis.ReasonDeterministicError, msg, gen)
		setCondition(obj, now, apis.CondReady, "False", apis.ReasonDeterministicError, msg, gen)
		updated, nrv, err := r.persist(ctx, obj, rv)
		if err != nil {
			return Result{}, 0, err
		}
		r.log(Decision{Key: obj.Metadata.Name, Action: "fail", Reason: apis.ReasonDeterministicError,
			Message: msg, ResultRV: nrv, Input: inputSummary(updated)})
		// no requeue: observable terminal state, no busy loop
		return Result{}, nrv, nil
	case errors.Is(aerr, ErrTransient):
		if r.backoff.Exhausted(st.Attempts) {
			st.FatalReason = apis.ReasonRetryBudgetExhausted
			st.FatalMessage = msg
			setCondition(obj, now, apis.CondProgressing, "False", apis.ReasonRetryBudgetExhausted, msg, gen)
			setCondition(obj, now, apis.CondReady, "False", apis.ReasonRetryBudgetExhausted, msg, gen)
			updated, nrv, err := r.persist(ctx, obj, rv)
			if err != nil {
				return Result{}, 0, err
			}
			r.log(Decision{Key: obj.Metadata.Name, Action: "fail", Reason: apis.ReasonRetryBudgetExhausted,
				Message: msg, ResultRV: nrv, Input: inputSummary(updated)})
			return Result{}, nrv, nil
		}
		delay := r.backoff.Delay(st.Attempts, r.jitter)
		setCondition(obj, now, apis.CondProgressing, "True", apis.ReasonTransientError,
			fmt.Sprintf("%s (attempt %d, retry in %s)", msg, st.Attempts, delay), gen)
		setCondition(obj, now, apis.CondReady, "False", apis.ReasonTransientError, msg, gen)
		updated, nrv, err := r.persist(ctx, obj, rv)
		if err != nil {
			return Result{}, 0, err
		}
		r.log(Decision{Key: obj.Metadata.Name, Action: "retry", Reason: apis.ReasonTransientError,
			Message: fmt.Sprintf("%s after=%s attempt=%d", msg, delay, st.Attempts), ResultRV: nrv, Input: inputSummary(updated)})
		return Result{RequeueAfter: delay, Reason: apis.ReasonTransientError}, nrv, nil
	default:
		// unknown error class: treat as transient to stay safe
		delay := r.backoff.Delay(st.Attempts, r.jitter)
		setCondition(obj, now, apis.CondProgressing, "True", apis.ReasonTransientError, msg, gen)
		_, nrv, err := r.persist(ctx, obj, rv)
		if err != nil {
			return Result{}, 0, err
		}
		return Result{RequeueAfter: delay, Reason: apis.ReasonTransientError}, nrv, nil
	}
}

// rejectAdmission persists a rejected admission condition without
// creating any actual resource and without requeueing.
func (r *Reconciler) rejectAdmission(ctx context.Context, obj *apis.RawObject, rv int64, now time.Time, ae *apis.AdmissionError) (Result, int64, error) {
	st := ensureStatus(obj)
	gen := obj.Metadata.Generation
	st.ObservedGeneration = gen
	st.ObservedVersion = obj.APIVersion
	st.FatalReason = ae.Reason
	st.FatalMessage = ae.Message
	setCondition(obj, now, apis.CondAdmission, "False", ae.Reason, ae.Message, gen)
	setCondition(obj, now, apis.CondReady, "False", ae.Reason, ae.Message, gen)
	setCondition(obj, now, apis.CondProgressing, "False", ae.Reason, ae.Message, gen)
	updated, nrv, err := r.maybePersist(ctx, obj, rv)
	if err != nil {
		return Result{}, 0, err
	}
	r.log(Decision{Key: obj.Metadata.Name, Action: "reject", Reason: ae.Reason,
		Message: ae.Message, ResultRV: nrv, Input: inputSummary(updated)})
	return Result{}, nrv, nil
}

// buildGraph decodes every stored object into canonical specs. Objects
// that themselves fail admission are omitted (they cannot be valid deps).
func (r *Reconciler) buildGraph(ctx context.Context) (map[string]*apis.WidgetSpec, error) {
	all, err := r.store.List(ctx)
	if err != nil {
		return nil, err
	}
	g := make(map[string]*apis.WidgetSpec, len(all))
	for _, o := range all {
		spec, _, derr := apis.DecodeSpec(o)
		if derr != nil {
			continue
		}
		g[o.Metadata.Name] = spec
	}
	return g, nil
}

// dependenciesNotReady returns the names of dependencies that either do
// not yet have a converged resource or are being deleted.
func (r *Reconciler) dependenciesNotReady(graph map[string]*apis.WidgetSpec, self string, spec *apis.WidgetSpec) []string {
	// Readiness requires the dependency object to exist with a Ready=True
	// condition at the same generation; we need status, so fetch once per
	// dependency is avoided by a single List at call sites is complex;
	// here graph membership plus a cheap cached status map suffices.
	all, _ := r.store.List(context.Background())
	statusByName := map[string]*apis.RawObject{}
	for _, o := range all {
		statusByName[o.Metadata.Name] = o
	}
	var blocking []string
	for _, dep := range spec.DependsOn {
		o, ok := statusByName[dep]
		if !ok || o.Metadata.DeletionTimestamp != nil {
			blocking = append(blocking, dep)
			continue
		}
		c := getCond(o, apis.CondReady)
		// A dependency counts as ready only once it is admitted
		// (finalizer present) and Ready=True at the current generation.
		if !hasFinalizer(o) || c == nil || c.Status != "True" || c.ObservedGeneration != o.Metadata.Generation {
			blocking = append(blocking, dep)
		}
	}
	sort.Strings(blocking)
	return blocking
}

// liveDependents returns non-deleting objects that still reference name.
func (r *Reconciler) liveDependents(ctx context.Context, name string) []string {
	all, err := r.store.List(ctx)
	if err != nil {
		return nil
	}
	var out []string
	for _, o := range all {
		if o.Metadata.Name == name || o.Metadata.DeletionTimestamp != nil {
			continue
		}
		spec, _, derr := apis.DecodeSpec(o)
		if derr != nil {
			continue
		}
		if hasString(spec.DependsOn, name) {
			out = append(out, o.Metadata.Name)
		}
	}
	sort.Strings(out)
	return out
}

// enqueueDependents re-enqueues all objects that depend on changed.
func (r *Reconciler) enqueueDependents(ctx context.Context, changed string) {
	if r.deps == nil {
		return
	}
	all, err := r.store.List(ctx)
	if err != nil {
		return
	}
	for _, o := range all {
		if o.Metadata.Name == changed {
			continue
		}
		spec, _, derr := apis.DecodeSpec(o)
		if derr != nil {
			continue
		}
		if hasString(spec.DependsOn, changed) {
			if err := r.deps.Add(o.Metadata.Name); err != nil && !errors.Is(err, ErrQueueFull) {
				return
			}
		}
	}
}

// maybePersist writes only when the in-memory status actually changed.
// Because pass mutates a detached copy, a shallow before/after compare
// via reflection on the status pointer and finalizers is sufficient here.
func (r *Reconciler) maybePersist(ctx context.Context, obj *apis.RawObject, rv int64) (*apis.RawObject, int64, error) {
	return r.store.Update(ctx, obj, rv)
}

func (r *Reconciler) persist(ctx context.Context, obj *apis.RawObject, expectedRV int64) (*apis.RawObject, int64, error) {
	return r.store.Update(ctx, obj, expectedRV)
}

func ensureStatus(obj *apis.RawObject) *apis.Status {
	if obj.Status == nil {
		obj.Status = &apis.Status{}
	}
	return obj.Status
}

// setCondition replaces a same-typed condition, preserving
// LastTransitionTime when the observable facts are unchanged. It reports
// whether anything changed.
func setCondition(obj *apis.RawObject, now time.Time, typ, status, reason, msg string, gen int64) bool {
	st := ensureStatus(obj)
	for i := range st.Conditions {
		c := &st.Conditions[i]
		if c.Type != typ {
			continue
		}
		if c.Status == status && c.Reason == reason && c.Message == msg && c.ObservedGeneration == gen {
			return false
		}
		// Only semantic facts make a condition "changed". Re-stamping
		// LastTransitionTime on an identical condition must not count as a
		// change, otherwise every reconcile would rewrite the object.
		semantic := c.Status != status || c.Reason != reason || c.Message != msg || c.ObservedGeneration != gen
		if semantic {
			c.LastTransition = now
		}
		c.Status, c.Reason, c.Message, c.ObservedGeneration = status, reason, msg, gen
		return semantic
	}
	st.Conditions = append(st.Conditions, apis.Condition{
		Type: typ, Status: status, Reason: reason, Message: msg,
		ObservedGeneration: gen, LastTransition: now,
	})
	sort.SliceStable(st.Conditions, func(i, j int) bool { return st.Conditions[i].Type < st.Conditions[j].Type })
	return true
}

func getCond(obj *apis.RawObject, typ string) *apis.Condition {
	if obj.Status == nil {
		return nil
	}
	for i := range obj.Status.Conditions {
		if obj.Status.Conditions[i].Type == typ {
			return &obj.Status.Conditions[i]
		}
	}
	return nil
}

func hasFinalizer(obj *apis.RawObject) bool {
	for _, f := range obj.Metadata.Finalizers {
		if f == apis.FinalizerReconciler {
			return true
		}
	}
	return false
}

func addFinalizer(obj *apis.RawObject) bool {
	if hasFinalizer(obj) {
		return false
	}
	obj.Metadata.Finalizers = append(obj.Metadata.Finalizers, apis.FinalizerReconciler)
	sort.Strings(obj.Metadata.Finalizers)
	return true
}

func removeFinalizer(obj *apis.RawObject) bool {
	out := obj.Metadata.Finalizers[:0]
	found := false
	for _, f := range obj.Metadata.Finalizers {
		if f == apis.FinalizerReconciler {
			found = true
			continue
		}
		out = append(out, f)
	}
	obj.Metadata.Finalizers = out
	return found
}

func defaultsEqual(a, b *apis.AppliedDefaults) bool {
	if a == nil || b == nil {
		return a == b
	}
	if len(a.Fields) != len(b.Fields) || len(a.Values) != len(b.Values) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i] != b.Fields[i] {
			return false
		}
	}
	for k, v := range a.Values {
		if b.Values[k] != v {
			return false
		}
	}
	return true
}

func actualEqual(a, b *apis.ActualState) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.ResourceID == b.ResourceID && a.Replicas == b.Replicas &&
		a.Mode == b.Mode && a.GraceSeconds == b.GraceSeconds && a.Ready == b.Ready
}

func hasString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func inputSummary(obj *apis.RawObject) string {
	if obj == nil {
		return "<nil>"
	}
	dp := ""
	if obj.Status != nil {
		dp = fmt.Sprintf(" observedGen=%d hash=%s attempts=%d deleting=%v finalizers=%d",
			obj.Status.ObservedGeneration, obj.Status.SpecHash, obj.Status.Attempts,
			obj.Metadata.DeletionTimestamp != nil, len(obj.Metadata.Finalizers))
	}
	return fmt.Sprintf("gen=%d ver=%s%s", obj.Metadata.Generation, obj.APIVersion, dp)
}

func (r *Reconciler) log(d Decision) {
	if d.At.IsZero() {
		d.At = r.clock.Now()
	}
	r.sink.Log(d)
}

// avoid an unused import in trimmed builds if reflection helpers change
var _ = reflect.DeepEqual
