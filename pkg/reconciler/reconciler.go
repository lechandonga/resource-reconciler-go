// Package reconciler converges a single Workload object towards its spec
// using a fixed ordering, bounded work, and explicit failure classification.
//
// One Reconcile call performs exactly one bounded pass:
//
//	delete path:   data-plane children are removed first; only when the data
//	               plane is empty (and no other object still depends on this
//	               one) is the finalizer removed and the object deleted.
//	apply  path:   dependencies must be Available; then the data plane is
//	               diffed against the spec and at most MaxDataPlaneOps
//	               mutations are applied per pass.
//
// Status is written back exactly once per pass using optimistic concurrency;
// a stale write returns ErrConflict and the pass asks for an immediate retry
// against the newest state.
package reconciler

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
	"github.com/lechandonga/resource-reconciler-go/pkg/dataplane"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

// MaxDataPlaneOps bounds the data-plane work performed by one reconcile pass.
const MaxDataPlaneOps = 3

// Backoff policy for transient failures: bounded exponential with jitter.
const (
	BaseBackoff = 10 * time.Millisecond
	MaxBackoff  = 2 * time.Second
	MaxAttempts = 5
)

// Deps groups collaborators.
type Deps struct {
	Store storage.Store
	DP    dataplane.Client
	// Now and Backoff are indirected so tests are deterministic. When nil,
	// production defaults are used.
	Now     func() time.Time
	Backoff func(attempt int) time.Duration
}

// Reconciler executes one convergence pass per invocation.
type Reconciler struct {
	deps Deps
	rng  *rand.Rand
}

// New constructs a Reconciler.
func New(deps Deps) *Reconciler {
	if deps.Now == nil {
		deps.Now = func() time.Time { return time.Now().UTC() }
	}
	if deps.Backoff == nil {
		deps.Backoff = DefaultBackoff
	}
	return &Reconciler{deps: deps, rng: rand.New(rand.NewSource(1))}
}

// DefaultBackoff computes a bounded exponential delay with ±20% jitter.
// attempt is 0-based: attempt 0 => BaseBackoff.
func DefaultBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := BaseBackoff << attempt // exponential
	if d <= 0 || d > MaxBackoff {
		d = MaxBackoff
	}
	jitter := int64(d) / 5
	return d + time.Duration(rand.Int63n(2*jitter+1)-jitter)
}

// Result describes the outcome of a pass.
type Result struct {
	// Requeue is true when another pass is required.
	Requeue bool
	// RequeueAfter, when > 0, asks for a delayed requeue (backoff).
	RequeueAfter time.Duration
	// Reason is the stable reason code explaining the outcome.
	Reason string
	// Conflict is true when the write-back lost the optimistic-concurrency
	// race; the controller must retry immediately against fresh state.
	Conflict bool
	// Shed is true when the bounded work budget was exhausted this pass.
	Shed bool
}

// Reconcile performs exactly one bounded pass for the named object.
func (r *Reconciler) Reconcile(ctx context.Context, name string) Result {
	w, err := r.deps.Store.Get(ctx, name)
	if err != nil {
		// Object disappeared (e.g. physically deleted by another worker):
		// nothing to do, not an error.
		if _, ok := err.(*storage.NotFoundError); ok {
			return Result{Reason: v1.ReasonDeleted}
		}
		return Result{Requeue: true, Reason: v1.ReasonTransientFailure, RequeueAfter: r.backoff(0)}
	}
	expectedRV := w.ObjectMeta.ResourceVersion

	if w.ObjectMeta.DeletionTimestamp != nil {
		return r.reconcileDelete(ctx, w, expectedRV)
	}
	return r.reconcileApply(ctx, w, expectedRV)
}

// reconcileDelete enforces teardown ordering:
//  1. other objects depending on this one must be gone (finalizer blocked);
//  2. all data-plane children must be removed;
//  3. then the finalizer is removed and the object physically deleted.
func (r *Reconciler) reconcileDelete(ctx context.Context, w *v1.Workload, expectedRV int64) Result {
	now := r.deps.Now()
	r.setPhase(w, v1.PhaseDeleting, now)

	// (1) Dependents block deletion.
	all, err := r.deps.Store.List(ctx)
	if err != nil {
		return r.failTransient(ctx, w, expectedRV, "ListDependents", err, now)
	}
	blockers := []string{}
	for _, other := range all {
		if other.ObjectMeta.Name == w.ObjectMeta.Name {
			continue
		}
		// An object already under deletion is committed to teardown and does
		// not block us: its own finalizer guarantees its data-plane children
		// (which may depend on us) will be removed before it disappears. This
		// also covers the race where our wakeup runs in the same pass in
		// which a dependent was physically deleted but another worker's List
		// snapshot still contains its terminating record.
		if other.ObjectMeta.DeletionTimestamp != nil {
			continue
		}
		for _, dep := range other.Spec.DependsOn {
			if dep == w.ObjectMeta.Name {
				blockers = append(blockers, other.ObjectMeta.Name)
				break
			}
		}
	}
	if len(blockers) > 0 {
		SetCondition(w, v1.ConditionFinalizer, v1.CondFalse, v1.ReasonFinalizerBlocked,
			"waiting for dependents to be deleted: "+joinNames(blockers),
			w.ObjectMeta.Generation, now)
		return r.writeBack(ctx, w, expectedRV, Result{Reason: v1.ReasonFinalizerBlocked})
	}

	// (2) Remove data-plane children, bounded per pass. Instances with the
	// highest index go first (scale-down ordering is stable).
	instances, err := r.deps.DP.ListInstances(ctx, w.ObjectMeta.Name)
	if err != nil {
		return r.failTransient(ctx, w, expectedRV, "ListInstances", err, now)
	}
	ops := 0
	for i := len(instances) - 1; i >= 0 && ops < MaxDataPlaneOps; i-- {
		if err := r.deps.DP.RemoveInstance(ctx, instances[i].ID); err != nil {
			return r.classifyDataPlaneError(ctx, w, expectedRV, err, now, "remove")
		}
		ops++
	}
	if ops == len(instances) {
		// (3) children gone: remove finalizer and physically delete.
		// Deletion itself is an unconditional remove in the store, executed
		// via a finalizer-less update path.
		if err := r.removeFinalizerAndDelete(ctx, w, expectedRV); err != nil {
			if isConflict(err) {
				return Result{Requeue: true, Reason: v1.ReasonConflict, Conflict: true}
			}
			return r.failTransient(ctx, w, expectedRV, "PhysicalDelete", err, now)
		}
		return Result{Reason: v1.ReasonDeleted}
	}

	SetCondition(w, v1.ConditionFinalizer, v1.CondFalse, v1.ReasonDeleting,
		"data-plane teardown in progress", w.ObjectMeta.Generation, now)
	return r.writeBack(ctx, w, expectedRV, Result{
		Requeue: true,
		Reason:  v1.ReasonDeleting,
		Shed:    true,
	})
}

// reconcileApply converges the desired state.
func (r *Reconciler) reconcileApply(ctx context.Context, w *v1.Workload, expectedRV int64) Result {
	now := r.deps.Now()

	// A spec change (new generation) resets the transient-failure budget:
	// the new spec is a fresh decision and must not be blocked by failures
	// of the previous one.
	if w.Status.RetryGeneration != w.ObjectMeta.Generation {
		w.Status.RetryGeneration = w.ObjectMeta.Generation
		w.Status.Attempts = 0
	}

	// Ensure the protective finalizer exists before any side effect.
	if !hasFinalizer(w) {
		addFinalizer(w)
		SetCondition(w, v1.ConditionFinalizer, v1.CondTrue, v1.ReasonAdmitted,
			"finalizer installed", w.ObjectMeta.Generation, now)
	}

	// Dependency gate (ordered lifecycle): deps are created before dependents.
	if res := r.checkDependencies(ctx, w, expectedRV, now); res != nil {
		return *res
	}

	// Diff the data plane against the desired spec.
	instances, err := r.deps.DP.ListInstances(ctx, w.ObjectMeta.Name)
	if err != nil {
		return r.failTransient(ctx, w, expectedRV, "ListInstances", err, now)
	}
	present := map[int]dataplane.Instance{}
	for _, in := range instances {
		present[in.ID.Index] = in
	}

	// Plan: removals (descending), then creates/updates (ascending). Never
	// mutate more than MaxDataPlaneOps instances per pass.
	type step struct {
		id         dataplane.InstanceID
		image      string
		generation int64
		remove     bool
	}
	var plan []step
	desired := w.Spec.Replicas
	for idx := range present {
		if idx >= desired {
			plan = append(plan, step{id: dataplane.InstanceID{Workload: w.ObjectMeta.Name, Index: idx}, remove: true})
		}
	}
	// sort removals descending
	for i := 1; i < len(plan); i++ {
		for j := i; j > 0 && plan[j-1].id.Index < plan[j].id.Index; j-- {
			plan[j-1], plan[j] = plan[j], plan[j-1]
		}
	}
	for idx := 0; idx < desired; idx++ {
		in, ok := present[idx]
		if !ok || in.Image != w.Spec.Image || in.Generation != w.ObjectMeta.Generation {
			plan = append(plan, step{
				id:         dataplane.InstanceID{Workload: w.ObjectMeta.Name, Index: idx},
				image:      w.Spec.Image,
				generation: w.ObjectMeta.Generation,
			})
		}
	}

	applied := 0
	for _, st := range plan {
		if applied >= MaxDataPlaneOps {
			break
		}
		var err error
		if st.remove {
			err = r.deps.DP.RemoveInstance(ctx, st.id)
		} else {
			err = r.deps.DP.EnsureInstance(ctx, st.id, st.image, st.generation)
		}
		if err != nil {
			op := "ensure"
			if st.remove {
				op = "remove"
			}
			return r.classifyDataPlaneError(ctx, w, expectedRV, err, now, op)
		}
		applied++
	}

	// Re-read after applying for an accurate status snapshot.
	after, err := r.deps.DP.ListInstances(ctx, w.ObjectMeta.Name)
	if err != nil {
		return r.failTransient(ctx, w, expectedRV, "ListInstances", err, now)
	}
	w.Status.Instances = make([]v1.InstanceStatus, 0, len(after))
	for _, in := range after {
		w.Status.Instances = append(w.Status.Instances, v1.InstanceStatus{
			ID:         instanceKey(in.ID),
			Status:     v1.InstancePresent,
			Generation: in.Generation,
		})
	}

	w.Status.Attempts = 0
	w.Status.LastError = nil
	converged := applied == 0 && len(after) == desired
	remaining := len(plan) - applied

	switch {
	case remaining > 0:
		// Budget exhausted but making progress.
		r.setPhase(w, v1.PhaseProgressing, now)
		SetCondition(w, v1.ConditionReconciled, v1.CondFalse, v1.ReasonReconcileBudgetExceeded,
			"per-pass op budget exhausted; continuing", w.ObjectMeta.Generation, now)
		return r.writeBack(ctx, w, expectedRV, Result{Requeue: true, Reason: v1.ReasonReconcileBudgetExceeded, Shed: true})
	case converged:
		w.Status.ObservedGeneration = w.ObjectMeta.Generation
		r.setPhase(w, v1.PhaseAvailable, now)
		SetCondition(w, v1.ConditionReconciled, v1.CondTrue, v1.ReasonNoop,
			"actual state matches desired state", w.ObjectMeta.Generation, now)
		return r.writeBack(ctx, w, expectedRV, Result{Reason: v1.ReasonNoop})
	default:
		r.setPhase(w, v1.PhaseProgressing, now)
		SetCondition(w, v1.ConditionReconciled, v1.CondFalse, v1.ReasonApplied,
			"changes applied; verifying", w.ObjectMeta.Generation, now)
		return r.writeBack(ctx, w, expectedRV, Result{Requeue: true, Reason: v1.ReasonApplied})
	}
}

// checkDependencies enforces create/update ordering.
func (r *Reconciler) checkDependencies(ctx context.Context, w *v1.Workload, expectedRV int64, now time.Time) *Result {
	all, err := r.deps.Store.List(ctx)
	if err != nil {
		res := r.failTransient(ctx, w, expectedRV, "ListDependencies", err, now)
		return &res
	}
	byName := map[string]*v1.Workload{}
	for _, o := range all {
		byName[o.ObjectMeta.Name] = o
	}

	// Self / cycle detection (deterministic DFS over the dependency graph).
	if cycle := findCycle(w.ObjectMeta.Name, byName); cycle != "" {
		r.setPhase(w, v1.PhaseFailed, now)
		w.Status.LastError = &v1.ErrorRecord{
			Reason:    v1.ReasonDependencyCycle,
			Message:   "dependency cycle detected at: " + cycle,
			Transient: false,
			Time:      now,
		}
		SetCondition(w, v1.ConditionDepsReady, v1.CondFalse, v1.ReasonDependencyCycle,
			"cycle: "+cycle, w.ObjectMeta.Generation, now)
		res := r.writeBack(ctx, w, expectedRV, Result{Reason: v1.ReasonDependencyCycle})
		return &res
	}

	missing, notReady := []string{}, []string{}
	for _, dep := range w.Spec.DependsOn {
		if dep == w.ObjectMeta.Name {
			// Cannot happen post-validation on admission, but an object
			// admitted historically may carry it.
			missing = append(missing, dep)
			continue
		}
		o, ok := byName[dep]
		switch {
		case !ok || o.ObjectMeta.DeletionTimestamp != nil:
			missing = append(missing, dep)
		case o.Status.Phase != v1.PhaseAvailable || o.Status.ObservedGeneration != o.ObjectMeta.Generation:
			notReady = append(notReady, dep)
		}
	}
	if len(missing) > 0 {
		// A missing dependency is a deterministic, event-driven wait: it
		// cannot become satisfied by retrying. The controller wakes this
		// object when a dependency is submitted, changes availability, is
		// deleted, or when the process restarts. We therefore do NOT requeue
		// here, which avoids a tight idle loop ("no spinning on a
		// deterministic condition").
		r.setPhase(w, v1.PhaseDegraded, now)
		SetCondition(w, v1.ConditionDepsReady, v1.CondFalse, v1.ReasonDependencyMissing,
			"missing dependencies: "+joinNames(missing), w.ObjectMeta.Generation, now)
		res := r.writeBack(ctx, w, expectedRV, Result{Reason: v1.ReasonDependencyMissing})
		return &res
	}
	if len(notReady) > 0 {
		r.setPhase(w, v1.PhaseProgressing, now)
		SetCondition(w, v1.ConditionDepsReady, v1.CondFalse, v1.ReasonDependencyNotReady,
			"dependencies not yet available: "+joinNames(notReady), w.ObjectMeta.Generation, now)
		res := r.writeBack(ctx, w, expectedRV, Result{Requeue: true, RequeueAfter: r.backoff(1), Reason: v1.ReasonDependencyNotReady})
		return &res
	}
	SetCondition(w, v1.ConditionDepsReady, v1.CondTrue, v1.ReasonAdmitted,
		"all dependencies available", w.ObjectMeta.Generation, now)
	return nil
}

// classifyDataPlaneError turns a data-plane failure into the appropriate
// observable status: deterministic failures stop retries; transient failures
// follow bounded backoff until MaxAttempts.
func (r *Reconciler) classifyDataPlaneError(ctx context.Context, w *v1.Workload, expectedRV int64, dpErr error, now time.Time, op string) Result {
	f, classified := dataplane.AsFailure(dpErr)
	if classified && f.Class == dataplane.ClassDeterministic {
		w.Status.Attempts = 0
		w.Status.LastError = &v1.ErrorRecord{Reason: f.Code, Message: f.Message, Transient: false, Time: now}
		r.setPhase(w, v1.PhaseFailed, now)
		SetCondition(w, v1.ConditionReconciled, v1.CondFalse, v1.ReasonDeterministicFailure,
			op+": "+f.Message, w.ObjectMeta.Generation, now)
		return r.writeBack(ctx, w, expectedRV, Result{Reason: v1.ReasonDeterministicFailure})
	}
	return r.failTransient(ctx, w, expectedRV, op, dpErr, now)
}

func (r *Reconciler) failTransient(ctx context.Context, w *v1.Workload, expectedRV int64, op string, cause error, now time.Time) Result {
	w.Status.Attempts++
	reason := v1.ReasonTransientFailure
	msg := op + ": " + cause.Error()
	if f, ok := dataplane.AsFailure(cause); ok {
		reason, msg = f.Code, op+": "+f.Message
	}
	w.Status.LastError = &v1.ErrorRecord{Reason: reason, Message: msg, Transient: true, Time: now}

	if w.Status.Attempts >= MaxAttempts {
		r.setPhase(w, v1.PhaseFailed, now)
		SetCondition(w, v1.ConditionReconciled, v1.CondFalse, v1.ReasonBackoffLimitExceeded,
			"transient failure retry budget exhausted", w.ObjectMeta.Generation, now)
		return r.writeBack(ctx, w, expectedRV, Result{Reason: v1.ReasonBackoffLimitExceeded})
	}
	r.setPhase(w, v1.PhaseDegraded, now)
	SetCondition(w, v1.ConditionReconciled, v1.CondFalse, v1.ReasonTransientFailure,
		msg+" (attempt "+itoa(w.Status.Attempts)+")", w.ObjectMeta.Generation, now)
	delay := r.backoff(w.Status.Attempts - 1)
	return r.writeBack(ctx, w, expectedRV, Result{Requeue: true, RequeueAfter: delay, Reason: v1.ReasonTransientFailure})
}

// writeBack persists status once using optimistic concurrency. On conflict it
// returns a Conflict result without mutating anything.
func (r *Reconciler) writeBack(ctx context.Context, w *v1.Workload, expectedRV int64, res Result) Result {
	_, err := r.deps.Store.Update(ctx, w, expectedRV)
	if err != nil {
		if isConflict(err) {
			return Result{Requeue: true, Reason: v1.ReasonConflict, Conflict: true}
		}
		// Persistence failure: treat as transient and retry, but keep the
		// caller's requeue intent.
		res.Requeue = true
		res.Reason = v1.ReasonTransientFailure
		if res.RequeueAfter <= 0 {
			res.RequeueAfter = r.backoff(0)
		}
		return res
	}
	return res
}

func (r *Reconciler) backoff(attempt int) time.Duration { return r.deps.Backoff(attempt) }

func (r *Reconciler) setPhase(w *v1.Workload, p v1.Phase, now time.Time) {
	w.Status.Phase = p
	_ = now
}

// SetCondition upserts a condition by type. LastTransition is preserved when
// the condition's status is unchanged; reason/message/observed generation
// are always refreshed.
func SetCondition(w *v1.Workload, t v1.ConditionType, st v1.ConditionStatus, reason, msg string, gen int64, now time.Time) {
	for i := range w.Status.Conditions {
		c := &w.Status.Conditions[i]
		if c.Type == t {
			if c.Status != st {
				c.LastTransition = now
			}
			c.Status = st
			c.Reason = reason
			c.Message = msg
			c.ObservedGeneration = gen
			return
		}
	}
	w.Status.Conditions = append(w.Status.Conditions, v1.Condition{
		Type:               t,
		Status:             st,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: gen,
		LastTransition:     now,
	})
}

func hasFinalizer(w *v1.Workload) bool {
	_, ok := w.ObjectMeta.Annotations[v1.AnnFinalizer]
	return ok
}

func addFinalizer(w *v1.Workload) {
	if w.ObjectMeta.Annotations == nil {
		w.ObjectMeta.Annotations = map[string]string{}
	}
	w.ObjectMeta.Annotations[v1.AnnFinalizer] = "true"
}

// removeFinalizerAndDelete performs the final teardown write: once all data
// plane children are gone, the object is physically deleted from the store
// under the same optimistic-concurrency guard. If the write loses the race
// the caller retries against the newest state; no half-state remains.
func (r *Reconciler) removeFinalizerAndDelete(ctx context.Context, w *v1.Workload, expectedRV int64) error {
	return r.deps.Store.Delete(ctx, w.ObjectMeta.Name, expectedRV)
}

// findCycle runs an iterative DFS from start over spec dependencies and
// returns the first node revisited on the current stack ("a -> b"), or "".
func findCycle(start string, byName map[string]*v1.Workload) string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var dfs func(node string) string
	dfs = func(node string) string {
		color[node] = gray
		if o, ok := byName[node]; ok {
			for _, dep := range o.Spec.DependsOn {
				if dep == node {
					return node + " -> " + dep
				}
				switch color[dep] {
				case gray:
					return node + " -> " + dep
				case white:
					if cyc := dfs(dep); cyc != "" {
						return cyc
					}
				}
			}
		}
		color[node] = black
		return ""
	}
	return dfs(start)
}

func joinNames(names []string) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += ","
		}
		out += n
	}
	return out
}

func instanceKey(id dataplane.InstanceID) string { return id.Workload + "-" + itoa(id.Index) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

func isConflict(err error) bool {
	return errors.Is(err, storage.ErrConflict)
}
