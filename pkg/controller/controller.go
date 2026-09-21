// Package controller wires the store, admission, queue, data plane and
// reconcilers into a local, restartable control loop.
//
// Concurrency model:
//   - a bounded, coalescing queue guarantees at most one pending entry per
//     object key, so spec storms cannot cause unbounded queuing;
//   - each key is additionally guarded by an in-process key lock so the same
//     object is never processed by two workers at once;
//   - every status write is optimistic-concurrency gated in the store; a
//     stale write returns Conflict and the pass is retried against the
//     newest state.
package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/admission"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
	"github.com/lechandonga/resource-reconciler-go/pkg/dataplane"
	"github.com/lechandonga/resource-reconciler-go/pkg/queue"
	"github.com/lechandonga/resource-reconciler-go/pkg/reconciler"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

// Controller owns the worker goroutines and key-level locking.
type Controller struct {
	cfg         Config
	q           *queue.BoundedQueue
	rec         *reconciler.Reconciler
	wg          sync.WaitGroup
	keyMu       sync.Mutex
	keyLocks    map[string]*sync.Mutex
	submitLocks map[string]*ticketLock

	// delayed requeues (backoff), guarded by delayMu
	delayMu     sync.Mutex
	delayTimers map[string]*time.Timer

	cancel context.CancelFunc
}

// Logger observes structured events. Tests inject a capturing logger; the
// demo wires stdout.
type Logger func(format string, args ...any)

// Config configures a Controller.
type Config struct {
	Store    storage.Store
	DP       dataplane.Client
	Reviewer admission.Reviewer
	Workers  int
	QueueCap int
	Now      func() time.Time
	Backoff  func(attempt int) time.Duration
	Log      Logger
}

// New constructs (but does not start) a Controller.
func New(cfg Config) *Controller {
	if cfg.Workers < 1 {
		cfg.Workers = 2
	}
	if cfg.QueueCap < 1 {
		cfg.QueueCap = 16
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.Backoff == nil {
		cfg.Backoff = reconciler.DefaultBackoff
	}
	if cfg.Log == nil {
		cfg.Log = func(string, ...any) {}
	}
	c := &Controller{
		cfg:         cfg,
		q:           queue.New(cfg.QueueCap),
		keyLocks:    map[string]*sync.Mutex{},
		submitLocks: map[string]*ticketLock{},
		delayTimers: map[string]*time.Timer{},
	}
	c.rec = reconciler.New(reconciler.Deps{
		Store:   cfg.Store,
		DP:      cfg.DP,
		Now:     cfg.Now,
		Backoff: cfg.Backoff,
	})
	return c
}

// Start launches workers and resumes any unfinished work found in the
// persisted state. Blocks until Stop is called or ctx is canceled.
func (c *Controller) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)

	// Restart recovery: every non-terminal, not-fully-converged object is
	// enqueued from persisted state. Effects that already took effect are
	// idempotent in the data plane, so they are never repeated. Objects that
	// exhausted their transient-retry budget before the crash get a fresh
	// budget on restart.
	existing, err := c.cfg.Store.List(ctx)
	if err != nil {
		return fmt.Errorf("controller: resume list: %w", err)
	}
	for _, w := range existing {
		phase := w.Status.Phase
		if phase == v1.PhaseTerminated {
			continue
		}
		if w.Status.Attempts >= reconciler.MaxAttempts &&
			w.Status.LastError != nil && w.Status.LastError.Transient {
			rv := w.ObjectMeta.ResourceVersion
			w.Status.Attempts = 0
			if updated, uerr := c.cfg.Store.Update(ctx, w, rv); uerr == nil {
				w = updated
			}
		}
		if err := c.q.Add(w.ObjectMeta.Name); err != nil {
			c.cfg.Log("event=resume_enqueue_shed name=%s reason=%s", w.ObjectMeta.Name, v1.ReasonQueueShed)
		} else {
			c.cfg.Log("event=resume_enqueue name=%s phase=%s rv=%d", w.ObjectMeta.Name, phase, w.ObjectMeta.ResourceVersion)
		}
	}

	for i := 0; i < c.cfg.Workers; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i)
	}
	c.cfg.Log("event=controller_started workers=%d resumed=%d", c.cfg.Workers, len(existing))
	return nil
}

func (c *Controller) worker(ctx context.Context, id int) {
	defer c.wg.Done()
	for {
		key, err := c.q.Get(ctx)
		if err != nil {
			if errors.Is(err, queue.ErrShutdown) || ctx.Err() != nil {
				return
			}
			continue
		}
		c.processKey(ctx, id, key)
		c.q.Done(key)
	}
}

func (c *Controller) processKey(ctx context.Context, workerID int, key string) {
	// Key-level mutual exclusion: same object never processed twice at once.
	lock := c.keyLock(key)
	lock.Lock()
	defer lock.Unlock()

	before, _ := c.cfg.Store.Get(ctx, key)
	c.cfg.Log("event=reconcile_start worker=%d input=%s", workerID, summarize(before))

	// Snapshot the dependency list before the pass: if the object is
	// physically deleted inside the pass, its record (and thus DependsOn) is
	// no longer readable afterwards.
	beforeDeps := []string{}
	if before != nil {
		beforeDeps = append(beforeDeps, before.Spec.DependsOn...)
	}

	res := c.rec.Reconcile(ctx, key)
	after, _ := c.cfg.Store.Get(ctx, key)
	c.cfg.Log("event=reconcile_decision worker=%d name=%s reason=%s requeue=%t after=%s",
		workerID, key, res.Reason, res.Requeue, summarize(after))

	// Dependency-driven wakeups:
	//  - physical deletion: dependents observe the missing dependency, and
	//    the deleted object's own dependencies may now be deletable;
	//  - deletion in progress: wake both sides so ordered teardown proceeds;
	//  - becoming Available: unblocks objects ordered behind it.
	switch {
	case after == nil:
		c.enqueueDependents(ctx, key)
		c.enqueueDependencies(beforeDeps)
	case after.ObjectMeta.DeletionTimestamp != nil:
		c.enqueueDependents(ctx, key)
		c.enqueueAllDependenciesOf(ctx, key)
	case after.Status.Phase == v1.PhaseAvailable:
		c.enqueueDependents(ctx, key)
	}

	if res.Requeue {
		switch {
		case res.Conflict:
			// Immediate retry against fresh state.
			c.enqueue(key, 0)
		case res.RequeueAfter > 0:
			c.schedule(key, res.RequeueAfter)
		default:
			c.enqueue(key, 0)
		}
		return
	}
	c.cancelDelay(key)
}

func (c *Controller) enqueue(key string, _ time.Duration) {
	if err := c.q.Add(key); err != nil {
		c.cfg.Log("event=enqueue_shed name=%s reason=%s", key, v1.ReasonQueueShed)
	}
}

func (c *Controller) schedule(key string, d time.Duration) {
	c.delayMu.Lock()
	defer c.delayMu.Unlock()
	if old := c.delayTimers[key]; old != nil {
		old.Stop()
	}
	c.delayTimers[key] = time.AfterFunc(d, func() {
		c.delayMu.Lock()
		delete(c.delayTimers, key)
		c.delayMu.Unlock()
		if err := c.q.Add(key); err != nil {
			c.cfg.Log("event=backoff_enqueue_shed name=%s reason=%s", key, v1.ReasonQueueShed)
		}
	})
}

func (c *Controller) cancelDelay(key string) {
	c.delayMu.Lock()
	defer c.delayMu.Unlock()
	if t := c.delayTimers[key]; t != nil {
		t.Stop()
		delete(c.delayTimers, key)
	}
}

func (c *Controller) keyLock(key string) *sync.Mutex {
	c.keyMu.Lock()
	defer c.keyMu.Unlock()
	m, ok := c.keyLocks[key]
	if !ok {
		m = &sync.Mutex{}
		c.keyLocks[key] = m
	}
	return m
}

// submitLock returns the per-key FIFO ticket lock serializing client writes.
func (c *Controller) submitLock(key string) *ticketLock {
	c.keyMu.Lock()
	defer c.keyMu.Unlock()
	tl, ok := c.submitLocks[key]
	if !ok {
		tl = newTicketLock()
		c.submitLocks[key] = tl
	}
	return tl
}

// Submit runs a raw manifest through admission and, if allowed, persists it
// (create or spec update with generation bump + OCC) and enqueues
// reconciliation. Both the object and its dependents are enqueued, because a
// change in availability may unblock an ordered dependency.
func (c *Controller) Submit(ctx context.Context, raw []byte) admission.Decision {
	dec := c.cfg.Reviewer.Review(ctx, raw)
	c.cfg.Log("event=admission input=%s allowed=%t reason=%s", compact(raw), dec.Allowed, dec.Reason)
	if !dec.Allowed {
		return dec
	}
	in := dec.Workload

	// Serialize all writes to the same object key in call order (FIFO ticket
	// lock). Together with optimistic concurrency in the store this
	// guarantees:
	//  - a Submit that returned successfully has its spec durably persisted;
	//  - the spec durable when the last Submit returns is exactly the spec
	//    admitted by that last call ("latest wins");
	//  - reconciliation never observes a half-applied read-modify-write.
	tl := c.submitLock(in.ObjectMeta.Name)
	tl.Lock()
	defer tl.Unlock()

	// Read-modify-write with bounded conflict retries: a lost update (e.g. a
	// concurrent resync or restart writer) is retried against the newest
	// persisted state. The retry count is bounded so contention can never
	// produce an unbounded spin.
	const maxSubmitRetries = 25
	var stored *v1.Workload
	for attempt := 0; ; attempt++ {
		r := c.persistSubmit(ctx, in)
		if r.reject != nil {
			return *r.reject
		}
		if !r.retry {
			stored = r.workload
			break
		}
		if attempt == maxSubmitRetries {
			c.cfg.Log("event=submit_conflict_budget_exhausted name=%s", in.ObjectMeta.Name)
			return admission.Decision{
				Allowed: false,
				Reason:  v1.ReasonConflict,
				Message: "too many concurrent updates; retry budget exhausted",
			}
		}
		select {
		case <-ctx.Done():
			return admission.Decision{Allowed: false, Reason: v1.ReasonTransientFailure, Message: ctx.Err().Error()}
		case <-time.After(time.Millisecond):
		}
	}
	dec.Workload = stored

	c.enqueue(in.ObjectMeta.Name, 0)
	c.enqueueDependents(ctx, in.ObjectMeta.Name)
	return dec
}

// submitAttempt is the result of one read-modify-write attempt.
type submitAttempt struct {
	workload *v1.Workload        // populated on success or no-op
	retry    bool                // conflict: caller should re-read and retry
	reject   *admission.Decision // terminal rejection
}

// persistSubmit performs one read-modify-write attempt.
func (c *Controller) persistSubmit(ctx context.Context, in *v1.Workload) submitAttempt {
	reject := func(reason, msg string) submitAttempt {
		d := admission.Decision{Allowed: false, Reason: reason, Message: msg}
		return submitAttempt{reject: &d}
	}

	existing, err := c.cfg.Store.Get(ctx, in.ObjectMeta.Name)
	switch {
	case err == nil:
		if existing.ObjectMeta.DeletionTimestamp != nil {
			return reject(v1.ReasonDeleting, "object is being deleted")
		}
		// Bump generation only when the spec actually changed, so equal
		// re-submits create no side effects and do not rewrite the object.
		if specEqual(existing.Spec, in.Spec) {
			c.cfg.Log("event=submit_noop name=%s rv=%d", in.ObjectMeta.Name, existing.ObjectMeta.ResourceVersion)
			return submitAttempt{workload: existing}
		}
		in.ObjectMeta.ResourceVersion = existing.ObjectMeta.ResourceVersion
		in.ObjectMeta.Generation = existing.ObjectMeta.Generation + 1
		in.ObjectMeta.CreationTimestamp = existing.ObjectMeta.CreationTimestamp
		in.ObjectMeta.DeletionTimestamp = existing.ObjectMeta.DeletionTimestamp
		in.Status = existing.Status
		updated, uerr := c.cfg.Store.Update(ctx, in, existing.ObjectMeta.ResourceVersion)
		if uerr != nil {
			if errors.Is(uerr, storage.ErrConflict) {
				c.cfg.Log("event=submit_conflict_retry name=%s", in.ObjectMeta.Name)
				return submitAttempt{retry: true}
			}
			c.cfg.Log("event=submit_update_failed name=%s err=%v", in.ObjectMeta.Name, uerr)
			return reject(v1.ReasonInvalidSpec, uerr.Error())
		}
		return submitAttempt{workload: updated}
	case isNotFound(err):
		created, cerr := c.cfg.Store.Create(ctx, in)
		if cerr == nil {
			return submitAttempt{workload: created}
		}
		// Lost a create race against a concurrent writer: retry as update.
		if strings.Contains(cerr.Error(), "already exists") {
			c.cfg.Log("event=submit_create_race_retry name=%s", in.ObjectMeta.Name)
			return submitAttempt{retry: true}
		}
		c.cfg.Log("event=submit_create_failed name=%s err=%v", in.ObjectMeta.Name, cerr)
		return reject(v1.ReasonInvalidSpec, cerr.Error())
	default:
		return reject(v1.ReasonTransientFailure, err.Error())
	}
}

// Delete marks an object for deletion (sets DeletionTimestamp; the finalizer
// blocks physical deletion until data-plane children are gone). The mark
// itself is serialized against Submit for the same key.
func (c *Controller) Delete(ctx context.Context, name string) error {
	tl := c.submitLock(name)
	tl.Lock()
	defer tl.Unlock()

	w, err := c.cfg.Store.Get(ctx, name)
	if err != nil {
		return err
	}
	if w.ObjectMeta.DeletionTimestamp != nil {
		c.enqueue(name, 0)
		return nil
	}
	now := c.cfg.Now()
	w.ObjectMeta.DeletionTimestamp = &now
	updated, err := c.cfg.Store.Update(ctx, w, w.ObjectMeta.ResourceVersion)
	if err != nil {
		return err
	}
	c.cfg.Log("event=delete_requested name=%s rv=%d", name, updated.ObjectMeta.ResourceVersion)
	c.enqueue(name, 0)
	// A deletion may unblock... nothing on delete, but dependents must react
	// to the missing dependency state.
	c.enqueueDependents(ctx, name)
	return nil
}

// enqueueDependents enqueues every live object whose DependsOn contains name.
// Shedded adds are recorded with the QueueShed reason rather than retried in
// a loop.
func (c *Controller) enqueueDependents(ctx context.Context, name string) {
	all, err := c.cfg.Store.List(ctx)
	if err != nil {
		return
	}
	for _, o := range all {
		for _, dep := range o.Spec.DependsOn {
			if dep == name {
				c.enqueue(o.ObjectMeta.Name, 0)
				break
			}
		}
	}
}

// enqueueDependencies enqueues every live object that the (possibly already
// deleted) object named name used to depend on, so ordered teardown can
// proceed. The dependency list is supplied by the caller from the pre-pass
// snapshot.
func (c *Controller) enqueueDependencies(deps []string) {
	for _, dep := range deps {
		c.enqueue(dep, 0)
	}
}

// enqueueAllDependenciesOf is the live-object variant used while the object
// still exists.
func (c *Controller) enqueueAllDependenciesOf(ctx context.Context, name string) {
	if w, err := c.cfg.Store.Get(ctx, name); err == nil {
		c.enqueueDependencies(w.Spec.DependsOn)
	}
}

// Enqueue re-adds a key to the work queue (wakeup / periodic resync). It is
// coalesced like any other Add and records QueueShed if the bound rejects it.
func (c *Controller) Enqueue(key string) error { return c.q.Add(key) }

// Get returns the persisted object (for observability/tests).
func (c *Controller) Get(ctx context.Context, name string) (*v1.Workload, error) {
	return c.cfg.Store.Get(ctx, name)
}

// WaitForPhase polls until the object reaches one of wanted phases or the
// deadline elapses. Test helper; deterministic failures and Available are
// terminal enough for callers to assert.
func (c *Controller) WaitForPhase(ctx context.Context, name string, wanted ...v1.Phase) (*v1.Workload, error) {
	deadline := time.Now().Add(10 * time.Second)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	for time.Now().Before(deadline) {
		w, err := c.cfg.Store.Get(ctx, name)
		if err == nil {
			for _, p := range wanted {
				if w.Status.Phase == p {
					return w, nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
	w, _ := c.cfg.Store.Get(ctx, name)
	return w, fmt.Errorf("controller: timeout waiting for phase %v of %s (last=%s)", wanted, name, phaseOf(w))
}

// Stop drains and shuts the controller down.
func (c *Controller) Stop() {
	c.q.Shutdown()
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	c.delayMu.Lock()
	for _, t := range c.delayTimers {
		t.Stop()
	}
	c.delayTimers = map[string]*time.Timer{}
	c.delayMu.Unlock()
	_ = c.cfg.Store.Close()
	_ = c.cfg.DP.Close()
	c.cfg.Log("event=controller_stopped")
}

func isNotFound(err error) bool {
	var nf *storage.NotFoundError
	return errors.As(err, &nf)
}

func phaseOf(w *v1.Workload) v1.Phase {
	if w == nil {
		return "<gone>"
	}
	return w.Status.Phase
}

func summarize(w *v1.Workload) string {
	if w == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Workload{name=%s gen=%d rv=%d phase=%s observed=%d attempts=%d instances=%d deleting=%t}",
		w.ObjectMeta.Name, w.ObjectMeta.Generation, w.ObjectMeta.ResourceVersion,
		w.Status.Phase, w.Status.ObservedGeneration, w.Status.Attempts,
		len(w.Status.Instances), w.ObjectMeta.DeletionTimestamp != nil)
}

func compact(raw []byte) string {
	s := string(raw)
	for i := range s {
		if s[i] == '\n' {
			return s[:i] + "...(" + itoa(len(s)) + " bytes)"
		}
	}
	if len(s) > 160 {
		return s[:160] + "..."
	}
	return s
}

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
