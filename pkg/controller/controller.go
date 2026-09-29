// Package controller wires the store, bounded keyed queue, worker pool
// and reconciler into a runnable controller.
package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/engine"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

// Defaults for controller configuration.
const (
	DefaultWorkers       = 2
	DefaultQueueCapacity = 128
	// DefaultRecoveryPageSize bounds how many keys one recovery page
	// holds, so a very large actual/control side cannot be pulled into
	// memory in one shot.
	DefaultRecoveryPageSize = 64
	// DefaultRecoveryTimeout bounds a single startup/resync audit pass;
	// cancellation or expiry ends the pass promptly with a distinct,
	// observable reason, and the next pass resumes paging.
	DefaultRecoveryTimeout = 30 * time.Second
	// maxObjectSize bounds an individual spec JSON blob so one huge object
	// cannot exhaust process memory.
	maxObjectSize = 64 * 1024

	// ReasonResync marks controller-level bookkeeping decisions.
	ReasonResync = "Resync"
)

// Config configures a Controller.
type Config struct {
	Workers       int
	QueueCapacity int
	Store         *storage.MemStore
	Actuator      engine.Actuator
	Clock         engine.Clock
	Backoff       engine.BackoffPolicy
	Sink          engine.DecisionSink
	Jitter        func() float64
	// ResyncPeriod re-enqueues every object periodically so missed
	// signals self-heal. Zero disables it.
	ResyncPeriod time.Duration
	// RecoveryPageSize bounds keys scanned per page during startup and
	// periodic actual-side reconciliation. Non-positive uses the default.
	RecoveryPageSize int
	// RecoveryTimeout bounds one audit pass. Non-positive uses default.
	RecoveryTimeout time.Duration
}

// Controller is the runnable declarative controller.
type Controller struct {
	cfg   Config
	store *storage.MemStore
	queue *engine.KeyQueue
	rec   *engine.Reconciler
	clk   engine.Clock
	sink  engine.DecisionSink

	idleMu     sync.Mutex
	inFlight   int
	processed  int64
	overflowed int64
	reclaimed  int64
}

// New constructs a controller with validated defaults.
func New(cfg Config) (*Controller, error) {
	if cfg.Store == nil {
		return nil, errors.New("controller: Store is required")
	}
	if cfg.Actuator == nil {
		return nil, errors.New("controller: Actuator is required")
	}
	if cfg.Clock == nil {
		cfg.Clock = engine.RealClock{}
	}
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = DefaultQueueCapacity
	}
	if cfg.Backoff == (engine.BackoffPolicy{}) {
		cfg.Backoff = engine.DefaultBackoff()
	}
	if cfg.Sink == nil {
		cfg.Sink = engine.NopSink{}
	}
	if cfg.RecoveryPageSize <= 0 {
		cfg.RecoveryPageSize = DefaultRecoveryPageSize
	}
	if cfg.RecoveryTimeout <= 0 {
		cfg.RecoveryTimeout = DefaultRecoveryTimeout
	}
	q := engine.NewKeyQueue(cfg.QueueCapacity, cfg.Clock)
	rec := engine.NewReconciler(cfg.Store, cfg.Actuator, cfg.Clock, cfg.Backoff, cfg.Sink).
		WithDependencyQueue(q).
		WithJitter(cfg.Jitter)
	return &Controller{
		cfg:   cfg,
		store: cfg.Store,
		queue: q,
		rec:   rec,
		clk:   cfg.Clock,
		sink:  cfg.Sink,
	}, nil
}

// Queue gives access to the bounded queue (used for overflow metrics).
func (c *Controller) Queue() *engine.KeyQueue { return c.queue }

// Stats is a point-in-time observability snapshot.
type Stats struct {
	Waiting    int
	InFlight   int
	Processed  int64
	Overflowed int64
	Reclaimed  int64
}

// Stats returns counters describing the bounded-work behaviour.
func (c *Controller) Stats() Stats {
	c.idleMu.Lock()
	defer c.idleMu.Unlock()
	return Stats{Waiting: c.queue.Len(), InFlight: c.inFlight, Processed: c.processed,
		Overflowed: c.overflowed, Reclaimed: c.reclaimed}
}

// Run starts the worker pool and an optional resync loop and blocks
// until ctx is cancelled. All objects already persisted are enqueued on
// start, which is exactly how a restart resumes unfinished work without
// redoing completed side effects.
func (c *Controller) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.worker(ctx)
		}()
	}
	if c.cfg.ResyncPeriod > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.resyncLoop(ctx)
		}()
	}
	// Restart resume and actual-side ownership audit run in their own
	// goroutine: startup must not block while a very large side is paged
	// through, and desired-state writes stay accepted throughout.
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.startupRecovery(ctx)
	}()
	<-ctx.Done()
	c.queue.Close()
	wg.Wait()
	return ctx.Err()
}

// startupRecovery enqueues every persisted object (bounded pages) and
// audits the durable actual side for orphans. Everything is paged,
// cancellable and time-bounded.
func (c *Controller) startupRecovery(ctx context.Context) {
	c.reconcileOnce(ctx, "startup")
}

func (c *Controller) worker(ctx context.Context) {
	for {
		key, err := c.queue.Get(ctx)
		if err != nil {
			if errors.Is(err, engine.ErrQueueClosed) || ctx.Err() != nil {
				return
			}
			continue
		}
		c.idleMu.Lock()
		c.inFlight++
		c.idleMu.Unlock()

		res, rerr := c.rec.Reconcile(ctx, key)

		c.queue.Done(key)
		c.idleMu.Lock()
		c.inFlight--
		c.processed++
		c.idleMu.Unlock()

		switch {
		case rerr != nil && errors.Is(rerr, context.Canceled):
			return
		case rerr != nil:
			c.log(key, "WorkerError", "%v", rerr)
		case res.RequeueAfter > 0:
			if err := c.queue.AddAfter(key, res.RequeueAfter); err != nil {
				c.recordOverflow(key, err)
			}
		case res.Reason == apis.ReasonConflict:
			// immediate bounded retry; AddAfter(0) == Add
			if err := c.queue.Add(key); err != nil {
				c.recordOverflow(key, err)
			}
		}
	}
}

func (c *Controller) resyncLoop(ctx context.Context) {
	// Resync uses the controller clock for tests; real clock still works.
	ticker := newClockTicker(ctx, c.clk, c.cfg.ResyncPeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker:
			c.reconcileOnce(ctx, "resync")
		}
	}
}

// reconcileOnce performs one bounded, cancellable audit pass:
//  1. enqueue every control-plane object in pages (resume unfinished
//     work / correct drift), then
//  2. page through the durable actual side and reclaim definite orphans.
//
// It never loads either side wholesale into memory and never blocks
// longer than RecoveryTimeout; on cancel/timeout it returns promptly and
// records a distinct reason. Re-entrant: interrupted passes are safely
// repeated by the next startup/resync.
func (c *Controller) reconcileOnce(parent context.Context, phase string) {
	ctx, cancel := context.WithTimeout(parent, c.cfg.RecoveryTimeout)
	defer cancel()

	page := c.cfg.RecoveryPageSize

	// Phase A: desired/control side, paged.
	var cursor string
	enqueued := 0
	for {
		if err := ctx.Err(); err != nil {
			c.recordRecoveryStop(phase, err)
			return
		}
		keys, next, err := c.store.ListKeysPage(ctx, cursor, page)
		if err != nil {
			c.log(phase, ReasonResync, "control-side page failed: %v", err)
			return
		}
		for _, k := range keys {
			if err := ctx.Err(); err != nil {
				c.recordRecoveryStop(phase, err)
				return
			}
			if err := c.queue.Add(k); err != nil {
				// Full queue under backlog is expected: record it; the
				// next pass picks the key up.
				c.recordOverflow(k, err)
			} else {
				enqueued++
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}

	// Phase B: actual side ownership audit, paged. Only an actuator with
	// a durable, scannable backend participates.
	lister, ok := c.cfg.Actuator.(engine.ActualLister)
	if !ok {
		c.log(phase, "Resumed", "enqueued %d persisted objects; actual side not scannable", enqueued)
		return
	}
	actualStore := lister.Actual()
	var acursor string
	reclaimed := 0
	kept := 0
	for {
		if err := ctx.Err(); err != nil {
			c.recordRecoveryStop(phase, err)
			return
		}
		keys2, next, err := actualStore.ListPage(ctx, acursor, page)
		if err != nil {
			c.log(phase, ReasonResync, "actual-side page failed: %v", err)
			return
		}
		for _, k := range keys2 {
			if err := ctx.Err(); err != nil {
				c.recordRecoveryStop(phase, err)
				return
			}
			dec, rerr := c.rec.ReapOrphan(ctx, k)
			if rerr != nil {
				c.log(k, "RecoveryError", "%v", rerr)
				continue
			}
			switch dec {
			case engine.ReapRemoved:
				reclaimed++
				c.idleMu.Lock()
				c.reclaimed++
				c.idleMu.Unlock()
			case engine.ReapKept:
				kept++
			case engine.ReapEnqueued:
				// Inconclusive ownership: ordinary reconciliation makes
				// the durable decision. Queue-full is handled like above.
				if err := c.queue.Add(k); err != nil {
					c.recordOverflow(k, err)
				}
				kept++
			}
		}
		if next == "" {
			break
		}
		acursor = next
	}
	c.log(phase, "Resumed",
		"audit complete: enqueued=%d actual-kept=%d orphans-reclaimed=%d", enqueued, kept, reclaimed)
}

// recordRecoveryStop logs a distinguishable reason when an audit pass is
// interrupted before completion.
func (c *Controller) recordRecoveryStop(phase string, err error) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		c.log(phase, apis.ReasonRecoveryTimedOut,
			"recovery pass exceeded %s; remaining pages deferred to next pass", c.cfg.RecoveryTimeout)
	default:
		c.log(phase, apis.ReasonRecoveryCancelled, "recovery pass cancelled: %v", err)
	}
}

func (c *Controller) recordOverflow(key string, err error) {
	if errors.Is(err, engine.ErrQueueFull) {
		c.idleMu.Lock()
		c.overflowed++
		c.idleMu.Unlock()
		c.log(key, apis.ReasonQueueOverflow, "bounded queue full; work rejected to preserve bounded resource use")
		return
	}
	c.log(key, "QueueError", "%v", err)
}

// Apply creates a new desired-state object or replaces an existing one.
// Admission (including graph validation) runs BEFORE any write, so an
// invalid spec is rejected with a distinguishable *apis.AdmissionError
// and never reaches the store, the queue or the actuator.
func (c *Controller) Apply(ctx context.Context, obj *apis.RawObject) error {
	if obj == nil {
		return apis.NewAdmissionError(apis.ReasonMalformedSpec, "object is nil")
	}
	if len(obj.Spec) > maxObjectSize {
		return apis.NewAdmissionError(apis.ReasonMalformedSpec, "spec exceeds %d bytes", maxObjectSize)
	}
	// decode + self/default validation first
	spec, _, err := apis.DecodeSpec(obj)
	if err != nil {
		return err
	}
	// cross-object validation over the existing store plus this candidate
	all, err := c.store.List(ctx)
	if err != nil {
		return err
	}
	graph := make(map[string]*apis.WidgetSpec, len(all)+1)
	for _, o := range all {
		s, _, derr := apis.DecodeSpec(o)
		if derr == nil {
			graph[o.Metadata.Name] = s
		}
	}
	graph[obj.Metadata.Name] = spec
	if err := apis.Validate(obj.Metadata.Name, spec, graph); err != nil {
		return err
	}

	existing, erv, gerr := c.store.Get(ctx, obj.Metadata.Name)
	if gerr != nil && !errors.Is(gerr, storage.ErrNotFound) {
		return gerr
	}
	if errors.Is(gerr, storage.ErrNotFound) {
		obj.Metadata.Generation = 1
		if _, _, err := c.store.Create(ctx, obj); err != nil {
			return err
		}
		c.log(obj.Metadata.Name, "Applied", "created desired state gen=1")
	} else {
		// Bump generation only when the canonical desired spec changes.
		// Comparison is semantic (defaults applied, v1/v2 normalised),
		// not textual, so adding/removing an explicit default does not
		// look like a change and never causes a store write.
		var same bool
		if oldSpec, _, derr := apis.DecodeSpec(existing); derr == nil {
			same = oldSpec.Equal(spec)
		} else {
			same = specJSONEqual(existing.Spec, obj.Spec)
		}
		if same {
			c.log(obj.Metadata.Name, "Applied", "desired state unchanged at gen=%d; no write", existing.Metadata.Generation)
			// Still ensure convergence is scheduled: this is a no-op if the
			// key is already queued or in flight.
			if err := c.queue.Add(obj.Metadata.Name); err != nil && !errors.Is(err, engine.ErrQueueFull) {
				return err
			}
			return nil
		}
		gen := existing.Metadata.Generation + 1
		obj.Metadata.Generation = gen
		// Preserve controller-owned metadata and the entire status: Apply
		// carries desired state only and must never wipe a finalizer
		// (which would orphan an actual resource) or observed status.
		obj.Metadata.Finalizers = existing.Metadata.Finalizers
		obj.Metadata.DeletionTimestamp = existing.Metadata.DeletionTimestamp
		if existing.Metadata.Annotations != nil {
			obj.Metadata.Annotations = existing.Metadata.Annotations
		}
		obj.Status = existing.Status
		if _, _, err := c.store.Update(ctx, obj, erv); err != nil {
			return err
		}
		c.log(obj.Metadata.Name, "Applied", "stored desired state gen=%d changed=true", gen)
	}

	if err := c.queue.Add(obj.Metadata.Name); err != nil {
		// desired state is persisted, but the controller could not accept
		// more work right now: surface a distinguishable overflow record.
		c.recordOverflow(obj.Metadata.Name, err)
		if errors.Is(err, engine.ErrQueueFull) {
			return &QueueFullError{Key: obj.Metadata.Name}
		}
		return err
	}
	return nil
}

// QueueFullError reports that desired state was accepted but the bounded
// work queue rejected immediate processing. The resync loop converges it.
type QueueFullError struct{ Key string }

func (e *QueueFullError) Error() string {
	return fmt.Sprintf("work queue full; %q persisted and will converge on resync", e.Key)
}

// Is reports QueueFullError against the sentinel-like engine.ErrQueueFull
// so callers can use errors.Is(err, engine.ErrQueueFull).
func (e *QueueFullError) Is(target error) bool { return target == engine.ErrQueueFull }

// Delete marks an object for deletion. Actual ordered removal happens
// asynchronously in the reconciler; Delete only persists the intent.
func (c *Controller) Delete(ctx context.Context, name string) error {
	obj, rv, err := c.store.Get(ctx, name)
	if err != nil {
		return err
	}
	if obj.Metadata.DeletionTimestamp != nil {
		return nil // already marked: idempotent
	}
	ts := c.clk.Now()
	obj.Metadata.DeletionTimestamp = &ts
	if _, _, err := c.store.Update(ctx, obj, rv); err != nil {
		return err
	}
	c.log(name, "DeleteRequested", "deletion timestamp set")
	if err := c.queue.Add(name); err != nil {
		c.recordOverflow(name, err)
		if errors.Is(err, engine.ErrQueueFull) {
			return &QueueFullError{Key: name}
		}
		return err
	}
	return nil
}

// AwaitIdle polls until the queue is drained and nothing is in flight,
// or until ctx expires. Intended for tests and the demo.
func (c *Controller) AwaitIdle(ctx context.Context, poll time.Duration) error {
	if poll <= 0 {
		poll = time.Millisecond
	}
	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if c.queue.Len() == 0 {
				c.idleMu.Lock()
				busy := c.inFlight
				c.idleMu.Unlock()
				if busy == 0 {
					return nil
				}
			}
		}
	}
}

func (c *Controller) log(key, action string, format string, args ...any) {
	c.sink.Log(engine.Decision{
		At:      c.clk.Now(),
		Key:     key,
		Action:  action,
		Reason:  action,
		Message: fmt.Sprintf(format, args...),
	})
}

// specJSONEqual compares two spec blobs semantically (key order
// independent) without depending on the versioned structs.
func specJSONEqual(a, b []byte) bool {
	var va, vb any
	if err := jsonUnmarshal(a, &va); err != nil {
		return false
	}
	if err := jsonUnmarshal(b, &vb); err != nil {
		return false
	}
	return jsonEqual(va, vb)
}
