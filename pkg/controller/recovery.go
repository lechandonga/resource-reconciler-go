// Startup reconciliation between the durable control plane and the
// durable actual backend.
//
// Ownership is determined exclusively from durable control-plane facts:
//
//   - An actual resource whose key has NO stored object, or whose object
//     exists WITHOUT the controller finalizer, has no live owner. It is a
//     residual and is removed. A fresh object never acts between the
//     "finalizer persisted" ordering and this check, because the finalizer
//     is always persisted before the actuator is ever called.
//   - An actual resource whose object still carries the finalizer is
//     always retained: it is either already recorded, or an in-flight
//     resource whose bookkeeping did not land (crash in the
//     action-then-record window). It is enqueued and adopted by the
//     normal reconcile path, NEVER mistaken for a residual.
//
// The scan is paged (bounded memory), paced (does not pin startup), and
// fully driven by ctx: cancellation or the timeout budget return promptly
// with a distinguishable reason; the periodic resync completes the
// remainder idempotently, so re-entering recovery after another crash
// converges to the same result.
package controller

import (
	"context"
	"errors"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

// ActualEnumerator is the bounded enumeration contract the durable
// actuator satisfies. Ephemeral actuators do not implement it; in that
// case startup reconciliation is a no-op (the old behaviour).
type ActualEnumerator interface {
	ActualKeys(ctx context.Context, cursor string, limit int) ([]string, string, error)
	RemoveIfPresent(ctx context.Context, key string) (bool, error)
}

// ActualHydrater restores an already-known actual resource without a
// side effect. The durable actuator implements it; it is used to take
// over an older installation whose actual state lived only inside the
// control-plane snapshot.
type ActualHydrater interface {
	Hydrate(key string, st apis.ActualState) (bool, error)
}

// RecoveryConfig bounds startup reconciliation.
type RecoveryConfig struct {
	// PageSize is the number of actual keys scanned per page.
	PageSize int
	// PagePause is yielded to the context/clock between pages so a huge
	// backend cannot pin startup; it also makes cancellation prompt.
	PagePause time.Duration
	// Timeout bounds the whole startup sweep. Zero means no explicit
	// deadline beyond the run context.
	Timeout time.Duration
}

// DefaultRecoveryConfig returns conservative bounded defaults.
func DefaultRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{PageSize: 64, PagePause: time.Millisecond, Timeout: 30 * time.Second}
}

// RecoveryStats describes one startup sweep outcome.
type RecoveryStats struct {
	Scanned   int
	Reclaimed int
	Adopted   int
	Retained  int
	Migrated  int
	// Done is false when the sweep stopped early (cancelled/timed out).
	Done bool
	// Reason distinguishes a normal completion from cancellation/timeout.
	Reason string
}

// reconcileActual performs one bounded, idempotent, re-entrant sweep of
// the durable actual backend against durable control-plane ownership.
func (c *Controller) reconcileActual(ctx context.Context, cfg RecoveryConfig) RecoveryStats {
	st := RecoveryStats{Done: true, Reason: "Complete"}
	enum, ok := c.cfg.Actuator.(ActualEnumerator)
	if !ok {
		return st // ephemeral backend: nothing durable to reconcile
	}
	// Phase 0: take over any actual state persisted by an older version
	// inside the control-plane snapshot. Bounded and paged; cancellation
	// aborts and the next restart/resync resumes idempotently.
	if h, migratable := c.cfg.Actuator.(ActualHydrater); migratable {
		if done, reason := c.migrateLegacyActual(ctx, h, cfg, &st); !done {
			st.Done = false
			st.Reason = reason
			return st
		}
	}
	if cfg.PageSize <= 0 {
		cfg.PageSize = 64
	}
	sweepCtx := ctx
	var cancel context.CancelFunc
	if cfg.Timeout > 0 {
		sweepCtx, cancel = context.WithTimeout(ctx, cfg.Timeout)
	}
	if cancel != nil {
		defer cancel()
	}

	cursor := ""
	for {
		if err := sweepCtx.Err(); err != nil {
			st.Done = false
			st.Reason = reasonFromCtx(err)
			c.log("recovery", st.Reason,
				"startup sweep stopped: scanned=%d reclaimed=%d retained=%d adopted=%d: %v",
				st.Scanned, st.Reclaimed, st.Retained, st.Adopted, err)
			return st
		}
		keys, next, err := enum.ActualKeys(sweepCtx, cursor, cfg.PageSize)
		if err != nil {
			st.Done = false
			st.Reason = "RecoveryError"
			c.log("recovery", st.Reason, "enumerate actual keys after cursor %q: %v", cursor, err)
			return st
		}
		for _, key := range keys {
			if sweepCtx.Err() != nil {
				st.Done = false
				st.Reason = reasonFromCtx(sweepCtx.Err())
				c.log("recovery", st.Reason,
					"startup sweep interrupted mid-page: scanned=%d reclaimed=%d", st.Scanned, st.Reclaimed)
				return st
			}
			c.classifyActual(sweepCtx, key, &st)
		}
		if next == "" {
			break
		}
		cursor = next
		if cfg.PagePause > 0 {
			if err := c.clk.Sleep(sweepCtx, cfg.PagePause); err != nil {
				st.Done = false
				st.Reason = reasonFromCtx(err)
				c.log("recovery", st.Reason, "sweep paused while cancelled/timeout: %v", err)
				return st
			}
		}
	}
	c.log("recovery", "RecoveryComplete",
		"startup sweep done: scanned=%d reclaimed=%d retained=%d adopted=%d migrated=%d",
		st.Scanned, st.Reclaimed, st.Retained, st.Adopted, st.Migrated)
	return st
}

// migrateLegacyActual pages through the control-plane store and restores
// owned actual resources (finalizer present, recorded actual state) into
// the durable actual backend, preserving their exact identities.
func (c *Controller) migrateLegacyActual(ctx context.Context, h ActualHydrater, cfg RecoveryConfig, st *RecoveryStats) (bool, string) {
	pageSize := cfg.PageSize
	if pageSize <= 0 {
		pageSize = 64
	}
	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return false, reasonFromCtx(err)
		}
		names, next, err := c.store.Keys(ctx, cursor, pageSize)
		if err != nil {
			c.log("recovery", "RecoveryError", "legacy migration enumerate: %v", err)
			return false, "RecoveryError"
		}
		for _, name := range names {
			if ctx.Err() != nil {
				return false, reasonFromCtx(ctx.Err())
			}
			obj, _, gerr := c.store.Get(ctx, name)
			if gerr != nil || obj == nil {
				continue
			}
			if !hasFinalizerIn(obj) || obj.Status == nil || obj.Status.Actual == nil ||
				obj.Status.Actual.ResourceID == "" {
				continue
			}
			added, herr := h.Hydrate(name, *obj.Status.Actual)
			if herr != nil {
				c.log(name, "RecoveryError", "legacy actual migration failed: %v", herr)
				return false, "RecoveryError"
			}
			if added {
				st.Migrated++
				c.log(name, "RecoveryMigrated",
					"restored persisted actual state in place (resourceID=%s)",
					obj.Status.Actual.ResourceID)
			}
		}
		if next == "" {
			return true, "Complete"
		}
		cursor = next
		if cfg.PagePause > 0 {
			if err := c.clk.Sleep(ctx, cfg.PagePause); err != nil {
				return false, reasonFromCtx(err)
			}
		}
	}
}

// classifyActual decides, from durable ownership facts alone, whether one
// actual resource is retained (and adopted by reconcile) or reclaimed.
func (c *Controller) classifyActual(ctx context.Context, key string, st *RecoveryStats) {
	enum := c.cfg.Actuator.(ActualEnumerator)
	st.Scanned++
	obj, _, err := c.store.Get(ctx, key)
	if err != nil {
		if !errors.Is(err, storage.ErrNotFound) {
			c.log(key, "RecoveryError", "ownership lookup failed: %v; retaining for resync", err)
			st.Retained++
			return
		}
		// No intent object at all: the resource cannot have a live owner.
		if _, rerr := enum.RemoveIfPresent(ctx, key); rerr != nil {
			c.log(key, "RecoveryError", "reclaim failed, retained for resync: %v", rerr)
			st.Retained++
			return
		}
		st.Reclaimed++
		c.log(key, apis.ReasonRecoveryReclaimed, "actual resource had no owning object; removed as residual")
		return
	}

	if !hasFinalizerIn(obj) {
		// The actuator is never called before the finalizer is persisted,
		// so a resource under an object without the finalizer cannot be an
		// in-flight creation: it is provably orphaned. Reclaim it, then
		// enqueue the object so it converges (finishes deletion, or
		// re-creates under a fresh finalizer) exactly once.
		if _, rerr := enum.RemoveIfPresent(ctx, key); rerr != nil {
			c.log(key, "RecoveryError", "orphan reclaim failed, retained for resync: %v", rerr)
			st.Retained++
			return
		}
		st.Reclaimed++
		c.log(key, apis.ReasonRecoveryReclaimed,
			"object present without finalizer (deleting=%v); removed orphaned actual resource",
			obj.Metadata.DeletionTimestamp != nil)
		if err := c.queue.Add(key); err != nil {
			c.recordOverflow(key, err)
		}
		return
	}

	// Finalizer present: the controller durably owns this resource.
	// Retain it unconditionally. Whether the control plane already
	// recorded it or the record is missing due to a crash is decided by
	// the normal reconcile pass (which adopts the existing identity).
	st.Retained++
	if obj.Status == nil || obj.Status.Actual == nil || obj.Status.Actual.ResourceID == "" {
		st.Adopted++
		c.log(key, apis.ReasonRecoveryAdopted,
			"finalizer present but actual record missing (deleting=%v); retaining and scheduling adoption",
			obj.Metadata.DeletionTimestamp != nil)
	} else {
		c.log(key, apis.ReasonRecoveryRetained,
			"finalizer present; actual resource retained as owned (resourceID=%s)",
			obj.Status.Actual.ResourceID)
	}
	if err := c.queue.Add(key); err != nil {
		c.recordOverflow(key, err)
	}
}

func reasonFromCtx(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return apis.ReasonRecoveryTimedOut
	case errors.Is(err, context.Canceled):
		return apis.ReasonRecoveryCancelled
	default:
		return apis.ReasonRecoveryPartial
	}
}

// hasFinalizerIn mirrors the reconciler's ownership marker.
func hasFinalizerIn(obj *apis.RawObject) bool {
	for _, f := range obj.Metadata.Finalizers {
		if f == apis.FinalizerReconciler {
			return true
		}
	}
	return false
}
