package engine

import (
	"context"
	"errors"

	"github.com/lechandonga/resource-reconciler-go/pkg/actual"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

// ReapDecision is the auditable outcome of auditing one durable actual
// resource against the control plane.
type ReapDecision int

const (
	// ReapKept means the resource is demonstrably owned (its object exists
	// and carries the controller finalizer); it was left untouched.
	ReapKept ReapDecision = iota
	// ReapRemoved means the resource had no owner object: a definite
	// orphan, removed from the actual backend.
	ReapRemoved
	// ReapEnqueued means ownership is inconclusive from the object alone
	// (object exists but does not carry the finalizer); the resource is
	// kept and normal reconciliation decides its fate.
	ReapEnqueued
)

// ReapOrphan audits one durable actual-resource key against the control
// plane and applies the only definite cleanup: resources without any
// owner object are orphans and are removed. Every other case keeps the
// resource (a crash between "created" and "finalizer/status recorded"
// must never be mistaken for an orphan) and returns the reason so the
// caller can schedule ordinary reconciliation.
func (r *Reconciler) ReapOrphan(ctx context.Context, key string) (ReapDecision, error) {
	if err := ctx.Err(); err != nil {
		return ReapKept, err
	}
	owned, _, err := r.store.Get(ctx, key)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		// unexpected storage error: do not touch the resource
		return ReapKept, err
	}
	if owned != nil {
		if hasFinalizer(owned) {
			r.log(Decision{Key: key, Action: "actual-kept", Reason: apis.ReasonActualKept,
				Message: "durable actual resource owned by object with finalizer", Input: inputSummary(owned)})
			return ReapKept, nil
		}
		r.log(Decision{Key: key, Action: "actual-kept", Reason: apis.ReasonActualKept,
			Message: "object exists without finalizer; defer to reconciliation (never delete an unbooked valid resource)",
			Input:   inputSummary(owned)})
		return ReapEnqueued, nil
	}

	// No owner object exists in the control plane: definite orphan. This
	// is the sole basis for deleting an actual resource during audit, and
	// it is re-determinable by every restart.
	if _, err := r.act.Remove(ctx, key, nil); err != nil {
		return ReapKept, err
	}
	r.log(Decision{Key: key, Action: "orphan-removed", Reason: apis.ReasonOrphanReclaimed,
		Message: "durable actual resource has no owner object; reclaimed"})
	return ReapRemoved, nil
}

// ActualLister is implemented by actuators whose actual side can be
// scanned (the durable fake backend). The controller pages through it so
// startup recovery never loads the whole actual world at once.
type ActualLister interface {
	Actual() actual.Store
}
