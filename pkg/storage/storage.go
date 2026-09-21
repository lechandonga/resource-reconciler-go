// Package storage implements the in-process simulated control plane with
// file-backed persistence and optimistic concurrency control.
package storage

import (
	"context"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
)

// ErrConflict is returned when a write-back is based on a stale resource
// version. Callers must re-read, reconcile against the newest state and retry.
var ErrConflict = errConflict{}

type errConflict struct{}

func (errConflict) Error() string {
	return "storage: resource version conflict (optimistic concurrency)"
}

// NotFoundError identifies a missing object.
type NotFoundError struct{ Name string }

func (e *NotFoundError) Error() string { return "storage: workload not found: " + e.Name }

// Store persists Workload objects. Implementations must be safe for
// concurrent use.
type Store interface {
	// Create inserts a new object. Its resource version is assigned by the store.
	Create(ctx context.Context, w *v1.Workload) (*v1.Workload, error)
	// Get returns a deep copy of the object.
	Get(ctx context.Context, name string) (*v1.Workload, error)
	// List returns deep copies of all objects ordered by name.
	List(ctx context.Context) ([]*v1.Workload, error)
	// Update writes back only if expectedRV matches the current resource
	// version; otherwise ErrConflict is returned and nothing is persisted.
	Update(ctx context.Context, w *v1.Workload, expectedRV int64) (*v1.Workload, error)
	// Delete physically removes the object, gated on expectedRV. A stale RV
	// yields ErrConflict. Deleting an already-absent object is a successful
	// no-op.
	Delete(ctx context.Context, name string, expectedRV int64) error
	// Close flushes any buffered state.
	Close() error
}
