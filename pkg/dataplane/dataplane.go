// Package dataplane simulates the data plane where Workload side effects
// (instances) materialize. All operations are idempotent and persisted, so a
// controller restart never repeats an effect that already took effect.
package dataplane

import (
	"context"
	"errors"
)

// ErrorClass categorizes a data-plane failure.
type ErrorClass int

const (
	ClassOK ErrorClass = iota
	// ClassTransient failures may disappear on retry (e.g. simulated flake).
	ClassTransient
	// ClassDeterministic failures persist until the spec changes
	// (e.g. illegal image); they must not be retried forever.
	ClassDeterministic
)

// Failure is a classified data-plane error.
type Failure struct {
	Class   ErrorClass
	Code    string
	Message string
}

func (f *Failure) Error() string { return f.Code + ": " + f.Message }

// AsFailure extracts a classified Failure from err.
func AsFailure(err error) (*Failure, bool) {
	var f *Failure
	return f, errors.As(err, &f)
}

// InstanceID identifies a data-plane instance belonging to a workload.
// Instances are addressed by (workload, index) so scaling up/down is
// deterministic and stable across retries.
type InstanceID struct {
	Workload string
	Index    int
}

// Instance is the materialized data-plane record.
type Instance struct {
	ID         InstanceID
	Image      string
	Generation int64
}

// Client is the idempotent data-plane API.
type Client interface {
	// EnsureInstance creates or updates the instance. Repeated calls with the
	// same (id, image, generation) produce no repeated side effect.
	EnsureInstance(ctx context.Context, id InstanceID, image string, generation int64) error
	// RemoveInstance deletes the instance. Removing an absent instance is a
	// successful no-op.
	RemoveInstance(ctx context.Context, id InstanceID) error
	// ListInstances returns all instances of one workload ordered by index.
	ListInstances(ctx context.Context, workload string) ([]Instance, error)
	// Close persists data-plane state.
	Close() error
}
