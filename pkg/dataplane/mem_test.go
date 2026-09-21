package dataplane

import (
	"context"
	"errors"
	"testing"
)

func TestEnsureIsIdempotent(t *testing.T) {
	c, err := NewMemClient("", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	id := InstanceID{Workload: "w", Index: 0}

	for i := 0; i < 5; i++ {
		if err := c.EnsureInstance(ctx, id, "nginx:1", 1); err != nil {
			t.Fatalf("ensure %d: %v", i, err)
		}
	}
	got, err := c.ListInstances(ctx, "w")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("instances = %d, want 1 (no repeated side effects)", len(got))
	}

	// Changed generation/image updates; identical call does not.
	if err := c.EnsureInstance(ctx, id, "nginx:2", 2); err != nil {
		t.Fatal(err)
	}
	got, _ = c.ListInstances(ctx, "w")
	if got[0].Image != "nginx:2" || got[0].Generation != 2 {
		t.Fatalf("update not applied: %+v", got[0])
	}
}

func TestRemoveAbsentIsNoop(t *testing.T) {
	c, _ := NewMemClient("", nil)
	ctx := context.Background()
	if err := c.RemoveInstance(ctx, InstanceID{Workload: "ghost", Index: 7}); err != nil {
		t.Fatalf("removing absent instance should be a successful no-op: %v", err)
	}
}

func TestDeterministicFailureClassification(t *testing.T) {
	c, _ := NewMemClient("", nil)
	ctx := context.Background()
	err := c.EnsureInstance(ctx, InstanceID{Workload: "w", Index: 0}, "registry/bad", 1)
	f, ok := AsFailure(err)
	if !ok || f.Class != ClassDeterministic {
		t.Fatalf("expected deterministic failure, got %v", err)
	}
	var fe *Failure
	if !errors.As(err, &fe) {
		t.Fatal("failure does not unwrap")
	}
	if left, _ := c.ListInstances(ctx, "w"); len(left) != 0 {
		t.Fatal("failed ensure left state behind")
	}
}

func TestHookFailureLeavesNoState(t *testing.T) {
	c, _ := NewMemClient("", func(string, InstanceID, int64) error {
		return &Failure{Class: ClassTransient, Code: "x", Message: "x"}
	})
	err := c.EnsureInstance(context.Background(), InstanceID{Workload: "w", Index: 0}, "nginx:1", 1)
	if err == nil {
		t.Fatal("expected hook failure")
	}
	if left, _ := c.ListInstances(context.Background(), "w"); len(left) != 0 {
		t.Fatal("hook failure left partial state")
	}
}
