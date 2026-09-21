package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
)

func TestEnsureIdempotent(t *testing.T) {
	a := NewFakeActuator()
	ctx := context.Background()
	spec := &apis.WidgetSpec{Replicas: 2, Mode: apis.ModeOn, GraceSeconds: 30}

	st, act, err := a.Ensure(ctx, "w", spec, nil)
	if err != nil || act != ActionCreated {
		t.Fatalf("first: act=%v err=%v", act, err)
	}
	if st.ResourceID == "" {
		t.Fatal("resource id assigned")
	}
	id := st.ResourceID
	for i := 0; i < 5; i++ {
		st2, act2, err := a.Ensure(ctx, "w", spec, st)
		if err != nil || act2 != ActionUnchanged {
			t.Fatalf("repeat %d: act=%v err=%v", i, act2, err)
		}
		if st2.ResourceID != id {
			t.Fatal("stable resource identity changed")
		}
	}
	if n := a.SideEffectCount("w"); n != 1 {
		t.Fatalf("side effects = %d, want exactly 1", n)
	}

	// drift is corrected with exactly one update side effect
	drifted := &apis.WidgetSpec{Replicas: 4, Mode: apis.ModeOn, GraceSeconds: 30}
	_, act3, err := a.Ensure(ctx, "w", drifted, a.SnapshotActual("w"))
	if err != nil || act3 != ActionUpdated {
		t.Fatalf("drift: act=%v err=%v", act3, err)
	}
	_, act4, _ := a.Ensure(ctx, "w", drifted, a.SnapshotActual("w"))
	if act4 != ActionUnchanged {
		t.Fatalf("repeat update: %v", act4)
	}
	if n := a.SideEffectCount("w"); n != 2 {
		t.Fatalf("side effects = %d, want 2 (create+update)", n)
	}
}

func TestRemoveIdempotent(t *testing.T) {
	a := NewFakeActuator()
	ctx := context.Background()
	spec := &apis.WidgetSpec{Replicas: 1, Mode: apis.ModeOff, GraceSeconds: 0}
	_, _, _ = a.Ensure(ctx, "w", spec, nil)

	deleted, err := a.Remove(ctx, "w", a.SnapshotActual("w"))
	if err != nil || !deleted {
		t.Fatalf("remove: deleted=%v err=%v", deleted, err)
	}
	// retried delete after the fact is a no-op
	deleted2, err := a.Remove(ctx, "w", nil)
	if err != nil || deleted2 {
		t.Fatalf("repeat remove: deleted=%v err=%v", deleted2, err)
	}
	if a.SnapshotActual("w") != nil {
		t.Fatal("resource still present")
	}
}

func TestTransientFailureRetriesThenSucceeds(t *testing.T) {
	a := NewFakeActuator()
	a.SetRule("w", FailureRule{Kind: FailTransient, Times: 2})
	ctx := context.Background()
	spec := &apis.WidgetSpec{Replicas: 1}
	for i := 1; i <= 2; i++ {
		_, _, err := a.Ensure(ctx, "w", spec, nil)
		if !errors.Is(err, ErrTransient) {
			t.Fatalf("call %d: err=%v, want transient", i, err)
		}
	}
	_, act, err := a.Ensure(ctx, "w", spec, nil)
	if err != nil || act != ActionCreated {
		t.Fatalf("after transient budget: act=%v err=%v", act, err)
	}
	// subsequent calls stay healthy
	if _, _, err := a.Ensure(ctx, "w", spec, a.SnapshotActual("w")); err != nil {
		t.Fatal(err)
	}
}

func TestDeterministicFailurePersists(t *testing.T) {
	a := NewFakeActuator()
	a.SetRule("w", FailureRule{Kind: FailDeterministic})
	ctx := context.Background()
	spec := &apis.WidgetSpec{Replicas: 1}
	for i := 0; i < 5; i++ {
		_, _, err := a.Ensure(ctx, "w", spec, nil)
		if !errors.Is(err, ErrDeterministic) {
			t.Fatalf("call %d: err=%v, want deterministic", i, err)
		}
	}
	if a.SnapshotActual("w") != nil {
		t.Fatal("deterministic failure must never create the resource")
	}
}

func TestOpsAuditTrail(t *testing.T) {
	a := NewFakeActuator()
	ctx := context.Background()
	s1 := &apis.WidgetSpec{Replicas: 1}
	_, _, _ = a.Ensure(ctx, "w", s1, nil)
	s2 := &apis.WidgetSpec{Replicas: 3}
	_, _, _ = a.Ensure(ctx, "w", s2, a.SnapshotActual("w"))
	_, _, _ = a.Ensure(ctx, "w", s2, a.SnapshotActual("w")) // unchanged, not logged as effect
	_, _ = a.Remove(ctx, "w", a.SnapshotActual("w"))
	_, _ = a.Remove(ctx, "w", nil) // no-op

	ops := a.Ops()
	mutating := []Action{}
	for _, op := range ops {
		if !op.Failed && op.Action != ActionUnchanged {
			mutating = append(mutating, op.Action)
		}
	}
	want := []Action{ActionCreated, ActionUpdated, ActionDeleted}
	if len(mutating) != len(want) {
		t.Fatalf("mutating ops = %v, want %v", mutating, want)
	}
	for i := range want {
		if mutating[i] != want[i] {
			t.Fatalf("op %d = %v, want %v", i, mutating[i], want[i])
		}
	}
}
