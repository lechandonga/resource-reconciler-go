package engine

import (
	"context"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/actual"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
)

// A brand-new actuator instance over the same durable directory is the
// in-process analogue of "process restarted": the in-memory op log starts
// empty but the real resource survives with stable identity.
func TestDurableActuatorSurvivesRestartWithSameIdentity(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	spec := &apis.WidgetSpec{Replicas: 3, Mode: apis.ModeOn, GraceSeconds: 30}

	s1, err := actual.NewDirStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	a1 := NewFakeActuatorWithStore(s1)
	st1, act1, err := a1.Ensure(ctx, "w", spec, nil)
	if err != nil || act1 != ActionCreated {
		t.Fatalf("create: act=%v err=%v", act1, err)
	}

	// restart: fresh actuator, fresh op counters, same durable directory
	s2, err := actual.NewDirStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	a2 := NewFakeActuatorWithStore(s2)
	st2, act2, err := a2.Ensure(ctx, "w", spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	if act2 != ActionUnchanged {
		t.Fatalf("post-restart Ensure = %v, want Unchanged (no repeated side effect)", act2)
	}
	if st2.ResourceID != st1.ResourceID {
		t.Fatalf("identity changed across restart: %q -> %q", st1.ResourceID, st2.ResourceID)
	}
	if n := a2.SideEffectCount("w"); n != 0 {
		t.Fatalf("post-restart side effects = %d, want 0", n)
	}
}

func TestDurableActuatorExternalDriftIsCorrectedWithSameIdentity(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := actual.NewDirStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	a := NewFakeActuatorWithStore(s)
	spec := &apis.WidgetSpec{Replicas: 1, Mode: apis.ModeOn, GraceSeconds: 30}
	st, _, err := a.Ensure(ctx, "w", spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := st.ResourceID

	// Someone mutates the real resource out of band.
	drifted := *st
	drifted.Replicas = 9
	if err := s.Save(ctx, "w", &drifted); err != nil {
		t.Fatal(err)
	}

	// Reconcile after a "restart": fresh actuator, same directory.
	s2, err := actual.NewDirStore(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	a2 := NewFakeActuatorWithStore(s2)
	fixed, act2, err := a2.Ensure(ctx, "w", spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	if act2 != ActionUpdated {
		t.Fatalf("drift correction = %v, want Updated", act2)
	}
	if fixed.ResourceID != id {
		t.Fatalf("identity changed on drift correction: %q -> %q", id, fixed.ResourceID)
	}
	if fixed.Replicas != 1 {
		t.Fatalf("drift not corrected: replicas=%d", fixed.Replicas)
	}
}

func TestDurableActuatorRemoveSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, _ := actual.NewDirStore(ctx, dir)
	a := NewFakeActuatorWithStore(s)
	if _, _, err := a.Ensure(ctx, "w", &apis.WidgetSpec{Replicas: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if deleted, err := a.Remove(ctx, "w", nil); err != nil || !deleted {
		t.Fatalf("remove deleted=%v err=%v", deleted, err)
	}

	// restart: a retried Remove must observe an already-gone resource.
	s2, _ := actual.NewDirStore(ctx, dir)
	a2 := NewFakeActuatorWithStore(s2)
	deleted, err := a2.Remove(ctx, "w", nil)
	if err != nil || deleted {
		t.Fatalf("post-restart Remove deleted=%v err=%v, want no-op", deleted, err)
	}
	if n := a2.SideEffectCount("w"); n != 0 {
		t.Fatalf("post-restart delete side effects = %d, want 0", n)
	}
}

func TestCrashHookFiresBetweenMutationAndReturn(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, _ := actual.NewDirStore(ctx, dir)
	a := NewFakeActuatorWithStore(s)

	fired := ""
	a.SetCrashHook(func(key, point string) { fired = point })

	spec := &apis.WidgetSpec{Replicas: 1}
	if _, act, err := a.Ensure(ctx, "w", spec, nil); err != nil || act != ActionCreated {
		t.Fatalf("ensure act=%v err=%v", act, err)
	}
	if fired != CrashAfterCreate {
		t.Fatalf("hook point = %q, want %q", fired, CrashAfterCreate)
	}
	// despite the "crash", the durable mutation must already be on disk
	if got, err := s.Load(ctx, "w"); err != nil || got == nil {
		t.Fatalf("durable create missing at crash point: %v", err)
	}

	spec2 := &apis.WidgetSpec{Replicas: 2}
	fired = ""
	if _, act, err := a.Ensure(ctx, "w", spec2, nil); err != nil || act != ActionUpdated {
		t.Fatalf("update act=%v err=%v", act, err)
	}
	if fired != CrashAfterUpdate {
		t.Fatalf("hook point = %q, want %q", fired, CrashAfterUpdate)
	}

	fired = ""
	if deleted, err := a.Remove(ctx, "w", nil); err != nil || !deleted {
		t.Fatalf("remove deleted=%v err=%v", deleted, err)
	}
	if fired != CrashAfterRemove {
		t.Fatalf("hook point = %q, want %q", fired, CrashAfterRemove)
	}
}
