package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
)

func TestDurableActuatorKeepsIdentityAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "actual.json")
	ctx := context.Background()
	spec := &apis.WidgetSpec{Replicas: 2, Mode: apis.ModeOn, GraceSeconds: 30}

	a1, err := NewDurableFakeActuator(path)
	if err != nil {
		t.Fatal(err)
	}
	st, act, err := a1.Ensure(ctx, "w", spec, nil)
	if err != nil || act != ActionCreated {
		t.Fatalf("first lifetime: act=%v err=%v", act, err)
	}
	id := st.ResourceID
	if a1.SideEffectCount("w") != 1 {
		t.Fatal("first lifetime side effects != 1")
	}

	// Simulate process restart: a brand-new actuator loads the durable
	// actual backend in place.
	a2, err := NewDurableFakeActuator(path)
	if err != nil {
		t.Fatal(err)
	}
	got, act2, err := a2.Ensure(ctx, "w", spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	if act2 != ActionAdopted {
		t.Fatalf("restart action = %v, want Adopted", act2)
	}
	if got.ResourceID != id {
		t.Fatalf("identity changed across restart: %s -> %s", id, got.ResourceID)
	}
	if n := a2.SideEffectCount("w"); n != 0 {
		t.Fatalf("adoption counted a side effect: %d", n)
	}

	// Another restart must be exactly as stable.
	a3, err := NewDurableFakeActuator(path)
	if err != nil {
		t.Fatal(err)
	}
	got3, act3, err := a3.Ensure(ctx, "w", spec, got)
	if err != nil || act3 != ActionUnchanged {
		t.Fatalf("third lifetime: act=%v err=%v", act3, err)
	}
	if got3.ResourceID != id {
		t.Fatalf("identity changed on second restart: %s", got3.ResourceID)
	}
	if a3.CountActual() != 1 {
		t.Fatalf("resource count = %d, want 1", a3.CountActual())
	}
}

func TestDurableActuatorRemovalSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "actual.json")
	ctx := context.Background()
	spec := &apis.WidgetSpec{Replicas: 1}

	a1, _ := NewDurableFakeActuator(path)
	if _, _, err := a1.Ensure(ctx, "w", spec, nil); err != nil {
		t.Fatal(err)
	}
	if removed, err := a1.Remove(ctx, "w", a1.SnapshotActual("w")); err != nil || !removed {
		t.Fatalf("remove: %v %v", removed, err)
	}

	a2, err := NewDurableFakeActuator(path)
	if err != nil {
		t.Fatal(err)
	}
	if a2.SnapshotActual("w") != nil {
		t.Fatal("removed resource reappeared after reload")
	}
	if removed, err := a2.Remove(ctx, "w", nil); err != nil || removed {
		t.Fatalf("idempotent remove after restart: %v %v", removed, err)
	}
}

func TestDurableActuatorActualKeysPaged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "actual.json")
	ctx := context.Background()
	a, _ := NewDurableFakeActuator(path)
	for _, k := range []string{"c", "a", "b"} {
		if _, _, err := a.Ensure(ctx, k, &apis.WidgetSpec{Replicas: 1}, nil); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	cursor := ""
	for {
		page, next, err := a.ActualKeys(ctx, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, page...)
		if next == "" {
			break
		}
		cursor = next
	}
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("keys = %v, want %v", got, want)
		}
	}
}
