package engine

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/actual"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

func newReaperHarness(t *testing.T) (*Reconciler, *FakeActuator, *storage.MemStore) {
	t.Helper()
	store := storage.NewMemStore()
	act := NewFakeActuatorWithStore(actual.NewMemStore())
	r := NewReconciler(store, act, NewFakeClock(time.Unix(1, 0)), NoBackoff(), NewSliceSink())
	return r, act, store
}

func TestReapOrphanRemovesOnlyOwnerlessResource(t *testing.T) {
	r, act, store := newReaperHarness(t)
	ctx := context.Background()

	// orphan: resource with no object
	if err := act.Actual().Save(ctx, "ghost", &apis.ActualState{ResourceID: "g"}); err != nil {
		t.Fatal(err)
	}
	// owned: object carries finalizer
	if err := act.Actual().Save(ctx, "owned", &apis.ActualState{ResourceID: "o"}); err != nil {
		t.Fatal(err)
	}
	ownedObj := &apis.RawObject{APIVersion: apis.APIVersionV2, Kind: apis.KindWidget,
		Metadata: apis.ObjectMeta{Name: "owned", Generation: 1, Finalizers: []string{apis.FinalizerReconciler}},
		Spec:     []byte(`{"replicas":1}`)}
	if _, _, err := store.Create(ctx, ownedObj); err != nil {
		t.Fatal(err)
	}
	// inconclusive: object exists without finalizer
	if err := act.Actual().Save(ctx, "pending", &apis.ActualState{ResourceID: "p"}); err != nil {
		t.Fatal(err)
	}
	pendingObj := &apis.RawObject{APIVersion: apis.APIVersionV2, Kind: apis.KindWidget,
		Metadata: apis.ObjectMeta{Name: "pending", Generation: 1},
		Spec:     []byte(`{"replicas":1}`)}
	if _, _, err := store.Create(ctx, pendingObj); err != nil {
		t.Fatal(err)
	}

	dec, err := r.ReapOrphan(ctx, "ghost")
	if err != nil || dec != ReapRemoved {
		t.Fatalf("ghost: dec=%d err=%v, want ReapRemoved", dec, err)
	}
	if _, err := act.Actual().Load(ctx, "ghost"); err == nil {
		t.Fatal("orphan resource not removed")
	}

	dec, err = r.ReapOrphan(ctx, "owned")
	if err != nil || dec != ReapKept {
		t.Fatalf("owned: dec=%d err=%v, want ReapKept", dec, err)
	}
	if _, err := act.Actual().Load(ctx, "owned"); err != nil {
		t.Fatal("owned resource wrongly removed")
	}

	dec, err = r.ReapOrphan(ctx, "pending")
	if err != nil || dec != ReapEnqueued {
		t.Fatalf("pending: dec=%d err=%v, want ReapEnqueued", dec, err)
	}
	if _, err := act.Actual().Load(ctx, "pending"); err != nil {
		t.Fatal("unbooked resource wrongly removed (must defer to reconciliation)")
	}
}

func TestReapOrphanRespectsCancellation(t *testing.T) {
	r, _, _ := newReaperHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.ReapOrphan(ctx, "anything"); err == nil {
		t.Fatal("ReapOrphan ignored cancelled context")
	}
}
