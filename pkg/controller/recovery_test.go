package controller

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/engine"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

type durableRig struct {
	dir        string
	storePath  string
	actualPath string
}

func newDurableRig(t *testing.T) *durableRig {
	t.Helper()
	dir := t.TempDir()
	return &durableRig{
		dir:        dir,
		storePath:  filepath.Join(dir, "store.json"),
		actualPath: filepath.Join(dir, "actual.json"),
	}
}

func (r *durableRig) boot(t *testing.T, hook engine.CrashHook, rec RecoveryConfig) (*Controller, *storage.MemStore, *engine.FakeActuator, context.CancelFunc, chan struct{}) {
	t.Helper()
	store := storage.NewMemStore()
	if data, err := os.ReadFile(r.storePath); err == nil {
		if err := store.LoadFile(r.storePath); err != nil {
			t.Fatalf("load store: %v (%s)", err, string(data))
		}
	}
	act, err := engine.NewDurableFakeActuator(r.actualPath)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Config{
		Workers: 4, QueueCapacity: 256, Store: store, Actuator: act,
		Clock: engine.RealClock{}, Backoff: engine.NoBackoff(), Sink: engine.NopSink{},
		CrashHook: hook, Recovery: rec,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	return c, store, act, cancel, done
}

func (r *durableRig) snapshot(t *testing.T, store *storage.MemStore) {
	t.Helper()
	if err := store.SaveToFile(r.storePath); err != nil {
		t.Fatal(err)
	}
}

func recCfg() RecoveryConfig {
	return RecoveryConfig{PageSize: 2, PagePause: 0, Timeout: 5 * time.Second}
}

func waitIdleCtx(t *testing.T, c *Controller, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := c.AwaitIdle(ctx, 2*time.Millisecond); err != nil {
		t.Fatalf("await idle: %v", err)
	}
}

func getReadyReason(o *apis.RawObject) string {
	for _, c := range o.Status.Conditions {
		if c.Type == apis.CondReady {
			return c.Status + "/" + c.Reason
		}
	}
	return "<missing>"
}

func TestRestartKeepsActualIdentityAndNoDuplicateSideEffects(t *testing.T) {
	r := newDurableRig(t)

	c1, store1, act1, cancel1, done1 := r.boot(t, nil, recCfg())
	if err := c1.Apply(context.Background(), obj(apis.APIVersionV2, "a", `{"replicas":3,"mode":"On"}`)); err != nil {
		t.Fatal(err)
	}
	waitIdleCtx(t, c1, time.Second)
	idBefore := act1.SnapshotActual("a").ResourceID
	if act1.SideEffectCount("a") != 1 {
		t.Fatalf("effects before restart = %d, want 1", act1.SideEffectCount("a"))
	}
	r.snapshot(t, store1)
	cancel1()
	<-done1

	for i := 0; i < 3; i++ {
		c2, store2, act2, cancel2, done2 := r.boot(t, nil, recCfg())
		waitIdleCtx(t, c2, time.Second)
		idAfter := act2.SnapshotActual("a").ResourceID
		if idAfter != idBefore {
			t.Fatalf("restart %d changed identity: %s -> %s", i+1, idBefore, idAfter)
		}
		if n := act2.SideEffectCount("a"); n != 0 {
			t.Fatalf("restart %d produced side effects: %d", i+1, n)
		}
		if act2.CountActual() != 1 {
			t.Fatalf("actual count after restart %d = %d", i+1, act2.CountActual())
		}
		got, _, _ := store2.Get(context.Background(), "a")
		if got.Status == nil || got.Status.Actual == nil || got.Status.Actual.ResourceID != idBefore {
			t.Fatalf("recorded identity diverged on restart %d", i+1)
		}
		r.snapshot(t, store2)
		cancel2()
		<-done2
	}
}

func TestCrashBetweenEnsureAndRecordIsRecovered(t *testing.T) {
	r := newDurableRig(t)

	var fired int32
	hook := func(key string, at engine.CrashPoint) bool {
		if key == "a" && at == engine.CrashAfterEnsure {
			if atomic.LoadInt32(&fired) == 0 {
				atomic.StoreInt32(&fired, 1)
			}
			// Keep aborting every pass once the barrier has been armed:
			// the "process" stays crashed until the test takes its
			// snapshot and starts a new lifetime.
			return true
		}
		return false
	}
	c1, store1, act1, cancel1, done1 := r.boot(t, hook, recCfg())
	if err := c1.Apply(context.Background(), obj(apis.APIVersionV2, "a", `{"replicas":2}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if act1.SnapshotActual("a") != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if act1.SnapshotActual("a") == nil {
		t.Fatal("resource was never created")
	}
	crashedID := act1.SnapshotActual("a").ResourceID
	time.Sleep(20 * time.Millisecond)
	got, _, _ := store1.Get(context.Background(), "a")
	if got.Status != nil && got.Status.Actual != nil {
		t.Fatal("control plane unexpectedly recorded actual before crash")
	}
	r.snapshot(t, store1)
	cancel1()
	<-done1

	c2, store2, act2, cancel2, done2 := r.boot(t, nil, recCfg())
	waitIdleCtx(t, c2, 2*time.Second)
	defer func() { cancel2(); <-done2 }()

	if act2.CountActual() != 1 {
		t.Fatalf("resources after recovery = %d, want 1", act2.CountActual())
	}
	if id := act2.SnapshotActual("a").ResourceID; id != crashedID {
		t.Fatalf("identity changed across crash recovery: %s -> %s", crashedID, id)
	}
	if n := act2.SideEffectCount("a"); n != 0 {
		t.Fatalf("recovery created side effects: %d", n)
	}
	o2, _, _ := store2.Get(context.Background(), "a")
	if o2.Status == nil || o2.Status.Actual == nil || o2.Status.Actual.ResourceID != crashedID {
		t.Fatal("adoption was not recorded after recovery")
	}
	if cc := getReadyReason(o2); cc != "True/"+apis.ReasonReady {
		t.Fatalf("not ready after recovery: %s", cc)
	}
}

func TestCrashBetweenRemoveAndRecordLeavesNoOrphan(t *testing.T) {
	r := newDurableRig(t)
	var armed int32
	hook := func(key string, at engine.CrashPoint) bool {
		if key == "d" && at == engine.CrashAfterRemove {
			if atomic.LoadInt32(&armed) == 0 {
				atomic.StoreInt32(&armed, 1)
				return true
			}
		}
		return false
	}
	c3, store3, act3, cancel3, done3 := r.boot(t, hook, recCfg())
	ctx := context.Background()
	if err := c3.Apply(ctx, obj(apis.APIVersionV2, "d", `{"replicas":1}`)); err != nil {
		t.Fatal(err)
	}
	waitIdleCtx(t, c3, time.Second)
	if err := c3.Delete(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if act3.SnapshotActual("d") == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	obj3, _, _ := store3.Get(ctx, "d")
	if obj3 == nil || obj3.Metadata.DeletionTimestamp == nil || obj3.Status == nil || obj3.Status.Actual == nil {
		t.Fatalf("crash state not as expected: %+v", obj3)
	}
	if act3.SnapshotActual("d") != nil {
		t.Fatal("backend removal did not commit before barrier")
	}
	r.snapshot(t, store3)
	cancel3()
	<-done3

	c4, store4, act4, cancel4, done4 := r.boot(t, nil, recCfg())
	waitIdleCtx(t, c4, 2*time.Second)
	defer func() { cancel4(); <-done4 }()
	if act4.CountActual() != 0 {
		t.Fatalf("orphan resource after crash recovery: %d", act4.CountActual())
	}
	if _, _, err := store4.Get(ctx, "d"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("object not fully removed after recovery")
	}
}

func TestStartupReclaimsResidualsButKeepsOwned(t *testing.T) {
	r := newDurableRig(t)
	c1, store1, act1, cancel1, done1 := r.boot(t, nil, recCfg())
	ctx := context.Background()
	if err := c1.Apply(ctx, obj(apis.APIVersionV2, "keep", `{"replicas":1}`)); err != nil {
		t.Fatal(err)
	}
	waitIdleCtx(t, c1, time.Second)
	keepID := act1.SnapshotActual("keep").ResourceID
	if _, err := act1.Hydrate("ghost", apis.ActualState{
		ResourceID: "res-999", Replicas: 1, Mode: apis.ModeOn, Ready: true,
	}); err != nil {
		t.Fatal(err)
	}
	r.snapshot(t, store1)
	cancel1()
	<-done1

	c2, store2, act2, cancel2, done2 := r.boot(t, nil, recCfg())
	waitIdleCtx(t, c2, 2*time.Second)
	defer func() { cancel2(); <-done2 }()

	if act2.SnapshotActual("ghost") != nil {
		t.Fatal("residual resource without owner was not reclaimed")
	}
	if act2.SnapshotActual("keep") == nil || act2.SnapshotActual("keep").ResourceID != keepID {
		t.Fatal("owned resource was incorrectly reclaimed or changed identity")
	}
	if n := act2.SideEffectCount("keep"); n != 0 {
		t.Fatalf("owned resource generated side effects on recovery: %d", n)
	}
	o, _, _ := store2.Get(ctx, "keep")
	if o == nil || o.Status == nil || o.Status.Actual == nil {
		t.Fatal("owned object lost during recovery")
	}
}

func TestDriftIsCorrectedAcrossRestartKeepingIdentity(t *testing.T) {
	r := newDurableRig(t)
	c1, store1, act1, cancel1, done1 := r.boot(t, nil, recCfg())
	ctx := context.Background()
	if err := c1.Apply(ctx, obj(apis.APIVersionV2, "a", `{"replicas":2}`)); err != nil {
		t.Fatal(err)
	}
	waitIdleCtx(t, c1, time.Second)
	id := act1.SnapshotActual("a").ResourceID
	r.snapshot(t, store1)
	cancel1()
	<-done1

	// External actor corrupts the durable actual backend file directly
	// (same identity, wrong content) after the process exited.
	if err := os.WriteFile(r.actualPath, []byte(`{"formatVersion":1,"nextID":2,`+
		`"resources":{"a":{"resourceID":"`+id+`","replicas":9,"mode":"Off","graceSeconds":30,"ready":false}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	c2, _, act2, cancel2, done2 := r.boot(t, nil, recCfg())
	waitIdleCtx(t, c2, 2*time.Second)
	defer func() { cancel2(); <-done2 }()
	got := act2.SnapshotActual("a")
	if got == nil {
		t.Fatal("resource disappeared during drift correction")
	}
	if got.ResourceID != id {
		t.Fatalf("drift correction changed identity: %s -> %s", id, got.ResourceID)
	}
	if got.Replicas != 2 || got.Mode != apis.ModeOn {
		t.Fatalf("drift not corrected: %+v", got)
	}
	if n := act2.SideEffectCount("a"); n != 1 {
		t.Fatalf("expected exactly one corrective update, got %d", n)
	}
}

func pad3(i int) string {
	if i < 10 {
		return "00" + itoa(i)
	}
	if i < 100 {
		return "0" + itoa(i)
	}
	return itoa(i)
}

func TestLegacySnapshotTakenOverInPlace(t *testing.T) {
	// OLD installation: ephemeral actual side, only status.actual was
	// persisted inside the control-plane snapshot.
	r := newDurableRig(t)
	{
		store := storage.NewMemStore()
		act := engine.NewFakeActuator()
		c, err := New(Config{Workers: 2, QueueCapacity: 16, Store: store, Actuator: act,
			Clock: engine.RealClock{}, Backoff: engine.NoBackoff(), Sink: engine.NopSink{}, Recovery: RecoveryConfig{}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = c.Run(ctx); close(done) }()
		if err := c.Apply(ctx, obj(apis.APIVersionV2, "legacy", `{"replicas":4}`)); err != nil {
			t.Fatal(err)
		}
		waitIdleCtx(t, c, time.Second)
		legacyID := act.SnapshotActual("legacy").ResourceID
		if legacyID == "" {
			t.Fatal("old lifetime never created resource")
		}
		if err := store.SaveToFile(r.storePath); err != nil {
			t.Fatal(err)
		}
		cancel()
		<-done
	}

	// New durable lifetime loads the OLD snapshot and takes over in
	// place: identity restored verbatim, zero recreate side effects.
	c2, _, act2, cancel2, done2 := r.boot(t, nil, recCfg())
	waitIdleCtx(t, c2, 2*time.Second)
	defer func() { cancel2(); <-done2 }()
	if n := act2.CountActual(); n != 1 {
		t.Fatalf("migrated actual count = %d, want 1", n)
	}
	if n := act2.SideEffectCount("legacy"); n != 0 {
		t.Fatalf("in-place takeover created side effects: %d", n)
	}
	if st := c2.Stats(); st.Recovery.Migrated != 1 {
		t.Fatalf("migration count = %d, want 1 (stats=%+v)", st.Recovery.Migrated, st.Recovery)
	}
}

func TestRecoveryCancellationReturnsPromptly(t *testing.T) {
	r := newDurableRig(t)
	{
		c1, store1, _, cancel1, done1 := r.boot(t, nil, RecoveryConfig{})
		ctx := context.Background()
		for i := 0; i < 20; i++ {
			if err := c1.Apply(ctx, obj(apis.APIVersionV2, "k-"+pad3(i), `{"replicas":1}`)); err != nil {
				t.Fatal(err)
			}
		}
		waitIdleCtx(t, c1, 3*time.Second)
		r.snapshot(t, store1)
		cancel1()
		<-done1
	}

	store2 := storage.NewMemStore()
	if err := store2.LoadFile(r.storePath); err != nil {
		t.Fatal(err)
	}
	act2, err := engine.NewDurableFakeActuator(r.actualPath)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := New(Config{Workers: 2, QueueCapacity: 256, Store: store2, Actuator: act2,
		Clock: engine.RealClock{}, Backoff: engine.NoBackoff(), Sink: engine.NopSink{},
		Recovery: RecoveryConfig{PageSize: 2, PagePause: 200 * time.Millisecond, Timeout: 30 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c2.Run(ctx) }()
	time.Sleep(40 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return promptly after recovery cancellation")
	}
}

func TestRecoveryTimeoutStopsAndResyncCompletes(t *testing.T) {
	r := newDurableRig(t)
	{
		c1, store1, _, cancel1, done1 := r.boot(t, nil, RecoveryConfig{})
		ctx := context.Background()
		for i := 0; i < 10; i++ {
			if err := c1.Apply(ctx, obj(apis.APIVersionV2, "t-"+pad3(i), `{"replicas":1}`)); err != nil {
				t.Fatal(err)
			}
		}
		waitIdleCtx(t, c1, 3*time.Second)
		r.snapshot(t, store1)
		cancel1()
		<-done1
	}
	store2 := storage.NewMemStore()
	if err := store2.LoadFile(r.storePath); err != nil {
		t.Fatal(err)
	}
	act2, _ := engine.NewDurableFakeActuator(r.actualPath)
	sink := engine.NewSliceSink()
	c2, err := New(Config{Workers: 2, QueueCapacity: 256, Store: store2, Actuator: act2,
		Clock: engine.RealClock{}, Backoff: engine.NoBackoff(), Sink: sink,
		Recovery:     RecoveryConfig{PageSize: 2, PagePause: 30 * time.Millisecond, Timeout: 5 * time.Millisecond},
		ResyncPeriod: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c2.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	var sawTimeout bool
	for time.Now().Before(deadline) {
		for _, d := range sink.Snapshot() {
			if d.Key == "recovery" && d.Reason == apis.ReasonRecoveryTimedOut {
				sawTimeout = true
			}
		}
		if sawTimeout {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !sawTimeout {
		t.Fatal("recovery timeout reason was not recorded")
	}
	// Owned resources survive the aborted sweep and eventually converge.
	waitIdleCtx(t, c2, 2*time.Second)
	if n := act2.CountActual(); n != 10 {
		t.Fatalf("owned resources lost after timed-out recovery: %d", n)
	}
}

func TestWritesDuringRecoveryAreAcceptedAndConverge(t *testing.T) {
	r := newDurableRig(t)
	// Seed a moderate backend so the startup sweep takes a little while.
	{
		c1, store1, _, cancel1, done1 := r.boot(t, nil, RecoveryConfig{})
		ctx := context.Background()
		for i := 0; i < 10; i++ {
			if err := c1.Apply(ctx, obj(apis.APIVersionV2, "w-"+pad3(i), `{"replicas":1}`)); err != nil {
				t.Fatal(err)
			}
		}
		waitIdleCtx(t, c1, 3*time.Second)
		r.snapshot(t, store1)
		cancel1()
		<-done1
	}

	store2 := storage.NewMemStore()
	if err := store2.LoadFile(r.storePath); err != nil {
		t.Fatal(err)
	}
	act2, _ := engine.NewDurableFakeActuator(r.actualPath)
	c2, err := New(Config{Workers: 4, QueueCapacity: 256, Store: store2, Actuator: act2,
		Clock: engine.RealClock{}, Backoff: engine.NoBackoff(), Sink: engine.NopSink{},
		Recovery: RecoveryConfig{PageSize: 2, PagePause: 20 * time.Millisecond, Timeout: 10 * time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c2.Run(ctx) }()

	// Immediately write new desired state while the sweep is in progress.
	// It must be accepted and converge even before the sweep finishes.
	if err := c2.Apply(ctx, obj(apis.APIVersionV2, "new-during-recovery", `{"replicas":7}`)); err != nil {
		t.Fatalf("write during recovery rejected: %v", err)
	}
	waitIdleCtx(t, c2, 3*time.Second)
	got := act2.SnapshotActual("new-during-recovery")
	if got == nil || got.Replicas != 7 {
		t.Fatalf("new desired state during recovery did not converge: %+v", got)
	}
	if n := act2.CountActual(); n != 11 {
		t.Fatalf("actual count = %d, want 11 (10 restarted + 1 new)", n)
	}
}

func TestRepeatedCrashesDuringRecoveryAreIdempotent(t *testing.T) {
	r := newDurableRig(t)

	// Lifetime 1: create, then crash after Ensure with no record.
	var fired int32
	hook := func(key string, at engine.CrashPoint) bool {
		if key == "x" && at == engine.CrashAfterEnsure {
			atomic.AddInt32(&fired, 1)
			return true
		}
		return false
	}
	c1, store1, act1, cancel1, done1 := r.boot(t, hook, recCfg())
	if err := c1.Apply(context.Background(), obj(apis.APIVersionV2, "x", `{"replicas":2}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if act1.SnapshotActual("x") != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	id := act1.SnapshotActual("x").ResourceID
	time.Sleep(20 * time.Millisecond)
	r.snapshot(t, store1)
	cancel1()
	<-done1

	// Lifetime 2 also crashes at the same barrier (re-crash during
	// recovery). Nothing must change on the actual side.
	_, store2, act2, cancel2, done2 := r.boot(t, hook, recCfg())
	time.Sleep(50 * time.Millisecond)
	r.snapshot(t, store2)
	cancel2()
	<-done2
	if n := act2.CountActual(); n != 1 {
		t.Fatalf("actual resources after re-crash = %d, want 1", n)
	}
	if act2.SnapshotActual("x").ResourceID != id {
		t.Fatal("identity changed across re-crash")
	}
	if n := act2.SideEffectCount("x"); n != 0 {
		t.Fatalf("re-crash produced side effects: %d", n)
	}

	// Lifetime 3 recovers cleanly: adopts and converges exactly once.
	c3, store3, act3, cancel3, done3 := r.boot(t, nil, recCfg())
	waitIdleCtx(t, c3, 2*time.Second)
	defer func() { cancel3(); <-done3 }()
	if n := act3.CountActual(); n != 1 {
		t.Fatalf("resources after final recovery = %d", n)
	}
	if act3.SnapshotActual("x").ResourceID != id {
		t.Fatal("identity changed on final recovery")
	}
	if n := act3.SideEffectCount("x"); n != 0 {
		t.Fatalf("final adoption counted a side effect: %d", n)
	}
	o, _, _ := store3.Get(context.Background(), "x")
	if o.Status == nil || o.Status.Actual == nil || o.Status.Actual.ResourceID != id {
		t.Fatal("final control-plane record did not adopt identity")
	}
	if cc := getReadyReason(o); cc != "True/"+apis.ReasonReady {
		t.Fatalf("not ready after repeated-crash recovery: %s", cc)
	}
}
