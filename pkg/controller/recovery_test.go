package controller

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/actual"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/engine"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

type durableRig struct {
	dir string
	act *engine.FakeActuator
}

func newDurableRig(t *testing.T) *durableRig {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "actual")
	st, err := actual.NewDirStore(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	return &durableRig{dir: dir, act: engine.NewFakeActuatorWithStore(st)}
}

func (d *durableRig) reopen(t *testing.T) *engine.FakeActuator {
	t.Helper()
	st, err := actual.NewDirStore(context.Background(), d.dir)
	if err != nil {
		t.Fatal(err)
	}
	return engine.NewFakeActuatorWithStore(st)
}

func (d *durableRig) load(t *testing.T, key string) *apis.ActualState {
	t.Helper()
	st, err := actual.NewDirStore(context.Background(), d.dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := st.Load(context.Background(), key)
	if err != nil {
		t.Fatalf("load actual %q: %v", key, err)
	}
	return got
}

func (d *durableRig) exists(key string) bool {
	st, err := actual.NewDirStore(context.Background(), d.dir)
	if err != nil {
		return false
	}
	_, err = st.Load(context.Background(), key)
	return err == nil
}

func runCtrl(t *testing.T, c *Controller) (context.CancelFunc, chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	return cancel, done
}

func pad(n int) string { return fmt.Sprintf("%02d", n) }

func newDurableController(t *testing.T, store *storage.MemStore, act *engine.FakeActuator, sink engine.DecisionSink, page int) *Controller {
	t.Helper()
	c, err := New(Config{
		Store: store, Actuator: act, Clock: engine.NewFakeClock(time.Unix(1, 0)),
		Backoff: engine.NoBackoff(), Sink: sink, QueueCapacity: 128,
		RecoveryPageSize: page, RecoveryTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestRestartKeepsActualIdentityAndNoDuplicateSideEffects(t *testing.T) {
	snapPath := filepath.Join(t.TempDir(), "snap.json")
	dr := newDurableRig(t)

	{
		store := storage.NewMemStore()
		c := newDurableController(t, store, dr.act, engine.NopSink{}, 2)
		cancel, done := runCtrl(t, c)
		if err := c.Apply(context.Background(), obj(apis.APIVersionV2, "a", `{"replicas":2}`)); err != nil {
			t.Fatal(err)
		}
		if err := c.AwaitIdle(context.Background(), time.Second); err != nil {
			t.Fatal(err)
		}
		if n := dr.act.SideEffectCount("a"); n != 1 {
			t.Fatalf("effects before restart = %d, want 1", n)
		}
		if err := store.SaveToFile(snapPath); err != nil {
			t.Fatal(err)
		}
		cancel()
		<-done
	}

	{
		store := storage.NewMemStore()
		if err := store.LoadFile(snapPath); err != nil {
			t.Fatal(err)
		}
		act2 := dr.reopen(t)
		c := newDurableController(t, store, act2, engine.NopSink{}, 2)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = c.Run(ctx); close(done) }()
		if err := c.AwaitIdle(ctx, time.Second); err != nil {
			t.Fatal(err)
		}
		cancel()
		<-done

		if n := act2.SideEffectCount("a"); n != 0 {
			t.Fatalf("post-restart side effects = %d, want 0", n)
		}
		got, _, _ := store.Get(ctx, "a")
		if got.Status == nil || got.Status.Actual == nil {
			t.Fatal("actual not recorded after restart")
		}
		if got.Status.Actual.ResourceID != dr.load(t, "a").ResourceID {
			t.Fatalf("identity differs: status=%q disk=%q",
				got.Status.Actual.ResourceID, dr.load(t, "a").ResourceID)
		}
		if got.Status.Actual.Replicas != 2 {
			t.Fatalf("content changed across restart: %+v", got.Status.Actual)
		}
	}
}

func TestRestartCorrectsManuallyBrokenActualResource(t *testing.T) {
	snapPath := filepath.Join(t.TempDir(), "snap.json")
	dr := newDurableRig(t)

	{
		store := storage.NewMemStore()
		c := newDurableController(t, store, dr.act, engine.NopSink{}, 8)
		cancel, done := runCtrl(t, c)
		if err := c.Apply(context.Background(), obj(apis.APIVersionV2, "a", `{"replicas":2}`)); err != nil {
			t.Fatal(err)
		}
		_ = c.AwaitIdle(context.Background(), time.Second)
		_ = store.SaveToFile(snapPath)
		cancel()
		<-done
	}

	// external tampering: wrong content AND a brand-new identity
	st, _ := actual.NewDirStore(context.Background(), dr.dir)
	broken := dr.load(t, "a")
	broken.Replicas = 9
	broken.ResourceID = "res-totally-different"
	if err := st.Save(context.Background(), "a", broken); err != nil {
		t.Fatal(err)
	}

	{
		store := storage.NewMemStore()
		_ = store.LoadFile(snapPath)
		act2 := dr.reopen(t)
		c := newDurableController(t, store, act2, engine.NopSink{}, 8)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = c.Run(ctx); close(done) }()
		_ = c.AwaitIdle(ctx, time.Second)
		cancel()
		<-done

		fixed := dr.load(t, "a")
		if fixed.Replicas != 2 {
			t.Fatalf("drift not corrected: replicas=%d", fixed.Replicas)
		}
		o, _, _ := store.Get(ctx, "a")
		if o.Status.Actual.ResourceID != fixed.ResourceID {
			t.Fatalf("status identity %q != disk %q", o.Status.Actual.ResourceID, fixed.ResourceID)
		}
	}
}

func TestStartupReclaimsOrphanButKeepsUnbookedValidResource(t *testing.T) {
	dr := newDurableRig(t)
	store := storage.NewMemStore()
	sink := engine.NewSliceSink()

	// definite orphan: durable actual resource, no owner object
	orphan := &apis.ActualState{ResourceID: "res-ghost", Replicas: 1, Mode: apis.ModeOn, Ready: true}
	if err := dr.act.Actual().Save(context.Background(), "ghost", orphan); err != nil {
		t.Fatal(err)
	}

	c := newDurableController(t, store, dr.act, sink, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()

	// an object that is admitted (finalizer) must never have its resource
	// deleted by the audit even if accounting lags.
	if err := c.Apply(ctx, obj(apis.APIVersionV2, "late", `{"replicas":1}`)); err != nil {
		t.Fatal(err)
	}
	_ = c.AwaitIdle(ctx, time.Second)
	// a periodic audit (same path as the resync tick) now observes the
	// owned resource alongside the orphan.
	c.reconcileOnce(ctx, "resync")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && dr.exists("ghost") {
		time.Sleep(2 * time.Millisecond)
	}
	if dr.exists("ghost") {
		t.Fatal("definite orphan was not reclaimed")
	}
	if !dr.exists("late") {
		t.Fatal("valid owned resource was wrongly deleted as an orphan")
	}
	cancel()
	<-done

	var sawOrphan, sawKept bool
	for _, d := range sink.Snapshot() {
		if d.Reason == apis.ReasonOrphanReclaimed {
			sawOrphan = true
		}
		if d.Reason == apis.ReasonActualKept && d.Key == "late" {
			sawKept = true
		}
	}
	if !sawOrphan {
		t.Fatal("no auditable OrphanReclaimed decision")
	}
	if !sawKept {
		t.Fatal("no auditable ActualKept decision for the owned resource")
	}
}

func TestRecoveryAcceptsConcurrentWritesAndConverges(t *testing.T) {
	dr := newDurableRig(t)
	store := storage.NewMemStore()
	c := newDurableController(t, store, dr.act, engine.NopSink{}, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_ = c.Apply(ctx, obj(apis.APIVersionV2, "obj-"+pad(n), `{"replicas":1}`))
		}(i)
	}
	wg.Wait()
	if err := c.AwaitIdle(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	list, _ := store.List(ctx)
	if len(list) != 30 {
		t.Fatalf("objects = %d, want 30", len(list))
	}
	for _, o := range list {
		if o.Status == nil || o.Status.Actual == nil {
			t.Fatalf("%s did not converge", o.Metadata.Name)
		}
		if !dr.exists(o.Metadata.Name) {
			t.Fatalf("%s has no durable actual resource", o.Metadata.Name)
		}
	}
	cancel()
	<-done
}

func TestRecoveryCancellationIsObservableAndPrompt(t *testing.T) {
	dr := newDurableRig(t)
	sink := engine.NewSliceSink()
	c := newDurableController(t, storage.NewMemStore(), dr.act, sink, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return promptly after cancel")
	}
	var found bool
	for _, d := range sink.Snapshot() {
		if d.Reason == apis.ReasonRecoveryCancelled {
			found = true
		}
	}
	if !found {
		t.Fatal("no RecoveryCancelled record")
	}
}

func TestRecoveryTimeoutIsObservable(t *testing.T) {
	dr := newDurableRig(t)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		k := "x" + pad(i) + "-zz"
		if err := dr.act.Actual().Save(ctx, k, &apis.ActualState{ResourceID: "r", Replicas: 1}); err != nil {
			t.Fatal(err)
		}
	}
	sink := engine.NewSliceSink()
	c, err := New(Config{
		Store: storage.NewMemStore(), Actuator: dr.act,
		Clock: engine.NewFakeClock(time.Unix(1, 0)), Sink: sink,
		RecoveryPageSize: 1, RecoveryTimeout: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	rctx, rcancel := context.WithCancel(context.Background())
	c.reconcileOnce(rctx, "startup")
	rcancel()

	var sawTimeout bool
	for _, d := range sink.Snapshot() {
		if d.Reason == apis.ReasonRecoveryTimedOut {
			sawTimeout = true
		}
	}
	if !sawTimeout {
		t.Fatal("no RecoveryTimedOut record after deadline exceeded")
	}
}

func TestLegacySnapshotAdoptedWithoutWipe(t *testing.T) {
	snapPath := filepath.Join(t.TempDir(), "snap.json")
	dr := newDurableRig(t)

	// lifetime 1 with the OLD in-memory backend semantics: persist control
	// plane carrying a previously recorded resource id.
	var legacyID string
	{
		store := storage.NewMemStore()
		oldAct := engine.NewFakeActuator()
		c := newDurableController(t, store, oldAct, engine.NopSink{}, 4)
		cancel, done := runCtrl(t, c)
		if err := c.Apply(context.Background(), obj(apis.APIVersionV1, "legacy", `{"replicas":1,"mode":"Off"}`)); err != nil {
			t.Fatal(err)
		}
		_ = c.AwaitIdle(context.Background(), time.Second)
		o, _, _ := store.Get(context.Background(), "legacy")
		legacyID = o.Status.Actual.ResourceID
		// place the actual resource into the durable directory the new
		// binary will use (migration on the spot), preserving identity.
		if err := dr.act.Actual().Save(context.Background(), "legacy", o.Status.Actual); err != nil {
			t.Fatal(err)
		}
		if err := store.SaveToFile(snapPath); err != nil {
			t.Fatal(err)
		}
		cancel()
		<-done
	}

	// new binary takes over the existing snapshot in place
	{
		store := storage.NewMemStore()
		if err := store.LoadFile(snapPath); err != nil {
			t.Fatal(err)
		}
		act2 := dr.reopen(t)
		c := newDurableController(t, store, act2, engine.NopSink{}, 4)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = c.Run(ctx); close(done) }()
		_ = c.AwaitIdle(ctx, time.Second)
		cancel()
		<-done

		if n := act2.SideEffectCount("legacy"); n != 0 {
			t.Fatalf("takeover produced %d side effects, want 0", n)
		}
		o, _, _ := store.Get(ctx, "legacy")
		if o.Status.Actual.ResourceID != legacyID {
			t.Fatalf("identity changed on takeover: %q -> %q", legacyID, o.Status.Actual.ResourceID)
		}
		if o.Status.Actual.GraceSeconds != 30 {
			t.Fatalf("v1 default lost on takeover: grace=%d", o.Status.Actual.GraceSeconds)
		}
	}
}
