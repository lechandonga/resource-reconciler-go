package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

type testEnv struct {
	store *storage.MemStore
	act   *FakeActuator
	rec   *Reconciler
	clk   *FakeClock
	sink  *SliceSink
}

func newEnv(t *testing.T, bo BackoffPolicy) *testEnv {
	t.Helper()
	st := storage.NewMemStore()
	act := NewFakeActuator()
	clk := NewFakeClock(time.Unix(1000, 0))
	sink := NewSliceSink()
	rec := NewReconciler(st, act, clk, bo, sink).WithJitter(nil)
	return &testEnv{store: st, act: act, rec: rec, clk: clk, sink: sink}
}

func widget(version, name, specJSON string, gen int64) *apis.RawObject {
	return &apis.RawObject{
		APIVersion: version,
		Kind:       apis.KindWidget,
		Metadata:   apis.ObjectMeta{Name: name, Generation: gen},
		Spec:       json.RawMessage(specJSON),
	}
}

func drive(t *testing.T, env *testEnv, key string, maxHops int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < maxHops; i++ {
		res, err := env.rec.Reconcile(ctx, key)
		if err != nil {
			t.Fatalf("hop %d: %v", i, err)
		}
		switch res.Reason {
		case "", apis.ReasonCreated, apis.ReasonUpdated, apis.ReasonUnchanged,
			apis.ReasonAccepted, apis.ReasonDeleted, apis.ReasonReady,
			apis.ReasonDependencyPending, apis.ReasonDeterministicError,
			apis.ReasonRetryBudgetExhausted:
			return // settled, observably blocked, or observably terminal
		case apis.ReasonTransientError, apis.ReasonConflict:
			// a delayed/immediate retry was requested
			if res.RequeueAfter > 0 {
				env.clk.Advance(res.RequeueAfter + 1)
			}
			continue
		default:
			if res.RequeueAfter > 0 {
				env.clk.Advance(res.RequeueAfter + 1)
				continue
			}
			return
		}
	}
	t.Fatalf("object %q did not settle within %d hops", key, maxHops)
}

func condStatus(obj *apis.RawObject, typ string) string {
	c := getCond(obj, typ)
	if c == nil {
		return "<missing>"
	}
	return c.Status + "/" + c.Reason
}

func TestReconcileCreatesThenIdempotent(t *testing.T) {
	env := newEnv(t, NoBackoff())
	ctx := context.Background()
	obj := widget(apis.APIVersionV2, "a", `{"replicas":2}`, 1)
	if _, _, err := env.store.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	drive(t, env, "a", 10)
	if n := env.act.SideEffectCount("a"); n != 1 {
		t.Fatalf("side effects = %d, want 1", n)
	}
	got, rv, _ := env.store.Get(ctx, "a")
	if c := condStatus(got, apis.CondReady); c != "True/"+apis.ReasonReady {
		t.Fatalf("ready = %s", c)
	}
	if got.Status.Actual == nil || got.Status.Actual.Replicas != 2 {
		t.Fatalf("actual not persisted: %+v", got.Status.Actual)
	}

	// second reconcile on the same state: no actuator call, no rewrite
	drive(t, env, "a", 10)
	if n := env.act.SideEffectCount("a"); n != 1 {
		t.Fatalf("side effects grew to %d on repeat reconcile", n)
	}
	got2, rv2, _ := env.store.Get(ctx, "a")
	if rv2 != rv {
		t.Fatalf("resource version changed without a state change: %d -> %d", rv, rv2)
	}
	_ = got2
}

func TestReconcileCorrectsDrift(t *testing.T) {
	env := newEnv(t, NoBackoff())
	ctx := context.Background()
	o1, _, _ := env.store.Create(ctx, widget(apis.APIVersionV2, "a", `{"replicas":1}`, 1))
	drive(t, env, "a", 10)

	// mutate desired state (new generation); actual must converge
	o1, rv, _ := env.store.Get(ctx, "a")
	o1.Spec = json.RawMessage(`{"replicas":5}`)
	o1.Metadata.Generation = 2
	if _, _, err := env.store.Update(ctx, o1, rv); err != nil {
		t.Fatal(err)
	}
	drive(t, env, "a", 10)
	got, _, _ := env.store.Get(ctx, "a")
	if got.Status.Actual.Replicas != 5 {
		t.Fatalf("drift not corrected: %+v", got.Status.Actual)
	}
	if n := env.act.SideEffectCount("a"); n != 2 {
		t.Fatalf("side effects = %d, want 2 (create+update)", n)
	}
}

func TestDependencyOrderingAndDeleteGuard(t *testing.T) {
	env := newEnv(t, NoBackoff())
	env.rec.WithDependencyQueue(nil) // drive manually for determinism
	ctx := context.Background()

	// b depends on a
	_, _, _ = env.store.Create(ctx, widget(apis.APIVersionV2, "a", `{"replicas":1}`, 1))
	_, _, _ = env.store.Create(ctx, widget(apis.APIVersionV2, "b", `{"replicas":1,"dependsOn":["a"]}`, 1))

	// b first: blocked, no actual resource
	drive(t, env, "b", 3)
	if env.act.SnapshotActual("b") != nil {
		t.Fatal("b created while dependency not ready")
	}
	gb, _, _ := env.store.Get(ctx, "b")
	if c := condStatus(gb, apis.CondReady); c != "False/"+apis.ReasonDependencyPending {
		t.Fatalf("b ready = %s, want blocked", c)
	}

	// now a converges; then b must be revisited
	drive(t, env, "a", 10)
	drive(t, env, "b", 10)
	if env.act.SnapshotActual("b") == nil {
		t.Fatal("b not created after dependency became ready")
	}
	gb, _, _ = env.store.Get(ctx, "b")
	if c := condStatus(gb, apis.CondReady); c != "True/"+apis.ReasonReady {
		t.Fatalf("b ready after unblock = %s", c)
	}

	// attempt to delete a while b still lives: must be blocked
	ga, rv, _ := env.store.Get(ctx, "a")
	ts := time.Unix(2000, 0)
	ga.Metadata.DeletionTimestamp = &ts
	if _, _, err := env.store.Update(ctx, ga, rv); err != nil {
		t.Fatal(err)
	}
	drive(t, env, "a", 3)
	if env.act.SnapshotActual("a") == nil {
		t.Fatal("a removed while dependent b still exists")
	}
	ga2, _, _ := env.store.Get(ctx, "a")
	if c := condStatus(ga2, apis.CondReady); c != "False/"+apis.ReasonDependencyPending {
		t.Fatalf("deleting a ready = %s, want blocked", c)
	}

	// delete b first, then a: ordered teardown
	gb, rv, _ = env.store.Get(ctx, "b")
	gb.Metadata.DeletionTimestamp = &ts
	if _, _, err := env.store.Update(ctx, gb, rv); err != nil {
		t.Fatal(err)
	}
	drive(t, env, "b", 10)
	if _, _, err := env.store.Get(ctx, "b"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("b should be fully deleted")
	}
	if env.act.SnapshotActual("b") != nil {
		t.Fatal("b actual resource leaked")
	}
	drive(t, env, "a", 10)
	if _, _, err := env.store.Get(ctx, "a"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal("a should be deleted after b")
	}
}

func TestTransientFailureBackoffThenConverge(t *testing.T) {
	env := newEnv(t, NoBackoff())
	ctx := context.Background()
	_, _, _ = env.store.Create(ctx, widget(apis.APIVersionV2, "a", `{"replicas":1}`, 1))
	env.act.SetRule("a", FailureRule{Kind: FailTransient, Times: 2})

	drive(t, env, "a", 10)
	got, _, _ := env.store.Get(ctx, "a")
	if got.Status.Actual == nil {
		t.Fatal("did not converge after transient budget")
	}
	if got.Status.Attempts != 0 {
		t.Fatalf("attempts not reset on success: %d", got.Status.Attempts)
	}
	// transient decisions observable in order
	reasons := env.sink.Reasons()
	sawTransient, sawCreated := false, false
	for _, r := range reasons {
		if r == apis.ReasonTransientError {
			sawTransient = true
		}
		if r == apis.ReasonCreated {
			sawCreated = true
		}
	}
	if !sawTransient || !sawCreated {
		t.Fatalf("expected transient then created, got %v", reasons)
	}
}

func TestDeterministicFailureIsTerminalObservable(t *testing.T) {
	env := newEnv(t, NoBackoff())
	ctx := context.Background()
	_, _, _ = env.store.Create(ctx, widget(apis.APIVersionV2, "a", `{"replicas":1}`, 1))
	env.act.SetRule("a", FailureRule{Kind: FailDeterministic})

	// one pass records terminal; repeated passes must not call backend again
	res, err := env.rec.Reconcile(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter != 0 || res.Reason == apis.ReasonTransientError {
		t.Fatalf("deterministic failure requeued: %+v", res)
	}
	callsAfterFirst := len(env.act.Ops())
	for i := 0; i < 5; i++ {
		if _, err := env.rec.Reconcile(ctx, "a"); err != nil {
			t.Fatal(err)
		}
	}
	if len(env.act.Ops()) != callsAfterFirst {
		t.Fatalf("deterministic failure kept hitting the backend: %d -> %d", callsAfterFirst, len(env.act.Ops()))
	}
	got, _, _ := env.store.Get(ctx, "a")
	if got.Status.FatalReason != apis.ReasonDeterministicError {
		t.Fatalf("fatal reason = %q", got.Status.FatalReason)
	}
	if c := condStatus(got, apis.CondReady); c != "False/"+apis.ReasonDeterministicError {
		t.Fatalf("ready = %s", c)
	}
}

func TestAdmissionRejectedWithoutSideEffect(t *testing.T) {
	env := newEnv(t, NoBackoff())
	ctx := context.Background()
	_, _, _ = env.store.Create(ctx, widget(apis.APIVersionV2, "a", `{"replicas":999}`, 1))
	if _, err := env.rec.Reconcile(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if env.act.SnapshotActual("a") != nil {
		t.Fatal("side effect happened for rejected spec")
	}
	got, _, _ := env.store.Get(ctx, "a")
	if got.Status.FatalReason != apis.ReasonReplicasOutOfRange {
		t.Fatalf("fatal = %q, want %s", got.Status.FatalReason, apis.ReasonReplicasOutOfRange)
	}
	if hasFinalizer(got) {
		t.Fatal("rejected object must not carry the finalizer")
	}
}

func TestSpecChangeResetsOldFailure(t *testing.T) {
	env := newEnv(t, NoBackoff())
	ctx := context.Background()
	o, _, _ := env.store.Create(ctx, widget(apis.APIVersionV2, "a", `{"replicas":1}`, 1))
	env.act.SetRule("a", FailureRule{Kind: FailDeterministic})
	drive(t, env, "a", 3)
	env.act.SetRule("a", FailureRule{Kind: FailNone})

	// unchanged generation: stays terminal
	drive(t, env, "a", 3)
	if env.act.SnapshotActual("a") != nil {
		t.Fatal("terminal state should persist for same generation")
	}
	// new generation retries and converges
	o, rv, _ := env.store.Get(ctx, "a")
	o.Spec = json.RawMessage(`{"replicas":2}`)
	o.Metadata.Generation = 2
	if _, _, err := env.store.Update(ctx, o, rv); err != nil {
		t.Fatal(err)
	}
	drive(t, env, "a", 10)
	got, _, _ := env.store.Get(ctx, "a")
	if got.Status.Actual == nil || got.Status.FatalReason != "" {
		t.Fatalf("new generation did not clear failure: %+v", got.Status)
	}
}

func TestTransientBudgetExhaustionBecomesTerminal(t *testing.T) {
	bo := NoBackoff()
	bo.MaxAttempts = 3 // up to 3 retries; the 4th failure is terminal
	env := newEnv(t, bo)
	ctx := context.Background()
	_, _, _ = env.store.Create(ctx, widget(apis.APIVersionV2, "a", `{"replicas":1}`, 1))
	// transient forever: must give up after the bounded budget
	env.act.SetRule("a", FailureRule{Kind: FailTransient, Times: 100000})

	var last Result
	for i := 0; i < 10; i++ {
		res, err := env.rec.Reconcile(ctx, "a")
		if err != nil {
			t.Fatal(err)
		}
		last = res
		if res.Reason == apis.ReasonTransientError {
			env.clk.Advance(2 * time.Millisecond)
			continue
		}
		break
	}
	if last.Reason == apis.ReasonTransientError {
		t.Fatalf("still backing off after budget: %+v", last)
	}
	got, _, _ := env.store.Get(ctx, "a")
	if got.Status.FatalReason != apis.ReasonRetryBudgetExhausted {
		t.Fatalf("fatal = %q, want %s", got.Status.FatalReason, apis.ReasonRetryBudgetExhausted)
	}
	c := getCond(got, apis.CondReady)
	if c == nil || c.Status != "False" || c.Reason != apis.ReasonRetryBudgetExhausted {
		t.Fatalf("ready cond = %+v", c)
	}
	// after exhaustion, extra passes neither call backend nor rewrite
	rvBefore := mustRV(t, env, "a")
	opsBefore := len(env.act.Ops())
	for i := 0; i < 3; i++ {
		if _, err := env.rec.Reconcile(ctx, "a"); err != nil {
			t.Fatal(err)
		}
	}
	if len(env.act.Ops()) != opsBefore {
		t.Fatal("backend called after retry budget exhausted")
	}
	if rv := mustRV(t, env, "a"); rv != rvBefore {
		t.Fatalf("rv churned while terminal: %d -> %d", rvBefore, rv)
	}
}

func mustRV(t *testing.T, env *testEnv, key string) int64 {
	t.Helper()
	_, rv, err := env.store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return rv
}
