package controller

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/actual"
	"github.com/lechandonga/resource-reconciler-go/pkg/apis"
	"github.com/lechandonga/resource-reconciler-go/pkg/engine"
	"github.com/lechandonga/resource-reconciler-go/pkg/storage"
)

// These tests kill the real test process immediately after a durable
// mutation (RECONCILER_CRASH_POINT), then start a fresh process that
// reopens the same control-plane snapshot and actual directory. They
// prove recovery across the action<->accounting windows with a hard kill.

const (
	envChildMode = "RECOVERY_CHILD_MODE"
	envWorkDir   = "RECOVERY_WORKDIR"
)

// crashChild runs a controller process that dies at the selected point.
func crashChild(t *testing.T, mode, workDir string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashChildProcess", "-test.v")
	cmd.Env = append(os.Environ(),
		envChildMode+"="+mode,
		envWorkDir+"="+workDir,
		engine.EnvCrashPoint+"="+crashPointFor(mode),
		engine.EnvCrashKey+"=crashy",
	)
	out, _ := cmd.CombinedOutput()
	// the child exits 77 when the injected hook fires
	if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != engine.ExitCrashSimulated {
		t.Fatalf("child did not die at crash point (mode=%s):\n%s", mode, out)
	}
}

func crashPointFor(mode string) string {
	switch mode {
	case "create":
		return engine.CrashAfterCreate
	case "update":
		return engine.CrashAfterUpdate
	case "remove":
		return engine.CrashAfterRemove
	}
	return ""
}

func paths(workDir string) (snapPath, actualDir string) {
	return filepath.Join(workDir, "snap.json"), filepath.Join(workDir, "actual")
}

// TestCrashChildProcess is the re-executed binary entry; it never runs in
// the parent.
func TestCrashChildProcess(t *testing.T) {
	mode := os.Getenv(envChildMode)
	if mode == "" {
		return
	}
	workDir := os.Getenv(envWorkDir)
	snapPath, actualDir := paths(workDir)

	ctx := context.Background()
	store := storage.NewMemStore()
	if data, err := os.ReadFile(snapPath); err == nil && len(data) > 0 {
		if err := store.LoadFile(snapPath); err != nil {
			t.Fatal(err)
		}
	}
	ast, err := actual.NewDirStore(ctx, actualDir)
	if err != nil {
		t.Fatal(err)
	}
	act := engine.NewFakeActuatorWithStore(ast)
	act.SetCrashHook(engine.EnvCrashHook())

	c, err := New(Config{
		Store: store, Actuator: act, Clock: engine.RealClock{},
		Backoff: engine.NoBackoff(), Sink: engine.NopSink{},
		QueueCapacity: 32, RecoveryPageSize: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = c.Run(rctx) }()

	switch mode {
	case "create":
		// The snapshot already holds the admitted object WITH finalizer
		// (the durable ownership claim). Wipe the actual resource and clear
		// status.actual, then reconcile: the resource is re-created
		// durably and the process dies right after, exactly in the
		// action<->accounting window.
		if err := ast.Remove(ctx, "crashy"); err != nil {
			t.Fatal(err)
		}
		o, rv, err := store.Get(ctx, "crashy")
		if err != nil {
			t.Fatal(err)
		}
		o.Status.Actual = nil
		if _, _, err := store.Update(ctx, o, rv); err != nil {
			t.Fatal(err)
		}
		if err := c.queue.Add("crashy"); err != nil {
			t.Fatal(err)
		}
	case "update":
		// snapshot already contains a converged gen 1 with finalizer; a
		// changed desired spec persists, the update lands durably, then
		// the process dies before status.actual is rewritten.
		o0, rv0, err := store.Get(ctx, "crashy")
		if err != nil {
			t.Fatal(err)
		}
		o0.Spec = json.RawMessage(`{"replicas":4}`)
		o0.Metadata.Generation = 2
		o0.Status.SpecHash = ""
		if _, _, err := store.Update(ctx, o0, rv0); err != nil {
			t.Fatal(err)
		}
		if err := store.SaveToFile(snapPath); err != nil {
			t.Fatal(err)
		}
		if err := c.queue.Add("crashy"); err != nil {
			t.Fatal(err)
		}
	case "remove":
		// persist the deletion intent (finalizer still present),
		// snapshot; the resource is removed durably then the process dies
		// before actual=nil is recorded.
		o0, rv0, err := store.Get(ctx, "crashy")
		if err != nil {
			t.Fatal(err)
		}
		ts := c.clk.Now()
		o0.Metadata.DeletionTimestamp = &ts
		if _, _, err := store.Update(ctx, o0, rv0); err != nil {
			t.Fatal(err)
		}
		if err := store.SaveToFile(snapPath); err != nil {
			t.Fatal(err)
		}
		if err := c.queue.Add("crashy"); err != nil {
			t.Fatal(err)
		}
	}
	// The crash hook kills the process inside the actuator call; if it
	// somehow returns, wait briefly then fail loudly.
	time.Sleep(500 * time.Millisecond)
	t.Fatal("crash hook did not fire")
}

// recoverChild runs a clean controller to steady state and snapshots.
func runCleanController(t *testing.T, workDir string, apply func(c *Controller)) {
	t.Helper()
	snapPath, actualDir := paths(workDir)
	ctx := context.Background()
	store := storage.NewMemStore()
	if data, err := os.ReadFile(snapPath); err == nil && len(data) > 0 {
		if err := store.LoadFile(snapPath); err != nil {
			t.Fatal(err)
		}
	}
	ast, err := actual.NewDirStore(ctx, actualDir)
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(Config{
		Store: store, Actuator: engine.NewFakeActuatorWithStore(ast),
		Clock: engine.NewFakeClock(time.Unix(1, 0)), Backoff: engine.NoBackoff(),
		Sink: engine.NopSink{}, QueueCapacity: 32, RecoveryPageSize: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { _ = c.Run(cctx); close(done) }()
	if apply != nil {
		apply(c)
	}
	if err := c.AwaitIdle(cctx, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveToFile(snapPath); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-done
}

func loadActual(t *testing.T, workDir, key string) *apis.ActualState {
	t.Helper()
	_, actualDir := paths(workDir)
	ast, err := actual.NewDirStore(context.Background(), actualDir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ast.Load(context.Background(), key)
	if errors.Is(err, actual.ErrNotFound) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func loadStatus(t *testing.T, workDir, key string) (*apis.RawObject, bool) {
	t.Helper()
	snapPath, _ := paths(workDir)
	store := storage.NewMemStore()
	if err := store.LoadFile(snapPath); err != nil {
		t.Fatal(err)
	}
	o, _, err := store.Get(context.Background(), key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, false
	}
	return o, true
}

func dumpJSON(t *testing.T, v any) {
	b, _ := json.Marshal(v)
	t.Log(string(b))
}

func TestCrashBetweenCreateAndRecordConverges(t *testing.T) {
	workDir := t.TempDir()
	// pre-seed a converged object: snapshot carries finalizer + actual.
	runCleanController(t, workDir, func(c *Controller) {
		if err := c.Apply(context.Background(), obj(apis.APIVersionV2, "crashy", `{"replicas":1}`)); err != nil {
			t.Fatal(err)
		}
	})
	crashChild(t, "create", workDir)

	// The resource must already exist durably even though accounting died.
	before := loadActual(t, workDir, "crashy")
	if before == nil {
		t.Fatal("resource missing at crash: durable create did not land")
	}

	runCleanController(t, workDir, nil)

	after := loadActual(t, workDir, "crashy")
	if after == nil {
		t.Fatal("valid unbooked resource was deleted during recovery")
	}
	if after.ResourceID != before.ResourceID {
		t.Fatalf("identity changed after recovery: %q -> %q", before.ResourceID, after.ResourceID)
	}
	o, ok := loadStatus(t, workDir, "crashy")
	if !ok || o.Status == nil || o.Status.Actual == nil {
		t.Fatal("accounting not reconciled after restart")
	}
	if o.Status.Actual.ResourceID != after.ResourceID {
		t.Fatalf("recorded identity %q != actual %q", o.Status.Actual.ResourceID, after.ResourceID)
	}
	dumpJSON(t, o.Status)
}

func TestCrashBetweenUpdateAndRecordConverges(t *testing.T) {
	workDir := t.TempDir()
	// establish gen 1 cleanly and snapshot
	runCleanController(t, workDir, func(c *Controller) {
		if err := c.Apply(context.Background(), obj(apis.APIVersionV2, "crashy", `{"replicas":1}`)); err != nil {
			t.Fatal(err)
		}
	})
	idBefore := loadActual(t, workDir, "crashy").ResourceID

	// crash after the update landed but before it was recorded
	crashChild(t, "update", workDir)
	mid := loadActual(t, workDir, "crashy")
	if mid == nil || mid.Replicas != 4 {
		t.Fatalf("update did not land durably: %+v", mid)
	}

	runCleanController(t, workDir, nil)
	after := loadActual(t, workDir, "crashy")
	if after == nil {
		t.Fatal("resource lost after update-window crash")
	}
	if after.ResourceID != idBefore {
		t.Fatalf("identity changed through update: %q -> %q", idBefore, after.ResourceID)
	}
	if after.Replicas != 4 {
		t.Fatalf("content wrong after recovery: %+v", after)
	}
	o, _ := loadStatus(t, workDir, "crashy")
	if o.Status.Actual.Replicas != 4 {
		t.Fatalf("accounting stale: %+v", o.Status.Actual)
	}
}

func TestCrashBetweenRemoveAndRecordConverges(t *testing.T) {
	workDir := t.TempDir()
	runCleanController(t, workDir, func(c *Controller) {
		if err := c.Apply(context.Background(), obj(apis.APIVersionV2, "crashy", `{"replicas":1}`)); err != nil {
			t.Fatal(err)
		}
	})

	crashChild(t, "remove", workDir)
	// durable resource already gone; snapshot still records it
	if got := loadActual(t, workDir, "crashy"); got != nil {
		t.Fatalf("resource not durably removed at crash: %+v", got)
	}

	runCleanController(t, workDir, nil)
	if got := loadActual(t, workDir, "crashy"); got != nil {
		t.Fatalf("resource resurrected after recovery: %+v", got)
	}
	if _, ok := loadStatus(t, workDir, "crashy"); ok {
		t.Fatal("object not fully removed after recovery")
	}
}

func TestRepeatedCrashesStayIdempotent(t *testing.T) {
	workDir := t.TempDir()
	runCleanController(t, workDir, func(c *Controller) {
		if err := c.Apply(context.Background(), obj(apis.APIVersionV2, "crashy", `{"replicas":1}`)); err != nil {
			t.Fatal(err)
		}
	})
	id := loadActual(t, workDir, "crashy").ResourceID

	// repeated crashy restarts must never fork the identity nor duplicate
	for i := 0; i < 3; i++ {
		crashChild(t, "create", workDir)
		if cur := loadActual(t, workDir, "crashy"); cur == nil || cur.ResourceID != id {
			t.Fatalf("identity forked across repeated crashes: got %+v want id=%s", cur, id)
		}
	}
	runCleanController(t, workDir, nil)
	o, ok := loadStatus(t, workDir, "crashy")
	if !ok || o.Status == nil || o.Status.Actual == nil || o.Status.Actual.ResourceID != id {
		t.Fatalf("final accounting wrong: %+v", o)
	}
}
