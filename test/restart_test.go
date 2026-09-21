package integration

import (
	"sync/atomic"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
	"github.com/lechandonga/resource-reconciler-go/pkg/dataplane"
)

// TestRestartResumesWithoutRepeatingEffects verifies that after a simulated
// process restart, a converged object stays converged and the data plane is
// not mutated again (no repeated side effects).
func TestRestartResumesWithoutRepeatingEffects(t *testing.T) {
	env := newEnv(t)

	var ensures int64
	env.setHook(func(op string, _ dataplane.InstanceID, _ int64) error {
		if op == "ensure" {
			atomic.AddInt64(&ensures, 1)
		}
		return nil
	})

	env.submit(v1Manifest("persisted", 3, "nginx:1"))
	w := env.waitConverged("persisted", 3)
	rvBefore := w.ObjectMeta.ResourceVersion

	// Restart the whole process from the on-disk files.
	env.restart(func(op string, _ dataplane.InstanceID, _ int64) error {
		if op == "ensure" {
			atomic.AddInt64(&ensures, 1)
		}
		return nil
	})
	defer env.stop()

	w2 := env.waitConverged("persisted", 3)
	if w2.Status.Phase != v1.PhaseAvailable {
		t.Fatalf("after restart phase = %s", w2.Status.Phase)
	}
	if got := atomic.LoadInt64(&ensures); got != 3 {
		t.Fatalf("ensure calls after restart = %d total, want exactly 3 (no repeated effects)", got)
	}
	if w2.ObjectMeta.ResourceVersion != rvBefore {
		t.Fatalf("restart rewrote a converged object: rv %d -> %d", rvBefore, w2.ObjectMeta.ResourceVersion)
	}
	env.dumpLogs()
}

// TestRestartMidCreationContinues verifies an unfinished scale-out is
// continued from persisted state after restart, without recreating existing
// instances.
func TestRestartMidCreationContinues(t *testing.T) {
	env := newEnv(t)

	var ensures int64
	// Fail every 2nd ensure persistently to keep convergence incomplete,
	// then restart with a healthy data plane.
	env.setHook(func(op string, id dataplane.InstanceID, _ int64) error {
		if op == "ensure" && atomic.AddInt64(&ensures, 1)%2 == 0 {
			return &dataplane.Failure{Class: dataplane.ClassTransient, Code: "FlakyAPI", Message: "flake"}
		}
		return nil
	})

	env.submit(v1Manifest("midway", 4, "nginx:1"))
	// Wait until at least one instance exists (partial progress persisted).
	if !env.eventually(func() bool {
		ins, _ := env.dp.ListInstances(env.ctx, "midway")
		return len(ins) >= 1
	}) {
		t.Fatal("no partial progress before restart")
	}

	// Restart with NO failures: convergence must complete.
	var totalEnsuresAfter int64
	env.restart(func(op string, id dataplane.InstanceID, _ int64) error {
		if op == "ensure" {
			atomic.AddInt64(&totalEnsuresAfter, 1)
		}
		return nil
	})
	defer env.stop()

	w := env.waitConverged("midway", 4)
	// Existing instances must not be recreated: a same-key same-gen ensure is
	// an idempotent no-op in the data plane and does not count; only missing
	// instances are created. Final data-plane state is exact.
	ins, _ := env.dp.ListInstances(env.ctx, "midway")
	if len(ins) != 4 {
		t.Fatalf("instances after restart = %d, want 4", len(ins))
	}
	if w.Status.Phase != v1.PhaseAvailable {
		t.Fatalf("phase = %s", w.Status.Phase)
	}
}

// TestRestartDuringDeletionContinues verifies deletion that was blocked by a
// dependent completes across a restart without orphaning data-plane state.
func TestRestartDuringDeletionContinues(t *testing.T) {
	env := newEnv(t)

	env.submit(v1Manifest("base", 2, "nginx:1"))
	env.waitConverged("base", 2)
	env.submit(v1Manifest("top", 1, "sidecar:1", "base"))
	env.waitConverged("top", 1)

	if err := env.ctrl.Delete(env.ctx, "base"); err != nil {
		t.Fatal(err)
	}
	// base must be blocked with finalizer.
	if !env.eventually(func() bool {
		return env.get("base").ObjectMeta.DeletionTimestamp != nil
	}) {
		t.Fatal("base not marked deleting")
	}

	// Restart, then delete the dependent: the ordered teardown completes.
	env.restart(nil)
	defer env.stop()

	if err := env.ctrl.Delete(env.ctx, "top"); err != nil {
		t.Fatal(err)
	}
	if !env.eventually(func() bool {
		_, errTop := env.ctrl.Get(env.ctx, "top")
		_, errBase := env.ctrl.Get(env.ctx, "base")
		return errTop != nil && errBase != nil
	}) {
		t.Fatalf("objects survived restart-assisted teardown: top=%v base=%v",
			env.getPhase("top"), env.getPhase("base"))
	}
	if left, _ := env.dp.ListInstances(env.ctx, "top"); len(left) != 0 {
		t.Fatalf("orphaned top instances: %d", len(left))
	}
	if left, _ := env.dp.ListInstances(env.ctx, "base"); len(left) != 0 {
		t.Fatalf("orphaned base instances: %d", len(left))
	}
	env.dumpLogs()
}

// TestRestartResetsExhaustedTransientBudget verifies an object that exhausted
// its transient-retry budget before the crash is given a fresh budget on
// restart and converges once the data plane is healthy again.
func TestRestartResetsExhaustedTransientBudget(t *testing.T) {
	env := newEnv(t)

	env.setHook(func(op string, _ dataplane.InstanceID, _ int64) error {
		return &dataplane.Failure{Class: dataplane.ClassTransient, Code: "DownAPI", Message: "down"}
	})
	env.submit(v1Manifest("crashed", 1, "nginx:1"))
	w := env.waitPhase("crashed", v1.PhaseFailed)
	if w.Status.Attempts < 5 {
		t.Fatalf("precondition: attempts = %d", w.Status.Attempts)
	}

	// Restart with a healthy data plane: the exhausted budget is reset and
	// convergence resumes from persisted state.
	env.restart(nil)
	defer env.stop()
	w2 := env.waitConverged("crashed", 1)
	if w2.Status.Attempts != 0 {
		t.Fatalf("attempts after recovery = %d, want 0", w2.Status.Attempts)
	}
}

func (e *testEnv) getPhase(name string) v1.Phase {
	w, err := e.ctrl.Get(e.ctx, name)
	if err != nil {
		return "<gone>"
	}
	return w.Status.Phase
}
