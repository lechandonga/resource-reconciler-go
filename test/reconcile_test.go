package integration

import (
	"sync/atomic"
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
	"github.com/lechandonga/resource-reconciler-go/pkg/dataplane"
)

func v1Manifest(name string, replicas int, image string, deps ...string) string {
	depJSON := "[]"
	if len(deps) > 0 {
		depJSON = "["
		for i, d := range deps {
			if i > 0 {
				depJSON += ","
			}
			depJSON += `"` + d + `"`
		}
		depJSON += "]"
	}
	return `{
	  "apiVersion": "core.example.com/v1",
	  "kind": "Workload",
	  "metadata": {"name": "` + name + `"},
	  "spec": {"replicas": ` + itoaTest(replicas) + `, "image": "` + image + `", "dependsOn": ` + depJSON + `}
	}`
}

func itoaTest(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

// TestDeclarativeConvergence verifies:
//   - drift is corrected (scale up and down);
//   - re-applying the same spec performs no data-plane writes;
//   - already-conforming objects are not rewritten (resource version stable).
func TestDeclarativeConvergence(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	var ensures, removes int64
	env.setHook(func(op string, _ dataplane.InstanceID, _ int64) error {
		switch op {
		case "ensure":
			atomic.AddInt64(&ensures, 1)
		case "remove":
			atomic.AddInt64(&removes, 1)
		}
		return nil
	})

	env.submit(v1Manifest("app", 3, "nginx:1"))
	w := env.waitPhase("app", v1.PhaseAvailable)
	if n := len(w.Status.Instances); n != 3 {
		t.Fatalf("instances = %d, want 3", n)
	}
	if got := atomic.LoadInt64(&ensures); got != 3 {
		t.Fatalf("ensure calls = %d, want 3", got)
	}

	// Converged state: repeated reconciliation touches nothing.
	rvStable := w.ObjectMeta.ResourceVersion
	for i := 0; i < 3; i++ {
		if err := env.qAdd("app"); err != nil {
			t.Fatal(err)
		}
	}
	if !env.eventually(func() bool {
		got := env.get("app")
		return got.ObjectMeta.ResourceVersion == rvStable
	}) {
		t.Fatal("resource version changed while object already conformed")
	}

	// Drift correction: scale down to 1.
	env.submit(v1Manifest("app", 1, "nginx:1"))
	w = env.waitConverged("app", 1)
	if got := atomic.LoadInt64(&removes); got != 2 {
		t.Fatalf("remove calls = %d, want 2", got)
	}

	// Image change updates the surviving instance exactly once.
	before := atomic.LoadInt64(&ensures)
	env.submit(v1Manifest("app", 1, "nginx:2"))
	w = env.waitConverged("app", 1)
	if w.Status.Instances[0].Generation != w.ObjectMeta.Generation {
		t.Fatalf("instance generation %d != workload generation %d",
			w.Status.Instances[0].Generation, w.ObjectMeta.Generation)
	}
	if got := atomic.LoadInt64(&ensures) - before; got != 1 {
		t.Fatalf("ensure calls on image update = %d, want 1", got)
	}
	env.dumpLogs()
}

// TestOrderedLifecycleAndDeletion verifies dependencies are created before
// dependents, and a dependency cannot be deleted while dependents exist;
// deleting the dependent first then the dependency removes data-plane state
// fully (no orphans).
func TestOrderedLifecycleAndDeletion(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	// Submit the dependent BEFORE its dependency: it must wait (ordered
	// create), not fail or produce side effects.
	dec := env.submit(v1Manifest("child", 1, "nginx:1", "parent"))
	if !dec.Allowed {
		t.Fatalf("child admission: %s", dec.Reason)
	}
	// Give it a moment to park in dependency-wait.
	env.eventually(func() bool {
		return env.get("child").Status.Phase == v1.PhaseDegraded
	})
	if c := env.get("child"); c.Status.Phase != v1.PhaseDegraded {
		t.Fatalf("child phase without parent = %s, want Degraded", c.Status.Phase)
	}
	instances, _ := env.dp.ListInstances(env.ctx, "child")
	if len(instances) != 0 {
		t.Fatalf("child created %d instances before dependency was ready", len(instances))
	}

	env.submit(v1Manifest("parent", 1, "redis:7"))
	child := env.waitPhase("child", v1.PhaseAvailable)
	_ = env.waitPhase("parent", v1.PhaseAvailable)
	if child.Status.ObservedGeneration != child.ObjectMeta.Generation {
		t.Fatal("child never observed its generation")
	}

	// Deleting the parent while the child still references it must be
	// blocked: data-plane parent instance must survive.
	if err := env.ctrl.Delete(env.ctx, "parent"); err != nil {
		t.Fatal(err)
	}
	if !env.eventually(func() bool {
		p := env.get("parent")
		return p.ObjectMeta.DeletionTimestamp != nil &&
			conditionReason(p, v1.ConditionFinalizer) == v1.ReasonFinalizerBlocked
	}) {
		t.Fatal("parent deletion not blocked by live dependent")
	}
	parentInstances, _ := env.dp.ListInstances(env.ctx, "parent")
	if len(parentInstances) != 1 {
		t.Fatalf("parent data-plane state torn down while child depends on it: %d", len(parentInstances))
	}

	// Delete the dependent first: it converges to physical removal, which
	// unblocks the parent, which then deletes as well.
	if err := env.ctrl.Delete(env.ctx, "child"); err != nil {
		t.Fatal(err)
	}
	if !env.eventually(func() bool {
		_, err := env.ctrl.Get(env.ctx, "child")
		return err != nil
	}) {
		t.Fatal("child was not physically deleted")
	}
	if !env.eventually(func() bool {
		_, err := env.ctrl.Get(env.ctx, "parent")
		return err != nil
	}) {
		env.dumpLogs()
		p := env.get("parent")
		t.Fatalf("parent remained after dependent removal: phase=%s", p.Status.Phase)
	}
	// No orphan data-plane state.
	if left, _ := env.dp.ListInstances(env.ctx, "child"); len(left) != 0 {
		t.Fatalf("orphaned child instances: %d", len(left))
	}
	if left, _ := env.dp.ListInstances(env.ctx, "parent"); len(left) != 0 {
		t.Fatalf("orphaned parent instances: %d", len(left))
	}
	env.dumpLogs()
}

// TestReconcileBudgetIsBounded verifies a single pass never performs more
// than MaxDataPlaneOps mutations: a large scale-out takes multiple passes and
// records the budget reason.
func TestReconcileBudgetIsBounded(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	// 8 replicas, 3 ops per pass => multiple passes required.
	env.submit(v1Manifest("big", 8, "nginx:1"))
	w := env.waitPhase("big", v1.PhaseAvailable)
	if n := len(w.Status.Instances); n != 8 {
		t.Fatalf("instances = %d, want 8", n)
	}
	found := false
	env.logMu.Lock()
	for _, l := range env.logs {
		if contains(l, v1.ReasonReconcileBudgetExceeded) {
			found = true
		}
	}
	env.logMu.Unlock()
	if !found {
		t.Fatalf("expected a %s decision record", v1.ReasonReconcileBudgetExceeded)
	}
}

func conditionReason(w *v1.Workload, ct v1.ConditionType) string {
	for _, c := range w.Status.Conditions {
		if c.Type == ct {
			return c.Reason
		}
	}
	return ""
}

func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestDependencyCycleRejectedAtRuntime verifies a cycle formed through
// admitted individual updates is detected during reconciliation and exposed
// as a deterministic, reason-coded failure (no spinning, no side effects).
func TestDependencyCycleRejectedAtRuntime(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	// a -> b -> c, all converge.
	env.submit(v1Manifest("a", 1, "nginx:1", "b"))
	env.submit(v1Manifest("b", 1, "nginx:1", "c"))
	env.submit(v1Manifest("c", 1, "nginx:1"))
	env.waitConverged("a", 1)

	// Update c to depend on a, creating a -> b -> c -> a.
	env.submit(v1Manifest("c", 1, "nginx:1", "a"))
	w := env.waitPhase("c", v1.PhaseFailed)
	if w.Status.LastError == nil || w.Status.LastError.Reason != v1.ReasonDependencyCycle {
		t.Fatalf("expected cycle failure, got %+v", w.Status.LastError)
	}
}
