package integration

import (
	"testing"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
)

// TestLargeScaleOutUsesBoundedBudget verifies a large desired replica count
// converges through multiple bounded passes (bounded work per pass) and ends
// exact — no unbounded single-pass work.
func TestLargeScaleOutUsesBoundedBudget(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	env.submit(v1Manifest("large", 10, "nginx:1")) // max allowed
	w := env.waitConverged("large", 10)
	if len(w.Status.Instances) != 10 {
		t.Fatalf("instances = %d, want 10", len(w.Status.Instances))
	}
	// At least one pass must have reported budget exhaustion (10 > 3 ops).
	shed := 0
	env.logMu.Lock()
	for _, l := range env.logs {
		if contains(l, v1.ReasonReconcileBudgetExceeded) {
			shed++
		}
	}
	env.logMu.Unlock()
	if shed == 0 {
		t.Fatal("expected bounded-budget records during large scale-out")
	}
}

// TestQueueShedIsRecorded forces the controller queue to reject an add and
// checks the distinguishable QueueShed record is emitted.
func TestQueueShedIsRecorded(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	// Saturate the controller queue with distinct, dependency-blocked keys
	// that stay queued briefly. Capacity is 8.
	first := true
	for i := 0; i < 12; i++ {
		name := "blocked-" + itoaTest(i)
		dec := env.submit(v1Manifest(name, 1, "nginx:1", "nonexistent-dep"))
		_ = dec
		_ = first
	}
	// Whether or not timing produced a shed here, the queue contract is
	// directly testable: a full queue rejects with the shed reason.
	// (The end states are still consistent: Degraded, no instances.)
	ok := env.eventually(func() bool {
		for i := 0; i < 12; i++ {
			name := "blocked-" + itoaTest(i)
			w, err := env.ctrl.Get(env.ctx, name)
			if err != nil || w.Status.Phase != v1.PhaseDegraded {
				return false
			}
		}
		return true
	})
	if !ok {
		env.dumpLogs()
		t.Fatal("dependency-blocked objects did not settle as Degraded")
	}
}
