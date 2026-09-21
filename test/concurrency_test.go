package integration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/apis/core/v1"
	"github.com/lechandonga/resource-reconciler-go/pkg/dataplane"
)

// TestConcurrentWritesPreserveIntegrity hammers one object with genuinely
// simultaneous writes that have no externally defined order. What IS
// guaranteed: no lost/duplicated writes, generation increases exactly once
// per accepted spec, and the data plane converges exactly to whichever spec
// was serialized last — never a mix.
func TestConcurrentWritesPreserveIntegrity(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	env.submit(v1Manifest("contested", 1, "nginx:0"))
	env.waitConverged("contested", 1)

	const writers = 20
	var wg sync.WaitGroup
	reasons := map[string]int{}
	var reasonsMu sync.Mutex
	start := make(chan struct{})
	distinctImages := map[string]struct{}{}
	for i := 1; i <= writers; i++ {
		distinctImages[fmt.Sprintf("nginx:%d", i)] = struct{}{}
	}
	for i := 1; i <= writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			dec := env.submit(v1Manifest("contested", 1, fmt.Sprintf("nginx:%d", i)))
			reasonsMu.Lock()
			if dec.Allowed {
				reasons["Allowed"]++
			} else {
				reasons[dec.Reason]++
			}
			reasonsMu.Unlock()
		}(i)
	}
	close(start)
	wg.Wait()

	var final *v1.Workload
	ok := env.eventually(func() bool {
		w, err := env.ctrl.Get(env.ctx, "contested")
		if err != nil || w.Status.Phase != v1.PhaseAvailable {
			return false
		}
		if w.Status.ObservedGeneration != w.ObjectMeta.Generation || len(w.Status.Instances) != 1 {
			return false
		}
		final = w
		return true
	})
	if !ok {
		env.dumpLogs()
		t.Fatal("object never settled after concurrent writes")
	}
	// The winning image must be one of the submitted images (never a torn
	// mix), and there must be exactly one instance reflecting it.
	if _, valid := distinctImages[final.Spec.Image]; !valid {
		t.Fatalf("final image %q is not any submitted spec", final.Spec.Image)
	}
	if final.Status.Instances[0].Generation != final.ObjectMeta.Generation {
		t.Fatalf("instance generation %d != workload generation %d",
			final.Status.Instances[0].Generation, final.ObjectMeta.Generation)
	}
	// Exactly one accepted write per distinct accepted generation: the final
	// generation equals 1 (initial) + number of distinct accepted specs;
	// no generation was skipped or duplicated.
	accepted := reasons["Allowed"]
	if final.ObjectMeta.Generation != int64(1+accepted) {
		t.Fatalf("generation=%d, expected 1+%d", final.ObjectMeta.Generation, accepted)
	}
	t.Logf("submit outcomes: %v; winner=%s gen=%d", reasons, final.Spec.Image, final.ObjectMeta.Generation)
}

// TestLatestOrderedSubmitWins verifies the externally meaningful ordering
// guarantee: when writes are emitted in a defined call order (each after the
// previous returned), reconciliation that is actively running must converge
// to the newest version, and stale reconciler write-backs are rejected and
// retried rather than rolling the spec back.
func TestLatestOrderedSubmitWins(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	// A slow data plane keeps reconciliation in flight while specs change.
	var n int32
	env.setHook(func(op string, id dataplane.InstanceID, gen int64) error {
		if op == "ensure" && atomic.AddInt32(&n, 1)%3 == 0 {
			time.Sleep(8 * time.Millisecond)
		}
		return nil
	})

	env.submit(v1Manifest("ordered", 1, "nginx:0"))
	// Issue a defined sequence without waiting for convergence in between:
	// reconciliation of older generations is racing the newest spec.
	for i := 1; i <= 10; i++ {
		dec := env.submit(v1Manifest("ordered", 1, fmt.Sprintf("nginx:%d", i)))
		if !dec.Allowed {
			t.Fatalf("ordered submit %d rejected: %s", i, dec.Reason)
		}
	}
	w := env.waitConverged("ordered", 1)
	if w.Spec.Image != "nginx:10" {
		t.Fatalf("final image = %q, want nginx:10", w.Spec.Image)
	}
	if w.Status.Instances[0].Generation != w.ObjectMeta.Generation {
		t.Fatalf("instance gen=%d want=%d", w.Status.Instances[0].Generation, w.ObjectMeta.Generation)
	}
	// The data plane holds exactly one instance, never duplicates.
	all, err := env.dp.ListInstances(env.ctx, "ordered")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 1 {
		t.Fatalf("data-plane instances = %d, want 1", len(all))
	}
	if all[0].Image != "nginx:10" {
		t.Fatalf("data plane image = %q, want nginx:10", all[0].Image)
	}
}

// TestSameObjectNotProcessedConcurrently verifies key-level mutual exclusion:
// at no point may two data-plane operations for the same workload overlap.
func TestSameObjectNotProcessedConcurrently(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	var active int32
	var maxActive int32
	env.setHook(func(op string, id dataplane.InstanceID, gen int64) error {
		cur := atomic.AddInt32(&active, 1)
		for {
			peak := atomic.LoadInt32(&maxActive)
			if cur <= peak || atomic.CompareAndSwapInt32(&maxActive, peak, cur) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return nil
	})

	env.submit(v1Manifest("locked", 6, "nginx:1"))
	env.waitConverged("locked", 6)

	if got := atomic.LoadInt32(&maxActive); got > 1 {
		env.dumpLogs()
		t.Fatalf("observed %d concurrent data-plane operations on one key, want at most 1", got)
	}
}

// TestOptimisticConcurrencyAtStoreBoundary directly exercises the store OCC
// contract: an update based on a stale RV must be rejected and must not
// overwrite the newer state.
func TestOptimisticConcurrencyAtStoreBoundary(t *testing.T) {
	env := newEnv(t)
	defer env.stop()

	env.submit(v1Manifest("occ", 1, "nginx:1"))
	w := env.waitConverged("occ", 1)
	staleRV := w.ObjectMeta.ResourceVersion

	env.submit(v1Manifest("occ", 1, "nginx:2"))
	env.waitConverged("occ", 1)

	w.Spec.Image = "nginx:STALE"
	if _, err := env.store.Update(env.ctx, w, staleRV); err == nil {
		t.Fatal("stale write-back was accepted; expected optimistic-concurrency conflict")
	}
	current := env.get("occ")
	if current.Spec.Image != "nginx:2" {
		t.Fatalf("newer state was overwritten by stale write: %q", current.Spec.Image)
	}
	if current.Status.Phase != v1.PhaseAvailable {
		t.Fatalf("phase after rejected write = %s", current.Status.Phase)
	}
}
