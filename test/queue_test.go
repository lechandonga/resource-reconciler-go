package integration

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/resource-reconciler-go/pkg/queue"
)

// TestQueueIsBounded verifies capacity is enforced with a distinguishable
// ErrDropped instead of blocking or growing without bound.
func TestQueueIsBounded(t *testing.T) {
	q := queue.New(3)
	defer q.Shutdown()

	for _, k := range []string{"a", "b", "c"} {
		if err := q.Add(k); err != nil {
			t.Fatalf("add %s: %v", k, err)
		}
	}
	if err := q.Add("d"); !errors.Is(err, queue.ErrDropped) {
		t.Fatalf("4th add error = %v, want ErrDropped", err)
	}
	if q.Len() != 3 {
		t.Fatalf("len = %d, want 3", q.Len())
	}
}

// TestQueueCoalesces verifies a storm of Adds for one key collapses to a
// single queued entry (no unbounded growth, no retry amplification).
func TestQueueCoalesces(t *testing.T) {
	q := queue.New(100)
	defer q.Shutdown()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = q.Add("hot") }()
	}
	wg.Wait()
	if q.Len() != 1 {
		t.Fatalf("queued entries for one key = %d, want 1", q.Len())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := q.Get(ctx)
	if err != nil || got != "hot" {
		t.Fatalf("get = %q, %v", got, err)
	}

	// While in flight, Adds coalesce into a single dirty requeue.
	for i := 0; i < 50; i++ {
		_ = q.Add("hot")
	}
	if q.Len() != 0 {
		t.Fatalf("in-flight adds must not appear queued before Done")
	}
	q.Done("hot")
	if q.Len() != 1 {
		t.Fatalf("dirty key not requeued exactly once: len=%d", q.Len())
	}
}

// TestQueueDeliversAllKeys checks FIFO-ish delivery across many goroutines
// under -race (primarily a concurrency safety check).
func TestQueueConcurrentDeliveries(t *testing.T) {
	q := queue.New(64)
	defer q.Shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var enqueued int64
	const n = 200
	for i := 0; i < n; i++ {
		if err := q.Add(itoaTest(i)); err == nil {
			atomic.AddInt64(&enqueued, 1)
		}
	}
	want := atomic.LoadInt64(&enqueued)

	var seen int64
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt64(&seen) < want {
				k, err := q.Get(ctx)
				if err != nil {
					return
				}
				atomic.AddInt64(&seen, 1)
				q.Done(k)
			}
		}()
	}
	if !waitFor(&seen, want, 5*time.Second) {
		t.Fatalf("only %d/%d enqueued keys delivered", atomic.LoadInt64(&seen), want)
	}
	wg.Wait()
}

func waitFor(p *int64, target int64, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(p) >= target {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return atomic.LoadInt64(p) >= target
}
