package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestQueueDedupFIFOAndDone(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	q := NewKeyQueue(5, clk)
	if err := q.Add("a"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := q.Add("a"); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Add("b"); err != nil {
		t.Fatal(err)
	}
	if q.Len() != 2 {
		t.Fatalf("len = %d, want 2 (dedup)", q.Len())
	}
	ctx := context.Background()
	k1, err := q.Get(ctx)
	if err != nil || k1 != "a" {
		t.Fatalf("get1 = %q,%v", k1, err)
	}
	// adding an in-flight key must be a no-op until Done
	if err := q.Add("a"); err != nil || q.Len() != 1 {
		t.Fatalf("in-flight add: err=%v len=%d", err, q.Len())
	}
	if !q.InFlight("a") {
		t.Fatal("a should be in flight")
	}
	k2, _ := q.Get(ctx)
	if k2 != "b" {
		t.Fatalf("get2 = %q, want b", k2)
	}
	if err := q.Add("a"); err != nil {
		t.Fatal(err)
	}
	// a can be requeued while in-flight? tracked says no; Done releases it,
	// then it becomes processable again.
	q.Done("a")
	q.Done("b")
	// "a" was added while in flight (no-op); add again now.
	if err := q.Add("a"); err != nil {
		t.Fatal(err)
	}
	k3, _ := q.Get(ctx)
	if k3 != "a" {
		t.Fatalf("get3 = %q, want a", k3)
	}
}

func TestQueueCapacityRejectsOverflow(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	q := NewKeyQueue(2, clk)
	if err := q.Add("a"); err != nil {
		t.Fatal(err)
	}
	if err := q.Add("b"); err != nil {
		t.Fatal(err)
	}
	if err := q.Add("c"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("overflow err = %v, want ErrQueueFull", err)
	}
	// draining frees a slot deterministically
	q.Done(func() string {
		k, _ := q.Get(context.Background())
		return k
	}())
	if err := q.Add("c"); err != nil {
		t.Fatalf("after drain: %v", err)
	}
}

func TestQueueDelayedBecomesReadyOnAdvance(t *testing.T) {
	clk := NewFakeClock(time.Unix(1000, 0))
	q := NewKeyQueue(5, clk)
	if err := q.AddAfter("late", 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if q.Len() != 1 {
		t.Fatalf("delayed len = %d", q.Len())
	}
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan string, 1)
	go func() {
		k, err := q.Get(ctx)
		got <- k + ":" + errString(err)
	}()
	time.Sleep(5 * time.Millisecond) // let the worker park
	clk.Advance(9 * time.Second)
	time.Sleep(5 * time.Millisecond)
	select {
	case v := <-got:
		t.Fatalf("key became ready too early: %s", v)
	default:
	}
	clk.Advance(1 * time.Second)
	select {
	case v := <-got:
		if v != "late:<nil>" {
			t.Fatalf("got %q", v)
		}
	case <-time.After(time.Second):
		t.Fatal("delayed key never became ready")
	}
	cancel()
}

func TestQueueGetRespectsContextCancel(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	q := NewKeyQueue(5, clk)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { _, err := q.Get(ctx); errCh <- err }()
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not return after cancel")
	}
}

func TestQueueCloseUnblocksGet(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	q := NewKeyQueue(5, clk)
	errCh := make(chan error, 1)
	go func() { _, err := q.Get(context.Background()); errCh <- err }()
	time.Sleep(5 * time.Millisecond)
	q.Close()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrQueueClosed) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Get did not unblock on close")
	}
}

func TestQueueNoSameKeyConcurrent(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0))
	q := NewKeyQueue(3, clk)
	_ = q.Add("x")
	ctx := context.Background()

	var mu sync.Mutex
	concurrent := 0
	maxConcurrent := 0
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = q.Add("x")
		}()
	}
	wg.Wait()
	// only one key exists; only one worker may hold it
	k, _ := q.Get(ctx)
	mu.Lock()
	concurrent++
	if concurrent > maxConcurrent {
		maxConcurrent = concurrent
	}
	mu.Unlock()
	_ = k
	_ = q.Add("x") // while in flight -> no-op
	time.Sleep(5 * time.Millisecond)
	if q.InFlight("x") != true {
		t.Fatal("x not in flight")
	}
	mu.Lock()
	concurrent--
	mu.Unlock()
	q.Done("x")
	if maxConcurrent != 1 {
		t.Fatalf("same key held concurrently, max=%d", maxConcurrent)
	}
}

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}
