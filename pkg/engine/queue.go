package engine

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// ErrQueueFull is returned when the bounded queue is saturated. Callers
// record a distinguishable QueueOverflow rejection instead of blocking
// forever or growing memory without bound.
var (
	ErrQueueFull   = errQueueFull{}
	ErrQueueClosed = errQueueClosed{}
)

// deadlineHorizon caps how long Get waits before rechecking even when
// nothing is delayed, ensuring closed/cancelled states are noticed.
const deadlineHorizon = 1 << 62

type errQueueClosed struct{}

func (errQueueClosed) Error() string { return "work queue is closed" }

type errQueueFull struct{}

func (errQueueFull) Error() string { return "work queue is full" }

type delayedItem struct {
	key   string
	ready time.Time
	seq   int64
}

type delayedHeap []delayedItem

func (h delayedHeap) Len() int { return len(h) }
func (h delayedHeap) Less(i, j int) bool {
	if h[i].ready.Equal(h[j].ready) {
		return h[i].seq < h[j].seq
	}
	return h[i].ready.Before(h[j].ready)
}
func (h delayedHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *delayedHeap) Push(x any)   { *h = append(*h, x.(delayedItem)) }
func (h *delayedHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// KeyQueue is a bounded, deduplicating FIFO queue of object keys with
// per-key in-flight exclusion and delayed requeue support.
//
// A key is "tracked" from Add/AddAfter until Done: tracked keys are not
// counted twice, so churning updates to one object cannot fill the queue.
// The hard capacity bounds the number of distinct tracked keys, which
// bounds memory use and work under backlog.
type KeyQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	capacity int
	clock    Clock
	seq      int64

	ready    []string            // FIFO of keys available now
	readySet map[string]struct{} // present in ready list
	delayed  delayedHeap
	delSet   map[string]struct{} // waiting in delayed heap
	inflight map[string]struct{}
	closed   bool
}

// NewKeyQueue creates a queue with a hard distinct-key capacity.
func NewKeyQueue(capacity int, clock Clock) *KeyQueue {
	q := &KeyQueue{
		capacity: capacity,
		clock:    clock,
		readySet: map[string]struct{}{},
		delSet:   map[string]struct{}{},
		inflight: map[string]struct{}{},
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *KeyQueue) trackedLocked(key string) bool {
	if _, ok := q.readySet[key]; ok {
		return true
	}
	if _, ok := q.delSet[key]; ok {
		return true
	}
	_, ok := q.inflight[key]
	return ok
}

// Add enqueues key immediately. It returns ErrQueueFull if the queue is
// saturated with other keys. Adding an already tracked key is a no-op
// (an in-flight key must finish before it is reprocessed).
func (q *KeyQueue) Add(key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.trackedLocked(key) {
		return nil
	}
	if len(q.readySet)+len(q.delSet)+len(q.inflight) >= q.capacity {
		return ErrQueueFull
	}
	q.ready = append(q.ready, key)
	q.readySet[key] = struct{}{}
	q.cond.Signal()
	return nil
}

// AddAfter enqueues key no earlier than after d. A zero or negative d is
// equivalent to Add. Re-adding a delayed key earlier moves it forward.
func (q *KeyQueue) AddAfter(key string, d time.Duration) error {
	if d <= 0 {
		return q.Add(key)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.trackedLocked(key) {
		return nil
	}
	if len(q.readySet)+len(q.delSet)+len(q.inflight) >= q.capacity {
		return ErrQueueFull
	}
	q.seq++
	heap.Push(&q.delayed, delayedItem{key: key, ready: q.clock.Now().Add(d), seq: q.seq})
	q.delSet[key] = struct{}{}
	q.cond.Broadcast() // wake Get to recompute its wait deadline
	return nil
}

// nextReadyLocked promotes due delayed items in ready-time order and
// returns the next ready key, or "".
func (q *KeyQueue) nextReadyLocked(now time.Time) string {
	for q.delayed.Len() > 0 && !q.delayed[0].ready.After(now) {
		it := heap.Pop(&q.delayed).(delayedItem)
		if _, stillDelayed := q.delSet[it.key]; !stillDelayed {
			continue // removed while in the heap
		}
		delete(q.delSet, it.key)
		if _, already := q.readySet[it.key]; already {
			continue
		}
		q.ready = append(q.ready, it.key)
		q.readySet[it.key] = struct{}{}
	}
	if len(q.ready) == 0 {
		return ""
	}
	key := q.ready[0]
	q.ready = q.ready[1:]
	delete(q.readySet, key)
	q.inflight[key] = struct{}{}
	return key
}

// Get blocks until a key is ready. The returned key is reserved
// (in-flight) until Done, so the same object is never processed by two
// workers concurrently.
func (q *KeyQueue) Get(ctx context.Context) (string, error) {
	type result struct {
		key string
		err error
	}
	resultCh := make(chan result, 1)

	// Wake every waiter when ctx is cancelled so Get returns promptly.
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	go func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		for {
			if err := ctx.Err(); err != nil {
				resultCh <- result{err: err}
				return
			}
			now := q.clock.Now()
			if k := q.nextReadyLocked(now); k != "" {
				resultCh <- result{key: k}
				return
			}
			if q.closed {
				resultCh <- result{err: ErrQueueClosed}
				return
			}
			waitUntil := now.Add(deadlineHorizon)
			if q.delayed.Len() > 0 {
				waitUntil = q.delayed[0].ready
				// Cond cannot wait with a deadline, so a sleeper
				// broadcasts when the clock moves.
				go func(wake time.Time) {
					_ = q.clock.Sleep(ctx, wake.Sub(now))
					q.mu.Lock()
					q.cond.Broadcast()
					q.mu.Unlock()
				}(waitUntil)
			}
			q.cond.Wait()
		}
	}()

	select {
	case r := <-resultCh:
		return r.key, r.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// Done releases the in-flight reservation. Delayed requeue (if any) was
// registered separately by the controller with AddAfter.
func (q *KeyQueue) Done(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inflight, key)
	q.cond.Broadcast()
}

// Len returns the number of keys waiting (ready + delayed), excluding
// keys currently in flight.
func (q *KeyQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.readySet) + len(q.delSet)
}

// InFlight reports whether key is currently being processed.
func (q *KeyQueue) InFlight(key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.inflight[key]
	return ok
}

// Close wakes all blocked Get calls with ErrQueueClosed.
func (q *KeyQueue) Close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}
