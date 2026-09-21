// Package queue provides a bounded, coalescing work queue: at most one
// pending entry per key, so frequent spec changes cannot cause unbounded
// queuing or retry storms. A change arriving while a key is being processed
// sets a dirty marker; when the worker calls Done the key is requeued once.
package queue

import (
	"context"
	"errors"
	"sync"
)

// ErrDropped is returned by Add when the queue is at capacity. Callers must
// record a distinguishable shed event (reason QueueShed) rather than block
// forever.
var ErrDropped = errors.New("queue: shed: capacity exhausted")

// ErrShutdown is returned by Get after Shutdown once the queue is drained.
var ErrShutdown = errors.New("queue: shut down")

// BoundedQueue coalesces repeated Add calls for the same key while it is
// already queued or in flight.
type BoundedQueue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	queued   map[string]struct{}
	dirty    map[string]struct{} // key in flight that must be requeued
	inFlight map[string]struct{}
	order    []string
	capacity int
	shutdown bool
}

// New creates a queue with at most capacity pending keys (in-flight keys
// with a dirty marker count against the bound).
func New(capacity int) *BoundedQueue {
	if capacity < 1 {
		capacity = 1
	}
	q := &BoundedQueue{
		queued:   map[string]struct{}{},
		dirty:    map[string]struct{}{},
		inFlight: map[string]struct{}{},
		capacity: capacity,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Add enqueues key. If key is already queued, this is a no-op (coalescing).
// If key is in flight, it is marked dirty and requeued exactly once by Done.
// If the capacity is exhausted, ErrDropped is returned.
func (q *BoundedQueue) Add(key string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shutdown {
		return ErrShutdown
	}
	if _, ok := q.queued[key]; ok {
		return nil // coalesce: latest state will be read at processing time
	}
	if _, ok := q.inFlight[key]; ok {
		// Bound the dirty set as well, so a storm during processing cannot
		// grow memory without limit.
		if len(q.queued)+len(q.dirty) >= q.capacity {
			return ErrDropped
		}
		q.dirty[key] = struct{}{}
		return nil
	}
	if len(q.queued) >= q.capacity {
		return ErrDropped
	}
	q.queued[key] = struct{}{}
	q.order = append(q.order, key)
	q.cond.Signal()
	return nil
}

// Get blocks until a key is available. Returns ErrShutdown when shut down
// and fully drained.
func (q *BoundedQueue) Get(ctx context.Context) (string, error) {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			q.cond.Broadcast()
		case <-done:
		}
	}()

	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.order) == 0 {
		if q.shutdown {
			return "", ErrShutdown
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		q.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	key := q.order[0]
	q.order = q.order[1:]
	delete(q.queued, key)
	q.inFlight[key] = struct{}{}
	return key, nil
}

// Done marks the in-flight key finished. If Add was called during
// processing (dirty), the key is requeued once.
func (q *BoundedQueue) Done(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inFlight, key)
	if _, need := q.dirty[key]; need {
		delete(q.dirty, key)
		if !q.shutdown && len(q.queued) < q.capacity {
			q.queued[key] = struct{}{}
			q.order = append(q.order, key)
			q.cond.Signal()
		}
	}
}

// Len returns the number of keys waiting (not counting in-flight keys).
func (q *BoundedQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queued)
}

// InFlight reports whether key is currently being processed.
func (q *BoundedQueue) InFlight(key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.inFlight[key]
	return ok
}

// Shutdown unblocks waiting consumers.
func (q *BoundedQueue) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.shutdown = true
	q.cond.Broadcast()
}
