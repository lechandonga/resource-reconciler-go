// Package engine contains the reconciliation machinery: the bounded
// work queue, failure-classifying actuator, backoff policy and the
// reconcile loop with optimistic-concurrency retry.
package engine

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Clock abstracts time so tests run deterministically and instantly.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

// RealClock uses wall-clock time and a timer that cancellation aborts.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// waiter is one pending FakeClock.Sleep call.
type waiter struct {
	until time.Time
	ch    chan struct{}
}

// FakeClock is a manually controlled clock. Sleep blocks until the
// clock advances past the deadline or ctx is cancelled.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

// NewFakeClock starts the fake clock at t.
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{now: t} }

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d (d must be non-negative) and
// wakes every waiter whose deadline has now passed.
func (c *FakeClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	due := make([]*waiter, 0)
	remaining := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.until.After(c.now) {
			due = append(due, w)
		} else {
			remaining = append(remaining, w)
		}
	}
	c.waiters = remaining
	c.mu.Unlock()
	// Close channels without holding the lock so woken goroutines may
	// re-enter the clock immediately.
	for _, w := range due {
		close(w.ch)
	}
}

// Sleep blocks until the fake clock reaches now+d. Non-positive d
// returns immediately (still respecting cancellation).
func (c *FakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	c.mu.Lock()
	if !c.now.Add(d).After(c.now) {
		c.mu.Unlock()
		return ctx.Err()
	}
	w := &waiter{until: c.now.Add(d), ch: make(chan struct{})}
	if !w.until.After(c.now) {
		c.mu.Unlock()
		return ctx.Err()
	}
	c.waiters = append(c.waiters, w)
	sort.Slice(c.waiters, func(i, j int) bool { return c.waiters[i].until.Before(c.waiters[j].until) })
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		c.remove(w)
		return ctx.Err()
	case <-w.ch:
		return nil
	}
}

func (c *FakeClock) remove(w *waiter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, x := range c.waiters {
		if x == w {
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			return
		}
	}
}
