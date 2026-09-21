package controller

import "sync"

// ticketLock is a strictly FIFO lock: waiters are woken in exactly the order
// in which they called Lock. FIFO ordering gives "latest submit wins"
// semantics under concurrent writers — the goroutine whose Submit call is
// admitted last enters the critical section last, so its spec is the durable
// one when all Submit calls have returned.
//
// Each ticket owns its own channel, eliminating turn-number wrap/ABA hazards.
type ticketLock struct {
	mu      sync.Mutex
	counter int // next ticket to assign
	serving int // ticket currently holding the lock
	waiters []chan struct{}
}

func newTicketLock() *ticketLock { return &ticketLock{} }

// Lock acquires the lock in FIFO order; it returns the assigned ticket.
func (t *ticketLock) Lock() int {
	t.mu.Lock()
	ticket := t.counter
	t.counter++
	if ticket == t.serving {
		// First in line: enter immediately, no waiter slot needed.
		t.mu.Unlock()
		return ticket
	}
	// Waiter slots must exist for every non-holder ticket.
	for len(t.waiters) <= ticket {
		t.waiters = append(t.waiters, make(chan struct{}))
	}
	ch := t.waiters[ticket]
	t.mu.Unlock()
	<-ch
	return ticket
}

// Unlock releases the lock and wakes exactly the next waiter in FIFO order.
func (t *ticketLock) Unlock() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.serving++
	if t.serving < len(t.waiters) {
		close(t.waiters[t.serving])
	}
}
