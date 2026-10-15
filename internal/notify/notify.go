// Package notify has the two ways goroutines wake each other without a lock or
// a sync.Cond: Kick, a wake-up that coalesces, and Counter, a number that only
// goes up and that goroutines wait on. Both are channels underneath, so a wait
// can sit in a select next to a context or a timer.
package notify

import (
	"context"
	"sync"
)

// Kick is a wake-up for one waiting goroutine. Any number of Kicks before the
// waiter looks count as one. The zero value is not usable: use NewKick.
type Kick struct{ c chan struct{} }

// NewKick returns a Kick with nothing pending.
func NewKick() *Kick { return &Kick{c: make(chan struct{}, 1)} }

// Kick marks a wake-up pending. It never blocks.
func (k *Kick) Kick() {
	select {
	case k.c <- struct{}{}:
	default:
	}
}

// C is ready when a wake-up is pending, and receiving from it consumes the
// wake-up.
func (k *Kick) C() <-chan struct{} { return k.c }

// Counter is a number that only goes up. Goroutines wait for it to reach a
// value, which is how a test (or a caller) waits for "the cycle has run" or "the
// shard was let go" without polling. The zero value is a counter at 0.
type Counter struct {
	mu      sync.Mutex
	n       uint64
	waiters []waiter
}

type waiter struct {
	n uint64
	c chan struct{}
}

// Add raises the counter by d and returns the new value.
func (c *Counter) Add(d uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n += d
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if c.n >= w.n {
			close(w.c)
		} else {
			kept = append(kept, w)
		}
	}
	clear(c.waiters[len(kept):])
	c.waiters = kept
	return c.n
}

// Inc raises the counter by one and returns the new value.
func (c *Counter) Inc() uint64 { return c.Add(1) }

// Load is the counter's value.
func (c *Counter) Load() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// Reached returns a channel that is closed once the counter is at least n
// (already closed if it is).
func (c *Counter) Reached(n uint64) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan struct{})
	if c.n >= n {
		close(ch)
		return ch
	}
	c.waiters = append(c.waiters, waiter{n: n, c: ch})
	return ch
}

// Wait blocks until the counter is at least n or ctx ends, and returns ctx's
// error in that case.
func (c *Counter) Wait(ctx context.Context, n uint64) error {
	select {
	case <-c.Reached(n):
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
