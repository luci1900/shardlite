package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/luci1900/shardlite/internal/blob"
)

// ErrConfirmerStopped means the Confirmer's service ended before a read began
// that could answer.
var ErrConfirmerStopped = errors.New("lease: confirmer stopped")

// Confirmer answers one question for the node: has nobody begun to take this
// life's shards since I asked? It reads the node's own lease and checks that it
// still shows this life (Record.Holds), which proves it because every takeover
// from a live owner writes the lease first (docs/internals.md, Confirming
// ownership). The clocks play no part.
//
// At most one read is in flight. A waiter joins only a read that begins after it
// arrived, as a gate joins only a cycle that begins after its mark: a read that
// began earlier proves nothing about what the waiter read from the app. So a
// burst of waiters shares one read, and a waiter that arrives while a read is
// in flight waits for the next one. A waiter that has other evidence leaves, and
// a read that nobody waits for is not begun.
//
// A lease read that finds the lease taken fails the keeper (ErrLost), so
// the node stops as a lost renewal stops it. A read that cannot reach the store
// fails only its waiters. It is a service; its loop owns the reads.
type Confirmer struct {
	keeper *Keeper
	reg    *Registry
	me     Incarnation

	mu      sync.Mutex
	next    *Round // the round waiters join, whose read has not begun
	stopped bool

	wake  chan struct{}
	reads atomic.Int64
}

// Round is the confirmation waiters share: Done closes once its read has
// finished, and Err then says how it went.
type Round struct {
	c       *Confirmer
	waiters int // under c.mu, until the read begins
	done    chan struct{}
	err     error // set before done closes
}

// NewConfirmer returns the Confirmer of keeper's life, reading through reg.
func NewConfirmer(reg *Registry, keeper *Keeper) *Confirmer {
	return &Confirmer{keeper: keeper, reg: reg, me: Incarnation{Node: keeper.Lease().Name, Epoch: keeper.Lease().Epoch}, wake: make(chan struct{}, 1)}
}

func (*Confirmer) String() string { return "confirmer" }

// Reads is how many lease reads have begun.
func (c *Confirmer) Reads() int64 { return c.reads.Load() }

// Join returns the round of the next read to begin, which the caller leaves
// again if it no longer needs the answer (Round.Leave).
func (c *Confirmer) Join() *Round {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return &Round{c: c, done: closed, err: ErrConfirmerStopped}
	}
	if c.next == nil {
		c.next = &Round{c: c, done: make(chan struct{})}
	}
	c.next.waiters++
	select {
	case c.wake <- struct{}{}:
	default:
	}
	return c.next
}

var closed = func() chan struct{} { ch := make(chan struct{}); close(ch); return ch }()

// Done closes when the round's read has finished.
func (r *Round) Done() <-chan struct{} { return r.done }

// Err is nil if the lease still held, once Done has closed.
func (r *Round) Err() error { return r.err }

// Leave withdraws a waiter that no longer needs the answer. A round whose
// waiters have all left before its read begins has no read.
func (r *Round) Leave() {
	r.c.mu.Lock()
	defer r.c.mu.Unlock()
	r.waiters--
}

// Serve is the loop that reads. It ends when ctx does, failing the round that
// waits and every later Join with ErrConfirmerStopped.
func (c *Confirmer) Serve(ctx context.Context) error {
	defer c.stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.wake:
		}
		c.mu.Lock()
		r := c.next
		c.next = nil
		if r != nil && r.waiters <= 0 {
			r = nil
		}
		c.mu.Unlock()
		if r == nil {
			continue
		}
		c.reads.Add(1)
		r.err = c.read(ctx)
		close(r.done)
	}
}

func (c *Confirmer) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	if r := c.next; r != nil {
		c.next = nil
		r.err = ErrConfirmerStopped
		close(r.done)
	}
}

// read reads the node's lease once and checks it, failing the keeper if the
// lease is no longer this life's. It is cut off when the keeper's own deadline
// runs out, since an answer after that is worth nothing.
func (c *Confirmer) read(ctx context.Context) error {
	rctx, cancel := context.WithDeadline(ctx, c.keeper.validUntil(c.keeper.Lease()))
	defer cancel()
	l, err := c.reg.Get(rctx, c.me.Node)
	switch {
	case errors.Is(err, blob.ErrNotFound):
		err = fmt.Errorf("%w: %s has no lease", ErrLost, c.me.Node)
	case err != nil:
		if ctx.Err() != nil {
			return ErrConfirmerStopped
		}
		return fmt.Errorf("confirming the lease: %w", err)
	case l.Holds(c.me):
		return nil
	default:
		err = fmt.Errorf("%w: %s no longer holds its lease at epoch %d", ErrLost, c.me.Node, c.me.Epoch)
	}
	c.keeper.fail(err)
	return c.keeper.Check()
}
