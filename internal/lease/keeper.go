package lease

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"
)

// KeeperOptions tune a Keeper.
type KeeperOptions struct {
	Interval time.Duration   // renew this often, 3 s if zero
	Margin   time.Duration   // treat the lease as expired this long before its TTL runs out, 2 s if zero
	Info     func() NodeInfo // what to publish at each renewal (the journal range)
	Logger   *slog.Logger
	Now      func() time.Time // time.Now if nil, for tests
}

// Keeper renews a node's lease and answers the one question
// the node asks before acknowledging anything: is the lease still good?
//
// The holder judges its own lease expired a safety margin before others do,
// counting from the moment it began the write that last renewed it (the
// lease's Renewed time), so a slow write can't leave it acting on a lease
// others consider dead. Others judge by elapsed time too: an adopter waits until
// it has seen the lease unchanged for a TTL on its own clock, so only the
// clocks' rates have to agree (docs/internals.md, Node leases). The deadline is
// kept by both the monotonic and the wall clock, whichever runs out first: the
// monotonic clock doesn't count time a machine spends suspended, and a VM
// pause can hide from it. Expiry is final: a
// node whose lease has run out may have lost shards to other nodes, so it
// stops for good and a restart takes a new epoch.
//
// A Keeper renews only while it is served (it is a service): a lease
// that is never served runs out like any other.
type Keeper struct {
	reg  *Registry
	opts KeeperOptions

	lease atomic.Pointer[Record] // the latest, stored only by the renewal loop
	err   atomic.Pointer[error]  // set once, final

	lost chan struct{}
}

// NewKeeper returns the Keeper of lease, which Acquire just returned. It renews
// nothing until it is served.
func NewKeeper(reg *Registry, lease Record, opts KeeperOptions) *Keeper {
	if opts.Interval <= 0 {
		opts.Interval = 3 * time.Second
	}
	if opts.Margin <= 0 {
		opts.Margin = 2 * time.Second
	}
	if opts.Info == nil {
		info := lease.NodeInfo
		opts.Info = func() NodeInfo { return info }
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	k := &Keeper{reg: reg, opts: opts, lost: make(chan struct{})}
	k.lease.Store(&lease)
	return k
}

func (k *Keeper) validUntil(l Record) time.Time { return l.Renewed.Add(l.TTL - k.opts.Margin) }

// expired reports whether now is past until by either clock. A time with a
// monotonic reading compares by it, and Round(0) drops it to compare the wall
// clock.
func expired(now, until time.Time) bool {
	return !now.Before(until) || !now.Round(0).Before(until.Round(0))
}

// Lease returns the latest lease.
func (k *Keeper) Lease() Record { return *k.lease.Load() }

// Check returns nil while the lease is good for acknowledging work, and the
// reason (ErrLost or ErrExpired) once it is not.
func (k *Keeper) Check() error {
	if k.err.Load() == nil && expired(k.opts.Now(), k.validUntil(k.Lease())) {
		k.fail(ErrExpired)
	}
	if e := k.err.Load(); e != nil {
		return *e
	}
	return nil
}

// Lost is closed once the lease is lost or has expired.
func (k *Keeper) Lost() <-chan struct{} { return k.lost }

// fail makes err the final one, if none was set yet.
func (k *Keeper) fail(err error) {
	if k.err.CompareAndSwap(nil, &err) {
		close(k.lost)
		l := k.Lease()
		k.opts.Logger.Error("node lease is no longer valid, the node must stop", "node", l.Name, "epoch", l.Epoch, "err", err)
	}
}

func (*Keeper) String() string { return "keeper" }

// Serve is the renewal loop, which owns the lease: cur is the latest one, and
// each renewal publishes it for the readers. It ends when ctx does or the lease
// is lost or has expired, and is never restarted. The lease is left as it is:
// Release gives it up.
func (k *Keeper) Serve(ctx context.Context) error {
	cur := k.Lease()
	delay := time.Until(cur.Renewed.Add(k.opts.Interval))
	for {
		until := k.validUntil(cur)

		// Wake for the next renewal or for expiry, whichever comes first.
		wake, expire := time.NewTimer(delay), time.NewTimer(time.Until(until))
		select {
		case <-ctx.Done():
			wake.Stop()
			expire.Stop()
			return nil
		case <-expire.C:
			wake.Stop()
			k.fail(ErrExpired)
			return nil
		case <-wake.C:
			expire.Stop()
		}
		// The timers run on the monotonic clock. A pause it didn't count shows on
		// the wall clock.
		if expired(k.opts.Now(), until) {
			k.fail(ErrExpired)
			return nil
		}

		// A renewal that is still going when the margin runs out is worth nothing.
		rctx, cancel := context.WithDeadline(ctx, until)
		renewed, err := k.reg.Renew(rctx, cur, k.opts.Info())
		cancel()
		switch {
		case err == nil:
			cur = renewed
			k.lease.Store(&renewed)
			delay = time.Until(renewed.Renewed.Add(k.opts.Interval))
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, ErrLost):
			k.fail(err)
			return nil
		default:
			// Storage trouble. Try again soon, until the margin runs out.
			k.opts.Logger.Warn("lease renewal failed", "node", cur.Name, "err", err)
			delay = k.opts.Interval / 3
		}
	}
}

// Release marks the lease released (a graceful shutdown), unless it was already
// lost. The Keeper must have stopped being served first.
func (k *Keeper) Release(ctx context.Context) error {
	if k.err.Load() != nil {
		return nil
	}
	l := k.Lease()
	name := l.Name
	l.NodeInfo = k.opts.Info()
	l.NodeInfo.Name = name
	return k.reg.Release(ctx, l)
}
