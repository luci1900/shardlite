package coord

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/supervisor"
)

// KeeperOptions tune a Keeper.
type KeeperOptions struct {
	Interval time.Duration   // renew this often, 3 s if zero
	Margin   time.Duration   // treat the lease as expired this long before its TTL runs out, 2 s if zero
	Info     func() NodeInfo // what to publish at each renewal (counts, journal range)
	Logger   *slog.Logger
}

// Keeper renews a node's lease and answers the one question
// the node asks before acknowledging anything: is the lease still good?
//
// The holder judges its own lease expired a safety margin before others do,
// counting from the moment it began the write that last renewed it (the
// lease's Renewed time), so a slow write or a clock that runs a little behind
// can't leave it acting on a lease others consider dead. Expiry is final: a
// node whose lease has run out may have lost shards to other nodes, so it
// stops for good and a restart takes a new epoch.
//
// A Keeper renews only while it is served (it is a suture service): a lease
// that is never served runs out like any other.
type Keeper struct {
	reg  *Registry
	opts KeeperOptions

	lease atomic.Pointer[NodeLease] // the latest, stored only by the renewal loop
	err   atomic.Pointer[error]     // set once, final

	lost chan struct{}
}

// NewKeeper returns the Keeper of lease, which Acquire just returned. It renews
// nothing until it is served.
func NewKeeper(reg *Registry, lease NodeLease, opts KeeperOptions) *Keeper {
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
	k := &Keeper{reg: reg, opts: opts, lost: make(chan struct{})}
	k.lease.Store(&lease)
	return k
}

func (k *Keeper) validUntil(l NodeLease) time.Time { return l.Renewed.Add(l.TTL - k.opts.Margin) }

// Lease returns the latest lease.
func (k *Keeper) Lease() NodeLease { return *k.lease.Load() }

// Check returns nil while the lease is good for acknowledging work, and the
// reason (ErrLeaseLost or ErrLeaseExpired) once it is not.
func (k *Keeper) Check() error {
	if k.err.Load() == nil && !time.Now().Before(k.validUntil(k.Lease())) {
		k.fail(ErrLeaseExpired)
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

		// Wake for the next renewal, or for expiry, whichever comes first.
		wake, expire := time.NewTimer(delay), time.NewTimer(time.Until(until))
		select {
		case <-ctx.Done():
			wake.Stop()
			expire.Stop()
			return supervisor.NoRestart(nil)
		case <-expire.C:
			wake.Stop()
			k.fail(ErrLeaseExpired)
			return supervisor.NoRestart(nil)
		case <-wake.C:
			expire.Stop()
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
			return supervisor.NoRestart(nil)
		case errors.Is(err, ErrLeaseLost):
			k.fail(err)
			return supervisor.NoRestart(nil)
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
