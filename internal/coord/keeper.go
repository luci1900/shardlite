package coord

import (
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"sync"
	"time"
)

// KeeperOptions tune a Keeper.
type KeeperOptions struct {
	Interval time.Duration   // renew this often, 3 s if zero
	Margin   time.Duration   // treat the lease as expired this long before its TTL runs out, 2 s if zero
	Info     func() NodeInfo // what to publish at each renewal (counts, journal range)
	Logger   *slog.Logger
}

// Keeper renews a node's lease in the background and answers the one question
// the node asks before acknowledging anything: is the lease still good?
//
// The holder judges its own lease expired a safety margin before others do,
// counting from the moment it began the write that last renewed it (the
// lease's Renewed time), so a slow write or a clock that runs a little behind
// can't leave it acting on a lease others consider dead. Expiry is final: a
// node whose lease has run out may have lost shards to other nodes, so it
// stops for good and a restart takes a new epoch.
type Keeper struct {
	reg  NodeRegistry
	opts KeeperOptions

	mu    sync.Mutex
	lease NodeLease
	err   error // set once, final

	lost   chan struct{}
	stop   context.CancelFunc
	g      errgroup.Group // owns the renewal loop; Close joins it
	closed sync.Once
}

// StartKeeper starts renewing lease, which Acquire just returned.
func StartKeeper(reg NodeRegistry, lease NodeLease, opts KeeperOptions) *Keeper {
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
	ctx, cancel := context.WithCancel(context.Background())
	k := &Keeper{reg: reg, opts: opts, lease: lease, lost: make(chan struct{}), stop: cancel}
	k.g.Go(func() error { k.loop(ctx); return nil })
	return k
}

func (k *Keeper) validUntil() time.Time { return k.lease.Renewed.Add(k.lease.TTL - k.opts.Margin) }

// Lease returns the latest lease.
func (k *Keeper) Lease() NodeLease {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.lease
}

// Check returns nil while the lease is good for acknowledging work, and the
// reason (ErrLeaseLost or ErrLeaseExpired) once it is not.
func (k *Keeper) Check() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.err == nil && !time.Now().Before(k.validUntil()) {
		k.failLocked(ErrLeaseExpired)
	}
	return k.err
}

// Lost is closed once the lease is lost or has expired.
func (k *Keeper) Lost() <-chan struct{} { return k.lost }

func (k *Keeper) failLocked(err error) {
	if k.err == nil {
		k.err = err
		close(k.lost)
		k.opts.Logger.Error("node lease is no longer valid, the node must stop", "node", k.lease.Name, "epoch", k.lease.Epoch, "err", err)
	}
}

func (k *Keeper) fail(err error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.failLocked(err)
}

func (k *Keeper) loop(ctx context.Context) {
	k.mu.Lock()
	delay := time.Until(k.lease.Renewed.Add(k.opts.Interval))
	k.mu.Unlock()
	for {
		k.mu.Lock()
		until := k.validUntil()
		k.mu.Unlock()

		// Wake for the next renewal, or for expiry, whichever comes first.
		wake, expire := time.NewTimer(delay), time.NewTimer(time.Until(until))
		select {
		case <-ctx.Done():
			wake.Stop()
			expire.Stop()
			return
		case <-expire.C:
			wake.Stop()
			k.fail(ErrLeaseExpired)
			return
		case <-wake.C:
			expire.Stop()
		}

		k.mu.Lock()
		cur := k.lease
		k.mu.Unlock()
		// A renewal that is still going when the margin runs out is worth nothing.
		rctx, cancel := context.WithDeadline(ctx, until)
		renewed, err := k.reg.Renew(rctx, cur, k.opts.Info())
		cancel()
		switch {
		case err == nil:
			k.mu.Lock()
			k.lease = renewed
			delay = time.Until(renewed.Renewed.Add(k.opts.Interval))
			k.mu.Unlock()
		case ctx.Err() != nil:
			return
		case errors.Is(err, ErrLeaseLost):
			k.fail(err)
			return
		default:
			// Storage trouble. Try again soon, until the margin runs out.
			k.opts.Logger.Warn("lease renewal failed", "node", cur.Name, "err", err)
			delay = k.opts.Interval / 3
		}
	}
}

// Close stops renewing. With release, the lease is marked released (a
// graceful shutdown), unless it was already lost.
func (k *Keeper) Close(ctx context.Context, release bool) error {
	var err error
	k.closed.Do(func() {
		k.stop()
		k.g.Wait()
		if release {
			k.mu.Lock()
			l, ferr := k.lease, k.err
			k.mu.Unlock()
			if ferr == nil {
				l.NodeInfo = k.opts.Info()
				l.NodeInfo.Name = k.lease.Name
				err = k.reg.Release(ctx, l)
			}
		}
	})
	return err
}
