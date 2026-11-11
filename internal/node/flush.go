package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/notify"
)

// Defaults of the flush timing, chosen from the measurements in
// measure_flush_test.go.
const (
	// DefaultFlushMin is the shortest time between the starts of two cycles
	// (SHARDLITE_FLUSH_MIN): it caps the segment PUT rate when gated writes
	// keep arriving.
	DefaultFlushMin = 100 * time.Millisecond
	// DefaultFlushCoalesce is how long a cycle that output is waiting for is
	// held back, so that output arriving together shares it.
	DefaultFlushCoalesce = 5 * time.Millisecond
)

// Flusher is the flush loop. It runs one cycle every interval, and sooner when
// output is waiting for one (Wait): then a cycle starts after a short
// coalescing delay, or as soon as the running one completes, but never less
// than the minimum spacing after the previous start. Each cycle does:
//
//  1. every active shard whose wal-index header differs from the one at its last
//     completed capture, or whose header can't be read, is synced
//     (Shard.Capture), each in a goroutine of the cycle's own, a bounded number
//     at a time;
//  2. once all of those have queued their commits in the journal writer, the
//     writer is flushed: one segment for the whole node;
//  3. the cycle completes: shards whose capture and flush succeeded are marked
//     captured if the lease is still good, and the counter advances.
//
// The counters are what the durability gates use: a chunk of output read from
// the app is held until a cycle that began after it was read has completed,
// and the cycle's capture has queued the commits before it. Which store the
// chunk waits for is the kind's tier (docs/guide.md, Durability).
type Flusher struct {
	tab      *table           // the shards it syncs
	writer   *journal.Writer  // the node's journal, flushed once per cycle
	keeper   *lease.Keeper    // the lease, which an acknowledgement needs
	conf     *lease.Confirmer // reads the lease, to confirm that the node still owns what it read
	log      *slog.Logger
	interval time.Duration
	min      time.Duration // shortest time between two cycle starts
	coalesce time.Duration // delay of a cycle that was asked for, so output arriving together shares it
	noEarly  bool          // tests: cycles run on the interval alone
	// unconfirmed is the Unconfirmed fault: confirm proves nothing.
	unconfirmed bool

	kick      chan struct{} // output is waiting for a cycle (capacity 1)
	waiting   atomic.Int64  // waiters in Wait that have not returned
	wantAfter atomic.Uint64 // the highest mark a waiter has asked for a cycle after

	started atomic.Uint64
	done    notify.Counter // cycles completed
	// landed is the number of the latest cycle that created a segment in
	// blob storage, which proves the node still owned its shards when it landed
	// (confirm). Only the loop stores it, before it counts the cycle done.
	landed atomic.Uint64

	exited chan struct{} // closed when the loop has ended (the shards are stopping)
}

// FlushOptions configure the flush loop.
type FlushOptions struct {
	Interval time.Duration // flush interval, 250 ms if zero
	// Min is the shortest time between the starts of two flush cycles, DefaultFlushMin if zero.
	Min time.Duration
	// Coalesce is how long a cycle that output is waiting for is held back so
	// that output arriving together shares it, DefaultFlushCoalesce if zero.
	Coalesce time.Duration
	// NoEarly makes cycles run on the interval alone, for tests that decide when one runs.
	NoEarly bool
}

func newFlusher(tab *table, writer *journal.Writer, keeper *lease.Keeper, conf *lease.Confirmer, log *slog.Logger, opts FlushOptions) *Flusher {
	return &Flusher{tab: tab, writer: writer, keeper: keeper, conf: conf, log: log,
		interval: opts.Interval, min: opts.Min, coalesce: opts.Coalesce, noEarly: opts.NoEarly,
		kick: make(chan struct{}, 1), exited: make(chan struct{})}
}

// request asks for a cycle that begins after mark.
func (f *Flusher) request(mark uint64) {
	for {
		cur := f.wantAfter.Load()
		if mark <= cur || f.wantAfter.CompareAndSwap(cur, mark) {
			break
		}
	}
	select {
	case f.kick <- struct{}{}:
	default:
	}
}

// pending reports whether somebody who is still waiting needs a cycle that has
// not completed. A waiter that gave up (its context ended) doesn't count.
func (f *Flusher) pending() bool {
	return f.waiting.Load() > 0 && f.done.Load() <= f.wantAfter.Load()
}

// sleep waits d, and reports false if ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (f *Flusher) String() string { return "flusher" }

// Serve is the flusher's loop, as a service.
func (f *Flusher) Serve(ctx context.Context) error {
	defer close(f.exited)
	tick := time.NewTimer(f.interval)
	defer tick.Stop()
	var last time.Time // when the last cycle started
	kicked := false    // output asked for a cycle while the last one ran
	for {
		early := kicked
		if !early {
			select {
			case <-ctx.Done():
				return nil
			case <-tick.C:
			case <-f.kick:
				if f.noEarly || !f.pending() {
					continue
				}
				early = true
				if !sleep(ctx, f.coalesce) { // so that output arriving together shares the cycle
					return nil
				}
			}
		}
		// Never closer to the last start than the minimum, whatever starts this one.
		if !last.IsZero() && !sleep(ctx, f.min-time.Since(last)) {
			return nil
		}
		if early && !f.pending() {
			kicked = false // everybody who waited has gone: nobody is flushed for
			continue
		}
		last = time.Now()
		tick.Reset(f.interval) // the interval only bounds the writes that nobody waits for
		f.cycle(ctx)
		kicked = false
		select {
		case <-f.kick:
			kicked = !f.noEarly && f.pending()
		default:
		}
	}
}

// syncLimit is how many shards a cycle syncs at once. A sync is CPU work
// (reading frames, encoding and checksumming LTX) and short syscalls, so more
// syncs than cores only adds OS threads (each blocking cgo call holds one).
// GOMAXPROCS follows container CPU limits.
func syncLimit() int { return runtime.GOMAXPROCS(0) }

type pendingSync struct {
	a   *actor
	h   capture.Header
	ok  bool
	err error // the sync's result
}

// targets are what a cycle syncs: the active shards (shards being let go of are
// left to their final sync).
func (f *Flusher) targets() []*actor {
	var out []*actor
	f.tab.shards(func(a *actor) { out = append(out, a) }, entryActive)
	return out
}

func (f *Flusher) cycle(ctx context.Context) {
	n := f.started.Add(1)
	queuedBefore := f.writer.Queued()

	// 1. Sync what changed, and 2. when every sync has queued what it captured
	// (however many files that is), write them all as one segment. The writer is
	// flushed on every cycle, even with no syncs of our own, in case something
	// else queued a file.
	var ops []*pendingSync
	var sg errgroup.Group
	sg.SetLimit(syncLimit())
	for _, a := range f.targets() {
		sh := a.cap.load()
		if sh == nil {
			continue
		}
		h, ok, changed := a.cap.changed()
		if !changed {
			continue
		}
		p := &pendingSync{a: a, h: h, ok: ok}
		ops = append(ops, p)
		sg.Go(func() error {
			p.err = sh.Capture(ctx) // each shard's own result: one failing must not cancel the rest
			return nil
		})
	}
	sg.Wait()
	if ctx.Err() != nil {
		return
	}
	queuedAfter := f.writer.Queued()
	flushErr := f.writer.Flush(ctx)
	if flushErr != nil && ctx.Err() == nil {
		f.log.Error("flushing the node journal failed", "err", flushErr)
		// A fenced or closed writer never recovers.
	}
	if flushErr == nil {
		// Acknowledging is the moment the lease has to be good: nothing written
		// after it ran out counts as durable (the keeper logs the loss).
		flushErr = f.keeper.Check()
	}
	if flushErr == nil && queuedAfter > queuedBefore {
		// The capture queued entries after the cycle began, and the flush
		// returned once they were in blob storage: a segment was created by a
		// cycle that began after any mark below n.
		f.landed.Store(n)
	}

	// 3. Completion.
	for _, p := range ops {
		err := p.err
		if err == nil {
			err = flushErr // queued, but never written
		}
		if err != nil {
			p.a.cap.synced(err)
			// A shard being let go of meanwhile fails with no news.
			if ctx.Err() == nil && p.a.e.is(entryActive) {
				f.log.Error("capturing a shard failed", "shard", p.a.Name, "err", err)
			}
			continue
		}
		p.a.cap.synced(nil)
		p.a.cap.captured(p.h, p.ok)
		// Its new rows can be read, and are durable.
		if o := p.a.loop.load(); o != nil {
			o.kick()
		}
	}
	f.done.Inc()
}

// WaitCycle waits for a cycle that begins after the call to complete: every
// file queued before it is then in blob storage. Shards use it as their
// Config.Durable, so deactivations share segments with everything else. The
// flush loop stops only once every shard has been let go of, so there is always
// a cycle to wait for.
func (f *Flusher) WaitCycle(ctx context.Context) error { return f.Wait(ctx, f.Mark()) }

var errFlusherStopped = errors.New("node: the flush loop has stopped")

// Mark is taken when a chunk is read. Wait with it returns once a cycle that
// began after the Mark has completed.
func (f *Flusher) Mark() uint64 { return f.started.Load() }

// Completed is the number of cycles completed so far.
func (f *Flusher) Completed() uint64 { return f.done.Load() }

// Wait blocks until a cycle that began after mark has completed, and fails if
// the node's lease is no longer good by then (or ctx ends first).
func (f *Flusher) Wait(ctx context.Context, mark uint64) error {
	keeper := f.keeper
	f.waiting.Add(1)
	defer f.waiting.Add(-1)
	if f.done.Load() <= mark && f.started.Load() <= mark {
		// No cycle has begun since the mark: ask for one. (If one has, it is the one
		// this waits for, and the loop starts the next if more output waits.)
		f.request(mark)
	}
	if f.done.Load() > mark {
		// Acknowledging is the moment the lease has to be good.
		return keeper.Check()
	}
	if err := keeper.Check(); err != nil {
		return err
	}
	select {
	case <-f.done.Reached(mark + 1):
		return keeper.Check()
	case <-f.exited:
		return errFlusherStopped
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-keeper.Lost():
		return keeper.Check()
	}
}

// Barrier is what POST /_shardlite/sync does: it returns once everything
// committed before the call is in blob storage (the next completed cycle) and
// the node has confirmed that it still owns its shards, and fails once the
// node's lease is lost. A process that reads, calls sync and answers its client
// is then linearizable.
func (m *Shardset) Barrier(ctx context.Context) error {
	mark := m.flush.Mark()
	if err := m.flush.Wait(ctx, mark); err != nil {
		return err
	}
	return m.flush.confirm(ctx, mark)
}

// Name is the node's own name.
func (m *Shardset) Name() cluster.Node { return m.n.Name }

// Gate holds a chunk of a shard's output until what the shard committed before
// it is durable (docs/internals.md, Durability gates):
//
//   - whatever the tier, the node's lease must be good: a node whose lease
//     has run out may have lost the shard to another node, and answers nothing;
//   - the chunk then waits for the store its tier names: blob
//     storage or the node's own disk, unless the shard has had no new frames
//     since its last capture (a shard whose header can't be read counts as
//     having them).
//
// Call it once the chunk has been read from the app. sync is the kind's tier
// or a Shardlite-Sync override.
func (m *Shardset) Gate(ctx context.Context, h *Hold, sync kind.Sync) error {
	err := h.a.gate(ctx, sync, h.proven.Load())
	if err == nil && sync == kind.SyncStorage {
		h.proven.Store(true)
	}
	return err
}

// gate is Gate for the shard's own output. proven says an earlier chunk of the request has confirmed ownership.
func (a *actor) gate(ctx context.Context, sync kind.Sync, proven bool) error {
	return a.d.flush.gate(ctx, &a.cap, sync, proven)
}

// gate holds what depends on c's commits until they are durable, as Gate does
// for a shard: the lease must be good, and a cycle that began after the call
// must have captured the commits and written them to the store the tier names,
// unless c has nothing new. A storage chunk also waits to be confirmed (confirm),
// unless an earlier chunk of its request was (proven). A tier the node cannot
// meet fails the chunk: an answer of a weaker tier is never given instead.
func (f *Flusher) gate(ctx context.Context, c *captureState, sync kind.Sync, proven bool) error {
	if err := f.keeper.Check(); err != nil {
		return err
	}
	if !sync.Valid() {
		err := tierUnmet(fmt.Sprintf("sync is %d, which is not a durability tier", int(sync)))
		f.log.Error("an answer was gated on no durability tier, which is a bug", "err", err)
		return err
	}
	mark := f.Mark()
	if !c.hasNewFrames() {
		if sync == kind.SyncStorage && !proven {
			return f.confirm(ctx, mark)
		}
		return nil
	}
	if sync == kind.SyncStorage {
		if err := f.Wait(ctx, mark); err != nil {
			return err
		}
		// The cycle that passed may have found the commits captured already by
		// one that began before the mark, and created no segment of its own.
		if !proven {
			if err := f.confirm(ctx, mark); err != nil {
				return err
			}
		}
	}
	return c.err()
}

// confirm holds a storage answer that has nothing new to commit until the node
// has proof that it still owned its shards after the chunk was read (the mark),
// whichever comes first: a segment created in blob storage by a cycle that began
// after the mark, or a read of the node's own lease that began after it
// (docs/internals.md, Confirming ownership). A lease found taken fails the node
// as a lost renewal does. A lease that can't be read fails the answer with a
// retry hint, never an unconfirmed one.
func (f *Flusher) confirm(ctx context.Context, mark uint64) error {
	if err := f.keeper.Check(); err != nil {
		return err
	}
	if f.unconfirmed || f.landed.Load() > mark {
		return f.keeper.Check()
	}
	round := f.conf.Join()
	defer round.Leave()
	for {
		select {
		case <-round.Done():
			switch err := round.Err(); {
			case err == nil:
				return f.keeper.Check()
			case errors.Is(err, lease.ErrLost):
				return f.keeper.Check()
			case errors.Is(err, lease.ErrConfirmerStopped):
				return errFlusherStopped
			default:
				return tierUnmet("sync is storage, but the node could not confirm that it still owns its shards: " + err.Error())
			}
		case <-f.done.Reached(f.done.Load() + 1):
			// A cycle completed: it may have created a segment. (A wait on the
			// counter of landings would stay registered on a node with no writes.)
			if f.landed.Load() > mark {
				return f.keeper.Check()
			}
		case <-f.exited:
			return errFlusherStopped
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-f.keeper.Lost():
			return f.keeper.Check()
		}
	}
}

// tierUnmet is the error a tier's gate returns when the node cannot meet it:
// the answer fails with 503 and a retry hint, and a weaker tier is never
// given instead (docs/guide.md, Durability).
func tierUnmet(msg string) error { return &unavailable{reasonSyncUnmet, msg, time.Second} }
