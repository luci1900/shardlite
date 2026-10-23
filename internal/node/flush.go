package node

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shard"
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
//  2. once all of those have queued their commits in the segment writer, the
//     writer is flushed: one segment for the whole node;
//  3. the cycle completes: shards whose capture and flush succeeded are marked
//     captured if the lease is still good, and the counter advances.
//
// The counters are what the durability gates use: a chunk of output read from
// the app is held until a cycle that began after it was read has completed,
// and the cycle's capture has queued the commits before it. Which store the
// chunk waits for is the kind's tier (docs/guide.md, Durability).
type Flusher struct {
	tab      *table          // the shards it syncs
	writer   *segment.Writer // the node's journal, flushed once per cycle
	keeper   *coord.Keeper   // the lease, which an acknowledgement needs
	log      *slog.Logger
	interval time.Duration
	min      time.Duration // shortest time between two cycle starts
	coalesce time.Duration // delay of a cycle that was asked for, so output arriving together shares it
	noEarly  bool          // tests: cycles run on the interval alone

	kick      chan struct{} // output is waiting for a cycle (capacity 1)
	waiting   atomic.Int64  // waiters in Wait that have not returned
	wantAfter atomic.Uint64 // the highest mark a waiter has asked for a cycle after

	started  atomic.Uint64
	captured notify.Counter // cycles whose capture has queued its commits
	queued   atomic.Pointer[queuedCount]
	done     notify.Counter // cycles completed

	exited chan struct{} // closed when the loop has ended (the shards are stopping)
}

// queuedCount is what a cycle's capture queued: the number of entries the
// writer holds, which the stores have to have written for the output that was
// read before the cycle may leave.
type queuedCount struct {
	cycle uint64
	count uint64
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

func newFlusher(tab *table, writer *segment.Writer, keeper *coord.Keeper, log *slog.Logger, opts FlushOptions) *Flusher {
	return &Flusher{tab: tab, writer: writer, keeper: keeper, log: log,
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

// syncTarget is one capture a cycle syncs: an active shard's, or the node
// database's.
type syncTarget struct {
	name    shard.Name
	cap     *captureState
	closing func() bool // it is being let go of meanwhile, so a failure is no news
	kick    func()      // after a capture: its new rows can be read, and are durable
}

type pendingSync struct {
	t   syncTarget
	h   capture.Header
	ok  bool
	err error // the sync's result
}

// targets are what a cycle syncs: the active shards (shards being let go of are
// left to their final sync).
func (f *Flusher) targets() []syncTarget {
	var out []syncTarget
	f.tab.shards(func(a *actor) {
		out = append(out, syncTarget{name: a.Name, cap: &a.cap,
			closing: func() bool { return !a.e.is(entryActive) },
			kick: func() {
				if o := a.loop.load(); o != nil {
					o.kick()
				}
			}})
	}, entryActive)
	return out
}

func (f *Flusher) cycle(ctx context.Context) {
	n := f.started.Add(1)

	// 1. Sync what changed, and 2. when every sync has queued what it captured
	// (however many files that is), write them all as one segment. The writer is
	// flushed on every cycle, even with no syncs of our own, in case something
	// else queued a file.
	var ops []*pendingSync
	var sg errgroup.Group
	sg.SetLimit(syncLimit())
	for _, t := range f.targets() {
		sh := t.cap.load()
		if sh == nil {
			continue
		}
		h, ok, changed := t.cap.changed()
		if !changed {
			continue
		}
		p := &pendingSync{t: t, h: h, ok: ok}
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
	// What the capture queued is published: a gate that waits for a store
	// waits for that many entries to be written there. The publication comes
	// before the counter, so a waiter that sees the counter sees this too.
	f.queued.Store(&queuedCount{cycle: n, count: f.writer.Queued()})
	f.captured.Inc()
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

	// 3. Completion.
	for _, p := range ops {
		err := p.err
		if err == nil {
			err = flushErr // queued, but never written
		}
		if err != nil {
			p.t.cap.synced(err)
			if ctx.Err() == nil && !p.t.closing() {
				f.log.Error("capturing a shard failed", "shard", p.t.name, "err", err)
			}
			continue
		}
		p.t.cap.synced(nil)
		p.t.cap.captured(p.h, p.ok)
		p.t.kick()
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
// committed before the call is in blob storage (the next completed cycle), and
// fails once the node's lease is lost.
func (m *Catalog) Barrier(ctx context.Context) error { return m.flush.Wait(ctx, m.flush.Mark()) }

// Gate holds a chunk of a shard's output until what the shard committed before
// it is durable (docs/internals.md, Durability gates):
//
//   - whatever the tier, the node's lease must be good: a node whose lease
//     has run out may have lost the shard to another node, and answers nothing;
//   - the chunk then waits for the store its tier names: the record (blob
//     storage) or the node's own disk, unless the shard has had no new frames
//     since its last capture (a shard whose header can't be read counts as
//     having them).
//
// Call it once the chunk has been read from the app. sync is the kind's tier
// or a Shardlite-Sync override.
func (m *Catalog) Gate(ctx context.Context, h *Hold, sync kind.Sync) error {
	return h.a.gate(ctx, sync)
}

// gate is Gate for the shard's own output, and System.Sync with the kind's
// tier.
func (a *actor) gate(ctx context.Context, sync kind.Sync) error {
	return a.d.flush.gate(ctx, &a.cap, sync)
}

// gate holds what depends on c's commits until they are durable, as Gate does
// for a shard: the lease must be good, and a cycle that began after the call
// must have captured the commits and written them to the store the tier names,
// unless c has nothing new.
func (f *Flusher) gate(ctx context.Context, c *captureState, sync kind.Sync) error {
	if err := f.keeper.Check(); err != nil {
		return err
	}
	mark := f.Mark()
	if !c.hasNewFrames() {
		return nil
	}
	switch sync {
	case kind.SyncLocal:
		if err := f.waitLocal(ctx, mark); err != nil {
			return err
		}
	case kind.SyncStorage:
		if err := f.Wait(ctx, mark); err != nil {
			return err
		}
	}
	return c.err()
}

// waitLocal waits until what was committed before the mark is durable in the
// node's own disk. The record is the fallback: the local tier's promise is the
// weaker one, and the gate waits for the record when the local store is slow
// or has given up.
func (f *Flusher) waitLocal(ctx context.Context, mark uint64) error {
	keeper := f.keeper
	f.waiting.Add(1)
	defer f.waiting.Add(-1)
	if f.done.Load() <= mark && f.started.Load() <= mark {
		// No cycle has begun since the mark: ask for one.
		f.request(mark)
	}
	target, err := f.queuedAfter(ctx, mark)
	if err != nil {
		return err
	}
	select {
	case <-f.writer.Reached(segment.StoreLocal, target):
		return keeper.Check()
	case <-f.writer.Reached(segment.StoreRecord, target):
		return keeper.Check() // the fallback: the record is the truth
	case <-f.exited:
		return errFlusherStopped
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-keeper.Lost():
		return keeper.Check()
	}
}

// queuedAfter waits for a capture that began after the mark and returns how
// many entries it queued. That capture holds the commits the chunk was read
// after, so the store has to have written that many entries before the chunk
// may leave.
func (f *Flusher) queuedAfter(ctx context.Context, mark uint64) (uint64, error) {
	for {
		pub := f.queued.Load()
		if pub != nil && pub.cycle > mark {
			return pub.count, nil
		}
		select {
		case <-f.captured.Reached(mark + 1):
		case <-f.exited:
			return 0, errFlusherStopped
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		}
	}
}
