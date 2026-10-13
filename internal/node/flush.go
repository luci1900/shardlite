package node

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/capture"
)

// Defaults of the flush timing, chosen from the measurements in plans/stage-1.2-report.md.
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
//     completed capture is synced (Shard.Capture), each in a goroutine of the
//     cycle's own, a bounded number at a time;
//  2. once all of those have queued their commits in the segment writer, the
//     writer is flushed: one segment for the whole node;
//  3. the cycle completes: shards whose sync and flush succeeded are marked
//     captured, and the counter advances.
//
// The counters are what the durability gates use: a chunk of output read from
// the app is held until a cycle that began after it was read has completed.
type Flusher struct {
	m        *Manager
	interval time.Duration
	min      time.Duration // shortest time between two cycle starts
	coalesce time.Duration // delay of a cycle that was asked for, so output arriving together shares it
	noEarly  bool          // tests: cycles run on the interval alone

	kick      chan struct{} // output is waiting for a cycle (capacity 1)
	waiting   atomic.Int64  // waiters in Wait that have not returned
	wantAfter atomic.Uint64 // the highest mark a waiter has asked for a cycle after

	started atomic.Uint64
	done    atomic.Uint64

	exited chan struct{} // closed when the loop has ended (the manager is closing)

	mu      sync.Mutex
	changed chan struct{} // closed and replaced whenever done changes or the loop fails
	fatal   error         // the journal's writer was fenced or closed: nothing will ever be flushed again
}

func newFlusher(m *Manager, interval, min, coalesce time.Duration, noEarly bool) *Flusher {
	return &Flusher{m: m, interval: interval, min: min, coalesce: coalesce, noEarly: noEarly,
		kick: make(chan struct{}, 1), changed: make(chan struct{}), exited: make(chan struct{})}
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

func (f *Flusher) broadcast() {
	f.mu.Lock()
	close(f.changed)
	f.changed = make(chan struct{})
	f.mu.Unlock()
}

func (f *Flusher) loop(ctx context.Context) {
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
				return
			case <-tick.C:
			case <-f.kick:
				if f.noEarly || !f.pending() {
					continue
				}
				early = true
				if !sleep(ctx, f.coalesce) { // so that output arriving together shares the cycle
					return
				}
			}
		}
		// Never closer to the last start than the minimum, whatever starts this one.
		if !last.IsZero() && !sleep(ctx, f.min-time.Since(last)) {
			return
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
// (reading frames, encoding and checksumming LTX) and short syscalls, with
// Litestream's metadata on tmpfs, so more syncs than cores only adds OS threads
// (each blocking cgo call holds one). GOMAXPROCS follows container CPU limits.
func syncLimit() int { return runtime.GOMAXPROCS(0) }

type pendingSync struct {
	a   *ActiveShard
	h   capture.Header
	ok  bool
	err error // the sync's result

	snap *snapshot // what was read just before the capture, delivered once it is durable
}

func (f *Flusher) cycle(ctx context.Context) {
	m := f.m
	f.started.Add(1)

	m.mu.Lock()
	var durable []*ActiveShard
	for _, a := range m.active {
		if !a.closing {
			durable = append(durable, a)
		}
	}
	m.mu.Unlock()

	// 1. Sync what changed, and 2. when every sync has queued what it captured
	// (however many files that is), write them all as one segment. The writer is
	// flushed on every cycle, even with no syncs of our own, in case something
	// else queued a file.
	var ops []*pendingSync
	var sg errgroup.Group
	sg.SetLimit(syncLimit())
	msgs := m.msgs.enabled()
	for _, a := range durable {
		sh := a.cap.Load()
		if sh == nil {
			continue
		}
		h, ok, changed := a.changed()
		// A shard that did not change is still read when rows were left unread
		// (activation, or more than fit): it has nothing new to capture, so what
		// it holds is durable once this cycle completes.
		read := msgs && (changed || a.msgs.wantsRead())
		if !changed && !read {
			continue
		}
		p := &pendingSync{a: a, h: h, ok: ok}
		ops = append(ops, p)
		sg.Go(func() error {
			if read {
				// Before the capture starts, so that everything the read shows is in it.
				var err error
				if p.snap, err = a.msgs.read(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, capture.ErrClosed) {
					m.log.Warn("could not read a shard's outbox and inbox", "shard", a.Name, "err", err)
				}
			}
			p.err = sh.Capture(ctx) // each shard's own result: one failing must not cancel the rest
			return nil
		})
	}
	sg.Wait()
	if ctx.Err() != nil {
		return
	}
	flushErr := m.n.Writer.Flush(ctx)
	if flushErr != nil && ctx.Err() == nil {
		m.log.Error("flushing the node journal failed", "err", flushErr)
		// A fenced or closed writer never recovers.
	}

	// 3. Completion.
	for _, p := range ops {
		err := p.err
		if err == nil {
			err = flushErr // queued, but never written
		}
		m.mu.Lock()
		closing := p.a.closing
		m.mu.Unlock()
		if err != nil {
			p.a.syncErr.Store(&err)
			if ctx.Err() == nil && !closing {
				m.log.Error("capturing a shard failed", "shard", p.a.Name, "err", err)
			}
			continue
		}
		p.a.syncErr.Store(nil)
		p.a.captured(p.h, p.ok)
		p.a.msgs.afterRead(p.snap)
		m.msgs.start(p.a.msgs, p.snap) // durable now
	}
	f.done.Add(1)
	f.broadcast()
}

// WaitCycle waits for a cycle that begins after the call to complete: every
// file queued before it is then in blob storage. Shards use it as their
// Config.Durable, so deactivations share segments with everything else.
//
// Once the loop has ended (the manager is closing and deactivating its shards)
// there are no more cycles, and it flushes the writer itself.
func (f *Flusher) WaitCycle(ctx context.Context) error {
	err := f.Wait(ctx, f.Mark())
	if errors.Is(err, errFlusherStopped) {
		return f.m.n.Writer.Flush(ctx)
	}
	return err
}

var errFlusherStopped = errors.New("node: the flush loop has stopped")

// Mark is taken when a chunk is read. Wait with it returns once a cycle that
// began after the Mark has completed.
func (f *Flusher) Mark() uint64 { return f.started.Load() }

// Completed is the number of cycles completed so far.
func (f *Flusher) Completed() uint64 { return f.done.Load() }

// Wait blocks until a cycle that began after mark has completed, and fails if
// the node's lease is no longer good by then (or ctx ends first).
func (f *Flusher) Wait(ctx context.Context, mark uint64) error {
	keeper := f.m.n.Keeper
	f.waiting.Add(1)
	defer f.waiting.Add(-1)
	if f.done.Load() <= mark && f.started.Load() <= mark {
		// No cycle has begun since the mark: ask for one. (If one has, it is the one
		// this waits for, and the loop starts the next if more output waits.)
		f.request(mark)
	}
	for {
		f.mu.Lock()
		ch := f.changed
		f.mu.Unlock()
		if f.done.Load() > mark {
			// Acknowledging is the moment the lease has to be good.
			return keeper.Check()
		}
		if err := keeper.Check(); err != nil {
			return err
		}
		select {
		case <-ch:
		case <-f.exited:
			return errFlusherStopped
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-keeper.Lost():
			return keeper.Check()
		}
	}
}

// Barrier is what POST /_shardlite/sync does: it returns once everything
// committed before the call is in blob storage (the next completed cycle), and
// fails once the node's lease is lost.
func (m *Manager) Barrier(ctx context.Context) error { return m.BarrierFor(ctx, "") }

// BarrierFor is Barrier for a call made by the shard named caller (""
// if it came from the app).
func (m *Manager) BarrierFor(ctx context.Context, caller string) error {
	return m.flush.Wait(ctx, m.flush.Mark())
}

// Gate holds a chunk of a shard's output until what the shard committed before
// it is durable (docs/internals.md, Durability gates):
//
//   - with sync off the chunk goes straight out;
//   - with sync on it waits for a flush cycle that began after the chunk was
//     read, unless the shard has had no new frames since its last capture.
//
// Call it once the chunk has been read from the app. sync is the kind's
// setting or a Shardlite-Sync override.
func (m *Manager) Gate(ctx context.Context, r *Ref, sync bool) error {
	if !sync {
		return nil
	}
	mark := m.flush.Mark()
	if !r.a.hasNewFrames() {
		return nil
	}
	if err := m.flush.Wait(ctx, mark); err != nil {
		return err
	}
	if e := r.a.syncErr.Load(); e != nil && *e != nil {
		return *e
	}
	return nil
}
