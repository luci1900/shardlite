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

// Flusher is the flush loop. Every interval it runs one cycle:
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

	started atomic.Uint64
	done    atomic.Uint64

	exited chan struct{} // closed when the loop has ended (the manager is closing)

	mu      sync.Mutex
	changed chan struct{} // closed and replaced whenever done changes or the loop fails
	fatal   error         // the journal's writer was fenced or closed: nothing will ever be flushed again
}

func newFlusher(m *Manager, interval time.Duration) *Flusher {
	return &Flusher{m: m, interval: interval, changed: make(chan struct{}), exited: make(chan struct{})}
}

func (f *Flusher) broadcast() {
	f.mu.Lock()
	close(f.changed)
	f.changed = make(chan struct{})
	f.mu.Unlock()
}

func (f *Flusher) loop(ctx context.Context) {
	defer close(f.exited)
	t := time.NewTicker(f.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		f.cycle(ctx)
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
	for _, a := range durable {
		sh := a.cap.Load()
		if sh == nil {
			continue
		}
		h, ok, changed := a.changed()
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
// if it came from the app). An ephemeral shard has nothing to wait for: the call
// returns at once, but still fails if the lease is lost.
func (m *Manager) BarrierFor(ctx context.Context, caller string) error {
	if caller != "" {
		m.mu.Lock()
		a := m.active[caller]
		m.mu.Unlock()
		if a != nil && a.ephemeral {
			return m.n.Keeper.Check()
		}
	}
	return m.flush.Wait(ctx, m.flush.Mark())
}

// Gate holds a chunk of a shard's output until what the shard committed before
// it is durable (design.md, Durability gates):
//
//   - with sync off the chunk goes straight out;
//   - with sync on it waits for a flush cycle that began after the chunk was
//     read, unless the shard has had no new frames since its last capture.
//
// Call it once the chunk has been read from the app. sync is the kind's
// setting or a Shardlite-Sync override.
func (m *Manager) Gate(ctx context.Context, r *Ref, sync bool) error {
	if !sync || r.a.ephemeral { // nothing of an ephemeral shard is shipped, so there is nothing to wait for
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
