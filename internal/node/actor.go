package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/system"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// LifecycleOptions configure each shard's lifecycle loop: its idle and delta
// timers and what a snapshot is.
type LifecycleOptions struct {
	// NoIdle keeps shards active however long they are idle, for tests that decide when one is let go of.
	NoIdle bool
	// A shard gets a delta when its tail is DeltaAge old or holds DeltaSize bytes,
	// whichever comes first. A tail older than 3 times DeltaAge raises an alert.
	DeltaAge      time.Duration // shardfile.DefaultDeltaAge if zero
	DeltaSize     int64         // shardfile.DefaultDeltaSize if zero
	MaxFiles      int           // files a manifest may list before the next snapshot rebases the shard, shardfile.DefaultMaxFiles if zero
	SnapshotEvery time.Duration // how often each active shard looks whether it needs a snapshot (a delta or a rebase), and the tail alert is checked, 2 s if zero
	HoldOff       time.Duration // how long to leave a shard alone after it couldn't be deactivated, 5 s if zero
	IdleRecheck   time.Duration // the first wait before an idle shard that its reminders keep active is looked at again (it doubles up to 10 s), 1 s if zero
}

// actorDeps is what the shards' loops use, shared by every shard: the parts of
// the Catalog and of the node that a shard calls, and the node-wide limits. The
// Catalog makes it, and no shard reaches the Catalog itself.
type actorDeps struct {
	opts    LifecycleOptions
	kinds   *Kinds
	sys     systemKinds                                 // the system kinds' registry: fixed ids, migrations and handlers
	call    func(*http.Request) (*http.Response, error) // the node's calls to system shards (routing)
	clock   Clock
	log     *slog.Logger
	stats   *Stats
	tab     *table
	cache   *cache
	flush   *Flusher
	rem     *reminders
	files   *shardfile.Files
	writer  *segment.Writer
	tempDir *tempfile.Dir // the node's temporary files (Config.TempDir)

	// deletedKinds holds the kinds that have had a shard deleted for holding
	// nothing since this process started, so only the first of each is logged at
	// info. Written once per kind (LoadOrStore), by whichever loop gets there first.
	deletedKinds sync.Map

	// base is the context everything the shards do runs on: it ends only once
	// the Catalog has let go of them (or abandoned them).
	base context.Context

	// The node-wide limits, which shards' loops take a place in.
	deactivations chan struct{}
	snapshots     chan struct{}
	rebases       chan struct{}
}

func (d *actorDeps) now() time.Time { return d.clock.Now() }

// actor is one shard's actor on this node (docs/internals.md, Concurrency):
// the table runs its Serve, the lifecycle loop, which runs the shard's reminders
// loop while the shard is captured. The loop does one operation at a time:
// activation (its first job), a delta or rebase when its delta timer says so,
// deactivation once its idle timer finds it unused for its kind's timeout, and
// the deactivations, evictions and deletes that other goroutines ask for (do).
// Everything it does runs on the shards' base context, not on the context the
// table gives it, so stopping the shards doesn't cut off a release.
//
// References are not the loop's: requests take them in the shard's entry. The
// shard's state is in plain parts, each with the methods that change it: its
// capture, its idle timer's bookkeeping, its reminders loop, its share of the
// tail and the counters tests wait on.
type actor struct {
	d *actorDeps
	e *entry

	Name shard.Name
	Gen  shard.Gen
	Path string
	h    *shardfile.Handle
	read *shardfile.Handle // the manifest Acquire read before adding the entry, nil if it read none; activation starts from it

	sys *kind.Kind   // the shard's system kind, nil for an app kind's shard (whose settings are looked up as they are used, since a reload may change them)
	app http.Handler // its system app, built for this activation

	ctx   context.Context // the table's context for the loop, which the reminders loop runs on (set as the loop starts)
	ops   chan func()     // requests for the lifecycle loop (do)
	ready chan struct{}   // closed once the shard is activated
	ended chan struct{}   // closed once the lifecycle loop has ended
	gone  bool            // the shard was let go of or deleted (the loop's own until ended is closed)

	cap   captureState
	idle  idleState
	loop  loopSlot
	tail  tailState
	looks actorLooks
}

func newActor(d *actorDeps, e *entry) *actor {
	a := &actor{d: d, e: e, Name: e.name, ops: make(chan func()), ready: make(chan struct{}), ended: make(chan struct{})}
	if a.sys = d.sys[a.Name.Kind()]; a.sys != nil {
		a.app = a.sys.Handler(&system.System{Name: a.Name, Tx: a.tx, Call: a.call, Sync: func(ctx context.Context) error { return a.gate(ctx, kind.SyncStorage, false) }})
	}
	return a
}

// captureState is the shard's capture as the rest of the node sees it: the
// capture itself, the one the lease counts, the last sync's result and the
// wal-index header as of the last completed capture, which tells the flush
// cycle and the gates whether the shard has new frames.
type captureState struct {
	cur     atomic.Pointer[capture.Shard]
	lease   atomic.Pointer[capture.Shard] // the capture the lease counts and whose tail its journal range covers, from before the capture's first sync until it is let go
	syncErr atomic.Pointer[error]

	last atomic.Pointer[lastHeader] // nil until a capture completes
}

// lastHeader is the wal-index header as of the last completed capture. ok is
// false if it couldn't be read, and then it is not compared.
type lastHeader struct {
	h  capture.Header
	ok bool
}

func (c *captureState) load() *capture.Shard { return c.cur.Load() }

// start makes sh the shard's capture, with the header read now as its last
// completed one.
func (c *captureState) start(sh *capture.Shard) {
	c.cur.Store(sh)
	c.captured(c.header())
}

// counted makes the lease count sh; uncounted stops it, if sh is the one counted.
func (c *captureState) counted(sh *capture.Shard)   { c.lease.Store(sh) }
func (c *captureState) uncounted(sh *capture.Shard) { c.lease.CompareAndSwap(sh, nil) }

// synced records the result of the shard's last sync.
func (c *captureState) synced(err error) {
	if err == nil {
		c.syncErr.Store(nil)
		return
	}
	c.syncErr.Store(&err)
}

// err is the last sync's failure, nil if it succeeded.
func (c *captureState) err() error {
	if e := c.syncErr.Load(); e != nil {
		return *e
	}
	return nil
}

// header reads the wal-index header of a captured shard: ok is false if it
// could not be read, and the shard counts as changed then.
func (c *captureState) header() (h capture.Header, ok bool) {
	sh := c.load()
	if sh == nil {
		return h, false
	}
	h, err := sh.Header()
	return h, err == nil
}

// captured records the header read before a capture started: commits that
// landed after the read are captured too, and just cause one more sync.
func (c *captureState) captured(h capture.Header, ok bool) {
	c.last.Store(&lastHeader{h, ok})
}

// changed reports whether the header differs from the last completed capture,
// and returns the header it read.
func (c *captureState) changed() (h capture.Header, ok, changed bool) {
	h, ok = c.header()
	last := c.last.Load()
	return h, ok, !ok || last == nil || !last.ok || h != last.h
}

// hasNewFrames reports whether the shard has commits that are not captured.
func (c *captureState) hasNewFrames() bool {
	_, _, ch := c.changed()
	return ch
}

// idleState is the idle timer's bookkeeping: when a shard that was held off is
// looked at again, whether the app held the file at the last deactivation, and
// how long the shard's reminders have kept it. Only the lifecycle loop changes
// it (tests read nextLook).
type idleState struct {
	nextLook  atomic.Int64  // when an idle shard that was held off is looked at again (Unix ns, 0 until it first is)
	held      bool          // the last deactivation found the app holding the file
	heldSince time.Time     // when live rows were first found holding the shard
	warned    time.Time     // when that was last logged
	wait      time.Duration // the next wait while live rows hold it
}

// holdOff delays the next look by d, and returns d.
func (s *idleState) holdOff(now time.Time, d time.Duration) time.Duration {
	s.nextLook.Store(now.Add(d).UnixNano())
	return d
}

// keptBy notes whether live rows keep the shard active, doubling the wait
// before the next look from recheck up to 10 s while they do. warn is how long
// they have kept it, when that is due to be logged again (0 otherwise).
func (s *idleState) keptBy(live bool, now time.Time, recheck time.Duration) (held bool, warn time.Duration) {
	if !live {
		s.heldSince, s.wait = time.Time{}, 0
		return false, 0
	}
	if s.heldSince.IsZero() {
		s.heldSince, s.warned = now, now
	}
	s.wait = min(max(2*s.wait, recheck), 10*time.Second)
	if now.Sub(s.heldSince) >= holdWarnAfter && now.Sub(s.warned) >= holdWarnAfter {
		s.warned = now
		return true, now.Sub(s.heldSince)
	}
	return true, 0
}

// loopSlot is the shard's reminders loop: the one running (the last one stays
// once it is removed) and the group it runs in. Only the lifecycle loop changes
// it. Others load the loop.
type loopSlot struct {
	cur   atomic.Pointer[reminderLoop]
	run   *service.Group
	on    bool
	quiet bool // stopped for the shards' shutdown: no loop starts any more
}

func (s *loopSlot) load() *reminderLoop { return s.cur.Load() }

// tailState is the shard's share of Stats.TailBytes (the loop's) and when the
// watchdog last raised the tail alert for it (the watchdog's).
type tailState struct {
	bytes     int64
	alertedAt time.Time
}

// count makes n the shard's part of TailBytes.
func (t *tailState) count(st *Stats, n int64) {
	total := st.TailBytes.Add(n - t.bytes)
	t.bytes = n
	for {
		top := st.TailBytesMax.Load()
		if total <= top || st.TailBytesMax.CompareAndSwap(top, total) {
			return
		}
	}
}

// actorLooks count the looks of the shard's timers, for tests.
type actorLooks struct {
	idle notify.Counter // looks of the idle timer
	snap notify.Counter // looks of the delta timer
}

func (a *actor) String() string { return a.Name.String() }

// Serve is the lifecycle loop. It runs until the shard is let go of, its
// activation fails or ctx ends.
func (a *actor) Serve(ctx context.Context) {
	defer close(a.ended)
	a.ctx = ctx
	d := a.d
	if err := a.activate(d.base); err != nil {
		d.tab.remove(a.e, err)
		return
	}
	a.addLoop()
	d.stats.Activations.Add(1)
	close(a.ready)
	a.e.activated(d.now())
	defer func() {
		// A wake in progress ends before the name is free again.
		if err := a.removeLoop(); err != nil && ctx.Err() == nil {
			d.log.Error("could not stop a shard's reminders", "shard", a.Name, "err", err)
		}
		a.tail.count(d.stats, 0)
		if a.gone {
			d.tab.remove(a.e, nil)
		} else {
			// Not let go of (the node is stopping): the capture's activation ends
			// with the shard's loop, and nothing more is written.
			if sh := a.cap.load(); sh != nil {
				sh.Abandon()
			}
			a.e.stop()
		}
	}()

	var idle <-chan time.Time
	stopIdle := func() bool { return false }
	if !d.opts.NoIdle && a.sys == nil { // a system shard's fixed id never goes idle
		idle, stopIdle = d.clock.NewTimer(a.idleTimeout())
	}
	defer func() { stopIdle() }()
	snap, stopSnap := d.clock.NewTicker(d.opts.SnapshotEvery)
	defer stopSnap()
	for !a.gone {
		select {
		case <-ctx.Done():
			return // the shards stopped without letting go of this one
		case <-idle:
			wait := a.lookIdle(d.base)
			if !a.gone {
				idle, stopIdle = d.clock.NewTimer(wait)
			}
			a.looks.idle.Inc() // once the next look is set, so that a test moving the clock after it reaches that look
		case <-snap:
			a.snapshot(d.base)
			a.looks.snap.Inc()
		case op := <-a.ops:
			op()
		}
	}
}

func (a *actor) idleTimeout() time.Duration {
	if k, err := a.d.kinds.Lookup(a.Name.Kind()); err == nil {
		return k.IdleTimeout
	}
	return DefaultIdleTimeout
}

// lookIdle is what the idle timer does: a shard nobody has used for its kind's
// idle timeout is let go of, unless its reminders keep it or it can't be. It
// returns when to look again.
func (a *actor) lookIdle(ctx context.Context) time.Duration {
	d := a.d
	idle := a.idleTimeout()
	refs, last := a.e.use()
	if refs > 0 {
		return idle
	}
	if left := last.Add(idle).Sub(d.now()); left > 0 {
		return left
	}
	if a.holds(ctx) {
		return a.idle.holdOff(d.now(), a.idle.wait) // due reminders keep the shard active
	}
	ok, err := a.deactivate(ctx)
	switch {
	case err != nil:
		if ctx.Err() == nil {
			d.log.Warn("could not deactivate an idle shard", "shard", a.Name, "err", err)
		}
		return a.idle.holdOff(d.now(), d.opts.HoldOff)
	case !ok && a.idle.held:
		return a.idle.holdOff(d.now(), d.opts.HoldOff) // the app still has the file open
	}
	return idle // let go of, or a request took it meanwhile
}

// snapshot is what the delta timer does: a delta for a tail that has aged, or a
// rebase instead when the shard's base is old or it has many files
// (docs/internals.md, Node journals and snapshots), under the node's limits.
func (a *actor) snapshot(ctx context.Context) {
	d := a.d
	sh := a.cap.load()
	if sh == nil {
		return
	}
	a.tail.count(d.stats, sh.TailBytes())
	defer func() { a.tail.count(d.stats, sh.TailBytes()) }()
	if !acquireSem(ctx, d.snapshots) {
		return
	}
	defer func() { <-d.snapshots }()
	policy := shardfile.Policy{MaxFiles: d.opts.MaxFiles, MaxBytes: d.opts.DeltaSize}
	if a.h.RebaseDue(policy) {
		// A rebase moves the whole database through a temporary file, so only a
		// few run at once whatever the snapshot limit.
		if !acquireSem(ctx, d.rebases) {
			return
		}
		defer func() { <-d.rebases }()
	}
	hooks := shardfile.Hooks{BeforeUpdate: a.reminderAtDelta}
	if _, err := a.h.MaybeSnapshot(ctx, sh, d.opts.DeltaAge, policy, hooks); err != nil && ctx.Err() == nil {
		d.log.Warn("snapshot failed", "shard", a.Name, "err", err)
	}
}

// addLoop starts the reminders loop: once the shard is captured, and again when
// capture resumes after a release that didn't go through. Only the shard's loop
// calls it, and it runs on the loop's context, so it ends with the loop's too.
func (a *actor) addLoop() {
	s := &a.loop
	if s.on || s.quiet {
		return
	}
	o := newReminderLoop(a.d.rem, a, a.useDB, readGate{a.d.flush, &a.cap})
	s.cur.Store(o)
	s.run = service.New(a.ctx, a.d.log)
	s.run.Add(o)
	s.on = true
}

// removeLoop removes the reminders loop and waits for it (and a wake in
// progress) to end. Only the shard's lifecycle loop calls it.
func (a *actor) removeLoop() error {
	s := &a.loop
	if !s.on {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	if err := s.run.Stop(ctx); err != nil {
		return fmt.Errorf("stopping the shard's reminders: %w", err) // still on: no second loop starts beside it
	}
	s.on = false
	return nil
}

// quiet stops the shard's reminders loop for the shards' shutdown, through its
// lifecycle loop, and no reminders loop starts again: a wake in progress has ended when it returns.
func (a *actor) quiet(ctx context.Context) error {
	err, lerr := do(ctx, a.ops, a.ended, func() error {
		a.loop.quiet = true
		return a.removeLoop()
	})
	if errors.Is(lerr, errLoopEnded) {
		return nil // its reminders loop was stopped as the lifecycle loop ended
	}
	return errors.Join(lerr, err)
}

// deactivateNow lets go of the shard through its loop, as an idle deactivation
// does but whether or not it has been idle, and returns once its loop has ended
// and its name is free. The node keeps owning it, cached. It reports false if
// the shard is in use or the app has it open. A shard that was let go of
// meanwhile, or never activated, counts as let go of.
func (a *actor) deactivateNow(ctx context.Context) (bool, error) {
	type result struct {
		ok  bool
		err error
	}
	r, err := do(ctx, a.ops, a.ended, func() result {
		ok, err := a.deactivate(ctx)
		return result{ok, err}
	})
	if errors.Is(err, errLoopEnded) {
		select {
		case <-a.ready:
		default:
			return true, nil // never activated: nothing to let go of
		}
		if a.gone {
			return true, nil
		}
		return false, errShardsStopped
	}
	if err != nil {
		return false, err
	}
	if r.ok {
		if err := a.waitEnded(ctx); err != nil {
			return false, err
		}
	}
	return r.ok, r.err
}

// evict lets go of the shard through its loop, as deactivateNow does, and
// gives up its ownership too: once it is deactivated it is dropped as the cache
// drops an evicted shard (its manifest released by compare-and-swap, then its
// files), before its name is free, so the next node to need it claims it. It
// returns once the loop has ended. A shard in use is not evicted (ErrBusy), nor
// one the app has open (ErrAppHolds). done is false if the shard was let go of
// meanwhile (or never activated): the caller looks again.
func (a *actor) evict(ctx context.Context) (done bool, err error) {
	err, lerr := do(ctx, a.ops, a.ended, func() error {
		if a.gone {
			return errLoopEnded
		}
		ok, err := a.deactivate(ctx)
		switch {
		case err != nil:
			return err
		case !ok && a.idle.held:
			return fmt.Errorf("%w: %s", ErrAppHolds, a.Name)
		case !ok:
			return fmt.Errorf("%w: %s", ErrBusy, a.Name)
		}
		// Cached now, or deleted for holding nothing. If the drop fails the shard
		// stays cached here.
		row, ok, err := a.d.cache.idx.get(a.Name)
		if err == nil && ok && row.State == stateCached {
			err = a.d.cache.drop(ctx, row)
		}
		return err
	})
	if lerr != nil {
		err = lerr
	}
	switch {
	case errors.Is(err, errLoopEnded):
		return false, nil
	case err != nil:
		return true, err
	}
	return true, a.waitEnded(ctx)
}

// waitEnded waits for the lifecycle loop of a shard that was let go of to end:
// its reminders loop has ended and its entry is removed by then.
func (a *actor) waitEnded(ctx context.Context) error {
	select {
	case <-a.ended:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
