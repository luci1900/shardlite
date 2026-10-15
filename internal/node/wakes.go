package node

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/supervisor"
)

// Wake hints (docs/internals.md, Message delivery): an empty object wakes/{minute}/{shard} for a
// shard that was let go of with live outbox rows or inbox rows due later, or
// whose tail an adoption replayed, so a node finds it and activates it, whichever node it was and whether or not it kept its data directory.

const (
	wakePrefix = "wakes/"
	wakeLayout = "2006-01-02T15:04Z" // fixed width, so keys sort by time
)

// wakeKey is the key of the hint that wakes shard at t.
func wakeKey(t time.Time, shard string) string {
	return wakePrefix + t.UTC().Truncate(time.Minute).Format(wakeLayout) + "/" + shard
}

// hintStore is where the wake hints are: the node's blob storage.
type hintStore struct{ store *blob.Store }

// writeWake records that shard needs waking at t. An existing hint is success.
func (h hintStore) writeWake(ctx context.Context, shard string, t time.Time) (string, error) {
	key := wakeKey(t, shard)
	_, err := h.store.Create(ctx, key, strings.NewReader(""))
	if err != nil && !errors.Is(err, blob.ErrExists) {
		return key, err
	}
	return key, nil
}

func (h hintStore) deleteWakes(ctx context.Context, keys ...string) error {
	var errs []error
	for _, k := range keys {
		if err := h.store.Delete(ctx, k, ""); err != nil && !errors.Is(err, blob.ErrNotFound) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// hintMinute is the time a hint is for.
func hintMinute(key string) (time.Time, bool) {
	minute, _, ok := strings.Cut(strings.TrimPrefix(key, wakePrefix), "/")
	t, err := time.Parse(wakeLayout, minute)
	return t, ok && err == nil
}

// wakeHints is the owner's side of the shard's wake hints: what it knows of
// them (in kept), the earliest inbox row they follow, and the child that writes
// and deletes them in blob storage, one at a time. Only the owner's loop calls
// it. The child's result comes back on result, which the loop takes.
type wakeHints struct {
	k      *kept // hints and hintDirty
	store  wakeStore
	name   string
	result chan hintResult // buffered: the child never waits for the loop
	child  errgroup.Group

	read1       bool // a read is durable (earliest is meaningful)
	earliest    int64
	hasEarliest bool
	tidy        bool        // the hints are to be brought in line with what the shard holds
	busy        bool        // the child is running
	release     *releaseAsk // the hints for a release, waiting for the child to end
	released    bool        // the hints for the release are written: nothing changes them any more
}

// hintJob is a change to the shard's hints in blob storage, which the child
// makes: the writes in order, then the deletes. A write that fails stops it.
type hintJob struct {
	write  []time.Time
	delete []string
	clean  bool             // nothing is left for later once it succeeds
	done   func(hintResult) // run on the loop once the child has ended
}

type hintResult struct {
	job            *hintJob
	wrote, deleted []string
	op             string // what failed: "write" or "delete"
	err            error
}

// noteEarliest is for a read that is durable now: the hints follow its earliest
// inbox row.
func (h *wakeHints) noteEarliest(snap *snapshot) {
	changed := !h.read1 || h.hasEarliest != snap.hasEarliest || h.earliest != snap.earliest || h.k.hintDirty
	h.read1, h.earliest, h.hasEarliest = true, snap.earliest, snap.hasEarliest
	if changed {
		h.tidy = true
	}
}

// woken notes the hints that woke the shard: they are the shard's now, and go
// once they have done their job.
func (h *wakeHints) woken(keys []string) {
	for _, k := range keys {
		h.k.hints[k] = true
	}
	h.tidy = true
}

// reconcileJob is what bringing the hints in line takes: a write of the hint for
// the earliest inbox row if there is none (one for any number of rows in that
// minute), and the deletes of hints that have done their job, for a minute the
// earliest row has moved past (a hint that woke the shard is one of these). The
// owner drops the deletes while an outbox row is live.
func (h *wakeHints) reconcileJob() *hintJob {
	job := &hintJob{clean: true}
	want := ""
	if h.hasEarliest {
		t := time.Unix(h.earliest, 0)
		want = wakeKey(t, h.name)
		if !h.k.hints[want] {
			job.write = []time.Time{t}
		}
	}
	for k := range h.k.hints {
		if k == want {
			continue
		}
		t, ok := hintMinute(k)
		if !h.hasEarliest || !ok || h.earliest >= t.Add(time.Minute).Unix() {
			job.delete = append(job.delete, k)
		}
	}
	return job
}

// releaseJob is what the hints before a release take: one for now if the shard
// has live outbox rows and one for its earliest inbox row not given up on, and
// the others deleted.
func (h *wakeHints) releaseJob(now time.Time, live bool, earliest int64, has bool) *hintJob {
	want := map[string]time.Time{}
	if live {
		want[wakeKey(now, h.name)] = now
	}
	if has {
		t := time.Unix(earliest, 0)
		want[wakeKey(t, h.name)] = t
	}
	job := &hintJob{clean: true}
	for k, t := range want {
		if !h.k.hints[k] {
			job.write = append(job.write, t)
		}
	}
	for k := range h.k.hints {
		if _, ok := want[k]; !ok {
			job.delete = append(job.delete, k)
		}
	}
	return job
}

// run starts the child for a job.
func (h *wakeHints) run(ctx context.Context, job *hintJob) {
	h.busy = true
	h.child.Go(func() error {
		r := hintResult{job: job}
		for _, t := range job.write {
			k, err := h.store.writeWake(ctx, h.name, t)
			if err != nil {
				r.op, r.err = "write", err
				break
			}
			r.wrote = append(r.wrote, k)
		}
		if r.err == nil && len(job.delete) > 0 {
			if err := h.store.deleteWakes(ctx, job.delete...); err != nil {
				r.op, r.err = "delete", err
			} else {
				r.deleted = job.delete
			}
		}
		h.result <- r
		return nil
	})
}

// ended takes the child's result.
func (h *wakeHints) ended(r hintResult) {
	h.busy = false
	h.note(r)
	r.job.done(r)
}

// note records what the child wrote and deleted.
func (h *wakeHints) note(r hintResult) {
	for _, k := range r.wrote {
		h.k.hints[k] = true
	}
	for _, k := range r.deleted {
		delete(h.k.hints, k)
	}
}

// stop waits for the child, whose context has ended, and keeps what it did.
func (h *wakeHints) stop() {
	h.child.Wait()
	select {
	case r := <-h.result:
		h.busy = false
		h.note(r)
	default:
	}
}

// woken is the wake scan telling the owner which hints woke the shard.
func (o *msgOwner) woken(ctx context.Context, keys []string) {
	o.send(ctx, func() { o.hints.woken(keys) })
}

// tend starts the hint work that is waiting, once the child is free: the hints
// for a release first, then bringing the hints in line (after the shard's first
// durable read: before that the node doesn't know what it holds).
func (o *msgOwner) tend() {
	h := &o.hints
	switch {
	case h.busy:
	case h.release != nil:
		ask := h.release
		h.release = nil
		o.releaseHints(ask)
	case h.tidy && h.read1 && !h.released:
		h.tidy = false
		o.reconcile()
	}
}

// reconcile brings the hints in line (reconcileJob). A hint also means "look at
// the outbox", so none is deleted while any outbox row is live.
func (o *msgOwner) reconcile() {
	ctx := o.ctx
	fail := func(what string, err error) {
		if ctx.Err() == nil {
			o.mg.log.Warn("could not "+what+" a wake hint", "shard", o.a.Name, "err", err)
		}
		o.k.hintDirty = true
	}
	job := o.hints.reconcileJob()
	if len(job.delete) > 0 {
		var live bool
		err := o.db(ctx, func(db *sql.DB) error {
			var err error
			live, err = firstOutbox(ctx, db, o.k.givenUp)
			return err
		})
		switch {
		case err != nil && !errors.Is(err, capture.ErrClosed):
			fail("check for", err)
			return
		case err != nil:
			return
		case live:
			job.delete, job.clean = nil, false
		}
	}
	if len(job.write) == 0 && len(job.delete) == 0 {
		if job.clean {
			o.k.hintDirty = false
		}
		return
	}
	job.done = func(r hintResult) {
		switch {
		case r.err != nil:
			fail(r.op, r.err)
		case job.clean:
			o.k.hintDirty = false
		}
	}
	o.hints.run(ctx, job)
}

// releaseAsk is the lifecycle loop asking for the hints before a release.
type releaseAsk struct {
	ctx context.Context
	res chan error // buffered
}

// hintsBeforeRelease is what a shard does before it is let go of: the hints
// that will wake it again exist before its final sync (an early one is only a
// stale one). A failure keeps the shard active. After it, nothing changes the
// hints until the owner is removed. Only the lifecycle loop calls it.
func (o *msgOwner) hintsBeforeRelease(ctx context.Context) error {
	ask := &releaseAsk{ctx: ctx, res: make(chan error, 1)}
	if !o.send(ctx, func() { o.hints.release = ask }) {
		return fmt.Errorf("writing the wake hints: %w", errLoopEnded)
	}
	select {
	case err := <-ask.res:
		return err
	case <-o.ended:
		select {
		case err := <-ask.res:
			return err
		default:
			return fmt.Errorf("writing the wake hints: %w", errLoopEnded)
		}
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// releaseHints is hintsBeforeRelease on the loop, once the child is free.
func (o *msgOwner) releaseHints(ask *releaseAsk) {
	ctx := ask.ctx
	var live, has bool
	var earliest int64
	err := o.db(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if live, err = firstOutbox(ctx, tx, o.k.givenUp); err != nil {
			return err
		}
		earliest, has, err = firstInbox(ctx, tx, o.k.givenUp, ``)
		return err
	})
	if err != nil {
		ask.res <- fmt.Errorf("checking for hints to write: %w", err)
		return
	}
	job := o.hints.releaseJob(o.mg.now(), live, earliest, has)
	if len(job.write) == 0 && len(job.delete) == 0 {
		o.hints.released = true
		ask.res <- nil
		return
	}
	// The child stops with the owner or with the release.
	cctx, cancel := context.WithCancel(o.ctx)
	stop := context.AfterFunc(ctx, cancel)
	job.done = func(r hintResult) {
		stop()
		cancel()
		switch {
		case r.err != nil && r.op == "write":
			ask.res <- fmt.Errorf("writing a wake hint: %w", r.err)
			return
		case r.err != nil:
			o.mg.log.Warn("could not delete wake hints", "shard", o.a.Name, "err", r.err) // stale ones are harmless
		}
		o.hints.released = true
		ask.res <- nil
	}
	o.hints.run(cctx, job)
}

// WakeOptions configure the scan for wake hints.
type WakeOptions struct {
	Every           time.Duration // how often wake hints are looked for, 1 min if zero
	ActivationLimit int           // shards activated at once by the scan, 32 if zero
}

// wakeScan is the part of Shards that finds the wake hints that are due and
// activates the shards they name.
type wakeScan struct {
	store   *blob.Store // listed for hints
	hints   hintStore
	files   *shardfile.Files
	keeper  *coord.Keeper
	kinds   *Kinds
	acquire func(ctx context.Context, target string) (*Ref, error) // activates a shard (Shards.Acquire)
	clock   Clock
	opts    WakeOptions
	log     *slog.Logger
}

func (w *wakeScan) String() string { return "wake scan" }

// Serve scans for hints at startup (after adoption) and then every
// WakeOptions.Every. It is the wake scan's loop, as a service.
func (w *wakeScan) Serve(ctx context.Context) error {
	tick, stop := w.clock.NewTicker(w.opts.Every)
	defer stop()
	for {
		w.scan(ctx)
		select {
		case <-ctx.Done():
			return supervisor.NoRestart(nil)
		case <-tick:
		}
	}
}

// scan lists the hints from the start up to now and wakes each shard they
// name: activated through the normal path under the activation limit. A hint
// that can't be used (a name that doesn't parse, a kind that isn't declared a
// shard with no manifest) is deleted: a hint never creates a shard.
func (w *wakeScan) scan(ctx context.Context) {
	if w.keeper.Check() != nil {
		return
	}
	now := w.clock.Now().UTC().Format(wakeLayout)
	byShard := map[string][]string{}
	var order []string
	var junk []string
	for info, err := range w.store.List(ctx, wakePrefix) {
		if err != nil {
			if ctx.Err() == nil {
				w.log.Warn("could not list wake hints", "err", err)
			}
			return
		}
		minute, shard, ok := strings.Cut(strings.TrimPrefix(info.Key, wakePrefix), "/")
		if _, perr := time.Parse(wakeLayout, minute); !ok || perr != nil {
			junk = append(junk, info.Key)
			continue
		}
		if minute > now {
			break
		}
		if byShard[shard] == nil {
			order = append(order, shard)
		}
		byShard[shard] = append(byShard[shard], info.Key)
	}
	if len(junk) > 0 {
		w.log.Error("deleting wake hints that don't parse", "keys", junk)
		w.hints.deleteWakes(ctx, junk...)
	}
	var g errgroup.Group
	g.SetLimit(w.opts.ActivationLimit)
	for _, shard := range order {
		keys := byShard[shard]
		g.Go(func() error {
			w.wake(ctx, shard, keys)
			return nil // independent: one shard failing must not stop the others
		})
	}
	g.Wait()
}

func (w *wakeScan) wake(ctx context.Context, shard string, keys []string) {
	drop := func(why string) {
		w.log.Warn("deleting a wake hint: "+why, "shard", shard)
		if err := w.hints.deleteWakes(ctx, keys...); err != nil && ctx.Err() == nil {
			w.log.Warn("could not delete wake hints", "shard", shard, "err", err)
		}
	}
	kind, _, err := ParseTarget(shard)
	if err != nil {
		drop("the shard name doesn't parse")
		return
	}
	if _, err := w.kinds.Lookup(kind); err != nil {
		drop("the kind is not declared")
		return
	}
	if _, _, err := w.files.GetManifest(ctx, shard); errors.Is(err, blob.ErrNotFound) {
		drop("the shard has no manifest")
		return
	} else if err != nil {
		if ctx.Err() == nil {
			w.log.Warn("could not read a woken shard's manifest", "shard", shard, "err", err)
		}
		return
	}
	ref, err := w.acquire(ctx, shard)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Warn("could not wake a shard", "shard", shard, "err", err)
		}
		return
	}
	defer ref.Release()
	ref.a.msgs.load().woken(ctx, keys)
}

// anyLiveMessages reports whether an active shard has live outbox rows or inbox
// rows that are due. Each shard is looked at on its own, and a row moving from
// a sender's outbox to a receiver's inbox is inserted before it is deleted, so
// a look can only miss it if an intake landed during the look: then it looks
// again. A look that can't tell counts as live: the shard is being let go of
// (its owner is removed for the final sync, and comes back if the release
// doesn't go through) or its database didn't answer, and either way its rows
// may still be there.
func (m *Shards) anyLiveMessages(ctx context.Context) bool {
	for ctx.Err() == nil {
		intakes := m.msgs.intakes.Load()
		var list []*shard
		m.tab.shards(func(a *shard) { list = append(list, a) }, entryActive, entryBusy)
		for _, a := range list {
			o := a.msgs.load()
			if o == nil {
				continue
			}
			if live, err := o.live(ctx, false, false); err != nil || live {
				return ctx.Err() == nil
			}
		}
		if m.msgs.intakes.Load() == intakes {
			return false
		}
	}
	return false
}

// Drain keeps delivering messages for up to the drain time (ShardsOptions.Drain),
// or until no active shard has live rows. It is for shutdown, before the shards
// are let go of: the flush loop and the deliveries keep running, receivers are
// activated as messages need them, and the messages they send are delivered too.
// It does nothing once the node may not write any more.
func (m *Shards) Drain(ctx context.Context) {
	if m.n.Err() != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, m.opts.Drain)
	defer cancel()
	for m.anyLiveMessages(ctx) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}
