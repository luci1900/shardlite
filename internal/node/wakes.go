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
)

// Wake hints (docs/internals.md, Message delivery): an empty object index/wakes/{minute}/{shard} for a
// shard that was let go of with live outbox rows or inbox rows due later, or
// whose tail an adoption replayed, so a node finds it and activates it, whichever node it was and whether or not it kept its data directory.

const (
	wakePrefix = "index/wakes/"
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

// latestMinute is the latest minute any of the hints is for.
func latestMinute(keys []string) time.Time {
	var last time.Time
	for _, k := range keys {
		if t, ok := hintMinute(k); ok && t.After(last) {
			last = t
		}
	}
	return last
}

// wake is when the shard next needs waking, as of one snapshot of its tables:
// now if it has an outbox row or an inbox row due now that the node hasn't given
// up on (or the delete marker, if marker is set), otherwise the due time of its
// earliest inbox row not given up on, and 0 if there is none. It is what the
// manifest's wake time records (docs/internals.md, Message delivery).
//
// at is the captured position the snapshot is exactly, or 0 if it can't tell
// (capture.Shard.UseAt). A snapshot that holds commits that aren't captured yet
// would make the wake time late for files that end before them, if one of them
// removed work.
func (a *shard) wake(ctx context.Context, marker bool) (w int64, at uint64, err error) {
	now := a.d.now().Unix()
	given := a.msgs.kept.given()
	sh := a.cap.load()
	if sh == nil {
		return 0, 0, capture.ErrClosed
	}
	at, err = sh.UseAt(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		w, err = wakeOf(ctx, tx, given, now, marker)
		return err
	})
	if err != nil {
		return 0, 0, fmt.Errorf("reading the wake time: %w", err)
	}
	return w, at, nil
}

// wakeOf is wake's snapshot.
func wakeOf(ctx context.Context, tx *sql.Tx, given map[rowKey]bool, now int64, marker bool) (int64, error) {
	if live, err := firstOutbox(ctx, tx, given); err != nil || live {
		return now, err
	}
	if marker {
		var marked bool
		if err := tx.QueryRowContext(ctx, capture.MarkedQuery).Scan(&marked); err != nil || marked {
			return now, err
		}
	}
	due, ok, err := firstInbox(ctx, tx, given, ``)
	if !ok {
		return 0, err
	}
	return max(due, now), err
}

// wakeFor is the wake time to record for files that reach end, given what wake
// read: exact if its snapshot was at end, and now otherwise, since the shard's
// state at end is then unknown and now is never late.
func (a *shard) wakeFor(w int64, at, end uint64) int64 {
	if at != end {
		return a.d.now().Unix()
	}
	return w
}

// hint creates the wake hint for wake (none for 0). It comes before the
// manifest write that records wake: a stale hint is harmless and a missing one
// isn't. An existing hint counts as written.
func (a *shard) hint(ctx context.Context, wake int64) error {
	if wake == 0 {
		return nil
	}
	if _, err := a.d.hints.writeWake(ctx, a.Name, time.Unix(wake, 0)); err != nil {
		return fmt.Errorf("writing a wake hint: %w", err)
	}
	return nil
}

// wakeAtDelta is the Wake hook of a background delta or rebase: it reads the
// wake time once the tail is taken, records it if the read was exactly the
// state the files reach (and now otherwise), and creates its hint. A failure
// fails the delta, which the delta timer tries again.
func (a *shard) wakeAtDelta(ctx context.Context, end uint64) (int64, error) {
	w, at, err := a.wake(ctx, true)
	if err != nil {
		return 0, err
	}
	w = a.wakeFor(w, at, end)
	return w, a.hint(ctx, w)
}

// WakeOptions configure the scan for wake hints.
type WakeOptions struct {
	Every           time.Duration // how often wake hints are looked for, 1 min if zero
	ActivationLimit int           // shards activated at once by the scan, 32 if zero
}

// wakeScan is the part of the Catalog that finds the wake hints that are due and
// activates the shards they name.
type wakeScan struct {
	store   *blob.Store // listed for hints
	hints   hintStore
	files   *shardfile.Files
	keeper  *coord.Keeper
	kinds   *Kinds
	acquire func(ctx context.Context, target string) (*Ref, error) // activates a shard (Catalog.Acquire)
	self    string                                                 // this node's name
	active  func(target string) bool                               // whether a shard is active in this node's table (Catalog.IsActive)
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
			return nil
		case <-tick:
		}
	}
}

// scan lists the hints from the start up to now and wakes each shard they
// name: activated through the normal path under the activation limit. A hint
// that can't be used (a name that doesn't parse, a kind that isn't declared a
// shard with no manifest) is deleted: a hint never creates a shard. The others
// are deleted once their shard has nothing live for them (done), or at once if
// the shard is at rest and its manifest says it has no work due.
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
	m, _, err := w.files.GetManifest(ctx, shard)
	if errors.Is(err, blob.ErrNotFound) {
		drop("the shard has no manifest")
		return
	} else if err != nil {
		if ctx.Err() == nil {
			w.log.Warn("could not read a woken shard's manifest", "shard", shard, "err", err)
		}
		return
	}
	// A shard at rest (unowned, or owned here and not active) has a manifest
	// written by its last deactivation, eviction or adoption, so its wake time
	// is exact: a hint it doesn't back is stale and costs no activation. A
	// shard owned elsewhere is left to acquire, which refuses it.
	atRest := m.Owner == "" || (m.Owner == w.self && !w.active(shard))
	if atRest && (m.Wake == 0 || m.Wake >= latestMinute(keys).Add(time.Minute).Unix()) {
		w.log.Debug("deleting a stale wake hint", "shard", shard)
		if err := w.hints.deleteWakes(ctx, keys...); err != nil && ctx.Err() == nil {
			w.log.Warn("could not delete wake hints", "shard", shard, "err", err)
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
	w.done(ctx, ref.a, keys)
}

// done deletes the due hints of an active shard once it has nothing live for
// them: no outbox row and no inbox row due before the end of the last hint's
// minute (rows due later have hints of their own, which the shard's owner
// writes). While it has, the hints stay, so a crash still leaves the shard
// something to wake it, and the next scan looks again.
func (w *wakeScan) done(ctx context.Context, a *shard, keys []string) {
	live, err := a.live(ctx, latestMinute(keys).Add(time.Minute).Unix())
	if err != nil || live {
		return
	}
	if err := w.hints.deleteWakes(ctx, keys...); err != nil && ctx.Err() == nil {
		w.log.Warn("could not delete wake hints", "shard", a.Name, "err", err)
	}
}

// anyLiveMessages reports whether an active shard has live outbox rows or inbox
// rows that are due. Each shard is looked at on its own, and a row moving from
// a sender's outbox to a receiver's inbox is inserted before it is deleted, so
// a look can only miss it if an intake landed during the look: then it looks
// again. A look that can't tell counts as live: the shard is being let go of
// (its capture is closed for the final sync, and comes back if the release
// doesn't go through) or its database didn't answer, and either way its rows
// may still be there.
func (m *Catalog) anyLiveMessages(ctx context.Context) bool {
	for ctx.Err() == nil {
		intakes := m.msgs.intakes.Load()
		var list []*shard
		m.tab.shards(func(a *shard) { list = append(list, a) }, entryActive, entryBusy)
		now := m.now().Unix()
		for _, a := range list {
			if live, err := a.live(ctx, now+1); err != nil || live {
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
func (m *Catalog) Drain(ctx context.Context) {
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
