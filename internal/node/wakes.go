package node

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// Wake hints (docs/internals.md, Message delivery): an empty object wakes/{minute}/{shard} for a
// shard that was let go of with live outbox rows or inbox rows due later, or
// whose tail an adoption replayed, or that still had a live outbox row when a snapshot took its tail away,
// so a node finds it and activates it, whichever node it was and whether or not it kept its data directory.

const (
	wakePrefix = shardfile.WakePrefix
	wakeLayout = shardfile.WakeLayout
)

func wakeKey(t time.Time, shard string) string { return shardfile.WakeKey(t, shard) }

// writeWake records that shard needs waking at t. An existing hint is success.
func (m *Manager) writeWake(ctx context.Context, shard string, t time.Time) (string, error) {
	key := wakeKey(t, shard)
	_, err := m.n.Store.Create(ctx, key, strings.NewReader(""))
	if err != nil && !errors.Is(err, blob.ErrExists) {
		return key, err
	}
	return key, nil
}

func (m *Manager) deleteWakes(ctx context.Context, keys ...string) error {
	var errs []error
	for _, k := range keys {
		if err := m.n.Store.Delete(ctx, k, ""); err != nil && !errors.Is(err, blob.ErrNotFound) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *shardMsgs) addHints(keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hints == nil {
		s.hints = map[string]bool{}
	}
	for _, k := range keys {
		s.hints[k] = true
	}
}

// hintMinute is the time a hint is for.
func hintMinute(key string) (time.Time, bool) {
	minute, _, ok := strings.Cut(strings.TrimPrefix(key, wakePrefix), "/")
	t, err := time.Parse(wakeLayout, minute)
	return t, ok && err == nil
}

// scheduleReconcile asks for the shard's hints to be brought in line with what
// it holds, in the background (calls that arrive meanwhile share the next run).
func (s *shardMsgs) scheduleReconcile() {
	s.mu.Lock()
	if s.recRunning {
		s.recAgain = true
		s.mu.Unlock()
		return
	}
	s.recRunning = true
	s.mu.Unlock()
	mg := s.a.m.msgs
	mg.spawn(func() {
		for {
			mg.reconcile(s.ctx, s.a)
			s.mu.Lock()
			if !s.recAgain {
				s.recRunning = false
				s.mu.Unlock()
				return
			}
			s.recAgain = false
			s.mu.Unlock()
		}
	})
}

// reconcile writes the hint for the shard's earliest inbox row if it has none
// (one write for any number of rows in that minute), and deletes the hints
// that have done their job: a hint for a minute the earliest row has moved
// past, once the shard has no live outbox rows either. A hint that woke the shard
// is one of these. It does nothing until the shard's first read (before that the
// node doesn't know what it holds).
func (mg *messenger) reconcile(ctx context.Context, a *ActiveShard) {
	s := a.msgs
	s.hintMu.Lock()
	defer s.hintMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	if !s.read1 {
		s.mu.Unlock()
		return
	}
	earliest, has := s.earliest, s.hasEarliest
	have := make([]string, 0, len(s.hints))
	for k := range s.hints {
		have = append(have, k)
	}
	s.mu.Unlock()
	fail := func(what string, err error) {
		if ctx.Err() == nil {
			mg.m.log.Warn("could not "+what+" a wake hint", "shard", a.Name, "err", err)
		}
		s.mu.Lock()
		s.hintDirty = true
		s.mu.Unlock()
	}
	want := ""
	if has {
		want = wakeKey(time.Unix(earliest, 0), a.Name)
		if !slices.Contains(have, want) {
			if _, err := mg.ensureHint(ctx, a, time.Unix(earliest, 0)); err != nil {
				fail("write", err)
				return
			}
		}
	}
	var stale []string
	for _, k := range have {
		if k == want {
			continue
		}
		t, ok := hintMinute(k)
		if !has || !ok || earliest >= t.Add(time.Minute).Unix() {
			stale = append(stale, k)
		}
	}
	if len(stale) == 0 {
		s.mu.Lock()
		s.hintDirty = false
		s.mu.Unlock()
		return
	}
	// A hint also means "look at the outbox": it stays while any row is live.
	live, err := s.liveOutbox(ctx, a)
	if err != nil {
		if !errors.Is(err, capture.ErrClosed) {
			fail("check for", err)
		}
		return
	}
	if live {
		return
	}
	if err := mg.m.deleteWakes(ctx, stale...); err != nil {
		fail("delete", err)
		return
	}
	s.mu.Lock()
	for _, k := range stale {
		delete(s.hints, k)
	}
	s.hintDirty = false
	s.mu.Unlock()
}

// liveOutbox reports whether the shard has an outbox row the node has not given up on.
func (s *shardMsgs) liveOutbox(ctx context.Context, a *ActiveShard) (bool, error) {
	var live bool
	given := s.given()
	err := a.useDB(ctx, func(db *sql.DB) error {
		var err error
		live, err = firstOutbox(ctx, db, given)
		return err
	})
	return live, err
}

// ensureHint writes the shard's hint for t unless the shard already knows it, and
// remembers it. wrote says whether it made a request. The caller holds hintMu.
func (mg *messenger) ensureHint(ctx context.Context, a *ActiveShard, t time.Time) (wrote bool, err error) {
	s := a.msgs
	k := wakeKey(t, a.Name)
	s.mu.Lock()
	known := s.hints[k]
	s.mu.Unlock()
	if known {
		return false, nil
	}
	if _, err := mg.m.writeWake(ctx, a.Name, t); err != nil {
		return false, err
	}
	s.mu.Lock()
	if s.hints == nil {
		s.hints = map[string]bool{}
	}
	s.hints[k] = true
	s.mu.Unlock()
	return true, nil
}

// hintLiveOutbox is for a snapshot that is about to take the shard's tail away (a delta or a
// rebase, before the manifest adopts it). After a crash an adopter writes a hint only for a shard whose
// tail it replays, so a shard that still has a live outbox row (a delivery that is failing or backing off)
// would otherwise be inactive with work and no way to be woken once the tail is gone. So the hint for now
// goes first, and if it can't be written the snapshot doesn't happen and the next pass tries again.
// Healthy shards have no live row minutes after the commit and cost nothing here. The hint is deleted by
// reconcile once no row is live, like the one written when a shard is let go of.
func (mg *messenger) hintLiveOutbox(ctx context.Context, a *ActiveShard) error {
	if !mg.enabled() {
		return nil
	}
	s := a.msgs
	s.hintMu.Lock()
	defer s.hintMu.Unlock()
	live, err := s.liveOutbox(ctx, a)
	if err != nil {
		return fmt.Errorf("checking for live outbox rows: %w", err)
	}
	if !live {
		return nil
	}
	wrote, err := mg.ensureHint(ctx, a, mg.m.now())
	if err != nil {
		return fmt.Errorf("writing a wake hint: %w", err)
	}
	if wrote {
		mg.m.Stats.SnapshotHints.Add(1)
	}
	return nil
}

// hintsBeforeRelease is what a shard does before it is let go of: the hints
// that will wake it again exist before its final sync (an early one is only a
// stale one). It needs one for now if it has live outbox rows and one for its
// earliest inbox row that it has not given up on; the others are deleted. A
// failure keeps the shard active.
func (mg *messenger) hintsBeforeRelease(ctx context.Context, a *ActiveShard) error {
	s := a.msgs
	s.hintMu.Lock()
	defer s.hintMu.Unlock()
	var live, has bool
	var earliest int64
	given := s.given()
	err := a.useDB(ctx, func(db *sql.DB) error {
		var err error
		if live, err = firstOutbox(ctx, db, given); err != nil {
			return err
		}
		earliest, has, err = firstInbox(ctx, db, given, ``)
		return err
	})
	if err != nil {
		return fmt.Errorf("checking for hints to write: %w", err)
	}
	want := map[string]time.Time{}
	if live {
		want[wakeKey(mg.m.now(), a.Name)] = mg.m.now()
	}
	if has {
		t := time.Unix(earliest, 0)
		want[wakeKey(t, a.Name)] = t
	}
	for _, t := range want {
		if _, err := mg.ensureHint(ctx, a, t); err != nil {
			return fmt.Errorf("writing a wake hint: %w", err)
		}
	}
	var stale []string
	s.mu.Lock()
	for k := range s.hints {
		if _, ok := want[k]; !ok {
			stale = append(stale, k)
		}
	}
	s.mu.Unlock()
	if len(stale) > 0 {
		if err := mg.m.deleteWakes(ctx, stale...); err != nil {
			mg.m.log.Warn("could not delete wake hints", "shard", a.Name, "err", err)
			return nil // stale ones are harmless
		}
		s.mu.Lock()
		for _, k := range stale {
			delete(s.hints, k)
		}
		s.mu.Unlock()
	}
	return nil
}

// wakeLoop scans for hints at startup (after adoption) and then every WakeEvery.
func (m *Manager) wakeLoop(ctx context.Context) {
	tick, stop := m.opts.Clock.NewTicker(m.opts.WakeEvery)
	defer stop()
	for {
		m.scanWakes(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick:
		}
	}
}

// scanWakes lists the hints from the start up to now and wakes each shard
// they name: activated through the normal path under the activation limit. A
// hint that can't be used (a name that doesn't parse, a kind that isn't declared
// a shard with no manifest) is deleted: a hint never creates a shard.
func (m *Manager) scanWakes(ctx context.Context) {
	if m.n.Keeper.Check() != nil {
		return
	}
	now := m.now().UTC().Format(wakeLayout)
	byShard := map[string][]string{}
	var order []string
	var junk []string
	for info, err := range m.n.Store.List(ctx, wakePrefix) {
		if err != nil {
			if ctx.Err() == nil {
				m.log.Warn("could not list wake hints", "err", err)
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
		m.log.Error("deleting wake hints that don't parse", "keys", junk)
		m.deleteWakes(ctx, junk...)
	}
	var g errgroup.Group
	g.SetLimit(m.opts.ActivationLimit)
	for _, shard := range order {
		keys := byShard[shard]
		g.Go(func() error {
			m.wake(ctx, shard, keys)
			return nil // independent: one shard failing must not stop the others
		})
	}
	g.Wait()
}

func (m *Manager) wake(ctx context.Context, shard string, keys []string) {
	drop := func(why string) {
		m.log.Warn("deleting a wake hint: "+why, "shard", shard)
		if err := m.deleteWakes(ctx, keys...); err != nil && ctx.Err() == nil {
			m.log.Warn("could not delete wake hints", "shard", shard, "err", err)
		}
	}
	kind, _, err := ParseTarget(shard)
	if err != nil {
		drop("the shard name doesn't parse")
		return
	}
	if _, err := m.opts.Kinds.Lookup(kind); err != nil {
		drop("the kind is not declared")
		return
	}
	if _, _, err := m.n.Files.GetManifest(ctx, shard); errors.Is(err, blob.ErrNotFound) {
		drop("the shard has no manifest")
		return
	} else if err != nil {
		if ctx.Err() == nil {
			m.log.Warn("could not read a woken shard's manifest", "shard", shard, "err", err)
		}
		return
	}
	ref, err := m.Acquire(ctx, shard)
	if err != nil {
		if ctx.Err() == nil {
			m.log.Warn("could not wake a shard", "shard", shard, "err", err)
		}
		return
	}
	defer ref.Release()
	ref.a.msgs.addHints(keys)
	ref.a.msgs.scheduleReconcile()
}

// anyLiveMessages reports whether an active shard has live outbox rows or inbox rows that are due.
func (m *Manager) anyLiveMessages(ctx context.Context) bool {
	m.mu.Lock()
	list := make([]*ActiveShard, 0, len(m.active))
	for _, a := range m.active {
		list = append(list, a)
	}
	m.mu.Unlock()
	for _, a := range list {
		live, err := m.msgs.busy(ctx, a, false)
		if err == nil && live {
			return true
		}
	}
	return false
}

// Drain keeps delivering messages for up to the drain time (ManagerOptions.Drain),
// or until no active shard has live rows. It is for shutdown, before the shards
// are let go of: the flush loop and the deliveries keep running, receivers are
// activated as messages need them, and the messages they send are delivered too.
func (m *Manager) Drain(ctx context.Context) {
	if !m.msgs.enabled() || m.n.Keeper.Check() != nil {
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
