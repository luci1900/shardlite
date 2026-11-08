// Package collect is the collector, the orphan index's handler (docs/internals.md,
// Reconcilers): for each due entry, which names a generation of a shard that may hold
// LTX files no manifest lists, it reads the shard's manifest, lists the generation's
// files and deletes those the manifest doesn't list (all of them if the manifest is
// gone or names another generation) that are older than the age rule, checking the
// lease before each delete. The age is checked on each file, so a file still on its
// way to the manifest is never deleted, whichever write the entry came from. The
// reconcile loop (internal/reconcile) lists the entries, paces the handler and writes
// the entry the collector answers for. It keeps no state of its own, and two
// collectors handling one entry only delete what is garbage, so a change of the
// ranking loses nothing.
package collect

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/reconcile"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// Every is how often the collector passes when nothing is due. Its entries fall due a
// day after their write, so nothing waits on a pass: ten minutes costs the cluster a
// listing of each lane every ten minutes (16 requests), where the dispatcher's
// minute would cost ten times that for entries that are never urgent, and keeps a
// failed entry's wait (retry) meaningful. The shardset uses it when the collector's
// options leave Every zero.
const Every = 10 * time.Minute

// retry is how far an entry moves when the collector fails, in minutes: the longest
// of the dispatcher's backoff. The orphan's subject carries no move count to
// double, and nothing waits on the deletes.
const retry = 16

// Deps is what the collector uses of the node's.
type Deps struct {
	Store *blob.Store
	Clock reconcile.Clock
	Log   *slog.Logger
	// Age is the age rule (shardfile.Files.AgeRule), which the writer's entries are
	// made by, DefaultAgeRule if zero.
	Age time.Duration
}

// New returns the collector, which the shardset hands to the reconcile loop as the
// orphan index's handler.
func New(d Deps) *Collector {
	if d.Age <= 0 {
		d.Age = shardfile.DefaultAgeRule
	}
	return &Collector{Deps: d, manifests: shardfile.NewManifestStore(d.Store)}
}

// Collector deletes the LTX files that no manifest lists.
type Collector struct {
	Deps
	manifests shardfile.ManifestStore
}

// errLease marks a lease check that failed before a delete.
var errLease = errors.New("the node's lease failed")

// Handle deals with one due orphan entry: delete the generation's unlisted files that
// are past the age and answer done, or, if some unlisted file is too young, move the
// entry to when the youngest turns the age (a write that found the entry there
// already is still checked). A failure answers a move by retry, and a lease that
// fails or a budget that ended leaves the entry for the next pass.
func (c *Collector) Handle(ctx context.Context, e dueindex.Entry, check func() error) reconcile.Answer {
	o, err := shardfile.ParseOrphan(e)
	if err != nil {
		c.Log.Warn("an entry of the orphan index is not an orphan, leaving it", "subject", e.Subject, "err", err)
		return &reconcile.Leave{}
	}
	youngest, err := c.collect(ctx, o, check)
	switch {
	case err == nil && youngest.IsZero():
		return &reconcile.Done{}
	case err == nil:
		// One minute past the minute it turns, so the file is old by then and the key is new.
		return &reconcile.Move{Minute: dueindex.MinuteAt(youngest.Add(c.Age)) + 1, Subject: e.Subject}
	case errors.Is(err, errLease) || ctx.Err() != nil:
		return &reconcile.Leave{}
	}
	c.Log.Warn("could not collect the orphans of a generation, backing off", "shard", o.Shard, "generation", o.Generation, "err", err)
	return &reconcile.Move{Minute: dueindex.MinuteAt(c.Clock.Now()) + retry, Subject: e.Subject}
}

// collect deletes what the entry's generation holds that no manifest lists and that
// is past the age, and returns when the youngest unlisted file it kept was written
// (the zero time if it kept none).
func (c *Collector) collect(ctx context.Context, o shardfile.Orphan, check func() error) (youngest time.Time, err error) {
	listed, err := c.listed(ctx, o)
	if err != nil {
		return time.Time{}, err
	}
	old := c.Clock.Now().Add(-c.Age)
	prefix := shardfile.GenerationPrefix(o.Shard, o.Generation)
	deleted := 0
	defer func() {
		if deleted > 0 {
			c.Log.Info("deleted LTX files no manifest lists", "shard", o.Shard, "generation", o.Generation, "files", deleted)
		}
	}()
	for info, err := range c.Store.List(ctx, prefix) {
		if err != nil {
			return time.Time{}, err
		}
		if !strings.HasSuffix(info.Key, ".ltx") || listed[info.Key] {
			continue // not an LTX file, or one the manifest lists
		}
		if !info.LastModified.Before(old) {
			if info.LastModified.After(youngest) {
				youngest = info.LastModified
			}
			continue
		}
		if err := check(); err != nil {
			return time.Time{}, fmt.Errorf("%w: %w", errLease, err)
		}
		if err := c.Store.Delete(ctx, info.Key, ""); err != nil && !errors.Is(err, blob.ErrNotFound) {
			return time.Time{}, fmt.Errorf("deleting %s: %w", info.Key, err)
		}
		deleted++
	}
	return youngest, nil
}

// listed is the keys of the files the manifest lists in the entry's generation: none
// if the manifest is gone or names another generation.
func (c *Collector) listed(ctx context.Context, o shardfile.Orphan) (map[string]bool, error) {
	m, _, err := c.manifests.Get(ctx, o.Shard)
	if errors.Is(err, blob.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if m.Generation != o.Generation {
		return nil, nil
	}
	keys := make(map[string]bool, len(m.Files))
	for _, f := range m.Files {
		keys[shardfile.FileKey(o.Shard, o.Generation, f)] = true
	}
	return keys, nil
}
