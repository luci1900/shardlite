// Package audit is the check the fault tests run on a store after a scenario: every
// LTX file past the age rule is either listed by its shard's manifest or was
// collected. It is the full walk the collector replaced, kept to catch a path that
// writes LTX files without first creating an orphan index entry for them (the
// collector only ever looks where an entry points). Nothing in the node uses it.
package audit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/reconcile"
	"github.com/luci1900/shardlite/internal/reconcile/collect"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// Run is the audit. It stands at now, a time past the age rule for every file in the
// store, and the node that wrote them must be stopped: a file on its way to the
// manifest would look like an orphan. It runs the collector over every entry of the
// orphan index, due or not, and then walks the store and fails on any LTX file older
// than age that its shard's manifest doesn't list: a file no entry led the collector to.
func Run(ctx context.Context, store *blob.Store, age time.Duration, now time.Time) error {
	if err := collectAll(ctx, store, age, now); err != nil {
		return err
	}
	left, err := Unlisted(ctx, store, age, now)
	if err != nil {
		return err
	}
	if len(left) > 0 {
		return fmt.Errorf("%d LTX files older than the age rule are listed by no manifest and no orphan entry led the collector to them (a write path that makes no entry first):\n%s", len(left), strings.Join(left, "\n"))
	}
	return nil
}

// collectAll hands every entry of the orphan index to the collector as it would be
// at now, and fails unless each is done.
func collectAll(ctx context.Context, store *blob.Store, age time.Duration, now time.Time) error {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := collect.New(collect.Deps{Store: store, Clock: at(now), Log: log, Age: age})
	index := shardfile.OrphanIndex(store)
	for lane := range shard.Lanes {
		entries, err := index.Due(ctx, shard.Lane(lane), now, 1<<30)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if a := c.Handle(ctx, e, func() error { return nil }); !isDone(a) {
				return fmt.Errorf("the collector answered %#v for the entry %s", a, dueindex.Key(shardfile.OrphanPrefix, e))
			}
		}
	}
	return nil
}

func isDone(a reconcile.Answer) bool {
	_, ok := a.(*reconcile.Done)
	return ok
}

// Unlisted is the walk: the keys of the LTX files under the data prefix that were
// written before now less age and that their shard's manifest doesn't list (all of
// them if the manifest is gone or names another generation), in key order. A key
// under the prefix that is no shard's file is an error.
func Unlisted(ctx context.Context, store *blob.Store, age time.Duration, now time.Time) ([]string, error) {
	old := now.Add(-age)
	manifests := shardfile.NewManifestStore(store)
	var out []string
	var name shard.Name
	var listed map[string]bool // the files the manifest lists, of name's generation
	var gen shard.Gen
	for info, err := range store.List(ctx, shardfile.FilesPrefix) {
		if err != nil {
			return nil, err
		}
		n, g, err := parseKey(info.Key)
		if err != nil {
			return nil, err
		}
		if !strings.HasSuffix(info.Key, ".ltx") || !info.LastModified.Before(old) {
			continue
		}
		if n != name || g != gen || listed == nil {
			if listed, err = listedBy(ctx, manifests, n, g); err != nil {
				return nil, err
			}
			name, gen = n, g
		}
		if !listed[info.Key] {
			out = append(out, info.Key)
		}
	}
	slices.Sort(out)
	return out, nil
}

// parseKey splits data/shards/{lane}/{kind}/{id}/{generation}/{file} into the shard
// and the generation, and checks the lane.
func parseKey(key string) (shard.Name, shard.Gen, error) {
	parts := strings.Split(strings.TrimPrefix(key, shardfile.FilesPrefix), "/")
	if len(parts) != 5 {
		return shard.Name{}, "", fmt.Errorf("%s is no shard's file", key)
	}
	n, err := shard.ParseName(parts[1] + "/" + parts[2])
	if err != nil || n.Lane().String() != parts[0] || parts[3] == "" {
		return shard.Name{}, "", fmt.Errorf("%s is no shard's file", key)
	}
	return n, shard.Gen(parts[3]), nil
}

// listedBy is the keys of the files the shard's manifest lists in the generation: none
// if the manifest is gone or names another generation.
func listedBy(ctx context.Context, manifests shardfile.ManifestStore, n shard.Name, g shard.Gen) (map[string]bool, error) {
	keys := map[string]bool{} // never nil, so it marks the shard as read
	m, _, err := manifests.Get(ctx, n)
	if errors.Is(err, blob.ErrNotFound) {
		return keys, nil
	} else if err != nil {
		return nil, err
	}
	if m.Generation == g {
		for _, f := range m.Files {
			keys[shardfile.FileKey(n, g, f)] = true
		}
	}
	return keys, nil
}

// at is a clock that stands still.
type at time.Time

func (a at) Now() time.Time { return time.Time(a) }

func (at) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)
	return t.C, t.Stop
}
