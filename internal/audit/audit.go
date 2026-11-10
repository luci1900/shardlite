// Package audit lists the LTX files in a store that no manifest lists. The walk
// is the one piece the fault tests and a future operator command share: the
// fault rig runs it after a scenario, and an operator's cleanup (on the
// roadmap) would delete what it names, past the journal's expiry. Nothing in
// the node uses it.
package audit

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// Unlisted is the walk: the keys of the LTX files under the data prefix that were
// written before now less age and that their shard's manifest doesn't list (all of
// them if the manifest is gone or names another generation), in key order. A key
// under the prefix that is no shard's file is an error. A file older than age can
// never be listed later, since a writer that has not updated its manifest by then
// has lost its tail anyway (docs/internals.md, Garbage).
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
