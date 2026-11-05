package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// CacheOptions configure the local cache of deactivated shards.
type CacheOptions struct {
	Limit int64         // bytes of deactivated shards to keep, 0 for no limit
	Every time.Duration // how often the cache is trimmed and the files held from before a restart are looked at, 1 s if zero
}

// cache is the node's local cache of shard files (docs/internals.md,
// Concurrency): the index, the trim to its limit and the files still held from before
// a restart, whose loop is a part of the Catalog. Whatever it does to a shard that
// isn't active (eviction, the sweep of a held file, the delete of a cached
// shard) runs under an entry of its own in the table, so an activation of the
// shard waits for it.
type cache struct {
	idx   *index
	data  string
	tab   *table
	files *shardfile.Files
	node  cluster.Node // the node's name: eviction gives up only the manifests it owns
	clock Clock
	every time.Duration
	log   *slog.Logger
	stats *Stats

	limit atomic.Int64
	held  map[string]shard.Name // shard files from before the restart that an app still had open, and their shards' names (only the loop uses it)
}

// open does what a start does to the data directory (clearCache), then opens
// the cache index in the node's local database, which a start has just made
// empty: a restart is treated as a crash, so the index's rows would be stale.
func (c *cache) open(opts CacheOptions, db *LocalDB) error {
	left, err := clearCache(c.data, c.log)
	if err != nil {
		return err
	}
	if db == nil {
		return errors.New("the node has no local database for the cache index")
	}
	c.idx = newIndex(db.DB)
	c.held, c.every = left, opts.Every
	c.limit.Store(opts.Limit)
	return nil
}

// clearCache is what a start does to the data directory, after the node has
// adopted its own journal: it deletes the contents of the temporary directory and every shard file that no
// process has open, so the node starts with an empty cache, as a new one
// would. The index's rows go with it, the node's local database being made
// again at every start.
//
// The index runs with synchronous=FULL and the apps' files with NORMAL, so
// after an OS crash the index cannot be trusted to agree with the files or
// with blob storage, and the files cannot either.
//
// A file that an app still has open (from before the restart) is never deleted:
// the app could go on committing to it with no error. The returned map holds
// their paths and their shards' names ("" for a file that isn't a shard's); the
// cache loop looks at them again until the app has closed them, and activating
// the shard meanwhile is refused (ErrAppHolds).
func clearCache(data string, log *slog.Logger) (held map[string]shard.Name, err error) {
	if ents, err := os.ReadDir(filepath.Join(data, tempDirName)); err == nil {
		for _, e := range ents {
			if err := os.RemoveAll(filepath.Join(data, tempDirName, e.Name())); err != nil {
				return nil, err
			}
		}
	}
	bases := map[string]bool{} // the database file of each set of shard files
	local := filepath.Join(data, LocalDBName)
	localFiles := append(capture.WALSidecarsOf(local).Paths(), local, local+"-journal")
	err = filepath.WalkDir(data, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == filepath.Join(data, tempDirName) {
				return filepath.SkipDir
			}
			return nil
		}
		// The node's local database is not a shard's file: the index.
		if slices.Contains(localFiles, p) {
			return nil
		}
		// A database's files beside it: the WAL sidecars, and the leftover of
		// an interrupted restore.
		for _, suffix := range append(capture.WALSidecarSuffixes(), ".restoring") {
			p = strings.TrimSuffix(p, suffix)
		}
		bases[p] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	held = map[string]shard.Name{}
	for p := range bases {
		if isHeld, err := capture.WALSidecarsOf(p).Held(); err != nil {
			return nil, err
		} else if isHeld {
			held[p] = shardOfPath(data, p)
			log.Error("a shard file from before the restart is still open in another process, leaving it alone until it is closed", "path", p)
			continue
		}
		removeLocal(p)
	}
	return held, nil
}

// shardOfPath is the name of the shard whose file is at path (see pathFor),
// zero if it is not a shard's file.
func shardOfPath(data, path string) shard.Name {
	rel, err := filepath.Rel(data, path)
	if err != nil {
		return shard.Name{}
	}
	kind, file := filepath.Split(rel)
	id, _, _ := strings.Cut(file, ".")
	name, err := shard.ParseName(strings.TrimSuffix(filepath.ToSlash(kind), "/") + "/" + id)
	if err != nil {
		return shard.Name{}
	}
	return name
}

func (c *cache) String() string { return "cache loop" }

// Serve trims the cache to its limit and deletes the files held from before a
// restart once they are closed. It is the cache's loop, as a service.
func (c *cache) Serve(ctx context.Context) error {
	tick, stop := c.clock.NewTicker(c.every)
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick:
		}
		c.evictIfNeeded(ctx)
		c.sweepHeld()
		c.stats.CachePasses.Inc()
	}
}

// sweepHeld deletes the shard files that were still open at the start, as soon
// as no process has them open any more. Each is swept under an entry for its
// shard, so that an activation of the shard never sees half of it deleted.
func (c *cache) sweepHeld() {
	for p, name := range c.held {
		if !name.IsZero() {
			e, ok := c.tab.claim(name)
			if !ok {
				continue // the shard is being activated (which refuses while the file is held), or something else is happening to it
			}
			c.sweep(p)
			c.tab.remove(e, nil)
			continue
		}
		c.sweep(p)
	}
}

func (c *cache) sweep(path string) {
	if isHeld, err := capture.WALSidecarsOf(path).Held(); err != nil || isHeld {
		return
	}
	removeLocal(path)
	delete(c.held, path)
}

func removeLocal(path string) {
	for _, p := range append(capture.WALSidecarsOf(path).Paths(), path, path+".restoring") {
		os.RemoveAll(p)
	}
}

// pathFor is the opaque name of a shard's file: it carries the generation, so
// a shard deleted and created again never reuses a file. Kinds and ids need no
// escaping (see shard.ValidID).
func (c *cache) pathFor(name shard.Name, generation shard.Gen) string {
	return filepath.Join(c.data, string(name.Kind()), string(name.ID())+"."+string(generation)+".db")
}

// put records a shard in the index: active, or cached with what reactivating
// it in this process needs (its manifest and its files as they are at rest).
func (c *cache) put(a *actor, state rowState, pos pos.Position) error {
	row := indexRow{Name: a.Name, Path: a.Path, Generation: a.Gen, Position: pos, State: state, LastUsed: time.Now()}
	if state == stateCached {
		mj, err := json.Marshal(a.h.Manifest)
		if err != nil {
			return err
		}
		if _, err := os.Stat(a.Path); err != nil {
			return err
		}
		sig := statSig(a.Path)
		row.Manifest, row.ETag = string(mj), string(a.h.ETag)
		row.Size, row.MTimeNs, row.WALSize, row.WALMTimeNs = sig.db, sig.dbMod, sig.wal, sig.walMod
		row.Bytes = sig.db + sig.wal
		// The CRCs of the pages, so the shard is activated again without reading
		// them. They are of the database as the final sync left it.
		if sh := a.cap.load(); sh != nil {
			if cks := sh.Checksums(); cks != nil {
				b, err := cks.MarshalBinary()
				if err != nil {
					return err
				}
				row.CRCs = b
			}
		}
	}
	return c.idx.put(row)
}

// clearLocal removes leftovers at path, unless another process has the file open.
func (c *cache) clearLocal(path string) error {
	if held, err := capture.WALSidecarsOf(path).Held(); err != nil {
		return err
	} else if held {
		return fmt.Errorf("%w: %s", ErrAppHolds, path)
	}
	removeLocal(path)
	return nil
}

func (c *cache) discard(row indexRow) error {
	c.stats.Discards.Add(1)
	if err := c.clearLocal(row.Path); err != nil {
		return err
	}
	return c.idx.delete(row.Name)
}

// evict drops a cached shard, to make room or because the Catalog was asked
// to (Catalog.Evict). It runs under an entry of its own for the shard, and
// leaves alone a shard that has one already (active, or being activated or
// deleted). It reports whether it evicted the shard.
func (c *cache) evict(ctx context.Context, row indexRow) (bool, error) {
	e, ok := c.tab.claim(row.Name)
	if !ok {
		return false, nil
	}
	defer c.tab.remove(e, nil)
	cur, ok, err := c.idx.get(row.Name)
	if err != nil || !ok || cur.State != stateCached {
		return false, err
	}
	if err := c.drop(ctx, cur); err != nil {
		return false, err
	}
	return true, nil
}

// drop evicts a cached shard: its manifest is released by compare-and-swap
// (the delta was written when it was deactivated), then its files and its row
// are deleted. The caller holds an entry for the shard. A shard the app has open is not dropped (ErrAppHolds), and on any other
// error the files stay for a later try.
func (c *cache) drop(ctx context.Context, cur indexRow) error {
	if held, err := capture.WALSidecarsOf(cur.Path).Held(); err != nil {
		return err
	} else if held {
		return fmt.Errorf("%w: %s", ErrAppHolds, cur.Name)
	}
	h, err := c.files.Attach(ctx, cur.Name)
	switch {
	case errors.Is(err, blob.ErrNotFound):
	case err != nil:
		return err
	case h.Manifest.Owner == c.node:
		switch err := h.ReleaseOwnership(ctx); {
		case errors.Is(err, blob.ErrUnknown):
			// The release may have landed. The cached row would let the next
			// activation serve the shard with no request to blob storage, while the
			// manifest names no journal, so it goes as if the release had landed: the
			// next activation claims and restores.
			c.log.Warn("the release of a cached shard has an unknown outcome, dropping it as released", "shard", cur.Name, "err", err)
		case err != nil && !errors.Is(err, blob.ErrConflict):
			return err // storage trouble: nothing happened, keep the files and try again later
		}
	}
	removeLocal(cur.Path)
	return c.idx.delete(cur.Name)
}

// evictIfNeeded removes the least recently used cached shards while the cache
// is over its limit.
func (c *cache) evictIfNeeded(ctx context.Context) {
	limit := c.limit.Load()
	if limit <= 0 {
		return
	}
	for {
		used, err := c.idx.cachedBytes()
		if err != nil || used <= limit {
			return
		}
		rows, err := c.idx.leastRecentlyUsed(16)
		if err != nil || len(rows) == 0 {
			return
		}
		progress := false // a shard with an entry (being activated, say) is left for a later pass
		for _, r := range rows {
			evicted, err := c.evict(ctx, r)
			if err != nil {
				c.log.Warn("could not evict a cached shard", "shard", r.Name, "err", err)
				continue
			}
			if !evicted {
				continue
			}
			progress = true
			if used, err = c.idx.cachedBytes(); err != nil || used <= limit {
				return
			}
		}
		if !progress {
			return
		}
	}
}

// deleteReleased deletes a shard that nothing has open: its manifest by
// conditional delete (at h's version), then its generation's LTX files, then
// the local files at path and the cache index row. If the manifest is still
// there afterwards, nothing was deleted and resume starts capture again.
//
// A crash after the manifest is deleted leaves LTX files that nothing refers to
// in blob storage, and the local files, which the next start finds in the index
// as left by an earlier process and discards. A crash before it leaves the shard
// as it was.
func (c *cache) deleteReleased(ctx context.Context, target shard.Name, path string, h *shardfile.Handle, resume func() error) error {
	if err := h.DeleteShard(ctx); err != nil {
		// If the manifest is gone the shard is deleted, whatever else failed.
		if _, _, gerr := c.files.GetManifest(ctx, target); !errors.Is(gerr, blob.ErrNotFound) {
			if rerr := resume(); rerr != nil {
				c.log.Error("could not resume capture after a failed delete", "shard", target, "err", rerr)
			}
			return err
		}
		c.log.Error("the shard's manifest is deleted but some of its files are not", "shard", target, "err", err)
	}
	if path != "" {
		removeLocal(path)
	}
	c.idx.delete(target)
	return nil
}

// cached returns the names of the deactivated shards on disk.
func (c *cache) cached() ([]shard.Name, error) {
	rows, err := c.idx.rows(`WHERE state = ?`, stateCached)
	if err != nil {
		return nil, err
	}
	out := make([]shard.Name, len(rows))
	for i, r := range rows {
		out[i] = r.Name
	}
	return out, nil
}
