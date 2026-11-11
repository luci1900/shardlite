package shardfile

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/shard"
)

// Once a rebase's swap has landed, the write path deletes every unlisted LTX
// file of the generation (docs/internals.md, Garbage). What it cannot delete it
// reports here, since nothing else notices: the file costs storage until the
// shard's next rebase or an operator's walk takes it.

// logger is Files.Log, or the default logger.
func (f *Files) logger() *slog.Logger {
	if f.Log != nil {
		return f.Log
	}
	return slog.Default()
}

// leak reports an LTX file that no manifest lists and that the node could not
// delete: it logs the shard and the key, and counts the file in Files.Leaked,
// so a leak is seen without walking the store.
func (f *Files) leak(n shard.Name, key string, err error) {
	if f.Leaked != nil {
		f.Leaked.Inc()
	}
	f.logger().Warn("could not delete an LTX file that no manifest lists", "shard", n, "key", key, "err", err)
}

// leakRead reports a read that failed before a delete of garbage, so the
// files it would have deleted stay: the sweep's listing or its read of the
// lease. It counts once in Files.Leaked, like a file.
func (f *Files) leakRead(n shard.Name, msg string, err error, attrs ...any) {
	if f.Leaked != nil {
		f.Leaked.Inc()
	}
	f.logger().Warn(msg, append([]any{"shard", n, "err", err}, attrs...)...)
}

// sweepStays ends the message of a read that stopped a sweep.
const sweepStays = ", so the shard's unlisted LTX files stay until its next rebase"

// sweep deletes the garbage of the shard's generation: every LTX file the
// manifest doesn't list and the handle holds no write id for. A rebase runs it
// once its swap has landed, which reclaims not only the files the rebase
// supersedes but whatever earlier crashes, lost races and zombies left in the
// generation.
//
// What makes a file garbage is that nothing can list it later, since a manifest
// only ever adds files its own writer has just uploaded. From the swap on, the
// only other writer that can list a file in this generation is one taking the
// shard from this node, and every way of doing that writes the lease first (an
// adoption marks it before it uploads, a restart takes a new epoch, a release
// marks it adopted). So the lease is read after the listing, and the deletes
// happen only while it still holds this node's incarnation: a file such a
// writer uploaded before the listing is then never deleted. A writer adopting
// another node's journal can't list one either, since the manifest names this
// node's journal.
//
// A file the handle holds a write id for is skipped: an upload of its own
// still waiting for its manifest update.
//
// Failures are leaks (leak, leakRead): they cost storage until the next rebase
// and never the shard, which the swap has already put right.
func (h *Handle) sweep(ctx context.Context) {
	gen := h.Manifest.Generation
	prefix := GenerationPrefix(h.Name, gen)
	listed := make(map[string]bool, len(h.Manifest.Files))
	for _, f := range h.Manifest.Files {
		listed[FileKey(h.Name, gen, f)] = true
	}
	var garbage []string
	for info, err := range h.F.Blob.List(ctx, prefix) {
		if err != nil {
			h.F.leakRead(h.Name, "could not list the shard's generation"+sweepStays, err, "generation", gen)
			return
		}
		if !strings.HasSuffix(info.Key, ".ltx") || listed[info.Key] {
			continue
		}
		if _, held := h.writes[info.Key]; held {
			continue
		}
		garbage = append(garbage, info.Key)
	}
	if len(garbage) == 0 {
		return
	}
	// A lease that no longer holds is no failure: another incarnation is
	// taking the node's shards, and their next rebase sweeps the generation.
	if holds, err := h.stillHolds(ctx); err != nil {
		h.F.leakRead(h.Name, "could not read the node's lease"+sweepStays, err, "generation", gen)
		return
	} else if !holds {
		return
	}
	for _, k := range garbage {
		if err := h.F.Blob.Delete(ctx, k, ""); err != nil && !errors.Is(err, blob.ErrNotFound) {
			h.F.leak(h.Name, k, err)
		}
	}
}

// stillHolds reports whether the node's lease in storage still holds this
// handle's incarnation (lease.Record.Holds). A Files with no registry (a
// test's, where nobody else writes) counts as holding. An error means the lease
// couldn't be read, and the caller deletes nothing on a guess.
func (h *Handle) stillHolds(ctx context.Context) (bool, error) {
	if h.F.Leases == nil {
		return true, nil
	}
	l, err := h.F.Leases.Get(ctx, h.F.Node)
	if err != nil {
		return false, err
	}
	return l.Holds(lease.Incarnation{Node: h.F.Node, Epoch: h.F.Blob.Epoch()}), nil
}
