package node

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// cleanDataDir is what a start does to the data directory, after the node has
// adopted its own journal: it deletes the contents of the temporary directory and every shard file,
// so the node starts with nothing on disk, as a new one would. A node keeps a
// shard's files only while the shard is active, so what is found is what a
// crash or a stop that could not let go left behind.
//
// The apps' files are written with synchronous=NORMAL, so after an OS crash they
// cannot be trusted to agree with blob storage.
//
// A file that an app still has open (from before the restart) is deleted too: the
// app keeps its handle on the inode until it closes it, and a commit through that
// handle is lost, which is the app's bug (it must close its connections within the
// idle timeout). No later activation uses the path (see pathFor).
func cleanDataDir(data string) error {
	if ents, err := os.ReadDir(filepath.Join(data, tempDirName)); err == nil {
		for _, e := range ents {
			if err := os.RemoveAll(filepath.Join(data, tempDirName, e.Name())); err != nil {
				return err
			}
		}
	}
	bases := map[string]bool{} // the database file of each set of shard files
	err := filepath.WalkDir(data, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == filepath.Join(data, tempDirName) {
				return filepath.SkipDir
			}
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
		return err
	}
	for p := range bases {
		removeLocal(p)
	}
	return nil
}

// removeLocal deletes a shard's files: the database, its sidecars and a restore's leftover. A process that
// has them open keeps its handles on the inodes until it closes them.
func removeLocal(path string) {
	for _, p := range append(capture.WALSidecarsOf(path).Paths(), path, path+".restoring") {
		os.RemoveAll(p)
	}
}

// pathFor is the opaque name of a new local file of a shard: id.generation.unique.db
// in the kind's directory of data. A path is never used twice, so whatever an app still has open
// from an earlier file (one the node let go of, or one from before a restart) is never
// the file of a new activation, and old files are deleted at once, an open handle keeping
// the inode until the app closes it. The unique part is random, since a counter would need to outlive restarts.
// Kinds and ids need no escaping (see shard.ValidID).
func pathFor(data string, name shard.Name, generation shard.Gen) string {
	var b [6]byte
	rand.Read(b[:]) // never fails
	return filepath.Join(data, string(name.Kind()), string(name.ID())+"."+string(generation)+"."+hex.EncodeToString(b[:])+".db")
}

// deleteReleased deletes a shard that was released: its manifest by
// conditional delete (at the handle's version), then its generation's LTX files, then
// the local files at the shard's path. If the manifest is still there afterwards, nothing was deleted
// and resume starts capture again.
//
// A crash after the manifest is deleted leaves LTX files that nothing refers to
// in blob storage, and the local files, which the next start deletes. A crash before it leaves the shard
// as it was.
func (a *actor) deleteReleased(ctx context.Context, hooks shardfile.Hooks, resume func() error) error {
	d := a.d
	if err := a.h.DeleteShard(ctx, hooks); err != nil {
		// If the manifest is gone the shard is deleted, whatever else failed.
		if _, _, gerr := d.files.GetManifest(ctx, a.Name); !errors.Is(gerr, blob.ErrNotFound) {
			if rerr := resume(); rerr != nil {
				d.log.Error("could not resume capture after a failed delete", "shard", a.Name, "err", rerr)
			}
			return err
		}
		d.log.Error("the shard's manifest is deleted but some of its files are not", "shard", a.Name, "err", err)
	}
	if a.Path != "" {
		removeLocal(a.Path)
	}
	return nil
}
