package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// The operations of a shard's lifecycle loop: activation, deactivation and the
// delete of an active shard. Only the shard's loop runs them.

// activate follows the Activation in docs/internals.md for a single node. It is
// the first job of the shard's loop, which runs once the shard's entry is in
// the table, so nothing else activates the shard meanwhile:
//
//  1. a file cached since this process started is opened again, with no blob
//     storage call;
//  2. any other shard is claimed by compare-and-swap on its manifest and restored;
//  3. a shard with no manifest is created: a conditional create of its manifest
//     claims it, and its file starts empty. If another node creates it first,
//     it is claimed like any other shard.
func (a *shard) activate(ctx context.Context) error {
	d := a.d
	target, kind, id := a.Name, a.Kind, a.ID
	k, err := d.kinds.Lookup(kind)
	if err != nil {
		return err
	}
	if row, ok, err := d.cache.idx.get(target); err != nil {
		return err
	} else if ok && row.State == stateCached {
		done, err := a.activateCached(ctx, row)
		if done || err != nil {
			return err
		}
	}

	h, created, err := d.files.Open(ctx, target, k.AutoDelete)
	if err != nil {
		return err
	}
	noTail := h.Manifest.Journal == "" // known before the claim sets it
	if !created {
		if err := h.Claim(ctx); err != nil {
			return err
		}
	}
	a.Gen, a.Path = h.Manifest.Generation, d.cache.pathFor(kind, id, h.Manifest.Generation)
	a.autoDelete = h.Manifest.AutoDelete
	if err := d.cache.clearLocal(a.Path); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(a.Path), 0o755); err != nil {
		return err
	}
	a.h = h
	var restored *capture.Restored
	if created {
		d.stats.Creates.Add(1)
	} else {
		res, err := d.files.Restore(ctx, target, a.Path, shardfile.RestoreOptions{NoJournal: noTail})
		switch {
		case errors.Is(err, shardfile.ErrNothingToRestore):
			// A manifest with nothing durable behind it (its node died before the
			// first flush): the shard starts empty, as a new one does.
		case err != nil:
			return err
		default:
			a.h, restored = res.Handle, res.Capture
		}
		d.stats.Restores.Add(1)
	}
	if err := a.startCapture(ctx, restored); err != nil {
		return err
	}
	return d.cache.put(a, stateActive, 0)
}

// activateCached reuses a cached file. done is false when the file had to be
// discarded and the shard needs the normal path.
func (a *shard) activateCached(ctx context.Context, row indexRow) (done bool, err error) {
	d := a.d
	// Litestream leaves the -wal in place when it closes, and it is part of the
	// file's state, so both must be as they were at deactivation.
	_, serr := os.Stat(row.Path)
	if sig := statSig(row.Path); serr != nil || sig != (fileSig{db: row.Size, dbMod: row.MTimeNs, wal: row.WalSize, walMod: row.WalMTimeNs}) {
		d.log.Warn("cached shard files changed since it was deactivated, discarding them", "shard", a.Name, "path", row.Path, "err", serr)
		return false, d.cache.discard(row)
	}
	// Nothing else may have the file open (the node's own handles are closed).
	if held, err := capture.ProbeOnce(row.Path); err != nil {
		return false, err
	} else if held {
		d.log.Error("a process has a deactivated shard's file open", "shard", a.Name, "path", row.Path)
		return false, fmt.Errorf("%w: %s", ErrAppHolds, a.Name)
	}
	// The row was written by this process (the cache starts empty), so the manifest
	// it recorded is the one the node holds, and nothing needs asking of blob storage.
	var man shardfile.Manifest
	if err := json.Unmarshal([]byte(row.Manifest), &man); err != nil {
		return false, d.cache.discard(row)
	}
	h := &shardfile.Handle{F: d.files, Name: a.Name, Manifest: &man, ETag: blob.ETag(row.ETag)}
	a.h, a.Gen, a.Path, a.autoDelete = h, row.Generation, row.Path, h.Manifest.AutoDelete
	if err := a.startCapture(ctx, &capture.Restored{Position: row.Position}); err != nil {
		return false, err
	}
	d.stats.CachedHits.Add(1)
	if err := d.cache.put(a, stateActive, 0); err != nil {
		return false, err
	}
	return true, nil
}

// startCapture starts capturing the shard's file and runs Litestream's first
// sync before the app can open it. A restored (or cached) shard is capture's
// seeded start: Litestream continues from the restored state without reading
// the database, so that first sync writes nothing and the position stays the
// restored one. A new shard (restored is nil) gets an empty file first, and its
// first sync, of an empty database holding only Litestream's tables, is its
// first position.
func (a *shard) startCapture(ctx context.Context, restored *capture.Restored) error {
	d := a.d
	if restored == nil {
		// The node holds no locks on a file it has just created, so closing the
		// descriptor drops nothing.
		f, err := os.OpenFile(a.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		f.Close()
	}
	sh, err := capture.New(capture.Config{
		Name: a.Name, Path: a.Path, Generation: a.h.Manifest.Generation,
		MetaRoot: d.scratch, Writer: d.writer, Restored: restored, Logger: d.log, Durable: d.flush.WaitCycle,
		TempDir: d.tempDir, SpoolLimit: d.spool,
	})
	if err != nil {
		return err
	}
	// Counted before anything is captured, so that the lease's journal range
	// covers the first file from the moment it is queued.
	a.cap.counted(sh)
	if err := sh.Capture(ctx); err != nil {
		a.cap.uncounted(sh)
		sh.Close(ctx)
		sh.Discard()
		return err
	}
	// The reserved table goes in after the first sync, before the app has the shard.
	if err := ensureMessages(ctx, sh); err != nil {
		a.cap.uncounted(sh)
		sh.Close(ctx)
		sh.Discard()
		return err
	}
	a.cap.start(sh)
	return nil
}

// resume starts capture again on a shard whose capture was let go of but whose
// deactivation couldn't be finished, carrying the tail over.
func (a *shard) resume(ctx context.Context, old *capture.Shard) error {
	// The release that failed may have failed because ctx ended (the shutdown
	// deadline passed). Capture has to be back whatever happens: a shard left with
	// a closed database can never be let go of.
	ctx = context.WithoutCancel(ctx)
	err := a.startCapture(ctx, &capture.Restored{Position: old.Position(), Tail: old.Tail()})
	old.Discard() // the new capture holds the tail's files now (or New gave them up)
	return err
}

// resumeIfClosed resumes capture after a release that failed, unless the
// release reopened capture itself (it does when only its last step failed):
// a second capture would leave the first one's Litestream open.
func (a *shard) resumeIfClosed(ctx context.Context, sh *capture.Shard) {
	if sh.Open() {
		return
	}
	if err := a.resume(ctx, sh); err != nil {
		a.d.log.Error("could not resume capture after a failed release", "shard", a.Name, "err", err)
	}
}

// deactivate lets go of an idle shard: Release (a final sync and closing every
// handle, then the -shm check), a delta for its tail, and a row in the cache
// index. A shard the app never wrote to is left with an empty manifest instead,
// so requests for ids that don't exist leave almost nothing behind. It reports
// whether the shard was let go of. A shard the app still holds, or one in use,
// stays active.
func (a *shard) deactivate(ctx context.Context) (bool, error) {
	d := a.d
	if a.gone {
		return true, nil
	}
	a.idle.held = false
	if !acquireSem(ctx, d.deactivations) {
		return false, context.Cause(ctx)
	}
	defer func() { <-d.deactivations }()
	if !a.e.close() {
		return false, nil // a request holds it
	}
	defer func() {
		if !a.gone {
			a.addOwner() // capture resumed, or never stopped
			a.e.reopen()
		}
	}()

	// An auto-delete shard with rows in its outbox or inbox is released like any
	// other, and carries on at its next activation. Nothing is written for one
	// that is deleted.
	o := a.msgs.load()
	deleteIt := false
	if a.autoDeletes() {
		live, err := o.live(ctx, true, false)
		if err != nil {
			return false, err
		}
		deleteIt = !live
	}
	// The hints that wake the shard again go first (a stale one is harmless). If
	// one can't be written the shard stays active. Then the messages owner stops,
	// so nothing uses the capture during the final sync.
	if err := o.hintsBeforeRelease(ctx); err != nil {
		return false, err
	}
	if err := a.removeOwner(); err != nil {
		return false, err
	}
	sh := a.cap.load()
	released, empty, err := sh.Release(ctx)
	if err != nil {
		a.resumeIfClosed(ctx, sh)
		return false, err
	}
	if !released {
		a.idle.held = true
		d.log.Warn("the app still has the shard open, keeping it active", "shard", a.Name)
		return false, nil
	}
	// The tail is only in the journal until the delta (or the deletion) is done, so
	// the lease's range covers it until then. A resumed capture is counted instead.
	defer a.cap.uncounted(sh)
	if empty {
		// The app never wrote to it: the shard leaves nothing behind. A conflict
		// means somebody else changed the manifest, and the shard is kept.
		if err := a.h.Delete(ctx); err == nil || errors.Is(err, blob.ErrNotFound) {
			sh.Discard()
			removeLocal(a.Path)
			d.cache.idx.delete(a.Name)
			d.stats.Deletes.Add(1) // before the shard stops being active, so whoever sees it gone sees the count
			a.gone = true
			return true, nil
		} else if ctx.Err() == nil {
			d.log.Warn("could not delete the manifest of a shard the app never wrote to, keeping it", "shard", a.Name, "err", err)
		}
	}
	if deleteIt {
		// No final delta: the tail goes with the journal.
		defer sh.Discard() // the shard is deleted, or resumed with the tail handed on
		resume := func() error { return a.resume(ctx, sh) }
		if err := d.cache.deleteReleased(ctx, a.Name, a.Path, a.h, resume); err != nil {
			return false, err
		}
		d.stats.AutoDeletes.Add(1) // before the shard stops being active, as Deletes is
		a.gone = true
		return true, nil
	}
	// A tail is a delta by the time the shard is cached, so the lease's journal range
	// never has to cover a shard that isn't active.
	if err := a.h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		if rerr := a.resume(ctx, sh); rerr != nil {
			d.log.Error("could not resume capture after a failed delta", "shard", a.Name, "err", rerr)
		}
		return false, err
	}
	if err := d.cache.put(a, stateCached, a.h.Manifest.LastPosition()); err != nil {
		return false, err
	}
	a.gone = true
	return true, nil
}

// deleteActive deletes an active shard.
func (a *shard) deleteActive(ctx context.Context) error {
	if a.gone {
		return errLoopEnded // let go of already: the caller looks again
	}
	if !a.e.close() {
		return fmt.Errorf("%w: %s has requests or connections", ErrBusy, a.Name)
	}
	defer func() {
		if !a.gone {
			a.addOwner()
			a.e.reopen()
		}
	}()
	if err := a.removeOwner(); err != nil {
		return err
	}
	sh := a.cap.load()
	released, _, err := sh.Release(ctx)
	if err != nil || !released {
		if err == nil {
			err = fmt.Errorf("%w: the app has %s open", ErrBusy, a.Name)
		}
		a.resumeIfClosed(ctx, sh)
		return err
	}
	// Counted until the deletion is done: the manifest points at the tail until then.
	defer a.cap.uncounted(sh)
	defer sh.Discard() // the shard is deleted, or resumed with the tail handed on
	resume := func() error { return a.resume(ctx, sh) }
	if err := a.d.cache.deleteReleased(ctx, a.Name, a.Path, a.h, resume); err != nil {
		return err
	}
	a.gone = true
	return nil
}
