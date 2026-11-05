package node

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/migrate"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/remindex"
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
func (a *actor) activate(ctx context.Context) error {
	d := a.d
	target := a.Name
	if _, err := d.kinds.Lookup(target.Kind()); err != nil {
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

	h, created := a.read, false
	if h == nil {
		var err error
		if h, created, err = d.files.Open(ctx, target); err != nil {
			return err
		}
	}
	if !created {
		if err := h.Claim(ctx); err != nil {
			return err
		}
	}
	if err := a.setPath(h); err != nil {
		return err
	}
	a.h = h
	var restored *capture.Restored
	if created {
		d.stats.Creates.Add(1)
	} else {
		// A claimed shard has no tail in any journal: a claim needs the previous
		// owner's journal adopted (or nothing of the shard in it), and this node's
		// own tails become deltas before a shard stops being active here, or are
		// adopted at its next start. So its files are all of it, and the journal
		// the claim names (this node's own) isn't read.
		res, err := d.files.Restore(ctx, target, a.Path, shardfile.RestoreOptions{NoJournal: true, BeforeRename: func() error { d.at(kill.RestoreBeforeRename); return nil }})
		switch {
		case errors.Is(err, shardfile.ErrNothingToRestore):
			// A manifest with nothing durable behind it (its node died before the
			// first flush, or an activation here failed): the shard starts empty, as
			// a new one does, in a new generation.
			if err := h.StartOver(ctx); err != nil {
				return err
			}
			if err := a.setPath(h); err != nil {
				return err
			}
		case err != nil:
			return err
		default:
			a.h, restored = res.Handle, res.Capture
		}
		d.at(kill.Restored)
		d.stats.Restores.Add(1)
	}
	// The row goes in before capture starts, so that an activation that fails
	// leaves no capture running: nothing would ever close it.
	if err := d.cache.put(a, stateActive, 0); err != nil {
		return err
	}
	err := a.startCapture(ctx, restored)
	if errors.Is(err, migrate.ErrNewer) {
		// A newer node migrated this system shard. It is given up, so that a node
		// that can read it claims it instead of finding it owned here. Nothing was
		// committed (the migration rolled back) and the files are all of it, so
		// there is no tail to write first.
		if rerr := a.h.ReleaseOwnership(ctx); rerr != nil {
			err = errors.Join(err, rerr)
		}
	}
	return err
}

// setPath points the shard at its local file for h's generation, removing
// whatever an earlier activation left there.
func (a *actor) setPath(h *shardfile.Handle) error {
	a.Gen, a.Path = h.Manifest.Generation, a.d.cache.pathFor(a.Name, h.Manifest.Generation)
	if err := a.d.cache.clearLocal(a.Path); err != nil {
		return err
	}
	return os.MkdirAll(filepath.Dir(a.Path), 0o755)
}

// activateCached reuses a cached file. done is false when the file had to be
// discarded and the shard needs the normal path.
func (a *actor) activateCached(ctx context.Context, row indexRow) (done bool, err error) {
	d := a.d
	// Capture leaves the -wal in place when it closes, and it is part of the
	// file's state, so both must be as they were at deactivation.
	_, serr := os.Stat(row.Path)
	if sig := statSig(row.Path); serr != nil || sig != (fileSig{db: row.Size, dbMod: row.MTimeNs, wal: row.WALSize, walMod: row.WALMTimeNs}) {
		d.log.Warn("cached shard files changed since it was deactivated, discarding them", "shard", a.Name, "path", row.Path, "err", serr)
		return false, d.cache.discard(row)
	}
	// Nothing else may have the file open (the node's own handles are closed).
	if held, err := capture.WALSidecarsOf(row.Path).Held(); err != nil {
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
	// The CRCs of the pages, so the capture reads no page to continue.
	cks := capture.NewChecksums()
	if err := cks.UnmarshalBinary(row.CRCs); err != nil {
		d.log.Error("a cached shard's checksums are unreadable, discarding the file", "shard", a.Name, "err", err)
		return false, d.cache.discard(row)
	}
	h := &shardfile.Handle{F: d.files, Name: a.Name, Manifest: &man, ETag: blob.ETag(row.ETag)}
	a.h, a.Gen, a.Path = h, row.Generation, row.Path
	// Before capture starts, as in activate.
	if err := d.cache.put(a, stateActive, 0); err != nil {
		return false, err
	}
	if err := a.startCapture(ctx, &capture.Restored{Position: row.Position, Checksums: cks}); err != nil {
		return false, err
	}
	d.stats.CachedHits.Add(1)
	return true, nil
}

// startCapture starts capturing the shard's file and runs the first sync
// before the app can open it. A restored (or cached) shard continues from the
// restored position without reading the database, so that first sync writes
// nothing and the position stays the restored one. A new shard (restored is nil)
// gets an empty file first, and its first sync, of an empty database, is its
// first position.
func (a *actor) startCapture(ctx context.Context, restored *capture.Restored) error {
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
		Writer: d.writer, Restored: restored, Logger: d.log, Durable: d.flush.WaitCycle,
		TempDir:          d.tempDir,
		At:               d.opts.At,
		Now:              time.Now,
		ChecksumMismatch: func(_, _ ltx.Checksum) { d.stats.ChecksumMismatches.Add(1) },
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
	if restored != nil {
		d.at(kill.FirstSyncDone)
	}
	// The reserved table (and a system kind's schema and seed) go in after the
	// first sync, before the app has the shard.
	if err := ensureTables(ctx, sh, a.sys, d.now().Unix(), d.rem.since(d.now())); err != nil {
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
func (a *actor) resume(ctx context.Context, old *capture.Shard) error {
	// The release that failed may have failed because ctx ended (the shutdown
	// deadline passed). Capture has to be back whatever happens: a shard left with
	// a closed database can never be let go of.
	ctx = context.WithoutCancel(ctx)
	err := a.startCapture(ctx, &capture.Restored{Position: old.Position(), Tail: old.TakeTail(), Checksums: old.Checksums()})
	old.Discard() // the new capture holds the tail's files now (or New gave them up)
	return err
}

// resumeIfClosed resumes capture after a release that failed, unless the
// release reopened capture itself (it does when only its last step failed):
// a second capture would leave the first one's connections open.
func (a *actor) resumeIfClosed(ctx context.Context, sh *capture.Shard) {
	if sh.Open() {
		return
	}
	if err := a.resume(ctx, sh); err != nil {
		a.d.log.Error("could not resume capture after a failed release", "shard", a.Name, "err", err)
	}
}

// deactivate lets go of an idle shard: Release (a final sync and closing every
// handle, then the -shm check), a delta for its tail, and a row in the cache
// index. A shard that holds nothing of the app's (it was never written, or the
// app removed everything) is deleted instead, so requests for ids that don't
// exist leave nothing behind. It reports
// whether the shard was let go of. A shard the app still holds, or one in use,
// stays active.
func (a *actor) deactivate(ctx context.Context) (bool, error) {
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
			a.addLoop() // capture resumed, or never stopped
			a.e.reopen()
		}
	}()

	// The reminders loop stops first, so nothing of the node's changes the rows
	// meanwhile and nothing uses the capture during the final sync.
	if err := a.removeLoop(); err != nil {
		return false, err
	}
	// The next reminder is read inside the release, at the shard's final state. A
	// marked shard is deleted by this release, so the marker doesn't count.
	now := d.now()
	var next int64
	read := func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		next, err = nextReminderOf(ctx, tx, d.rem.since(now), now.Unix(), false)
		return err
	}
	sh := a.cap.load()
	released, final, err := sh.Release(ctx, read)
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
	// The reminder to record with the delta: the exact time when the release's
	// read saw the final state, and now otherwise, since the shard's state at
	// the end is then unknown and now is never late.
	var reminder func(end pos.Position) remindex.Minute
	switch r := final.(type) {
	case *capture.Deleted:
		// The shard leaves nothing behind, and no final delta: the tail goes with the
		// journal. If the manifest can't be deleted (somebody else changed it),
		// capture resumes and the shard stays active.
		defer sh.Discard() // the shard is deleted, or resumed with the tail handed on
		resume := func() error { return a.resume(ctx, sh) }
		if err := d.cache.deleteReleased(ctx, a.Name, a.Path, a.h, resume); err != nil {
			return false, err
		}
		d.logDeleted(a, r.Marked)
		d.stats.Deletes.Add(1) // before the shard stops being active, so whoever sees it gone sees the count
		a.gone = true
		return true, nil
	case *capture.Kept:
		reminder = func(end pos.Position) remindex.Minute { return a.recordedFor(next, r.At, end) }
	case *capture.Uncertain:
		// The read did not see the final state, so the shard's state at the end
		// is unknown and the reminder is now, never late. A shard with nothing
		// in it has no reminder to be late for, and none is recorded.
		reminder = func(end pos.Position) remindex.Minute {
			if end == 0 {
				return 0
			}
			return a.recordedNow()
		}
	}
	// A tail is a delta by the time the shard is cached, so the lease's journal range
	// never has to cover a shard that isn't active. The delta's manifest update
	// records the next reminder, with its entry in the reminder index created
	// first. With no tail there is no delta, and the next reminder is written
	// alone if it differs from the manifest's (a reminder that went stale, or an
	// adoption's). If either fails, capture resumes and the shard stays active.
	recorded := false
	record := func(_ context.Context, end pos.Position) (remindex.Minute, error) {
		recorded = true
		return reminder(end), nil
	}
	err = a.h.WriteDelta(ctx, sh, shardfile.Hooks{BeforeUpdate: record, At: d.opts.At})
	if err == nil && !recorded {
		err = a.h.SetReminder(ctx, reminder(sh.Position()))
	}
	if err != nil {
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

// logDeleted says a shard was deleted at deactivation because it held nothing.
// One the app marked for deletion is deliberate, so it is only a debug line. For
// an empty one the first of each kind on this node is logged at info, with the
// rule, since a new app with no migrations yet meets it as clients getting 404
// for a shard its call created. Later ones are debug, since lookups from app code
// can delete many.
func (d *actorDeps) logDeleted(a *actor, marked bool) {
	if marked {
		d.log.Debug("deleted at deactivation: the app marked it for deletion", "shard", a.Name)
		return
	}
	msg := "deleted at deactivation: it held nothing of the app's (no tables, user_version or reminders), and a call creates it again"
	if _, seen := d.deletedKinds.LoadOrStore(a.Name.Kind(), struct{}{}); seen {
		d.log.Debug(msg, "shard", a.Name)
		return
	}
	d.log.Info(msg, "shard", a.Name)
}
