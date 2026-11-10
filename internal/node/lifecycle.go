package node

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// The operations of a shard's lifecycle loop: activation, deactivation and the
// delete of an active shard. Only the shard's loop runs them.

// activate follows the Activation in docs/internals.md for a single node. It is
// the first job of the shard's loop, which runs once the shard's entry is in
// the table, so nothing else activates the shard meanwhile:
//
//  1. a shard with a manifest is claimed by compare-and-swap on it and restored;
//  2. a shard with no manifest is created: a conditional create of its manifest
//     claims it, and its file starts empty. If another node creates it first,
//     it is claimed like any other shard.
func (a *actor) activate(ctx context.Context) error {
	d := a.d
	target := a.Name
	if _, err := d.kinds.Lookup(target.Kind()); err != nil {
		return err
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
	return a.startCapture(ctx, restored)
}

// setPath points the shard at a new local file for h's generation (pathFor
// never gives the same path twice). A path this activation set before, when it
// starts over, is deleted with whatever a restore left there.
func (a *actor) setPath(h *shardfile.Handle) error {
	if a.Path != "" {
		removeLocal(a.Path)
	}
	a.Gen, a.Path = h.Manifest.Generation, pathFor(a.d.data, a.Name, h.Manifest.Generation)
	return os.MkdirAll(filepath.Dir(a.Path), 0o755)
}

// discardFiles deletes the files a failed activation left: the next activation
// restores into a new file.
func (a *actor) discardFiles() {
	if a.Path != "" {
		removeLocal(a.Path)
	}
}

// startCapture starts capturing the shard's file and runs the first sync
// before the app can open it. A restored shard continues from the
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
	// The reserved tables go in after the first sync, before the app has the
	// shard.
	if err := ensureTables(ctx, sh); err != nil {
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
// handle, then the -shm check) and a delta for its tail, which gives the shard
// up in the same manifest update, and deletes its files. A shard that holds
// nothing of the app's (it was never written, or the app removed everything) is
// deleted instead, so requests for ids that don't exist leave nothing behind.
// ifHeld is what to do with a file another process has open: resume capture
// and keep the shard active (what an idle deactivation does), or go ahead (what
// a stop does). It reports whether the shard was let go of. A shard the app
// still holds (unless ifHeld goes ahead), or one in use, stays active.
func (a *actor) deactivate(ctx context.Context, ifHeld capture.IfHeld) (bool, error) {
	d := a.d
	if a.gone {
		return true, nil
	}
	a.idle.held = false
	if !acquireSem(ctx, d.sems.deactivate) {
		return false, context.Cause(ctx)
	}
	defer func() { <-d.sems.deactivate }()
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
	released, final, err := sh.Release(ctx, read, ifHeld)
	if err != nil {
		a.resumeIfClosed(ctx, sh)
		return false, err
	}
	if !released {
		a.idle.held = true
		if ifHeld == capture.GoAheadIfHeld {
			// Going ahead releases whatever the app has open, so what stopped it is a commit made during the release.
			d.log.Warn("the app committed to the shard while it was being let go of, keeping it active", "shard", a.Name)
		}
		return false, nil
	}
	// Whether the app had the file open at the release (only a stop's release goes ahead over one that is).
	open := sh.OpenAtRelease()
	// The tail is only in the journal until the delta (or the deletion) is done, so
	// the lease's range covers it until then. A resumed capture is counted instead.
	defer a.cap.uncounted(sh)
	// The reminder to record with the delta: the exact time when the release's
	// read saw the final state, and now otherwise, since the shard's state at
	// the end is then unknown and now is never late.
	var reminder func(end pos.Position) shardfile.Minute
	switch r := final.(type) {
	case *capture.Deleted:
		// The shard leaves nothing behind, and no final delta: the tail goes with the
		// journal. If the manifest can't be deleted (somebody else changed it),
		// capture resumes and the shard stays active.
		defer sh.Discard() // the shard is deleted, or resumed with the tail handed on
		resume := func() error { return a.resume(ctx, sh) }
		if err := a.deleteReleased(ctx, resume); err != nil {
			return false, err
		}
		a.warnOpen(open)
		d.logDeleted(a, r.Marked)
		d.stats.Deletes.Add(1) // before the shard stops being active, so whoever sees it gone sees the count
		a.gone = true
		return true, nil
	case *capture.Kept:
		reminder = func(end pos.Position) shardfile.Minute { return a.recordedFor(next, r.At, end) }
	case *capture.Uncertain:
		// The read did not see the final state, so the shard's state at the end
		// is unknown and the reminder is now, never late. A shard with nothing
		// in it has no reminder to be late for, and none is recorded.
		reminder = func(end pos.Position) shardfile.Minute {
			if end == 0 {
				return 0
			}
			return a.recordedNow()
		}
	}
	// A tail is a delta by the time the shard is let go of, so the lease's journal range
	// never has to cover a shard that isn't active. The delta's manifest update
	// records the next reminder, with its entry in the reminder index created
	// first, and gives the shard up too (shardfile.Hooks.Release). With no tail
	// there is no delta, and the next reminder is written alone (the release with
	// it). If either fails, capture resumes and the shard stays active.
	err = a.finish(sh, reminder)
	if errors.Is(err, shardfile.ErrMovedOn) {
		// The release landed and another node has claimed the shard since: it was
		// given up after all, so it goes as a release does.
		err = nil
	}
	if err != nil {
		if a.h.Unsettled() {
			// The update may have landed, which left the shard unowned: it is never
			// served again from here, and capture does not resume. Nothing was
			// served or dropped on a guess, and the node's stop or adoption settles it.
			d.log.Error("the release of a shard has an unknown outcome and the node is stopping, abandoning it", "shard", a.Name, "err", err)
			a.gone = true
			return false, err
		}
		if rerr := a.resume(ctx, sh); rerr != nil {
			d.log.Error("could not resume capture after a failed delta", "shard", a.Name, "err", rerr)
		}
		return false, err
	}
	a.gone = true
	// The manifest names nobody now, and the files go: nothing here will be reused.
	a.warnOpen(open)
	removeLocal(a.Path)
	return true, nil
}

// warnOpen says a shard was let go of at a stop though the app had its file open, if it did (open), and counts it. The file is deleted: the app's handle keeps the inode until it closes it, and a commit through that handle is lost. Only a stop goes ahead over an open file, so nothing else gets here with open set.
func (a *actor) warnOpen(open bool) {
	if !open {
		return
	}
	a.d.stats.LetGoHeld.Add(1)
	a.d.log.Warn("the app still had the shard's file open when the node stopped, so the node let the shard go and deleted the file: close connections within the idle timeout, and never write a shard outside a request", "shard", a.Name)
}

// finish writes the shard's tail as a delta and its next reminder, and gives
// the shard up in the same update. Giving up cannot be told from not having
// landed when the outcome is unknown (the handle is unsettled), and a shard that was given up must
// not be served while one that wasn't must not be dropped, so it asks again until it
// learns, which is safe since the update is the same each time (the handle looks at the
// stored manifest first). It runs on the loop's context and not the caller's,
// since a caller giving up must not leave the outcome unlearned, and only the loop's ending
// (the node stopping) ends the asking.
func (a *actor) finish(sh *capture.Shard, reminder func(end pos.Position) shardfile.Minute) error {
	ctx := a.ctx
	wait := 100 * time.Millisecond
	for {
		recorded := false
		record := func(_ context.Context, end pos.Position) (shardfile.Minute, error) {
			recorded = true
			return reminder(end), nil
		}
		err := a.h.WriteDelta(ctx, sh, shardfile.Hooks{BeforeUpdate: record, At: a.d.opts.At, Release: true})
		if err == nil && !recorded {
			err = a.h.SetReminder(ctx, reminder(sh.Position()), true)
		}
		if err == nil || !a.h.Unsettled() || ctx.Err() != nil {
			return err
		}
		a.d.log.Warn("the release of a shard has an unknown outcome, trying again", "shard", a.Name, "err", err)
		if !sleep(ctx, wait) {
			return err
		}
		wait = min(2*wait, 2*time.Second)
	}
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
