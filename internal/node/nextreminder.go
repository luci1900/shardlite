package node

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/model/pos"
	"github.com/luci1900/shardlite/internal/storage/shardfile"
)

// A shard's next_reminder: when it next needs waking, read from its own tables at
// its manifest writes and recorded in the manifest, with its entry in the
// reminder index (docs/internals.md, Manifest and Reminder index).

// nextReminder is when the shard next needs waking, as of one snapshot of its
// tables: now if it is marked for deletion (a row in _shardlite_delete),
// otherwise the due time of its earliest reminder that isn't stale (never
// before now, since a reminder due now keeps the shard active), and 0 if there
// is none. Unix seconds.
//
// at is the captured position the snapshot is exactly, or 0 if it can't tell
// (capture.Shard.UseAt). A snapshot that holds commits that aren't captured yet
// would make the time late for files that end before them, if one of them
// removed work.
func (a *actor) nextReminder(ctx context.Context) (w int64, at pos.Position, err error) {
	sh := a.cap.load()
	if sh == nil {
		return 0, 0, capture.ErrClosed
	}
	at, err = sh.UseAt(ctx, readNextReminder(ctx, a.d, true, &w))
	if err != nil {
		return 0, 0, fmt.Errorf("reading the next reminder: %w", err)
	}
	return w, at, nil
}

// readNextReminder is the read of nextReminder and of a deactivation's release,
// one snapshot of the shard's tables, into *w (see nextReminder). marker says
// whether the delete marker counts: not at a deactivation, which deletes a
// marked shard.
func readNextReminder(ctx context.Context, d *actorDeps, marker bool, w *int64) func(*sql.DB) error {
	now := d.now()
	return func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if marker {
			var marked bool
			if err := tx.QueryRowContext(ctx, capture.MarkedQuery).Scan(&marked); err != nil || marked {
				*w = now.Unix()
				return err
			}
		}
		due, ok, err := earliest(ctx, tx, d.rem.since(now))
		if ok {
			*w = max(due, now.Unix())
		} else {
			*w = 0
		}
		return err
	}
}

// recordedFor is the next_reminder to record for files that reach end (Unix
// minutes, 0 for none), given what nextReminder read (w, in seconds, at the
// position at): exact if its snapshot was at end, and now otherwise, since the
// shard's state at end is then unknown and now is never late. It rounds down to
// the minute, which the index is in.
func (a *actor) recordedFor(w int64, at, end pos.Position) shardfile.Minute {
	if at == end {
		return shardfile.MinuteOf(w)
	}
	return a.recordedNow()
}

// recordedNow is the next_reminder for a shard whose state at the end is
// unknown: now, since now is never late. It rounds down to the minute, which
// the index is in.
func (a *actor) recordedNow() shardfile.Minute {
	return shardfile.MinuteOf(a.d.now().Unix())
}

// reminderAtDelta is what a background delta or rebase does before its manifest
// update: it reads the next reminder once the tail is taken, and records it if the
// read was exactly the state the files reach (and now otherwise). A failure fails
// the delta, which the delta timer tries again.
func (a *actor) reminderAtDelta(ctx context.Context, end pos.Position) (shardfile.Minute, error) {
	w, at, err := a.nextReminder(ctx)
	if err != nil {
		return 0, err
	}
	return a.recordedFor(w, at, end), nil
}
