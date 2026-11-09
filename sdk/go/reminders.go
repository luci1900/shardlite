package shardlite

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

// DBTX is what the helpers that write the shard's reserved tables run on: the
// request's *sql.DB or a *sql.Tx on it. The request's database has one
// connection, which an open transaction holds, so a handler in a transaction
// runs them on the transaction.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// staleReminder is how long past a reminder's due time the node still wakes for
// it (the guide's Stale reminders).
const staleReminder = 24 * time.Hour

// SetReminder sets the reminder key to be due at due, or moves it, on db. Run
// it in the transaction of the work that needs it so they commit together. The
// node wakes the app at RemindersPath when it is due (to the second). Anything
// the wake needs is in the app's own tables under the same key.
//
// The node ignores a reminder due more than a day ago. SetReminder still writes
// it, so a mistake in the due time does not fail the transaction, and logs a
// warning with log/slog naming the key and the due time. A handler that repeats
// a reminder sets the next due time from now, not from the old one.
func SetReminder(ctx context.Context, db DBTX, key string, due time.Time) error {
	_, err := db.ExecContext(ctx, `INSERT INTO _shardlite_reminders (key, due) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET due = excluded.due`, key, due.Unix())
	if err == nil && due.Unix() < time.Now().Unix()-int64(staleReminder/time.Second) {
		slog.Warn("shardlite: the reminder is due more than a day ago, which the node ignores", "key", key, "due", due.UTC().Format(time.RFC3339))
	}
	return err
}

// CancelReminder deletes the reminder key on db. A wake's handler runs it in
// the transaction of the work the reminder was for, so the reminder goes only
// with its work.
func CancelReminder(ctx context.Context, db DBTX, key string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM _shardlite_reminders WHERE key = ?`, key)
	return err
}

// DueReminders returns the keys of the reminders due now, the earliest first,
// read on db.
func DueReminders(ctx context.Context, db DBTX) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT key FROM _shardlite_reminders WHERE due <= unixepoch() ORDER BY due, key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}
