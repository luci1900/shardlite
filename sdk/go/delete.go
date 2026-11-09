package shardlite

import (
	"context"
)

// MarkDeleted marks the shard for deletion on db: when it next deactivates, its
// data goes, and a later call for the same id creates a new, empty shard. The
// marker is a row in _shardlite_delete, so run this in the transaction that
// decides the shard is finished (the last player leaving a lobby) and it
// commits with that decision. The shard is served as usual until it
// deactivates.
func MarkDeleted(ctx context.Context, db DBTX) error {
	_, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO _shardlite_delete VALUES (1)`)
	return err
}

// UnmarkDeleted takes the marker back on db, so the shard is kept when it
// deactivates. It does nothing for a shard that is not marked.
func UnmarkDeleted(ctx context.Context, db DBTX) error {
	_, err := db.ExecContext(ctx, `DELETE FROM _shardlite_delete`)
	return err
}

// IsMarkedDeleted reports whether the shard is marked for deletion, read on db.
// A handler asks it to refuse requests for a finished shard that is still
// served.
func IsMarkedDeleted(ctx context.Context, db DBTX) (bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT EXISTS (SELECT 1 FROM _shardlite_delete)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var marked bool
	if rows.Next() {
		if err := rows.Scan(&marked); err != nil {
			return false, err
		}
	}
	return marked, rows.Err()
}
