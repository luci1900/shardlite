package node

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Row states of the cache index.
const (
	stateActive = "active" // in use by this process
	stateCached = "cached" // deactivated, files at rest, still owned
)

// indexRow describes one shard file on this node's disk.
type indexRow struct {
	Name       string
	Path       string
	Generation string
	Position   uint64 // the shard's position in the file, once the shard was deactivated
	State      string
	LastUsed   time.Time
	Bytes      int64 // disk used by the file at rest
	Size       int64 // size and modification time of the database file and its -wal at deactivation: files that differ were touched since
	MTimeNs    int64
	WalSize    int64
	WalMTimeNs int64
	Manifest   string // the manifest and its version as of deactivation, so reactivating in the same epoch needs no blob storage call
	ETag       string
}

// index is the on-disk record of the node's shard files (a SQLite file), so a
// million cached shards don't cost memory.
type index struct{ db *sql.DB }

func openIndex(path string) (*index, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS shards (
		name TEXT PRIMARY KEY, path TEXT NOT NULL, generation TEXT NOT NULL, position INTEGER NOT NULL,
		state TEXT NOT NULL, last_used INTEGER NOT NULL, bytes INTEGER NOT NULL,
		size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, wal_size INTEGER NOT NULL, wal_mtime_ns INTEGER NOT NULL, manifest TEXT NOT NULL, etag TEXT NOT NULL);
		CREATE INDEX IF NOT EXISTS shards_lru ON shards (state, last_used)`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("cache index: %w", err)
	}
	return &index{db}, nil
}

func (x *index) close() error { return x.db.Close() }

func (x *index) put(r indexRow) error {
	_, err := x.db.Exec(`INSERT OR REPLACE INTO shards (`+cols+`)
		VALUES (:name, :path, :generation, :position, :state, :last_used, :bytes, :size, :mtime_ns, :wal_size, :wal_mtime_ns, :manifest, :etag)`,
		sql.Named("name", r.Name), sql.Named("path", r.Path), sql.Named("generation", r.Generation), sql.Named("position", r.Position),
		sql.Named("state", r.State), sql.Named("last_used", r.LastUsed.UnixNano()), sql.Named("bytes", r.Bytes),
		sql.Named("size", r.Size), sql.Named("mtime_ns", r.MTimeNs), sql.Named("wal_size", r.WalSize), sql.Named("wal_mtime_ns", r.WalMTimeNs),
		sql.Named("manifest", r.Manifest), sql.Named("etag", r.ETag))
	return err
}

const cols = `name, path, generation, position, state, last_used, bytes, size, mtime_ns, wal_size, wal_mtime_ns, manifest, etag`

func scan(s interface{ Scan(...any) error }) (indexRow, error) {
	var r indexRow
	var used int64
	err := s.Scan(&r.Name, &r.Path, &r.Generation, &r.Position, &r.State, &used, &r.Bytes, &r.Size, &r.MTimeNs, &r.WalSize, &r.WalMTimeNs, &r.Manifest, &r.ETag)
	r.LastUsed = time.Unix(0, used)
	return r, err
}

// get returns a shard's row, ok false if there is none.
func (x *index) get(name string) (indexRow, bool, error) {
	r, err := scan(x.db.QueryRow(`SELECT `+cols+` FROM shards WHERE name = ?`, name))
	if err == sql.ErrNoRows {
		return indexRow{}, false, nil
	}
	return r, err == nil, err
}

func (x *index) delete(name string) error {
	_, err := x.db.Exec(`DELETE FROM shards WHERE name = ?`, name)
	return err
}

func (x *index) rows(where string, args ...any) ([]indexRow, error) {
	rs, err := x.db.Query(`SELECT `+cols+` FROM shards `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []indexRow
	for rs.Next() {
		r, err := scan(rs)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rs.Err()
}

// all returns every row.
func (x *index) all() ([]indexRow, error) { return x.rows(``) }

// cachedBytes is the disk used by deactivated shards.
func (x *index) cachedBytes() (n int64, err error) {
	err = x.db.QueryRow(`SELECT COALESCE(SUM(bytes), 0) FROM shards WHERE state = ?`, stateCached).Scan(&n)
	return
}

// leastRecentlyUsed returns up to n deactivated shards, oldest use first.
func (x *index) leastRecentlyUsed(n int) ([]indexRow, error) {
	return x.rows(`WHERE state = ? ORDER BY last_used LIMIT ?`, stateCached, n)
}
