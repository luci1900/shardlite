package node

import (
	"database/sql"
	"time"

	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
)

// Row states of the cache index.
type rowState string

const (
	stateActive rowState = "active" // in use by this process
	stateCached rowState = "cached" // deactivated, files at rest, still owned
)

// indexRow describes one shard file on this node's disk.
type indexRow struct {
	Name       shard.Name
	Path       string
	Generation shard.Gen
	Position   pos.Position // the shard's position in the file, once the shard was deactivated
	State      rowState
	LastUsed   time.Time
	Bytes      int64 // disk used by the file at rest
	Size       int64 // size and modification time of the database file and its -wal at deactivation: files that differ were touched since
	MTimeNs    int64
	WALSize    int64
	WALMTimeNs int64
	Manifest   string // the manifest and its version as of deactivation, so reactivating in the same epoch needs no blob storage call
	ETag       string
	// CRCs is the CRC of each page of the database at deactivation, so a shard
	// activated from the cache reads no pages.
	CRCs []byte
}

// index is the on-disk record of the node's shard files (rows in the node's
// local database, localdb.go), so a million cached shards don't cost
// memory. Its rows are cleared at every start: the files they point at are
// deleted then too.
type index struct{ db *sql.DB }

// newIndex makes the index over the node's local database.
func newIndex(db *sql.DB) *index { return &index{db} }

func (x *index) put(r indexRow) error {
	_, err := x.db.Exec(`INSERT OR REPLACE INTO shards (`+cols+`)
		VALUES (:name, :path, :generation, :position, :state, :last_used, :bytes, :size, :mtime_ns, :wal_size, :wal_mtime_ns, :manifest, :etag, :crcs)`,
		sql.Named("name", r.Name), sql.Named("path", r.Path), sql.Named("generation", r.Generation), sql.Named("position", uint64(r.Position)),
		sql.Named("state", r.State), sql.Named("last_used", r.LastUsed.UnixNano()), sql.Named("bytes", r.Bytes),
		sql.Named("size", r.Size), sql.Named("mtime_ns", r.MTimeNs), sql.Named("wal_size", r.WALSize), sql.Named("wal_mtime_ns", r.WALMTimeNs),
		sql.Named("manifest", r.Manifest), sql.Named("etag", r.ETag), sql.Named("crcs", r.CRCs))
	return err
}

const cols = `name, path, generation, position, state, last_used, bytes, size, mtime_ns, wal_size, wal_mtime_ns, manifest, etag, crcs`

func scan(s interface{ Scan(...any) error }) (indexRow, error) {
	var r indexRow
	var used int64
	var position uint64
	err := s.Scan(&r.Name, &r.Path, &r.Generation, &position, &r.State, &used, &r.Bytes, &r.Size, &r.MTimeNs, &r.WALSize, &r.WALMTimeNs, &r.Manifest, &r.ETag, &r.CRCs)
	r.Position = pos.Position(position)
	r.LastUsed = time.Unix(0, used)
	return r, err
}

// get returns a shard's row, ok false if there is none.
func (x *index) get(name shard.Name) (indexRow, bool, error) {
	r, err := scan(x.db.QueryRow(`SELECT `+cols+` FROM shards WHERE name = ?`, name))
	if err == sql.ErrNoRows {
		return indexRow{}, false, nil
	}
	return r, err == nil, err
}

func (x *index) delete(name shard.Name) error {
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

// cachedBytes is the disk used by deactivated shards.
func (x *index) cachedBytes() (n int64, err error) {
	err = x.db.QueryRow(`SELECT COALESCE(SUM(bytes), 0) FROM shards WHERE state = ?`, stateCached).Scan(&n)
	return
}

// leastRecentlyUsed returns up to n deactivated shards, oldest use first.
func (x *index) leastRecentlyUsed(n int) ([]indexRow, error) {
	return x.rows(`WHERE state = ? ORDER BY last_used LIMIT ?`, stateCached, n)
}
