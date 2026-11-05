package node

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// LocalDBName is the local database's file in the node's data directory.
const LocalDBName = ".local.db"

// localDBSchema is the database as a start makes it. The table is STRICT: a value of
// the wrong type is refused instead of stored as whatever the column's affinity
// makes of it.
const localDBSchema = `
CREATE TABLE shards (
  name TEXT PRIMARY KEY, path TEXT NOT NULL, generation TEXT NOT NULL, position INTEGER NOT NULL,
  state TEXT NOT NULL, last_used INTEGER NOT NULL, bytes INTEGER NOT NULL,
  size INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, wal_size INTEGER NOT NULL, wal_mtime_ns INTEGER NOT NULL, manifest TEXT NOT NULL, etag TEXT NOT NULL,
  crcs BLOB
) STRICT;
CREATE INDEX shards_lru ON shards (state, last_used);
`

// LocalDB is the node's local database: the cache index's rows, in one SQLite
// file in the data directory. It is the node's only local state besides the
// shard files the app opens.
//
// Nothing in it outlives a start. The shard files the index points at are
// deleted at every start, since after a crash neither they nor the index can be
// trusted to agree with blob storage, so the file is deleted and made again
// with the schema in code, and a change to the schema is a change to that code.
// The index lives in SQLite rather than in memory because each row carries the
// CRC of every page of its shard, which is shard-sized.
type LocalDB struct {
	*sql.DB
}

// makeLocalDB makes the local database at path, deleting whatever file was there: an
// older node's database holds nothing worth keeping.
func makeLocalDB(path string) (*LocalDB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("local database: %w", err)
	}
	for _, p := range []string{path, path + "-wal", path + "-shm", path + "-journal"} {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("local database: %w", err)
		}
	}
	db, err := openLocalDBDSN("file:" + path + "?_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	return db, nil
}

// memoryLocalDB returns a database that is not on disk: the cache index's rows live
// in it and go when the process does. It is what a node falls back to when its
// local database cannot be made, which costs the cache and nothing else.
func memoryLocalDB() (*LocalDB, error) {
	db, err := openLocalDBDSN("file::memory:?_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // one connection, or each would have a database of its own
	return db, nil
}

// openLocalDBDSN opens the DSN and makes the schema in it. The page size and the
// write-ahead log are kept in the file's header, so every connection to it uses them.
func openLocalDBDSN(dsn string) (*LocalDB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("local database: %w", err)
	}
	// The page size is where SQLite's own measurements put large blob I/O,
	// which a shard's page checksums are. It takes only on an empty database.
	if _, err := db.Exec("PRAGMA page_size=8192"); err != nil {
		db.Close()
		return nil, fmt.Errorf("local database: %w", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("local database: %w", err)
	}
	if _, err := db.Exec(localDBSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("local database: %w", err)
	}
	return &LocalDB{DB: db}, nil
}
