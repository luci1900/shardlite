// Package localdb is the node's local database: the journal's segments and the
// cache index's rows, in one SQLite file on the node's disk. It is the node's
// only local state besides the shard files the app opens.
//
// The two tables have two fates, and the difference is the point. The index's
// rows are cleared at every start (the shard files they point at are deleted
// then too: after a crash neither the files nor the index can be trusted to
// agree with blob storage). The journal's rows are kept: they hold the node's
// own acknowledged writes, which are trustworthy across a crash in a way the
// app's files are not.
//
// Durability is SQLite's: a write is durable when its commit returns, and a
// crash is recovered by SQLite's own WAL recovery. The schema is numbered
// files (migrations/) that migrate applies in one immediate transaction at
// the first open, and a change to it is a migration rather than a deletion:
// the journal's rows hold acknowledged writes, so they are not thrown away
// when the index's columns change. A database newer than the migration files
// (migrate.ErrNewer) is left alone, and the node treats the local store as
// empty: it cannot read a newer node's chunks.
package localdb

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/migrate"
)

// Name is the local database's file in the node's data directory.
const Name = ".local.db"

// migrations is the database's schema: numbered {n}_{name}.up.sql and
// .down.sql files, from 1 with no gaps, applied by migrate. The database's
// version is its user_version.
//
//go:embed migrations/*.sql
var embedded embed.FS

var migrations, _ = fs.Sub(embedded, "migrations") // a fixed, valid name never fails

// DB is the node's local database: the journal's segments and the cache
// index's rows.
type DB struct {
	*sql.DB
	path string
}

// Open opens the local database at path, making it if it isn't there, and
// brings its schema to the newest version the migrations have. A database
// newer than the migrations is returned as migrate.ErrNewer and is left
// alone: a newer node's tail may be in it.
func Open(path string) (*DB, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("local database: %w", err)
		}
	}
	db, err := open(path)
	if err != nil {
		return nil, err
	}
	if err := db.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// open opens the file without touching its schema. The pragmas every
// connection needs are on the DSN: the busy timeout, so a writer waits for
// the other one instead of failing, the synchronous mode, which says what a
// commit costs, and the immediate transaction lock, which migrate needs (it
// reads user_version and then writes, which a deferred transaction can fail
// to upgrade) and the journal's create is fine with.
func open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)&_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("local database: %w", err)
	}
	db.SetMaxOpenConns(4)
	return &DB{DB: db, path: path}, nil
}

// migrate brings the schema to the newest version the migrations have. The
// pragmas that belong to the file go first, before the transactions: SQLite
// takes the page size only on an empty database, and the write-ahead log is
// kept in the header, so every connection to the file uses it.
func (d *DB) migrate() error {
	// The page size is 8192, which is where SQLite's own measurements put
	// large blob I/O.
	if _, err := d.Exec("PRAGMA page_size=8192"); err != nil {
		return fmt.Errorf("local database: %w", err)
	}
	if _, err := d.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("local database: %w", err)
	}
	tx, err := d.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("local database: %w", err)
	}
	defer tx.Rollback()
	if err := migrate.Migrate(context.Background(), tx, migrations, migrate.Latest); err != nil {
		return fmt.Errorf("local database: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("local database: %w", err)
	}
	return nil
}

// ClearIndex empties the cache index: the node's shards are restored from blob
// storage after a restart, and the files the index pointed at are gone, so its
// rows would be stale. The journal's rows are kept.
func (d *DB) ClearIndex(ctx context.Context) error {
	if _, err := d.ExecContext(ctx, "DELETE FROM shards"); err != nil {
		return fmt.Errorf("local database: %w", err)
	}
	return nil
}

// Persistent reports whether the database is a file on disk. A database that
// isn't holds nothing across a restart, so the node has no local store.
func (d *DB) Persistent() bool { return d.path != "" }

// Memory returns a database that is not on disk: the cache index's rows live
// in it and go when the process does. It is what a node falls back to when its
// local database cannot be opened, which costs the local store and the cache
// and nothing else.
func Memory() (*DB, error) {
	db, err := sql.Open("sqlite", "file::memory:?_pragma=busy_timeout(5000)&_pragma=synchronous(FULL)&_txlock=immediate")
	if err != nil {
		return nil, fmt.Errorf("local database: %w", err)
	}
	db.SetMaxOpenConns(1) // one connection, or each would have a database of its own
	d := &DB{DB: db}
	if err := d.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return d, nil
}
