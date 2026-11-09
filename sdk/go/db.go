package shardlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
)

// pragmas is what Opened.DB runs on a request's connection. The node sets WAL
// mode when it creates the file, so that only checks. The busy timeout makes a
// writer wait for the node's own bookkeeping commits rather than fail.
const pragmas = `PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;`

// Opener opens a database from a URI. The app brings the driver and sets its
// options, so the SDK names none:
//
//	func(uri string) (*sql.DB, error) {
//		return sql.Open("sqlite", uri+"&_txlock=immediate")
//	}
//
// The URI is "file:{path}?mode=rw", which opens the file read-write and never
// creates it. The opener may return before the file is opened, as sql.Open
// does: Opened.DB opens it by running the pragmas.
type Opener func(uri string) (*sql.DB, error)

// ErrRequestEnded is what DB returns once the handler it belongs to has
// returned and its database is closed.
var ErrRequestEnded = errors.New("shardlite: the handler has returned, so its database is closed")

type dbKey struct{}

// requestDB is the database of one request. The handler may ask for it from
// several goroutines.
type requestDB struct {
	n *Node

	mu     sync.Mutex
	shard  ShardName
	db     *sql.DB
	err    error
	closed bool
}

// Middleware gives each request a database that Opened.DB opens when a handler
// asks for it and closes when next returns. It opens nothing itself, so
// shardless requests and requests that never touch the database cost nothing.
// It wraps the response writer in nothing, so streams, upgrades and flushes
// work as they would without it.
func (n *Node) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rdb := &requestDB{n: n}
		defer rdb.close()
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), dbKey{}, rdb)))
	})
}

func (d *requestDB) get(ctx context.Context, shard ShardName, file string) (*sql.DB, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case d.closed:
		return nil, ErrRequestEnded
	case d.err != nil:
		return nil, d.err
	case d.db != nil:
		return d.db, nil
	}
	db, err := d.n.open(fileURI(file))
	if err != nil {
		d.err = fmt.Errorf("shardlite: opening %s at %s: %w", shard, file, err)
		return nil, d.err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.ExecContext(ctx, pragmas); err != nil {
		err = fmt.Errorf("shardlite: opening %s at %s: %w", shard, file, err)
	} else if d.n.onOpen != nil {
		if err = d.n.onOpen(ctx, shard, db); err != nil {
			err = fmt.Errorf("shardlite: OnOpen for %s: %w", shard, err)
		}
	}
	if err != nil {
		db.Close()
		d.err = err
		return nil, err
	}
	d.db, d.shard = db, shard
	return db, nil
}

func (d *requestDB) close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	if d.db == nil {
		return
	}
	if err := d.db.Close(); err != nil {
		slog.Warn("shardlite: closing a shard's database failed", "shard", d.shard.String(), "err", err)
	}
	d.db = nil
}

// fileURI is the URI that opens file read-write without creating it. The
// characters a SQLite URI gives meaning to are escaped.
func fileURI(file string) string {
	return "file:" + strings.NewReplacer("%", "%25", "?", "%3f", "#", "%23").Replace(file) + "?mode=rw"
}
