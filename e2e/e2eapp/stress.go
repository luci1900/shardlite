package e2eapp

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
)

// The fault tests' routes: an app that does what real apps do to a SQLite file
// that the node is copying from. Each takes its sizes as query parameters, writes
// rows with tokens of its own ("bulk", "ck0", "c0", ...), and answers when the
// work is done, which on a sync-on kind is when it is all in blob storage.

func intParam(r *http.Request, name string, def int) int {
	if v, err := strconv.Atoi(r.URL.Query().Get(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// bulk imports rows of random bytes in one transaction: ?rows=5000&bytes=1000.
func (a *App) bulk(w http.ResponseWriter, r *http.Request, s *shardReq) {
	rows, size := intParam(r, "rows", 1), intParam(r, "bytes", 1000)
	tx, err := s.db.sql.BeginTx(r.Context(), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()
	if err := s.migrate(r.Context(), tx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for range rows {
		if _, err := tx.ExecContext(r.Context(), `INSERT INTO rows (token, payload) VALUES ('bulk', randomblob(?))`, size); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// vacuum rewrites the whole database.
func (a *App) vacuum(w http.ResponseWriter, r *http.Request, s *shardReq) {
	if _, err := s.db.sql.ExecContext(r.Context(), `VACUUM`); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// checkpoints is an app that checkpoints by itself while it writes (the
// WAL-reset hazard): ?n=30&mode=PASSIVE|FULL|RESTART|TRUNCATE. It writes a row,
// then checkpoints, n times.
func (a *App) checkpoints(w http.ResponseWriter, r *http.Request, s *shardReq) {
	n, mode := intParam(r, "n", 30), r.URL.Query().Get("mode")
	switch mode {
	case "PASSIVE", "FULL", "RESTART", "TRUNCATE":
	default:
		http.Error(w, "mode is PASSIVE, FULL, RESTART or TRUNCATE", http.StatusBadRequest)
		return
	}
	// A connection of its own with a short busy timeout: the node's read
	// transaction makes the app's FULL, RESTART and TRUNCATE checkpoints wait for
	// it on every call.
	db, err := sql.Open("sqlite", "file:"+s.file+"?mode=rw&_pragma=journal_mode(WAL)&_pragma=busy_timeout(20)&_pragma=synchronous(NORMAL)&_txlock=immediate")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	busyN := 0
	for i := range n {
		if _, err := db.ExecContext(r.Context(), `INSERT INTO rows (token, payload) VALUES (?, randomblob(200))`, "ck"+strconv.Itoa(i)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var busy, log, ck int
		if err := db.QueryRowContext(r.Context(), `PRAGMA wal_checkpoint(`+mode+`)`).Scan(&busy, &log, &ck); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if busy != 0 {
			busyN++
		}
	}
	writeJSON(w, map[string]int{"n": n, "busy": busyN})
}

// churn is an app that closes and opens its connection all the time, each time
// with a write: ?n=300. It writes through connections of its own, not the pool's.
func (a *App) churn(w http.ResponseWriter, r *http.Request, s *shardReq) {
	n := intParam(r, "n", 300)
	for i := range n {
		db, err := sql.Open("sqlite", "file:"+s.file+"?mode=rw&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate")
		if err == nil {
			_, err = db.ExecContext(r.Context(), `INSERT INTO rows (token, payload) VALUES (?, zeroblob(64))`, fmt.Sprintf("c%d", i))
			if cerr := db.Close(); err == nil {
				err = cerr
			}
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
