package shardlite_test

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

func TestMarkDeletedWritesTheRowOnce(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	h := app.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, lobby.Shard("9"))
		if err != nil {
			return
		}
		db, err := opened.DB()
		if err != nil {
			t.Fatal(err)
		}
		ctx := r.Context()
		rows := func() (n int, marked sql.NullInt64) {
			t.Helper()
			if err := db.QueryRowContext(ctx, `SELECT count(*), max(marked) FROM _shardlite_delete`).Scan(&n, &marked); err != nil {
				t.Fatal(err)
			}
			return n, marked
		}

		// A marking that rolls back leaves no row.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := shardlite.MarkDeleted(ctx, tx); err != nil {
			t.Fatal(err)
		}
		tx.Rollback()
		if n, _ := rows(); n != 0 {
			t.Errorf("%d rows after a marking that rolled back, want none", n)
		}

		// Marking twice is one row, of the one value the table takes.
		for range 2 {
			if err := shardlite.MarkDeleted(ctx, db); err != nil {
				t.Fatal(err)
			}
		}
		if n, marked := rows(); n != 1 || !marked.Valid || marked.Int64 != 1 {
			t.Errorf("after marking twice: %d rows, marked %v, want one row holding 1", n, marked)
		}

	}))
	h.ServeHTTP(httptest.NewRecorder(), fake.Request(lobby.Shard("9"), http.MethodPost, "/close", nil))
}

func TestIsMarkedAndUnmarkDeleted(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	h := app.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, lobby.Shard("9"))
		if err != nil {
			return
		}
		db, err := opened.DB()
		if err != nil {
			t.Fatal(err)
		}
		ctx := r.Context()
		marked := func(db shardlite.DBTX, want bool) {
			t.Helper()
			if got, err := shardlite.IsMarkedDeleted(ctx, db); err != nil || got != want {
				t.Errorf("IsMarkedDeleted = %v, %v, want %v", got, err, want)
			}
		}

		marked(db, false)
		if err := shardlite.UnmarkDeleted(ctx, db); err != nil {
			t.Errorf("UnmarkDeleted on a shard that is not marked: %v", err)
		}
		if err := shardlite.MarkDeleted(ctx, db); err != nil {
			t.Fatal(err)
		}
		marked(db, true)
		if err := shardlite.UnmarkDeleted(ctx, db); err != nil {
			t.Fatal(err)
		}
		marked(db, false)

		// In a transaction: the transaction sees the marker, and a rollback
		// takes it away.
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := shardlite.MarkDeleted(ctx, tx); err != nil {
			t.Fatal(err)
		}
		marked(tx, true)
		tx.Rollback()
		marked(db, false)

		// The marker is the reserved table's row, which the app's own SQL also
		// sees.
		if err := shardlite.MarkDeleted(ctx, db); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM _shardlite_delete`).Scan(&n); err != nil || n != 1 {
			t.Errorf("%d rows, %v, want the marker", n, err)
		}
		// Marking again after unmarking works as the first time.
		if err := shardlite.UnmarkDeleted(ctx, db); err != nil {
			t.Fatal(err)
		}
		if err := shardlite.MarkDeleted(ctx, db); err != nil {
			t.Fatal(err)
		}
		marked(db, true)
	}))
	h.ServeHTTP(httptest.NewRecorder(), fake.Request(lobby.Shard("9"), http.MethodPost, "/close", nil))
}
