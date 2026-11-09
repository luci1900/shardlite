package shardlite_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	shardlite "github.com/luci1900/shardlite/sdk/go"
	"github.com/luci1900/shardlite/sdk/go/shardlitetest"
)

func dueOf(t *testing.T, db *sql.DB, key string) (due int64, ok bool) {
	t.Helper()
	err := db.QueryRow(`SELECT due FROM _shardlite_reminders WHERE key = ?`, key).Scan(&due)
	switch err {
	case nil:
		return due, true
	case sql.ErrNoRows:
		return 0, false
	}
	t.Fatal(err)
	return 0, false
}

func TestSetAndCancelReminder(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	var db *sql.DB
	h := app.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, users.Shard("42"))
		if err != nil {
			return
		}
		if db, err = opened.DB(); err != nil {
			t.Fatal(err)
		}
		ctx := r.Context()
		first, second := time.Unix(1_800_000_000, 0), time.Unix(1_800_000_600, 0)

		do := func(f func(tx *sql.Tx) error) {
			t.Helper()
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := f(tx); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		do(func(tx *sql.Tx) error { return shardlite.SetReminder(ctx, tx, "upgrade:barracks", first) })
		if due, ok := dueOf(t, db, "upgrade:barracks"); !ok || due != first.Unix() {
			t.Errorf("due = %d, %v, want %d", due, ok, first.Unix())
		}
		// The same key moves the reminder rather than adding a second.
		do(func(tx *sql.Tx) error { return shardlite.SetReminder(ctx, tx, "upgrade:barracks", second) })
		var n int
		db.QueryRow(`SELECT count(*) FROM _shardlite_reminders`).Scan(&n)
		if due, _ := dueOf(t, db, "upgrade:barracks"); due != second.Unix() || n != 1 {
			t.Errorf("after a reschedule due = %d with %d rows, want %d with 1", due, n, second.Unix())
		}
		// A reminder set in a transaction that rolls back is not set.
		tx, _ := db.BeginTx(ctx, nil)
		shardlite.SetReminder(ctx, tx, "never", first)
		tx.Rollback()
		if _, ok := dueOf(t, db, "never"); ok {
			t.Error("a rolled back reminder was kept")
		}
		do(func(tx *sql.Tx) error { return shardlite.CancelReminder(ctx, tx, "upgrade:barracks") })
		if _, ok := dueOf(t, db, "upgrade:barracks"); ok {
			t.Error("CancelReminder left the reminder")
		}
		// Cancelling one that is not there is no error.
		do(func(tx *sql.Tx) error { return shardlite.CancelReminder(ctx, tx, "upgrade:barracks") })
	}))
	h.ServeHTTP(httptest.NewRecorder(), fake.Request(users.Shard("42"), http.MethodPost, "/users/42/remind", nil))
}

// logged runs f with the default slog logger writing to a buffer and returns
// what it logged. Its tests do not run in parallel.
func logged(f func()) string {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(prev)
	f()
	return buf.String()
}

func TestSetReminderWritesAStaleDueAndWarns(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	h := app.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, users.Shard("42"))
		if err != nil {
			return
		}
		db, err := opened.DB()
		if err != nil {
			t.Fatal(err)
		}
		ctx := r.Context()
		// More than a day past is written all the same, with a warning naming
		// the key and the due time.
		stale := time.Now().Add(-25 * time.Hour)
		out := logged(func() {
			if err := shardlite.SetReminder(ctx, db, "old", stale); err != nil {
				t.Errorf("SetReminder with a due 25 hours ago = %v, want it written", err)
			}
		})
		if due, ok := dueOf(t, db, "old"); !ok || due != stale.Unix() {
			t.Errorf("due = %d, %v, want %d", due, ok, stale.Unix())
		}
		for _, want := range []string{"level=WARN", "key=old", "due=" + stale.UTC().Format(time.RFC3339), "more than a day ago"} {
			if !strings.Contains(out, want) {
				t.Errorf("the warning %q lacks %q", out, want)
			}
		}
		// The stale move of a reminder that is there writes too.
		if err := shardlite.SetReminder(ctx, db, "old", time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		// A due time less than a day past, or in the future, logs nothing.
		out = logged(func() {
			for _, due := range []time.Time{time.Now().Add(-23 * time.Hour), time.Now(), time.Now().Add(time.Hour)} {
				if err := shardlite.SetReminder(ctx, db, "recent", due); err != nil {
					t.Errorf("due %v: %v", due, err)
				}
			}
		})
		if out != "" {
			t.Errorf("a due within a day logged %q", out)
		}
		// A database error is returned, and a stale due that did not write is
		// not warned about.
		out = logged(func() {
			if err := shardlite.SetReminder(ctx, failing{}, "x", stale); err == nil {
				t.Error("the database's error was dropped")
			}
		})
		if out != "" {
			t.Errorf("a reminder that was not written logged %q", out)
		}
	}))
	h.ServeHTTP(httptest.NewRecorder(), fake.Request(users.Shard("42"), http.MethodPost, "/", nil))
}

// failing is a DBTX whose statements all fail.
type failing struct{}

func (failing) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, errors.New("disk full")
}

func (failing) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, errors.New("disk full")
}

// A wake reaches the app as the node makes it, and the handler a guide would
// write answers it. This one runs against a real node.
func TestWakeRunsTheHandlerForTheShardItNames(t *testing.T) {
	var mu sync.Mutex
	var woken []string
	var bad []string
	node := shardlitetest.Start(t, testConfig(), func(app *shardlite.Node) http.Handler {
		mux := http.NewServeMux()
		mux.Handle("GET "+shardlite.ReadyPath, app.Ready())
		// A call sets a reminder that is due now.
		mux.HandleFunc("POST /set", func(w http.ResponseWriter, r *http.Request) {
			opened, err := app.ExpectCall(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			db, err := opened.DB()
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			tx, _ := db.BeginTx(r.Context(), nil)
			if err := shardlite.SetReminder(r.Context(), tx, "close", time.Now().Add(-time.Second)); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			tx.Commit()
		})
		mux.HandleFunc("POST "+shardlite.RemindersPath, func(w http.ResponseWriter, r *http.Request) {
			opened, err := app.ExpectCall(r)
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if c, ok := opened.Caller().(*shardlite.CallerShard); !ok || c.Name != opened.Name() {
				bad = append(bad, fmt.Sprintf("the wake opened %v and names %#v as its caller", opened.Name(), opened.Caller()))
			}
			db, err := opened.DB()
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			keys, err := shardlite.DueReminders(r.Context(), db)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			for _, k := range keys {
				tx, _ := db.BeginTx(r.Context(), nil)
				if err := shardlite.CancelReminder(r.Context(), tx, k); err != nil {
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					return
				}
				tx.Commit()
				woken = append(woken, opened.Name().String()+" "+k)
			}
		})
		return app.Middleware(mux)
	})

	shard := lobby.Shard("wake-test-9")
	res, err := node.App.CallAsApp(t.Context(), shard, http.MethodPost, "/set", nil)
	closeBody(t, res, err)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("setting the reminder answered %d", res.StatusCode)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		mu.Lock()
		got, problems := slices.Clone(woken), slices.Clone(bad)
		mu.Unlock()
		if len(problems) > 0 {
			t.Fatal(problems)
		}
		if len(got) > 0 {
			if len(got) != 1 || got[0] != "lobby/wake-test-9 close" {
				t.Fatalf("woken = %v, want the one reminder of %v", got, shard)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no wake within 15s:\n%s", node.Output())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDueRemindersAreTheOnesDueEarliestFirst(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	h := app.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, users.Shard("42"))
		if err != nil {
			return
		}
		db, err := opened.DB()
		if err != nil {
			t.Fatal(err)
		}
		ctx := r.Context()
		if keys, err := shardlite.DueReminders(ctx, db); err != nil || len(keys) != 0 {
			t.Errorf("with no reminders: %v, %v", keys, err)
		}
		now := time.Now()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		for key, due := range map[string]time.Time{"b": now.Add(-time.Minute), "a": now.Add(-time.Hour), "c": now.Add(-time.Minute), "later": now.Add(time.Hour)} {
			if err := shardlite.SetReminder(ctx, tx, key, due); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		keys, err := shardlite.DueReminders(ctx, db)
		if err != nil || !slices.Equal(keys, []string{"a", "b", "c"}) {
			t.Errorf("due = %v, %v, want a, b, c (the earliest first, ties by key, and not the one in the future)", keys, err)
		}
		// On the database itself, outside a transaction: one statement,
		// committed on its own.
		if err := shardlite.SetReminder(ctx, db, "d", now); err != nil {
			t.Fatal(err)
		}
		// In a transaction, on the transaction (the request's one connection is
		// its).
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		keys, err = shardlite.DueReminders(ctx, tx)
		if err != nil || !slices.Equal(keys, []string{"a", "b", "c", "d"}) {
			t.Errorf("due in a transaction = %v, %v, want a, b, c, d", keys, err)
		}
		if err := shardlite.CancelReminder(ctx, tx, "a"); err != nil {
			t.Fatal(err)
		}
		keys, err = shardlite.DueReminders(ctx, tx)
		if err != nil || !slices.Equal(keys, []string{"b", "c", "d"}) {
			t.Errorf("due after cancelling a in the transaction = %v, %v, want b, c, d", keys, err)
		}
	}))
	h.ServeHTTP(httptest.NewRecorder(), fake.Request(users.Shard("42"), http.MethodPost, "/", nil))
}
