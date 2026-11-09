package shardlite_test

import (
	"bufio"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

// recorder is an opener that keeps what it opened.
type recorder struct {
	mu   sync.Mutex
	uris []string
	dbs  []*sql.DB
}

func (rc *recorder) open(uri string) (*sql.DB, error) {
	db, err := openSQLite(uri)
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.uris = append(rc.uris, uri)
	rc.dbs = append(rc.dbs, db)
	return db, err
}

func (rc *recorder) opens() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return len(rc.uris)
}

// recordingNode is the app's Node with an opener that records, for tests that
// count opens. The fake node opens its files through an opener of its own, so
// those opens are not counted.
func recordingNode(t *testing.T, rc *recorder) *shardlite.Node {
	t.Helper()
	cfg := testConfig()
	cfg.Open = rc.open
	return newNode(t, cfg)
}

func TestDBOpensOncePerRequestAndClosesWhenTheHandlerReturns(t *testing.T) {
	fake := newFake(t, testConfig())
	var rc recorder
	app := recordingNode(t, &rc)
	var db1 *sql.DB
	h := app.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, users.Shard("42"))
		if err != nil {
			return
		}
		if db1, err = opened.DB(); err != nil {
			t.Error(err)
			return
		}
		again, err := opened.DB()
		if err != nil || again != db1 {
			t.Errorf("a second call in the request gave %p, %v, want the first database %p", again, err, db1)
		}
		if err := db1.PingContext(r.Context()); err != nil {
			t.Errorf("the database is not open while the handler runs: %v", err)
		}
	}))
	req := fake.Request(users.Shard("42"), http.MethodGet, "/users/42/count", nil)
	file := req.Header.Get(headerFile)
	h.ServeHTTP(httptest.NewRecorder(), req)

	if got := rc.opens(); got != 1 {
		t.Fatalf("opened %d times, want once", got)
	}
	if want := "file:" + file + "?mode=rw"; rc.uris[0] != want {
		t.Errorf("opened %q, want %q", rc.uris[0], want)
	}
	if err := db1.Ping(); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Errorf("after the handler returned, Ping = %v, want the database closed", err)
	}

	h.ServeHTTP(httptest.NewRecorder(), req)
	if got := rc.opens(); got != 2 {
		t.Errorf("a second request opened %d times in all, want 2: nothing is kept between requests", got)
	}
}

func TestDBRunsThePragmasOnOneConnection(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	run(app, func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, users.Shard("42"))
		if err != nil {
			return
		}
		db, err := opened.DB()
		if err != nil {
			t.Error(err)
			return
		}
		if got := db.Stats().MaxOpenConnections; got != 1 {
			t.Errorf("MaxOpenConnections = %d, want 1", got)
		}
		for pragma, want := range map[string]string{"journal_mode": "wal", "busy_timeout": "5000"} {
			var got string
			if err := db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil {
				t.Error(pragma, err)
			} else if got != want {
				t.Errorf("PRAGMA %s = %q, want %q", pragma, got, want)
			}
		}
	}, fake.Request(users.Shard("42"), http.MethodGet, "/", nil))
}

func TestDBOpensNothingUntilAskedFor(t *testing.T) {
	fake := newFake(t, testConfig())
	var rc recorder
	app := recordingNode(t, &rc)
	h := app.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		app.Expect(r, users.Shard("42")) // checked, not opened
	}))
	h.ServeHTTP(httptest.NewRecorder(), fake.Request(users.Shard("42"), http.MethodGet, "/", nil))
	h.ServeHTTP(httptest.NewRecorder(), fake.ShardlessRequest(http.MethodGet, "/login", nil))
	if got := rc.opens(); got != 0 {
		t.Errorf("opened %d databases for requests that never asked", got)
	}
}

func TestDBFailsWhereThereIsNothingToOpen(t *testing.T) {
	fake := newFake(t, testConfig())
	var rc recorder
	app := recordingNode(t, &rc)
	req := fake.Request(users.Shard("42"), http.MethodGet, "/", nil)
	missing := filepath.Join(t.TempDir(), "gone.db")
	req.Header.Set(headerFile, missing)
	run(app, func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, users.Shard("42"))
		if err != nil {
			return
		}
		_, err1 := opened.DB()
		_, err2 := opened.DB()
		if err1 == nil || err2 == nil || err1.Error() != err2.Error() {
			t.Errorf("DB = %v, then %v, want the same error twice", err1, err2)
		}
	}, req)
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the open created the file: %v", err)
	}
	for _, db := range rc.dbs {
		if err := db.Ping(); err == nil {
			t.Error("a failed open left its database open")
		}
	}
}

func TestDBAfterTheHandlerReturnedIsRefused(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	var kept *shardlite.Opened
	run(app, func(w http.ResponseWriter, r *http.Request) {
		var err error
		if kept, err = app.Expect(r, users.Shard("42")); err != nil {
			return
		}
		if _, err := kept.DB(); err != nil {
			t.Error(err)
		}
	}, fake.Request(users.Shard("42"), http.MethodGet, "/", nil))
	if _, err := kept.DB(); !errors.Is(err, shardlite.ErrRequestEnded) {
		t.Errorf("DB after the handler returned = %v, want ErrRequestEnded", err)
	}
}

func TestDBLastsAsLongAsAStream(t *testing.T) {
	fake := newFake(t, testConfig())
	var rc recorder
	app := recordingNode(t, &rc)
	release := make(chan struct{})
	h := app.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, users.Shard("42"))
		if err != nil {
			return
		}
		db, err := opened.DB()
		if err != nil {
			t.Error(err)
			return
		}
		w.Write([]byte("first\n"))
		w.(http.Flusher).Flush()
		<-release
		var one int
		if err := db.QueryRow(`SELECT 1`).Scan(&one); err != nil {
			t.Errorf("the stream's database failed after it started: %v", err)
		}
		w.Write([]byte("second\n"))
	}))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	opened := fake.Request(users.Shard("42"), http.MethodGet, "/events", nil)
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = opened.Header
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	lines := bufio.NewReader(res.Body)
	if line, err := lines.ReadString('\n'); err != nil || line != "first\n" {
		t.Fatalf("first chunk = %q, %v", line, err)
	}
	// The stream is open and the handler is waiting: its database is still
	// open.
	if err := rc.dbs[0].Ping(); err != nil {
		t.Fatalf("the database closed while the stream was open: %v", err)
	}
	close(release)
	if line, err := lines.ReadString('\n'); err != nil || line != "second\n" {
		t.Fatalf("second chunk = %q, %v", line, err)
	}
	// The response ends after the middleware has returned.
	if _, err := lines.ReadString('\n'); err == nil {
		t.Fatal("the stream did not end")
	}
	if err := rc.dbs[0].Ping(); err == nil {
		t.Error("the database is still open after the stream ended")
	}
}
