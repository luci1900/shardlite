package shardlite_test

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

// run serves r on the app's Middleware around h.
func run(app *shardlite.Node, h http.HandlerFunc, r *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	app.Middleware(h).ServeHTTP(rec, r)
	return rec
}

func TestExpectOnAMatchGivesTheShard(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	var name shardlite.ShardName
	rec := run(app, func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, users.Shard("42"))
		if refusalOf(t, opened, err) != nil {
			return
		}
		name = opened.Name()
		db, err := opened.DB()
		if err != nil {
			t.Error(err)
			return
		}
		var one int
		if err := db.QueryRow(`SELECT 1`).Scan(&one); err != nil {
			t.Error(err)
		}
	}, fake.Request(users.Shard("42"), http.MethodGet, "/users/42/count", nil))
	if rec.Code != http.StatusOK || name != users.Shard("42") {
		t.Errorf("answered %d, opened %v", rec.Code, name)
	}
}

// refusalOf checks that Expect or ExpectCall gave a shard or an error and not
// both or neither, and returns the refusal the error is, if any. The error is
// compared with nil as the interface it is, so a nil *Refusal inside it would
// fail here.
func refusalOf(t *testing.T, opened *shardlite.Opened, err error) *shardlite.Refusal {
	t.Helper()
	if (opened == nil) == (err == nil) {
		t.Errorf("got shard %v and error %v, want exactly one", opened, err)
	}
	if err == nil {
		return nil
	}
	var ref *shardlite.Refusal
	if !errors.As(err, &ref) || ref == nil {
		t.Fatalf("the error is %#v, want a *Refusal", err)
	}
	if err.Error() != ref.Message {
		t.Errorf("Error() = %q, want the message %q", err.Error(), ref.Message)
	}
	return ref
}

func TestExpectRefusesWhatIsNotForTheOpenedShard(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	for name, tc := range map[string]struct {
		req  *http.Request
		want shardlite.ShardName
		body []string // in the message
	}{
		"another shard":      {fake.Request(users.Shard("7"), http.MethodGet, "/", nil), users.Shard("42"), []string{"users/42", "users/7"}},
		"another kind":       {fake.Request(guild.Shard("42"), http.MethodGet, "/", nil), users.Shard("42"), []string{"users/42", "guild/42"}},
		"a shardless route":  {fake.ShardlessRequest(http.MethodGet, "/", nil), users.Shard("42"), []string{"users/42", "no shard"}},
		"a name of nothing":  {fake.Request(users.Shard("42"), http.MethodGet, "/", nil), shardlite.ShardName{}, nil},
		"a name that's bad":  {fake.Request(users.Shard("42"), http.MethodGet, "/", nil), users.Shard("4 2"), nil},
		"a forged file only": {forgedFile(fake), users.Shard("42"), []string{"no shard"}},
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			var opened *shardlite.Opened
			var err error
			app.Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				opened, err = app.Expect(r, tc.want)
			})).ServeHTTP(rec, tc.req)
			ref := refusalOf(t, opened, err)
			if ref == nil {
				t.Fatal("Expect gave a shard, want a refusal")
			}
			if ref.Status != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", ref.Status)
			}
			for _, s := range tc.body {
				if !strings.Contains(ref.Message, s) {
					t.Errorf("the message %q does not mention %q", ref.Message, s)
				}
			}
			if rec.Flushed || rec.Body.Len() != 0 || len(rec.Header()) != 0 {
				t.Errorf("Expect wrote to the response: %v %q", rec.Header(), rec.Body.String())
			}
		})
	}
}

// forgedFile is a request with Shardlite-File and no Shardlite-Opened.
func forgedFile(fake *fakeNode) *http.Request {
	r := fake.ShardlessRequest(http.MethodGet, "/", nil)
	r.Header.Set(headerFile, "/etc/passwd")
	return r
}

func TestExpectOutsideMiddlewareIsTheAppsMistake(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	other := newNode(t, testConfig())
	for name, mw := range map[string]func(http.Handler) http.Handler{
		"no middleware":                  func(h http.Handler) http.Handler { return h },
		"the middleware of another node": other.Middleware,
	} {
		t.Run(name, func(t *testing.T) {
			for _, f := range []func(r *http.Request) (*shardlite.Opened, error){
				func(r *http.Request) (*shardlite.Opened, error) {
					return app.Expect(r, users.Shard("42"))
				},
				app.ExpectCall,
			} {
				var opened *shardlite.Opened
				var err error
				rec := httptest.NewRecorder()
				mw(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
					opened, err = f(r)
				})).ServeHTTP(rec, fake.Request(users.Shard("42"), http.MethodGet, "/", nil, fromCaller(&shardlite.CallerApp{})))
				ref := refusalOf(t, opened, err)
				if ref == nil || ref.Status != http.StatusInternalServerError || !strings.Contains(ref.Message, "Middleware") {
					t.Errorf("got %v, want a 500 refusal naming Middleware", ref)
				}
				if rec.Body.Len() != 0 || len(rec.Header()) != 0 {
					t.Errorf("wrote to the response: %v %q", rec.Header(), rec.Body.String())
				}
			}
		})
	}
}

func TestExpectCallTakesCallsOnly(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	serve := func(r *http.Request) (*shardlite.Opened, *shardlite.Refusal) {
		var opened *shardlite.Opened
		var err error
		rec := run(app, func(_ http.ResponseWriter, r *http.Request) {
			opened, err = app.ExpectCall(r)
		}, r)
		ref := refusalOf(t, opened, err)
		if rec.Flushed || rec.Body.Len() != 0 || len(rec.Header()) != 0 {
			t.Errorf("ExpectCall wrote to the response: %v %q", rec.Header(), rec.Body.String())
		}
		return opened, ref
	}

	// A client is refused, however good its shard.
	if _, ref := serve(fake.Request(guild.Shard("7"), http.MethodPost, "/join", nil)); ref == nil || ref.Status != http.StatusForbidden || !strings.Contains(ref.Message, "not for clients") {
		t.Errorf("a client's request got %v, want a 403 refusal", ref)
	}
	// The app and a shard are served, with the shard the node opened.
	opened, _ := serve(fake.Request(guild.Shard("7"), http.MethodPost, "/join", nil, fromCaller(&shardlite.CallerApp{})))
	if opened == nil || opened.Name() != guild.Shard("7") {
		t.Fatalf("a call from the app: %v", opened)
	}
	if _, ok := opened.Caller().(*shardlite.CallerApp); !ok {
		t.Errorf("caller = %#v", opened.Caller())
	}
	opened, _ = serve(fake.Request(guild.Shard("7"), http.MethodPost, "/join", nil, fromCaller(&shardlite.CallerShard{Name: users.Shard("42")})))
	if opened == nil {
		t.Fatal("a call from a shard was refused")
	}
	if c, ok := opened.Caller().(*shardlite.CallerShard); !ok || c.Name != users.Shard("42") {
		t.Errorf("caller = %#v", opened.Caller())
	}
	// A call with no shard opened has nothing to give.
	if _, ref := serve(fake.ShardlessRequest(http.MethodPost, "/join", nil, fromCaller(&shardlite.CallerApp{}))); ref == nil || ref.Status != http.StatusBadRequest || !strings.Contains(ref.Message, "Shardlite-Opened") {
		t.Errorf("a call with no shard got %v, want a 400 refusal", ref)
	}
}

func TestCallerIsEachVariant(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	for name, tc := range map[string]struct {
		opts []fakeOption
		want string
	}{
		"a client": {nil, "client"},
		"the app":  {[]fakeOption{fromCaller(&shardlite.CallerApp{})}, "app"},
		"a shard":  {[]fakeOption{fromCaller(&shardlite.CallerShard{Name: users.Shard("42")})}, "users/42"},
	} {
		t.Run(name, func(t *testing.T) {
			r := fake.Request(guild.Shard("7"), http.MethodPost, "/", nil, tc.opts...)
			if got := who(app.Request(r).Caller()); got != tc.want {
				t.Errorf("Request.Caller = %s, want %s", got, tc.want)
			}
			if name, ok := app.Request(r).Opened(); !ok || name != guild.Shard("7") {
				t.Errorf("Request.Opened = %v, %v", name, ok)
			}
			run(app, func(w http.ResponseWriter, r *http.Request) {
				opened, err := app.Expect(r, guild.Shard("7"))
				if err != nil {
					return
				}
				if got := who(opened.Caller()); got != tc.want {
					t.Errorf("Opened.Caller = %s, want %s", got, tc.want)
				}
			}, r)
		})
	}

	// A shardless request has a caller and no opened shard.
	r := fake.ShardlessRequest(http.MethodPost, "/login", nil)
	if _, ok := app.Request(r).Opened(); ok {
		t.Error("a shardless request has an opened shard")
	}
	if _, ok := app.Request(r).Caller().(*shardlite.CallerClient); !ok {
		t.Errorf("caller = %#v", app.Request(r).Caller())
	}
	// A caller the node would never write is taken as a client, the least
	// trusted.
	for _, v := range []string{"Users/1", "users", "App", "users/4 2"} {
		r.Header.Set(headerCaller, v)
		if _, ok := app.Request(r).Caller().(*shardlite.CallerClient); !ok {
			t.Errorf("Shardlite-Caller %q read as %#v", v, app.Request(r).Caller())
		}
	}
}

func TestOnOpenRunsOnceAfterThePragmas(t *testing.T) {
	var ran []shardlite.ShardName
	var busy string
	var mode string
	cfg := testConfig()
	cfg.OnOpen = func(ctx context.Context, shard shardlite.ShardName, db *sql.DB) error {
		ran = append(ran, shard)
		// The pragmas have run: the busy timeout is set, and the migration is
		// made under it.
		if err := db.QueryRowContext(ctx, `PRAGMA busy_timeout`).Scan(&busy); err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, `PRAGMA journal_mode`).Scan(&mode); err != nil {
			return err
		}
		_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS migrated (x INTEGER)`)
		return err
	}
	fake := newFake(t, cfg)
	app := fake.App()
	rec := run(app, func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, users.Shard("42"))
		if err != nil {
			return
		}
		for range 3 {
			db, err := opened.DB()
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := db.ExecContext(r.Context(), `INSERT INTO migrated VALUES (1)`); err != nil {
				t.Errorf("the handler found the shard unmigrated: %v", err)
			}
		}
	}, fake.Request(users.Shard("42"), http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("answered %d", rec.Code)
	}
	if len(ran) != 1 || ran[0] != users.Shard("42") {
		t.Errorf("OnOpen ran for %v, want once for users/42", ran)
	}
	if busy != "5000" || mode != "wal" {
		t.Errorf("OnOpen saw busy_timeout %q and journal_mode %q: it ran before the pragmas", busy, mode)
	}
	// A request that never asks for the database never runs it.
	ran = nil
	run(app, func(_ http.ResponseWriter, r *http.Request) { app.Expect(r, users.Shard("42")) },
		fake.Request(users.Shard("42"), http.MethodGet, "/", nil))
	if len(ran) != 0 {
		t.Errorf("OnOpen ran for a request that did not ask for the database: %v", ran)
	}
}

func TestOnOpenFailureFailsTheRequestsDatabase(t *testing.T) {
	boom := errors.New("migration 3 failed")
	runs := 0
	cfg := testConfig()
	cfg.OnOpen = func(ctx context.Context, shard shardlite.ShardName, db *sql.DB) error {
		runs++
		return boom
	}
	fake := newFake(t, cfg)
	app := fake.App()
	run(app, func(w http.ResponseWriter, r *http.Request) {
		opened, err := app.Expect(r, users.Shard("42"))
		if err != nil {
			return
		}
		_, err1 := opened.DB()
		_, err2 := opened.DB()
		if !errors.Is(err1, boom) || err2 == nil || err1.Error() != err2.Error() {
			t.Errorf("DB = %v, then %v, want the migration's error both times", err1, err2)
		}
	}, fake.Request(users.Shard("42"), http.MethodGet, "/", nil))
	if runs != 1 {
		t.Errorf("OnOpen ran %d times in the request, want once", runs)
	}
}
