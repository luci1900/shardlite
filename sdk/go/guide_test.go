package shardlite_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	shardlite "github.com/luci1900/shardlite/sdk/go"
	"github.com/luci1900/shardlite/sdk/go/shardlitetest"
)

// The app of the package guide: a users kind that counts visits, invites to a
// guild, sets a reminder and can be deleted, and a guild kind that takes the
// invitation. The guide quotes this code.

// The app's kinds, kept together in one place: their diff is the declaration's
// diff.
var (
	Users  = shardlite.Kind{Name: "users", Public: true}
	Guilds = shardlite.Kind{Name: "guild"}
)

// config is the app's Config. The tests' fake node takes the same one.
func config() shardlite.Config {
	return shardlite.Config{
		Open:    openSQLite,
		Version: "1",
		Kinds:   []shardlite.Kind{Users, Guilds},
		OnOpen:  migrate,
	}
}

// migrate brings a shard's schema up to date. It runs after the pragmas on each
// request that opens the database, so it waits for the node's own commits.
func migrate(ctx context.Context, shard shardlite.ShardName, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil || version >= 1 {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS visits (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL) STRICT;
		CREATE TABLE IF NOT EXISTS nudges (key TEXT PRIMARY KEY, done INTEGER NOT NULL DEFAULT 0) STRICT;
		PRAGMA user_version = 1`); err != nil {
		return err
	}
	return tx.Commit()
}

type app struct{ node *shardlite.Node }

// newApp returns the app's handler, wrapped in the node's Middleware.
func newApp(node *shardlite.Node) http.Handler {
	a := &app{node}
	mux := http.NewServeMux()
	mux.Handle("GET "+shardlite.ReadyPath, node.Ready())
	mux.HandleFunc("POST /users/{id}/visit", a.visit)
	mux.HandleFunc("POST /users/{id}/invite", a.invite)
	mux.HandleFunc("POST /users/{id}/nudge", a.nudge)
	mux.HandleFunc("DELETE /users/{id}", a.leave)
	mux.HandleFunc("POST /join", a.join)
	mux.HandleFunc("POST "+shardlite.RemindersPath, a.wake)
	return node.Middleware(mux)
}

// refuse answers what Expect and ExpectCall refused with, as the app would
// answer any error: in one place.
func (a *app) refuse(w http.ResponseWriter, err error) {
	var ref *shardlite.Refusal
	errors.As(err, &ref)
	http.Error(w, ref.Message, ref.Status)
}

func (a *app) visit(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.Expect(r, Users.Shard(r.PathValue("id")))
	if err != nil {
		a.refuse(w, err)
		return
	}
	db, err := opened.DB() // opened now, closed when the handler returns
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	// A user who left is served until the shard deactivates, so the handler
	// refuses it.
	if gone, err := shardlite.IsMarkedDeleted(r.Context(), db); err != nil || gone {
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.Error(w, "this user has left", http.StatusGone)
		return
	}
	var n int
	err = db.QueryRowContext(r.Context(), `INSERT INTO visits (id, n) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET n = n + 1 RETURNING n`).Scan(&n)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, n)
}

func (a *app) invite(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.Expect(r, Users.Shard(r.PathValue("id")))
	if err != nil {
		a.refuse(w, err)
		return
	}
	// The guild's id came from the client, so the call must not create the
	// guild.
	res, err := opened.Call(Guilds.Shard(r.URL.Query().Get("guild")), http.MethodPost, "/join", strings.NewReader(r.PathValue("id")), shardlite.IfExists())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer res.Body.Close()
	if nerr, ok := shardlite.NodeError(res); ok {
		// The node refused the call, which is not the guild's answer: the
		// reason says why and the hint says whether to retry.
		if nerr.Reason == shardlite.ReasonUnknownShard {
			http.Error(w, "no such guild", http.StatusNotFound)
			return
		}
		if after, ok := nerr.RetryAfter(); ok {
			w.Header().Set("Retry-After", strconv.Itoa(int(after.Seconds())))
		}
		http.Error(w, nerr.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(res.StatusCode)
	io.Copy(w, res.Body)
}

// leave deletes the user's shard: the marker is written in the transaction that
// decides it, and the data goes at deactivation.
func (a *app) leave(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.Expect(r, Users.Shard(r.PathValue("id")))
	if err != nil {
		a.refuse(w, err)
		return
	}
	db, err := opened.DB()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if err := shardlite.MarkDeleted(r.Context(), db); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// join takes calls only: the shard is the one the node opened and the caller
// says who invited.
func (a *app) join(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.ExpectCall(r)
	if err != nil {
		a.refuse(w, err)
		return
	}
	by := "the app"
	if c, ok := opened.Caller().(*shardlite.CallerShard); ok {
		by = c.Name.String()
	}
	body, _ := io.ReadAll(r.Body)
	fmt.Fprintf(w, "%s: user %s invited by %s\n", opened.Name(), body, by)
}

// nudge sets a reminder due after the "in" query parameter, in a transaction
// that also records why.
func (a *app) nudge(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.Expect(r, Users.Shard(r.PathValue("id")))
	if err != nil {
		a.refuse(w, err)
		return
	}
	in, err := time.ParseDuration(r.URL.Query().Get("in"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	db, err := opened.DB()
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	tx, err := db.BeginTx(r.Context(), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(r.Context(), `INSERT OR IGNORE INTO nudges (key) VALUES ('daily')`)
	if err == nil {
		err = shardlite.SetReminder(r.Context(), tx, "daily", time.Now().Add(in))
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// wake does the work of every due reminder, each in a transaction of its own
// that also cancels it. It answers 503 if one could not be finished, so the
// node wakes it again with backoff.
func (a *app) wake(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.ExpectCall(r)
	if err != nil {
		a.refuse(w, err)
		return
	}
	// The node names the shard itself as the caller of its wake. Anything else
	// here is the app or another shard calling by hand.
	if c, ok := opened.Caller().(*shardlite.CallerShard); !ok || c.Name != opened.Name() {
		http.Error(w, "this route is the node's wake", http.StatusForbidden)
		return
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
	failed := false
	for _, key := range keys {
		if err := finish(r.Context(), db, key); err != nil {
			failed = true
		}
	}
	if failed {
		http.Error(w, "a reminder could not be finished", http.StatusServiceUnavailable)
	}
}

// finish does the work of the reminder key and cancels it in one transaction.
func finish(ctx context.Context, db *sql.DB, key string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE nudges SET done = done + 1 WHERE key = ?`, key); err != nil {
		return err
	}
	if err := shardlite.CancelReminder(ctx, tx, key); err != nil {
		return err
	}
	return tx.Commit()
}

func serve(app http.Handler, r *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, r)
	return rec
}

func TestGuideDeclaresItsKinds(t *testing.T) {
	fake := newFake(t, config())
	rec := serve(newApp(fake.App()), httptest.NewRequest(http.MethodGet, shardlite.ReadyPath, nil))
	if want := `{"version":"1","kinds":{"guild":{},"users":{"public":true}}}`; rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Errorf("ready answered %d %q, want %q", rec.Code, rec.Body.String(), want)
	}
}

func TestGuideVisitCounts(t *testing.T) {
	fake := newFake(t, config())
	app := newApp(fake.App())
	for want := 1; want <= 3; want++ {
		rec := serve(app, fake.Request(Users.Shard("42"), http.MethodPost, "/users/42/visit", nil))
		if strings.TrimSpace(rec.Body.String()) != fmt.Sprint(want) {
			t.Fatalf("visit %d answered %d %q", want, rec.Code, rec.Body.String())
		}
	}
	// A request the node opened another shard for is refused.
	rec := serve(app, fake.Request(Users.Shard("7"), http.MethodPost, "/users/42/visit", nil))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("a request for another shard answered %d, want 400", rec.Code)
	}
}

func TestGuideLeaveDeletesTheShard(t *testing.T) {
	fake := newFake(t, config())
	app := newApp(fake.App())
	if rec := serve(app, fake.Request(Users.Shard("42"), http.MethodPost, "/users/42/visit", nil)); rec.Code != http.StatusOK {
		t.Fatalf("visit answered %d", rec.Code)
	}
	if rec := serve(app, fake.Request(Users.Shard("42"), http.MethodDelete, "/users/42", nil)); rec.Code != http.StatusOK {
		t.Fatalf("leave answered %d %q", rec.Code, rec.Body.String())
	}
	// The shard is served until it deactivates, and the handler refuses it.
	if rec := serve(app, fake.Request(Users.Shard("42"), http.MethodPost, "/users/42/visit", nil)); rec.Code != http.StatusGone {
		t.Errorf("a visit after leaving answered %d, want 410", rec.Code)
	}
}

func TestGuideJoinIsForCalls(t *testing.T) {
	fake := newFake(t, config())
	app := newApp(fake.App())
	rec := serve(app, fake.Request(Guilds.Shard("7"), http.MethodPost, "/join", nil))
	if rec.Code != http.StatusForbidden {
		t.Errorf("a client's request for a route of calls answered %d, want 403", rec.Code)
	}
	rec = serve(app, fake.Request(Guilds.Shard("7"), http.MethodPost, "/join", strings.NewReader("9"), fromCaller(&shardlite.CallerApp{})))
	if rec.Code != http.StatusOK || rec.Body.String() != "guild/7: user 9 invited by the app\n" {
		t.Errorf("a call from the app answered %d %q", rec.Code, rec.Body.String())
	}
}

func TestGuideWakeIsTheNodesOnly(t *testing.T) {
	fake := newFake(t, config())
	app := newApp(fake.App())
	rec := serve(app, fake.Request(Users.Shard("42"), http.MethodPost, shardlite.RemindersPath, nil, fromCaller(&shardlite.CallerApp{})))
	if rec.Code != http.StatusForbidden {
		t.Errorf("a wake from the app answered %d, want 403", rec.Code)
	}
}

// The same app behind a node, as a test of the app's own would run it. The
// node needs a shard to exist before a client can reach it, so the test makes
// the shards with calls, and it uses ids of its own to share its node safely
// with other tests.
func TestGuideAgainstANode(t *testing.T) {
	node := shardlitetest.Start(t, config(), func(n *shardlite.Node) http.Handler {
		// The test's own route, beside the app's, reads what a wake did.
		mux := http.NewServeMux()
		mux.Handle("/", newApp(n))
		mux.Handle("GET /users/{id}/nudges", n.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			opened, err := n.Expect(r, Users.Shard(r.PathValue("id")))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			db, err := opened.DB()
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			var done, left int
			err = db.QueryRowContext(r.Context(), `SELECT done, (SELECT count(*) FROM _shardlite_reminders) FROM nudges WHERE key = 'daily'`).Scan(&done, &left)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			fmt.Fprintf(w, "%d %d", done, left)
		})))
		return mux
	})
	user, guild := Users.Shard("node-test-42"), Guilds.Shard("node-test-7")
	for _, shard := range []shardlite.ShardName{user, guild} {
		path := "/join"
		if shard == user {
			path = "/users/node-test-42/visit"
		}
		res, err := node.App.CallAsApp(t.Context(), shard, http.MethodPost, path, nil)
		closeBody(t, res, err)
	}
	send := func(method string, shard shardlite.ShardName, path string) (int, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, node.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(shardlite.HeaderTarget, shard.String())
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}
	post := func(shard shardlite.ShardName, path string) (int, string) {
		t.Helper()
		return send(http.MethodPost, shard, path)
	}
	get := func(shard shardlite.ShardName, path string) (int, string) {
		t.Helper()
		return send(http.MethodGet, shard, path)
	}

	// The call above was the first visit.
	if status, body := post(user, "/users/node-test-42/visit"); status != http.StatusOK || strings.TrimSpace(body) != "2" {
		t.Errorf("a visit answered %d %q, want 2", status, body)
	}
	if status, body := post(user, "/users/node-test-42/invite?guild=node-test-7"); status != http.StatusOK || body != "guild/node-test-7: user node-test-42 invited by users/node-test-42\n" {
		t.Errorf("an invitation answered %d %q", status, body)
	}
	if status, body := post(user, "/users/node-test-42/invite?guild=nowhere"); status != http.StatusNotFound || !strings.Contains(body, "no such guild") {
		t.Errorf("an invitation to a guild that does not exist answered %d %q, want 404", status, body)
	}

	// A reminder that is due now is woken: the work is done and the reminder
	// is gone.
	wake := Users.Shard("node-test-wake")
	res, err := node.App.CallAsApp(t.Context(), wake, http.MethodPost, "/users/node-test-wake/visit", nil)
	closeBody(t, res, err)
	woken, err := node.App.CallAsApp(t.Context(), Users.Shard("node-test-wake"), http.MethodPost, "/users/node-test-wake/nudge?in=-1s", nil)
	closeBody(t, woken, err)
	deadline := time.Now().Add(15 * time.Second)
	for {
		status, body := get(Users.Shard("node-test-wake"), "/users/node-test-wake/nudges")
		if status == http.StatusOK && body == "1 0" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no wake within 15s, the last answer was %d %q:\n%s", status, body, node.Output())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
