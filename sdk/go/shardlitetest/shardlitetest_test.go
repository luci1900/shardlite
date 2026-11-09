package shardlitetest_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	shardlite "github.com/luci1900/shardlite/sdk/go"
	"github.com/luci1900/shardlite/sdk/go/shardlitetest"
)

var (
	users = shardlite.Kind{Name: "users", Public: true}
	guild = shardlite.Kind{Name: "guild"}
)

func config() shardlite.Config {
	return shardlite.Config{
		Open:  func(uri string) (*sql.DB, error) { return sql.Open("sqlite", uri+"&_txlock=immediate") },
		Kinds: []shardlite.Kind{users, guild},
		OnOpen: func(ctx context.Context, _ shardlite.ShardName, db *sql.DB) error {
			_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS visits (id INTEGER PRIMARY KEY CHECK (id = 1), n INTEGER NOT NULL) STRICT`)
			return err
		},
	}
}

// app is a small app: a users shard counts visits, calls a guild's join route
// and sets a reminder, and wakes are recorded.
type app struct {
	node *shardlite.Node

	mu    sync.Mutex
	woken map[shardlite.ShardName]int
}

func (a *app) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET "+shardlite.ReadyPath, a.node.Ready())
	mux.HandleFunc("POST /users/{id}/visit", a.visit)
	mux.HandleFunc("POST /users/{id}/invite", a.invite)
	mux.HandleFunc("POST /users/{id}/remind", a.remind)
	mux.HandleFunc("POST /hello", a.hello)
	mux.HandleFunc("POST /join", a.join)
	mux.HandleFunc("POST "+shardlite.RemindersPath, a.wake)
	return a.node.Middleware(mux)
}

func refuse(w http.ResponseWriter, err error) {
	var ref *shardlite.Refusal
	if errors.As(err, &ref) {
		http.Error(w, ref.Message, ref.Status)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (a *app) visit(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.Expect(r, users.Shard(r.PathValue("id")))
	if err != nil {
		refuse(w, err)
		return
	}
	db, err := opened.DB()
	if err != nil {
		refuse(w, err)
		return
	}
	var n int
	err = db.QueryRowContext(r.Context(), `INSERT INTO visits (id, n) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET n = n + 1 RETURNING n`).Scan(&n)
	if err != nil {
		refuse(w, err)
		return
	}
	fmt.Fprintln(w, n)
}

// invite calls the guild named by the query, from the request.
func (a *app) invite(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.Expect(r, users.Shard(r.PathValue("id")))
	if err != nil {
		refuse(w, err)
		return
	}
	res, err := opened.Call(guild.Shard(r.URL.Query().Get("guild")), http.MethodPost, "/join", nil)
	if err != nil {
		refuse(w, err)
		return
	}
	defer res.Body.Close()
	w.WriteHeader(res.StatusCode)
	io.Copy(w, res.Body)
}

// remind sets a reminder that is due now.
func (a *app) remind(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.Expect(r, users.Shard(r.PathValue("id")))
	if err != nil {
		refuse(w, err)
		return
	}
	db, err := opened.DB()
	if err != nil {
		refuse(w, err)
		return
	}
	tx, err := db.BeginTx(r.Context(), nil)
	if err != nil {
		refuse(w, err)
		return
	}
	defer tx.Rollback()
	if err := shardlite.SetReminder(r.Context(), tx, "now", time.Now()); err != nil {
		refuse(w, err)
		return
	}
	if err := tx.Commit(); err != nil {
		refuse(w, err)
	}
}

// hello takes calls only, and says who made it.
func (a *app) hello(w http.ResponseWriter, r *http.Request) { a.join(w, r) }

func (a *app) join(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.ExpectCall(r)
	if err != nil {
		refuse(w, err)
		return
	}
	fmt.Fprintf(w, "%s called by %s", opened.Name(), who(opened.Caller()))
}

// wake records the wake and cancels the due reminders.
func (a *app) wake(w http.ResponseWriter, r *http.Request) {
	opened, err := a.node.ExpectCall(r)
	if err != nil {
		refuse(w, err)
		return
	}
	db, err := opened.DB()
	if err != nil {
		refuse(w, err)
		return
	}
	keys, err := shardlite.DueReminders(r.Context(), db)
	if err != nil {
		refuse(w, err)
		return
	}
	for _, key := range keys {
		if err := shardlite.CancelReminder(r.Context(), db, key); err != nil {
			refuse(w, err)
			return
		}
	}
	a.mu.Lock()
	a.woken[opened.Name()] += len(keys)
	a.mu.Unlock()
}

func (a *app) wokenFor(shard shardlite.ShardName) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.woken[shard]
}

func who(c shardlite.Caller) string {
	switch c := c.(type) {
	case *shardlite.CallerClient:
		return "a client"
	case *shardlite.CallerApp:
		return "the app"
	case *shardlite.CallerShard:
		return c.Name.String()
	}
	return "nobody"
}

// ids hands out a shard id for each use, so tests that share a node never
// share a shard.
var ids atomic.Int64

func newID() string { return fmt.Sprintf("t%d", ids.Add(1)) }

// idOf is the id of a shard.
func idOf(s shardlite.ShardName) string {
	_, id, _ := strings.Cut(s.String(), "/")
	return id
}

// post sends a client's request to the node, for shard.
func post(t *testing.T, n *shardlitetest.Node, shard shardlite.ShardName, path string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, n.URL+path, nil)
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
	return res.StatusCode, strings.TrimSpace(string(body))
}

// create makes the shard exist, as the app does with a call: a client can only
// reach a shard that exists.
func create(t *testing.T, n *shardlitetest.Node, shard shardlite.ShardName) {
	t.Helper()
	res, err := n.App.CallAsApp(t.Context(), shard, http.MethodPost, "/hello", nil)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("creating %s answered %d", shard, res.StatusCode)
	}
}

// The tests share one node, since starting one takes a moment, and use ids of
// their own.
func TestAgainstANode(t *testing.T) {
	a := &app{woken: map[shardlite.ShardName]int{}}
	n := shardlitetest.Start(t, config(), func(node *shardlite.Node) http.Handler {
		a.node = node
		return a.handler()
	})
	if n.App != a.node || n.URL == "" || n.Control == "" || !strings.Contains(n.Output(), "memory") {
		t.Fatalf("Start returned %+v with the output %q", n, n.Output())
	}

	t.Run("a client's request reaches the app with the shard opened", func(t *testing.T) {
		shard := users.Shard(newID())
		create(t, n, shard)
		for want := 1; want <= 2; want++ {
			if status, body := post(t, n, shard, "/users/"+idOf(shard)+"/visit"); status != http.StatusOK || body != fmt.Sprint(want) {
				t.Fatalf("visit %d answered %d %q", want, status, body)
			}
		}
		// The route names another shard than the one the node opened, which
		// Expect refuses.
		if status, _ := post(t, n, shard, "/users/"+newID()+"/visit"); status != http.StatusBadRequest {
			t.Errorf("a request for another shard's route answered %d, want 400", status)
		}
	})

	t.Run("a call through the app's Node reaches the callee with its caller", func(t *testing.T) {
		g := guild.Shard(newID())
		res, err := n.App.CallAsApp(t.Context(), g, http.MethodPost, "/join", nil)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if want := g.String() + " called by the app"; res.StatusCode != http.StatusOK || string(body) != want {
			t.Errorf("a call from the app answered %d %q, want %q", res.StatusCode, body, want)
		}

		// A call made on a request is from the shard that request opened.
		user := users.Shard(newID())
		create(t, n, user)
		path := "/users/" + idOf(user) + "/invite?guild=" + idOf(g)
		if status, body := post(t, n, user, path); status != http.StatusOK || body != g.String()+" called by "+user.String() {
			t.Errorf("an invitation answered %d %q", status, body)
		}
	})

	t.Run("a reminder that is due is woken", func(t *testing.T) {
		user := users.Shard(newID())
		create(t, n, user)
		if status, body := post(t, n, user, "/users/"+idOf(user)+"/remind"); status != http.StatusOK {
			t.Fatalf("remind answered %d %q", status, body)
		}
		deadline := time.Now().Add(15 * time.Second)
		for a.wokenFor(user) == 0 {
			if time.Now().After(deadline) {
				t.Fatalf("no wake for %s within 15s:\n%s", user, n.Output())
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
}

// failingTB records a Fatal and ends the goroutine it was called on, as a
// testing.T does, without failing the test that holds it.
type failingTB struct {
	testing.TB
	mu     sync.Mutex
	failed string
}

func (f *failingTB) Fatal(args ...any) { f.Fatalf("%s", fmt.Sprint(args...)) }
func (f *failingTB) Fatalf(format string, args ...any) {
	f.mu.Lock()
	f.failed = fmt.Sprintf(format, args...)
	f.mu.Unlock()
	runtime.Goexit()
}

// Without a binary the test fails with how to get one. It does not skip.
func TestAMissingBinaryFailsWithHowToBuildOne(t *testing.T) {
	t.Setenv("SHARDLITE_BIN", t.TempDir()+"/nowhere/shardlite")
	ftb := &failingTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		shardlitetest.Start(ftb, config(), func(node *shardlite.Node) http.Handler { return http.NotFoundHandler() })
	}()
	<-done
	for _, want := range []string{"SHARDLITE_BIN", "nowhere", "go build -o <dir>/shardlite ./cmd/shardlite"} {
		if !strings.Contains(ftb.failed, want) {
			t.Errorf("the failure %q does not mention %q", ftb.failed, want)
		}
	}

	// Not set, and not on PATH.
	t.Setenv("SHARDLITE_BIN", "")
	t.Setenv("PATH", t.TempDir())
	ftb = &failingTB{TB: t}
	done = make(chan struct{})
	go func() {
		defer close(done)
		shardlitetest.Start(ftb, config(), func(node *shardlite.Node) http.Handler { return http.NotFoundHandler() })
	}()
	<-done
	if !strings.Contains(ftb.failed, "go build -o <dir>/shardlite ./cmd/shardlite") {
		t.Errorf("the failure %q does not say how to build the binary", ftb.failed)
	}
}
