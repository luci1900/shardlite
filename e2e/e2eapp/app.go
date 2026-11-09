// Package e2eapp is a stand-in for a real application, used by the end-to-end
// tests and the benchmark. It serves two kinds of shards, users (sync off) and
// wallet (sync on), over plain HTTP, server-sent events, WebSocket
// (github.com/coder/websocket) and gRPC (google.golang.org/grpc), with a
// shardless login and a handler that calls another shard through the node's
// control API.
//
// It follows the app contract in docs/guide.md and is built on the Go SDK
// (sdk/go). A route that belongs to a shard works out which shard a request is
// for from the request itself (the path, or x-shard metadata for gRPC) and asks
// Expect for it, which refuses the request unless that is the shard the node
// opened. Routes that only take calls, and the reminder wake, use ExpectCall.
// The shard's database is opened for the length of each request (WAL mode with
// default locking) and the app migrates lazily on the first write. It writes
// its declaration by hand from a file instead of using the SDK's Ready, since
// its scenarios rewrite the declaration while it runs.
package e2eapp

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/migrate"
	shardlite "github.com/luci1900/shardlite/sdk/go"
)

// Options configure an App.
type Options struct {
	Control string // the node's control API, host:port (127.0.0.1:2282 if empty)
	// Declare names a file holding the app's declaration (docs/guide.md, Shard
	// kinds), read again on every request to the ready route, so a
	// rewrite stands in for a new deploy. Empty serves DefaultDeclaration.
	Declare string
}

const DefaultDeclaration = `{"version": "1", "kinds": {
	"users":  {"public": true, "sync": "none", "idle_timeout": "2s"},
	"wallet": {"public": true, "idle_timeout": "2s"},
	"lobby":  {"public": true, "idle_timeout": "2s"},
	"arena":  {"public": true, "idle_timeout": "300ms"},
	"audit":  {}
}}`

// DefaultDeclaration is what the app declares when Options.Declare is empty:
// the kinds the e2e tests use.

// App is the application.
type App struct {
	opts    Options
	node    *shardlite.Node // the SDK's handle on the node
	grpc    *grpc.Server
	debug   Debug
	serving atomic.Int64 // requests being served on a shard, each of which may hold its shard's database

	ccMu sync.Mutex
	cc   *grpc.ClientConn // to the control API
}

// New returns an App. Close stops it.
func New(opts Options) (*App, error) {
	if opts.Control == "" {
		opts.Control = shardlite.DefaultAddr
	}
	hc := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}
	// No Kinds: the declaration is the file the ready route serves.
	node, err := shardlite.New(shardlite.Config{Open: openSQLite, Addr: opts.Control, HTTP: hc})
	if err != nil {
		return nil, err
	}
	a := &App{opts: opts, node: node}
	a.grpc = a.newGRPC()
	return a, nil
}

// Close stops the gRPC server and closes its connection to the node.
func (a *App) Close() {
	a.grpc.Stop()
	a.ccMu.Lock()
	if a.cc != nil {
		a.cc.Close()
	}
	a.ccMu.Unlock()
}

// openSQLite is the app's Opener: its driver and its options. Writers start with BEGIN IMMEDIATE, so a read-then-write transaction can't fail on upgrade.
func openSQLite(uri string) (*sql.DB, error) {
	return sql.Open("sqlite", uri+"&_pragma=synchronous(NORMAL)&_txlock=immediate")
}

// Handler returns the app's HTTP handler. It serves HTTP/1.1 and, for gRPC,
// HTTP/2 over cleartext (the server must have UnencryptedHTTP2 enabled).
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+shardlite.ReadyPath, a.ready)
	mux.HandleFunc("POST /login", a.login)
	// Routes that belong to a shard have its name in the path, and clients can
	// reach them.
	mux.HandleFunc("POST /{kind}/{id}/create", a.shard(pathShard, a.create))
	mux.HandleFunc("POST /{kind}/{id}/add", a.shard(pathShard, a.add))
	mux.HandleFunc("GET /{kind}/{id}/count", a.shard(pathShard, a.count))
	mux.HandleFunc("GET /{kind}/{id}/tokens", a.shard(pathShard, a.tokens))
	mux.HandleFunc("GET /{kind}/{id}/events", a.shard(pathShard, a.events))
	mux.HandleFunc("GET /{kind}/{id}/ws", a.shard(pathShard, a.ws))
	mux.HandleFunc("POST /{kind}/{id}/transfer", a.shard(pathShard, a.transfer))
	mux.HandleFunc("POST /{kind}/{id}/notify", a.shard(pathShard, a.notify))
	mux.HandleFunc("POST /{kind}/{id}/remind", a.shard(pathShard, a.remind))
	mux.HandleFunc("GET /{kind}/{id}/me", a.shard(pathShard, a.me))
	mux.HandleFunc("POST /{kind}/{id}/bulk", a.shard(pathShard, a.bulk))
	mux.HandleFunc("POST /{kind}/{id}/vacuum", a.shard(pathShard, a.vacuum))
	mux.HandleFunc("POST /{kind}/{id}/checkpoints", a.shard(pathShard, a.checkpoints))
	mux.HandleFunc("POST /{kind}/{id}/churn", a.shard(pathShard, a.churn))
	// Routes that only take calls, and the reminder wake, take the shard from
	// what the node opened.
	mux.HandleFunc("POST /credit", a.call(a.credit))
	mux.HandleFunc("POST /receive", a.call(a.receive))
	mux.HandleFunc("POST "+shardlite.RemindersPath, a.call(a.reminders))
	mux.HandleFunc("GET /debug", a.serveDebug)
	// gRPC shares the listener: HTTP/2 requests with a gRPC content type go to
	// the gRPC server (which is an http.Handler), the rest to the mux.
	rpc := a.shard(a.grpcShard, a.serveGRPC)
	app := a.node.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isGRPC(r) {
			rpc.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	// Outside the middleware, so a request counts as served only once its database is closed.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := a.node.Request(r).Opened(); ok {
			a.serving.Add(1)
			defer a.serving.Add(-1)
		}
		app.ServeHTTP(w, r)
	})
}

func (a *App) serveDebug(w http.ResponseWriter, r *http.Request) {
	a.debug.mu.Lock()
	defer a.debug.mu.Unlock()
	a.debug.Serving = int(a.serving.Load())
	writeJSON(w, &a.debug)
}

// ready answers the app's declaration: the node's poll is both the health check
// and how the app's kinds reach it. The route is under /_shardlite/, which the
// node refuses to client requests, so the declaration (which lists every kind,
// including ones clients may not name) only reaches the node. This app reads
// the declaration from a file on every request, to stand in for a new deploy,
// and is not ready while it can't. It writes the JSON by hand.
func (a *App) ready(w http.ResponseWriter, r *http.Request) {
	data := []byte(DefaultDeclaration)
	if a.opts.Declare != "" {
		b, err := os.ReadFile(a.opts.Declare)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		data = b
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// shardReq is a request for a shard, with the shard's database open for as long
// as the request lasts.
type shardReq struct {
	name   string // kind/id
	file   string // Shardlite-File, for routes that open their own connections
	db     *sql.DB
	opened *shardlite.Opened
}

type shardHandler func(w http.ResponseWriter, r *http.Request, s *shardReq)

// shard wraps a handler for a route that belongs to a shard and that clients
// can reach. of works out which shard the request is for. If that is the shard
// the node opened, h gets its database. Otherwise Expect refuses the request:
// the client's Shardlite-Target was missing or wrong, and the app never calls a
// shard with an id the client supplied, so it is the client's to fix.
//
// This app has no authentication. One that did would authenticate before this
// decision: Shardlite-Caller is never a reason to skip it on a route clients
// can reach.
func (a *App) shard(of func(*http.Request) (shardlite.ShardName, error), h shardHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name, err := of(r)
		if err != nil {
			refuse(w, r, http.StatusBadRequest, "the request does not say which shard it is for: "+err.Error())
			return
		}
		opened, err := a.node.Expect(r, name)
		if err != nil {
			refuseExpect(w, r, err)
			return
		}
		a.serveOpened(w, r, opened, h)
	}
}

// call wraps a handler for a route that only takes calls. ExpectCall wants a
// caller (the node sets Shardlite-Caller and strips Shardlite-* headers from
// clients) and the shard is the one the node opened. The reminder wake is the
// same, with the node as its caller.
func (a *App) call(h shardHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		opened, err := a.node.ExpectCall(r)
		if err != nil {
			refuseExpect(w, r, err)
			return
		}
		a.serveOpened(w, r, opened, h)
	}
}

// refuseExpect answers what Expect or ExpectCall refused r with, in the
// protocol r speaks (see refuse).
func refuseExpect(w http.ResponseWriter, r *http.Request, err error) {
	var ref *shardlite.Refusal
	errors.As(err, &ref)
	refuse(w, r, ref.Status, ref.Message)
}

// serveOpened runs h on the shard the node opened, with its database open until
// the request ends.
func (a *App) serveOpened(w http.ResponseWriter, r *http.Request, opened *shardlite.Opened, h shardHandler) {
	name := opened.Name().String()
	a.debug.mu.Lock()
	if a.debug.Served == nil {
		a.debug.Served = map[string]int{}
	}
	a.debug.Served[name]++
	a.debug.mu.Unlock()
	db, err := opened.DB()
	if err != nil {
		refuse(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	h(w, r, &shardReq{name: name, file: r.Header.Get("Shardlite-File"), db: db, opened: opened})
}

// pathShard is the shard of a route whose path is /{kind}/{id}/...
func pathShard(r *http.Request) (shardlite.ShardName, error) {
	return shardlite.ParseShard(r.PathValue("kind") + "/" + r.PathValue("id"))
}

// callerName is the caller as the app records it: the shard's name, "app" or ""
// for a client.
func callerName(c shardlite.Caller) string {
	switch c := c.(type) {
	case *shardlite.CallerClient:
		return ""
	case *shardlite.CallerApp:
		return "app"
	case *shardlite.CallerShard:
		return c.Name.String()
	}
	return ""
}

// Schema. Migrations happen in create and on the first write, not on open: a
// request that only reads a shard that was never written leaves nothing behind.
//
//go:embed migrations/*.sql
var embedded embed.FS

var migrations, _ = fs.Sub(embedded, "migrations") // a fixed, valid name never fails

// seenAge is how long the receiver of a durable send remembers it: the node's
// MaxAge for reminders, after which the sender has stopped trying.
const seenAge = 24 * 60 * 60

// insert adds a row, migrating first if the shard is new, and returns its id and the row count.
func (s *shardReq) insert(ctx context.Context, token string) (id, count int64, err error) {
	return s.insertWith(ctx, token, nil)
}

// insertWith is insert with more work in the same transaction.
func (s *shardReq) insertWith(ctx context.Context, token string, more func(tx *sql.Tx) error) (id, count int64, err error) {
	tx, err := s.db.BeginTx(ctx, nil) // BEGIN IMMEDIATE (the DSN says so): a read-then-write transaction can't fail on upgrade
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	if err := s.migrate(ctx, tx); err != nil {
		return 0, 0, err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO rows (token, payload) VALUES (?, zeroblob(64))`, token)
	if err != nil {
		return 0, 0, err
	}
	id, _ = res.LastInsertId()
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM rows`).Scan(&count); err != nil {
		return 0, 0, err
	}
	if more != nil {
		if err := more(tx); err != nil {
			return 0, 0, err
		}
	}
	return id, count, tx.Commit()
}

// migrate brings the shard's schema up to date in tx, and refuses a shard newer
// than this app reads.
func (s *shardReq) migrate(ctx context.Context, tx *sql.Tx) error {
	if err := migrate.Migrate(ctx, tx, migrations, migrate.Latest); err != nil {
		return fmt.Errorf("shard %s: %w", s.name, err)
	}
	return nil
}

// create sets up a new shard: it migrates, so the shard holds something and is
// kept. Clients can only reach shards that exist, so the app creates one by
// calling create before sending a client to it (docs/guide.md, Creating shards).
func (a *App) create(w http.ResponseWriter, r *http.Request, s *shardReq) {
	tx, err := s.db.BeginTx(r.Context(), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()
	if err := s.migrate(r.Context(), tx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// tokensOf lists the tokens in a shard (none if it was never written).
func (s *shardReq) tokensOf(ctx context.Context) ([]string, error) {
	var version int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil || version == 0 {
		return nil, err
	}
	rs, err := s.db.QueryContext(ctx, `SELECT token FROM rows ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var out []string
	for rs.Next() {
		var t string
		if err := rs.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rs.Err()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (a *App) add(w http.ResponseWriter, r *http.Request, s *shardReq) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	id, count, err := s.insert(r.Context(), string(b))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"shard": s.name, "id": id, "count": count})
}

func (a *App) count(w http.ResponseWriter, r *http.Request, s *shardReq) {
	toks, err := s.tokensOf(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"shard": s.name, "count": len(toks)})
}

func (a *App) tokens(w http.ResponseWriter, r *http.Request, s *shardReq) {
	toks, err := s.tokensOf(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, strings.Join(toks, "\n"))
}

func (a *App) me(w http.ResponseWriter, r *http.Request, s *shardReq) {
	writeJSON(w, map[string]any{"shard": s.name, "caller": callerName(s.opened.Caller()), "context": r.Header.Get(shardlite.HeaderContext)})
}

// events streams n server-sent events, each after a commit: ?n=3&prefix=x.
func (a *App) events(w http.ResponseWriter, r *http.Request, s *shardReq) {
	n, _ := strconv.Atoi(r.URL.Query().Get("n"))
	prefix := r.URL.Query().Get("prefix")
	w.Header().Set("Content-Type", "text/event-stream")
	for i := 1; i <= n; i++ {
		token := fmt.Sprintf("%s-%d", prefix, i)
		if _, _, err := s.insert(r.Context(), token); err != nil {
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", token)
		w.(http.Flusher).Flush()
	}
}

// login is shardless: it works out which shard the user's data lives in, creates
// it and answers with a URL that names it (docs/guide.md, Routing requests to shards).
func (a *App) login(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<10))
	name := strings.TrimSpace(string(b))
	if name == "" || strings.ContainsAny(name, "/?&# ") {
		http.Error(w, "bad user name", http.StatusBadRequest)
		return
	}
	shard := "users/" + name
	target, err := shardlite.ParseShard(shard)
	if err != nil {
		http.Error(w, "bad user name: "+err.Error(), http.StatusBadRequest)
		return
	}
	// The client can only reach a shard that exists, so create it first.
	resp, err := a.node.Request(r).Call(target, http.MethodPost, "/"+shard+"/create", nil)
	if err != nil {
		http.Error(w, "creating the user's shard: "+err.Error(), http.StatusBadGateway)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		http.Error(w, "creating the user's shard: "+resp.Status, http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]string{"shard": shard, "url": "/" + shard + "/me?shardlite-target=" + url.QueryEscape(shard)})
}

// transfer debits this shard, then credits another one by calling it through
// the node's control API: ?to=wallet/b&token=t. The debit is committed before the
// call, and the call is made from this request, so it carries the request's context (which names this shard as its caller) and the node holds it until
// the debit is durable if this shard's kind has sync on.
func (a *App) transfer(w http.ResponseWriter, r *http.Request, s *shardReq) {
	to, token := r.URL.Query().Get("to"), r.URL.Query().Get("token")
	if to == "" || token == "" {
		http.Error(w, "to and token are required", http.StatusBadRequest)
		return
	}
	target, err := shardlite.ParseShard(to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, _, err := s.insert(r.Context(), "debit:"+token); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp, err := s.opened.Call(target, http.MethodPost, "/credit", strings.NewReader(token))
	if err != nil {
		http.Error(w, "credit failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		http.Error(w, fmt.Sprintf("credit failed: %s", resp.Status), http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]string{"from": s.name, "to": to, "token": token})
}

// credit is internal: it only answers calls from another shard.
func (a *App) credit(w http.ResponseWriter, r *http.Request, s *shardReq) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if _, _, err := s.insert(r.Context(), "credit:"+string(b)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
}

// notify is a durable send (docs/guide.md, Durable send). It records a row, the
// call to another shard and a reminder due now under one key, in one
// transaction: ?to=wallet/b&token=t. The node wakes this shard's reminders
// handler once the transaction is durable, and the handler makes the call to
// /receive on the target.
func (a *App) notify(w http.ResponseWriter, r *http.Request, s *shardReq) {
	to, token := r.URL.Query().Get("to"), r.URL.Query().Get("token")
	if to == "" || token == "" {
		http.Error(w, "to and token are required", http.StatusBadRequest)
		return
	}
	if _, err := shardlite.ParseShard(to); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	key := "send:" + token
	_, _, err := s.insertWith(r.Context(), "sent:"+token, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(r.Context(), `INSERT INTO sends (key, target, path, body) VALUES (?, ?, '/receive', ?)`, key, to, []byte(token)); err != nil {
			return err
		}
		return shardlite.SetReminder(r.Context(), tx, key, time.Now())
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
}

var errSeen = errors.New("seen before")

// receive is internal: it records a send from another shard once per sender and
// key (the sender can call twice), in the same transaction as the work.
func (a *App) receive(w http.ResponseWriter, r *http.Request, s *shardReq) {
	caller, key := callerName(s.opened.Caller()), r.Header.Get("Idempotency-Key")
	if key == "" {
		http.Error(w, "an Idempotency-Key is required", http.StatusBadRequest)
		return
	}
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	_, _, err := s.insertWith(r.Context(), "msg:"+string(b), func(tx *sql.Tx) error {
		res, err := tx.ExecContext(r.Context(), `INSERT INTO seen (caller, key, at) VALUES (?, ?, unixepoch()) ON CONFLICT DO NOTHING`, caller, key)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return errSeen // roll back: the work was done by the first call
		}
		_, err = tx.ExecContext(r.Context(), `DELETE FROM seen WHERE at < unixepoch() - ?`, seenAge)
		return err
	})
	if err != nil && !errors.Is(err, errSeen) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
}

// remind records a row and a reminder for the shard itself in one transaction:
// ?key=k&in=2 (seconds). The node wakes the reminders handler once the
// transaction is durable and the time has come.
func (a *App) remind(w http.ResponseWriter, r *http.Request, s *shardReq) {
	key, in := r.URL.Query().Get("key"), r.URL.Query().Get("in")
	secs, err := strconv.Atoi(in)
	if key == "" || err != nil {
		http.Error(w, "key and in (seconds) are required", http.StatusBadRequest)
		return
	}
	_, _, err = s.insertWith(r.Context(), "set:"+key, func(tx *sql.Tx) error {
		return shardlite.SetReminder(r.Context(), tx, "timer:"+key, time.Now().Add(time.Duration(secs)*time.Second))
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
}

// reminders is the node's wake for the shard's reminders. It handles each due
// reminder in a transaction of its own: a timer does its work and deletes its
// reminder together, a send is called with its key as Idempotency-Key and both
// rows are deleted once the call answered 2xx. It answers 503 if a send failed,
// so the node wakes it again with backoff.
func (a *App) reminders(w http.ResponseWriter, r *http.Request, s *shardReq) {
	// The wake comes from the node and names this shard as its caller.
	if c, ok := s.opened.Caller().(*shardlite.CallerShard); !ok || c.Name != s.opened.Name() {
		http.Error(w, "internal", http.StatusForbidden)
		return
	}
	keys, err := shardlite.DueReminders(r.Context(), s.db)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	failed := false
	for _, key := range keys {
		if name, ok := strings.CutPrefix(key, "timer:"); ok {
			_, _, err = s.insertWith(r.Context(), "fired:"+name, func(tx *sql.Tx) error {
				return shardlite.CancelReminder(r.Context(), tx, key)
			})
		} else {
			var sent bool
			if sent, err = a.send(r, s, key); err == nil && !sent {
				failed = true
			}
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if failed {
		http.Error(w, "a send failed", http.StatusServiceUnavailable)
		return
	}
	io.WriteString(w, "ok\n")
}

// send makes the call recorded under key and, if it answered 2xx, deletes the
// row and its reminder in one transaction. It reports whether the call
// succeeded. A reminder with no row is deleted. The call is made on the
// wake's shard, so it carries the wake's context.
func (a *App) send(wake *http.Request, s *shardReq, key string) (sent bool, err error) {
	ctx := wake.Context()
	var target, path string
	var body []byte
	err = s.db.QueryRowContext(ctx, `SELECT target, path, body FROM sends WHERE key = ?`, key).Scan(&target, &path, &body)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err == nil {
		to, err := shardlite.ParseShard(target)
		if err != nil {
			return false, err
		}
		resp, err := s.opened.Call(to, http.MethodPost, path, bytes.NewReader(body), shardlite.Header("Idempotency-Key", key))
		if err != nil {
			return false, nil // try again at the next wake
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return false, nil
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM sends WHERE key = ?`, key); err != nil {
		return false, err
	}
	if err := shardlite.CancelReminder(ctx, tx, key); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
