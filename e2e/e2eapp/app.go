// Package e2eapp is a stand-in for a real application, used by the end-to-end
// tests and the benchmark. It serves two kinds of shards, users (sync off) and
// wallet (sync on), over plain HTTP, server-sent events, WebSocket (github.com/coder/websocket) and
// gRPC (google.golang.org/grpc),
// with a shardless login and a handler that calls another shard through the
// node's control API. It follows the app contract in docs/guide.md: the shard's
// identity comes from Shardlite-Target only, the file from Shardlite-Path, it
// uses WAL mode with default locking, migrates lazily on the first write and
// closes shards that have been idle for a while.
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

	"github.com/luci1900/shardlite/internal/migrate"
)

// Options configure an App.
type Options struct {
	Control   string        // the node's control API, host:port (127.0.0.1:2282 if empty)
	IdleClose time.Duration // close a shard's database after this long unused (500 ms if zero)
	// Declare names a file holding the app's declaration (docs/guide.md, The
	// ready route), read again on every request to the ready route, so a
	// rewrite stands in for a new deploy. Empty serves DefaultDeclaration.
	Declare string
}

const DefaultDeclaration = `{"app": "e2e", "version": "1", "kinds": {
	"users":  {"public": true, "sync": "local", "idle_timeout": "2s"},
	"wallet": {"public": true, "idle_timeout": "2s"},
	"lobby":  {"public": true, "idle_timeout": "2s"},
	"arena":  {"public": true, "idle_timeout": "300ms"},
	"audit":  {}
}}`

// DefaultDeclaration is what the app declares when Options.Declare is empty:
// the kinds the e2e tests use.

// App is the application.
type App struct {
	opts  Options
	pool  *pool
	http  *http.Client
	grpc  *grpc.Server
	debug Debug

	ccMu sync.Mutex
	cc   *grpc.ClientConn // to the control API
}

// New returns an App. Close stops it.
func New(opts Options) *App {
	if opts.Control == "" {
		opts.Control = "127.0.0.1:2282"
	}
	if opts.IdleClose <= 0 {
		opts.IdleClose = 500 * time.Millisecond
	}
	a := &App{opts: opts, pool: newPool(opts.IdleClose), http: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}}
	a.grpc = a.newGRPC()
	return a
}

// Close closes every database.
func (a *App) Close() {
	a.grpc.Stop()
	a.ccMu.Lock()
	if a.cc != nil {
		a.cc.Close()
	}
	a.ccMu.Unlock()
	a.pool.close()
}

// Handler returns the app's HTTP handler. It serves HTTP/1.1 and, for gRPC,
// HTTP/2 over cleartext (the server must have UnencryptedHTTP2 enabled).
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_shardlite/ready", a.ready)
	mux.HandleFunc("POST /login", a.login)
	mux.HandleFunc("POST /create", a.shard(a.create))
	mux.HandleFunc("POST /add", a.shard(a.add))
	mux.HandleFunc("GET /count", a.shard(a.count))
	mux.HandleFunc("GET /tokens", a.shard(a.tokens))
	mux.HandleFunc("GET /events", a.shard(a.events))
	mux.HandleFunc("GET /ws", a.shard(a.ws))
	mux.HandleFunc("POST /transfer", a.shard(a.transfer))
	mux.HandleFunc("POST /notify", a.shard(a.notify))
	mux.HandleFunc("POST /receive", a.shard(a.receive))
	mux.HandleFunc("POST /remind", a.shard(a.remind))
	mux.HandleFunc("POST /_shardlite/reminders", a.shard(a.reminders))
	mux.HandleFunc("POST /credit", a.shard(a.credit))
	mux.HandleFunc("GET /me", a.shard(a.me))
	mux.HandleFunc("GET /debug", a.serveDebug)
	// gRPC shares the listener: HTTP/2 requests with a gRPC content type go to
	// the gRPC server (which is an http.Handler), the rest to the mux.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			ctx := context.WithValue(r.Context(), rawHeadersKey{}, r.Header.Clone())
			a.grpc.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (a *App) serveDebug(w http.ResponseWriter, r *http.Request) {
	a.debug.mu.Lock()
	defer a.debug.mu.Unlock()
	a.debug.OpenDBs = int(a.pool.open.Load())
	writeJSON(w, &a.debug)
}

// ready answers the app's declaration: the node's poll is both the health
// check and how the app's kinds reach it. The route is under /_shardlite/,
// which the node refuses to client requests, so the declaration (which lists
// every kind, including ones clients may not name) only reaches the node.
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

// shardReq is a request for a shard.
type shardReq struct {
	name string // kind/id, from Shardlite-Target
	path string
	db   *handle
}

// shard wraps a handler that needs a shard: the node's headers name it. A
// request without them did not come through the node.
func (a *App) shard(h func(w http.ResponseWriter, r *http.Request, s *shardReq)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name, path := r.Header.Get("Shardlite-Target"), r.Header.Get("Shardlite-Path")
		if name == "" || path == "" {
			http.Error(w, "this endpoint is for a shard: name one with Shardlite-Target", http.StatusBadRequest)
			return
		}
		db, err := a.pool.get(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer db.release()
		h(w, r, &shardReq{name: name, path: path, db: db})
	}
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
	tx, err := s.db.sql.BeginTx(ctx, nil) // BEGIN IMMEDIATE (the DSN says so): a read-then-write transaction can't fail on upgrade
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
	if err := tx.Commit(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// tokensOf lists the tokens in a shard (none if it was never written).
func (s *shardReq) tokensOf(ctx context.Context) ([]string, error) {
	var version int
	if err := s.db.sql.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil || version == 0 {
		return nil, err
	}
	rs, err := s.db.sql.QueryContext(ctx, `SELECT token FROM rows ORDER BY id`)
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
	writeJSON(w, map[string]any{"shard": s.name, "caller": r.Header.Get("Shardlite-Caller")})
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
	shard := "e2e.users/" + name
	// The client can only reach a shard that exists, so create it first.
	q, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, "http://"+a.opts.Control+"/create", nil)
	q.Header.Set("Shardlite-Target", shard)
	q.Header.Set("Shardlite-Caller", "app")
	resp, err := a.http.Do(q)
	if err != nil {
		http.Error(w, "creating the user's shard: "+err.Error(), http.StatusBadGateway)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		http.Error(w, "creating the user's shard: "+resp.Status, http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]string{"shard": shard, "url": "/me?shardlite-target=" + url.QueryEscape(shard)})
}

// transfer debits this shard, then credits another one by calling it through
// the node's control API: ?to=e2e.wallet/b&token=t. The debit is committed before the
// call, and the call carries this shard as its caller, so the node holds it until
// the debit is durable if this shard's kind has sync on.
func (a *App) transfer(w http.ResponseWriter, r *http.Request, s *shardReq) {
	to, token := r.URL.Query().Get("to"), r.URL.Query().Get("token")
	if to == "" || token == "" {
		http.Error(w, "to and token are required", http.StatusBadRequest)
		return
	}
	if _, _, err := s.insert(r.Context(), "debit:"+token); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	q, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, "http://"+a.opts.Control+"/credit", strings.NewReader(token))
	q.Header.Set("Shardlite-Target", to)
	q.Header.Set("Shardlite-Caller", s.name) // as the app received it
	resp, err := a.http.Do(q)
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
	if r.Header.Get("Shardlite-Caller") == "" {
		http.Error(w, "internal", http.StatusForbidden)
		return
	}
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if _, _, err := s.insert(r.Context(), "credit:"+string(b)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
}

// notify is a durable send (docs/guide.md, Durable send). It records a row, the
// call to another shard and a reminder due now under one key, in one
// transaction: ?to=e2e.wallet/b&token=t. The node wakes this shard's reminders
// handler once the transaction is durable, and the handler makes the call to
// /receive on the target.
func (a *App) notify(w http.ResponseWriter, r *http.Request, s *shardReq) {
	to, token := r.URL.Query().Get("to"), r.URL.Query().Get("token")
	if to == "" || token == "" {
		http.Error(w, "to and token are required", http.StatusBadRequest)
		return
	}
	key := "send:" + token
	_, _, err := s.insertWith(r.Context(), "sent:"+token, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(r.Context(), `INSERT INTO sends (key, target, path, body) VALUES (?, ?, '/receive', ?)`, key, to, []byte(token)); err != nil {
			return err
		}
		_, err := tx.ExecContext(r.Context(), `INSERT INTO _shardlite_reminders (key, due) VALUES (?, unixepoch())`, key)
		return err
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
	caller, key := r.Header.Get("Shardlite-Caller"), r.Header.Get("Idempotency-Key")
	if caller == "" {
		http.Error(w, "internal", http.StatusForbidden)
		return
	}
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
		_, err := tx.ExecContext(r.Context(), `INSERT INTO _shardlite_reminders (key, due) VALUES (?, unixepoch() + ?) ON CONFLICT (key) DO UPDATE SET due = excluded.due`, "timer:"+key, secs)
		return err
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
	if r.Header.Get("Shardlite-Caller") != s.name {
		http.Error(w, "internal", http.StatusForbidden)
		return
	}
	keys, err := s.dueReminders(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	failed := false
	for _, key := range keys {
		if name, ok := strings.CutPrefix(key, "timer:"); ok {
			_, _, err = s.insertWith(r.Context(), "fired:"+name, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(r.Context(), `DELETE FROM _shardlite_reminders WHERE key = ?`, key)
				return err
			})
		} else {
			var sent bool
			if sent, err = a.send(r.Context(), s, key); err == nil && !sent {
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
// succeeded. A reminder with no row is deleted.
func (a *App) send(ctx context.Context, s *shardReq, key string) (sent bool, err error) {
	var target, path string
	var body []byte
	err = s.db.sql.QueryRowContext(ctx, `SELECT target, path, body FROM sends WHERE key = ?`, key).Scan(&target, &path, &body)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err == nil {
		q, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+a.opts.Control+path, bytes.NewReader(body))
		if err != nil {
			return false, err
		}
		q.Header.Set("Shardlite-Target", target)
		q.Header.Set("Shardlite-Caller", s.name)
		q.Header.Set("Idempotency-Key", key)
		resp, err := a.http.Do(q)
		if err != nil {
			return false, nil // try again at the next wake
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return false, nil
		}
	}
	tx, err := s.db.sql.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM sends WHERE key = ?`, key); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM _shardlite_reminders WHERE key = ?`, key); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// dueReminders lists the keys of the reminders that are due.
func (s *shardReq) dueReminders(ctx context.Context) ([]string, error) {
	rs, err := s.db.sql.QueryContext(ctx, `SELECT key FROM _shardlite_reminders WHERE due <= unixepoch() ORDER BY due, key`)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	var keys []string
	for rs.Next() {
		var k string
		if err := rs.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rs.Err()
}

// pool keeps each shard's database open between requests and closes it when it
// has been idle for a while, well within the node's idle timeout (the node
// can't let go of a shard the app has open).
type pool struct {
	idle time.Duration
	mu   sync.Mutex
	dbs  map[string]*handle
	stop chan struct{}
	wg   sync.WaitGroup
	open atomic.Int64 // databases opened and not yet closed
}

type handle struct {
	p     *pool
	path  string
	sql   *sql.DB
	refs  int
	last  time.Time
	ready chan struct{}
	err   error
}

func newPool(idle time.Duration) *pool {
	p := &pool{idle: idle, dbs: map[string]*handle{}, stop: make(chan struct{})}
	p.wg.Add(1)
	go p.janitor()
	return p
}

func (p *pool) get(path string) (*handle, error) {
	p.mu.Lock()
	h := p.dbs[path]
	if h == nil {
		h = &handle{p: p, path: path, ready: make(chan struct{})}
		p.dbs[path] = h
		h.refs++
		p.mu.Unlock()
		// WAL mode, the default locking mode, and write transactions that start with BEGIN IMMEDIATE.
		h.sql, h.err = sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_txlock=immediate")
		if h.err == nil {
			p.open.Add(1)
			h.sql.SetMaxOpenConns(8)
			h.err = h.sql.Ping()
		}
		close(h.ready)
	} else {
		h.refs++
		p.mu.Unlock()
		<-h.ready
	}
	if h.err != nil {
		h.release()
		return nil, h.err
	}
	return h, nil
}

func (h *handle) release() {
	h.p.mu.Lock()
	h.refs--
	h.last = time.Now()
	h.p.mu.Unlock()
}

func (p *pool) janitor() {
	defer p.wg.Done()
	t := time.NewTicker(p.idle / 5)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
		}
		var closing []*handle
		p.mu.Lock()
		for path, h := range p.dbs {
			if h.refs == 0 && time.Since(h.last) > p.idle {
				delete(p.dbs, path)
				closing = append(closing, h)
			}
		}
		p.mu.Unlock()
		for _, h := range closing {
			if h.sql != nil {
				h.sql.Close()
				p.open.Add(-1)
			}
		}
	}
}

func (p *pool) close() {
	close(p.stop)
	p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, h := range p.dbs {
		if h.sql != nil {
			h.sql.Close()
			p.open.Add(-1)
		}
	}
	p.dbs = map[string]*handle{}
}
