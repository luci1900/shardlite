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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	_ "modernc.org/sqlite"
)

// Options configure an App.
type Options struct {
	Control   string        // the node's control API, host:port (127.0.0.1:7000 if empty)
	IdleClose time.Duration // close a shard's database after this long unused (500 ms if zero)
}

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
		opts.Control = "127.0.0.1:7000"
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
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok\n") })
	mux.HandleFunc("POST /login", a.login)
	mux.HandleFunc("POST /add", a.shard(a.add))
	mux.HandleFunc("GET /count", a.shard(a.count))
	mux.HandleFunc("GET /tokens", a.shard(a.tokens))
	mux.HandleFunc("GET /events", a.shard(a.events))
	mux.HandleFunc("GET /ws", a.shard(a.ws))
	mux.HandleFunc("POST /transfer", a.shard(a.transfer))
	mux.HandleFunc("POST /notify", a.shard(a.notify))
	mux.HandleFunc("POST /inbox", a.shard(a.inbox))
	mux.HandleFunc("POST /remind", a.shard(a.remind))
	mux.HandleFunc("POST /fired", a.shard(a.fired))
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

// Schema. Migrations happen on the first write, not on open: a request that
// only reads a shard that was never written leaves nothing behind.
const (
	schemaVersion = 1
	applicationID = 0x5348_4c45 // "SHLE"
	migrateSQL    = `CREATE TABLE IF NOT EXISTS rows (id INTEGER PRIMARY KEY AUTOINCREMENT, token TEXT NOT NULL, payload BLOB)`
)

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
	var version int
	if err := tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return 0, 0, err
	}
	if version == 0 {
		if _, err := tx.ExecContext(ctx, migrateSQL); err != nil {
			return 0, 0, err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA application_id = %d`, applicationID)); err != nil {
			return 0, 0, err
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, schemaVersion)); err != nil {
			return 0, 0, err
		}
	} else if version > schemaVersion {
		return 0, 0, fmt.Errorf("shard %s is at schema version %d, newer than this app reads (%d)", s.name, version, schemaVersion)
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

// login is shardless: it works out which shard the user's data lives in and
// answers with a URL that names it (docs/guide.md, Routing requests to shards).
func (a *App) login(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<10))
	name := strings.TrimSpace(string(b))
	if name == "" || strings.ContainsAny(name, "/?&# ") {
		http.Error(w, "bad user name", http.StatusBadRequest)
		return
	}
	shard := "users/" + name
	writeJSON(w, map[string]string{"shard": shard, "url": "/me?shardlite-target=" + url.QueryEscape(shard)})
}

// transfer debits this shard, then credits another one by calling it through
// the node's control API: ?to=wallet/b&token=t. The debit is committed before the
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

// notify records a row and a message to another shard in one transaction:
// ?to=wallet/b&token=t. The node delivers the message once the transaction is
// durable, as a POST to /inbox on the target. The hops the app was called with
// go into the row.
func (a *App) notify(w http.ResponseWriter, r *http.Request, s *shardReq) {
	to, token := r.URL.Query().Get("to"), r.URL.Query().Get("token")
	if to == "" || token == "" {
		http.Error(w, "to and token are required", http.StatusBadRequest)
		return
	}
	hops, _ := strconv.Atoi(r.Header.Get("Shardlite-Hops"))
	_, _, err := s.insertWith(r.Context(), "sent:"+token, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(r.Context(), `INSERT INTO _shardlite_outbox (target, path, content_type, body, hops) VALUES (?, '/inbox', 'text/plain', ?, ?)`, to, token, hops)
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
}

// inbox is internal: it records a message from another shard once per token
// (a message can arrive twice), and consumes its inbox row in the same transaction.
func (a *App) inbox(w http.ResponseWriter, r *http.Request, s *shardReq) {
	id, err := strconv.ParseInt(r.Header.Get("Shardlite-Inbox-Id"), 10, 64)
	if r.Header.Get("Shardlite-Caller") == "" || err != nil {
		http.Error(w, "internal", http.StatusForbidden)
		return
	}
	b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	_, _, err = s.insertWith(r.Context(), "msg:"+string(b), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(r.Context(), `DELETE FROM _shardlite_inbox WHERE id = ?`, id)
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
}

// remind records a row and an inbox row for the shard itself in one
// transaction: ?key=k&in=2 (seconds). The node calls /fired on the same shard
// once the transaction is durable and the time has come.
func (a *App) remind(w http.ResponseWriter, r *http.Request, s *shardReq) {
	key, in := r.URL.Query().Get("key"), r.URL.Query().Get("in")
	secs, err := strconv.Atoi(in)
	if key == "" || err != nil {
		http.Error(w, "key and in (seconds) are required", http.StatusBadRequest)
		return
	}
	_, _, err = s.insertWith(r.Context(), "set:"+key, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(r.Context(), `INSERT INTO _shardlite_inbox (key, path, body, due) VALUES (?, '/fired', ?, unixepoch() + ?) ON CONFLICT (key) DO UPDATE SET due = excluded.due`, key, key, secs)
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
}

// fired is internal: an inbox row of the shard's own came due. It does its work
// and consumes the row in one transaction, as a handler should.
func (a *App) fired(w http.ResponseWriter, r *http.Request, s *shardReq) {
	id, err := strconv.ParseInt(r.Header.Get("Shardlite-Inbox-Id"), 10, 64)
	if r.Header.Get("Shardlite-Caller") != s.name || err != nil {
		http.Error(w, "internal", http.StatusForbidden)
		return
	}
	name, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	_, _, err = s.insertWith(r.Context(), "fired:"+string(name), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(r.Context(), `DELETE FROM _shardlite_inbox WHERE id = ?`, id)
		return err
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
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
