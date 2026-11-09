package shardlite_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

// fakeNode is what the SDK's own tests use in place of a node, for two things
// only. It builds a request as the node sends one (its headers, and a shard
// file with the node's reserved tables), and it stands at the control address
// and records what a call sends, answering a fixed status. It decides nothing
// the node decides. A test that needs a callee, a wake or the node's answers
// uses package shardlitetest.
type fakeNode struct {
	t    testing.TB
	open shardlite.Opener
	app  *shardlite.Node
	dir  string
	srv  *httptest.Server

	syncs atomic.Int64

	mu      sync.Mutex
	files   map[shardlite.ShardName]string // shard to its file
	calls   []fakeCall
	nfile   int
	reserve string // the node's reserved tables
}

// fakeCall is a call the app made through the control API, as it arrived.
type fakeCall struct {
	Target string // Shardlite-Target
	Method string
	Path   string // with its query
	Header http.Header
	Body   []byte
}

// newFake starts a fakeNode for t, with the app's Node built from cfg and
// pointed at it.
func newFake(t testing.TB, cfg shardlite.Config) *fakeNode {
	t.Helper()
	// The node's own file, so the tests' tables cannot drift from the node's.
	reserved, err := os.ReadFile(filepath.Join("..", "..", "internal", "node", "reserved.sql"))
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeNode{t: t, open: cfg.Open, dir: t.TempDir(), files: map[shardlite.ShardName]string{}, reserve: string(reserved)}
	f.srv = httptest.NewServer(http.HandlerFunc(f.control))
	t.Cleanup(f.srv.Close)
	cfg.Addr = f.srv.Listener.Addr().String()
	if f.app, err = shardlite.New(cfg); err != nil {
		t.Fatal(err)
	}
	return f
}

// App is the app's Node, whose calls arrive here.
func (f *fakeNode) App() *shardlite.Node { return f.app }

// Calls returns the calls the app has made, in arrival order.
func (f *fakeNode) Calls() []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCall(nil), f.calls...)
}

// Syncs returns how many times the app has called sync.
func (f *fakeNode) Syncs() int { return int(f.syncs.Load()) }

type fakeOption func(*fakeRequest)

type fakeRequest struct {
	caller shardlite.Caller
	hops   int64
}

// fromCaller makes the request a call from caller (a *CallerApp or a
// *CallerShard). The default is a client's request, with no Shardlite-Caller.
func fromCaller(caller shardlite.Caller) fakeOption {
	return func(o *fakeRequest) { o.caller = caller }
}

// withHops sets the request's place in its chain of calls.
func withHops(hops int64) fakeOption { return func(o *fakeRequest) { o.hops = hops } }

func requestOf(opts []fakeOption) fakeRequest {
	o := fakeRequest{caller: &shardlite.CallerClient{}}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// callerValue is the Shardlite-Caller for caller, "" for a client.
func callerValue(caller shardlite.Caller) string {
	switch c := caller.(type) {
	case *shardlite.CallerClient:
		return ""
	case *shardlite.CallerApp:
		return "app"
	case *shardlite.CallerShard:
		return c.Name.String()
	}
	return ""
}

// Request builds the request the node sends the app for a request it opened
// shard for, creating the shard's database file the first time.
func (f *fakeNode) Request(shard shardlite.ShardName, method, path string, body io.Reader, opts ...fakeOption) *http.Request {
	f.t.Helper()
	o := requestOf(opts)
	r := httptest.NewRequest(method, path, body)
	r.Header.Set("Shardlite-Opened", shard.String())
	r.Header.Set("Shardlite-File", f.file(shard))
	r.Header.Set(shardlite.HeaderContext, fmt.Sprintf(`shard=%q, hops=%d`, shard, o.hops))
	if v := callerValue(o.caller); v != "" {
		r.Header.Set("Shardlite-Caller", v)
	}
	return r
}

// ShardlessRequest builds the request the node sends the app when it opened no
// shard.
func (f *fakeNode) ShardlessRequest(method, path string, body io.Reader, opts ...fakeOption) *http.Request {
	o := requestOf(opts)
	r := httptest.NewRequest(method, path, body)
	if v := callerValue(o.caller); v != "" {
		r.Header.Set("Shardlite-Caller", v)
	}
	return r
}

// file returns the shard's database file, created in WAL mode with the node's
// reserved tables.
func (f *fakeNode) file(shard shardlite.ShardName) string {
	f.t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if p, ok := f.files[shard]; ok {
		return p
	}
	f.nfile++
	kind, id, _ := strings.Cut(shard.String(), "/")
	dir := filepath.Join(f.dir, kind)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		f.t.Fatal(err)
	}
	p := filepath.Join(dir, fmt.Sprintf("%s.k%d.test.db", id, f.nfile))
	// The path is under t.TempDir, which has no character a URI gives a meaning
	// to.
	db, err := f.open("file:" + p + "?mode=rwc")
	if err != nil {
		f.t.Fatalf("creating %s: %v", shard, err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL;` + f.reserve); err != nil {
		f.t.Fatalf("creating %s: %v", shard, err)
	}
	f.files[shard] = p
	return p
}

// control records a call and answers 200 with no body, or a sync and answers
// 204.
func (f *fakeNode) control(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/_shardlite/sync" && r.Method == http.MethodPost {
		f.syncs.Add(1)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCall{
		Target: r.Header.Get(shardlite.HeaderTarget),
		Method: r.Method,
		Path:   r.URL.RequestURI(),
		Header: r.Header.Clone(),
		Body:   body,
	})
}
