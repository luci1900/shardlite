package node

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

// srig is a node serving in front of a fake app. The fake app is in the test
// process, but it never opens a shard itself: commits go through the separate
// testapp process (see shardset_test.go for why).
type srig struct {
	*mrig
	fake    *httptest.Server
	handler atomic.Pointer[http.HandlerFunc]
	decl    atomic.Value // string: what the fake app's ready route answers
	sick    atomic.Bool  // the ready route answers 500
	srv     *Server
	stop    func() // stops the public and control servers and the declaration poll and joins them
	pub     string // http://host:port
	ctl     string
	client  *http.Client
	h2c     *http.Client

	omu    sync.Mutex
	opened map[string]bool

	wakeHook atomic.Pointer[wakeHookFn] // wraps the wake of the app, kept across restarts
}

func newSRig(t *testing.T) *srig { return newSRigWith(t, nil) }

func newSRigWith(t *testing.T, mutate func(*ShardsetOptions)) *srig {
	t.Helper()
	r := &srig{mrig: newUnstartedMRig(t, nil), opened: map[string]bool{}}
	r.decl.Store(testKinds)
	r.fake = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == readyPath {
			if r.sick.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			io.WriteString(w, r.decl.Load().(string))
			return
		}
		if h := r.handler.Load(); h != nil {
			(*h)(w, q)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	r.fake.Config.Protocols = protos
	r.fake.Start()
	t.Cleanup(r.fake.Close)
	r.newRouting = r.serverRouting
	r.startNode()
	r.startShardset(mutate)
	r.startServer()

	r.client = &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	tr := &http.Transport{}
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetUnencryptedHTTP2(true)
	r.h2c = &http.Client{Timeout: 20 * time.Second, Transport: tr}
	return r
}

// serverRouting makes the server the shards are built with, and returns its
// routing, with its wake of the app behind the hook.
func (r *srig) serverRouting(m *Shardset) routing {
	u, _ := url.Parse(r.fake.URL)
	r.srv = NewServer(m, m.opts.Kinds, m.n.Keeper, ServerOptions{App: u, ReadyEvery: 20 * time.Millisecond, Grace: 500 * time.Millisecond})
	wake := r.srv.wakeApp
	return routing{call: r.srv.call, wake: func(ctx context.Context, a *actor) (int, error) {
		if h := r.wakeHook.Load(); h != nil {
			return (*h)(ctx, func() (int, error) { return wake(ctx, a) })
		}
		return wake(ctx, a)
	}}
}

// startServer serves the server the shards were built with.
func (r *srig) startServer() {
	srv := r.srv
	pub, _ := net.Listen("tcp", "127.0.0.1:0")
	ctl, _ := net.Listen("tcp", "127.0.0.1:0")
	r.pub, r.ctl = "http://"+pub.Addr().String(), "http://"+ctl.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	var g errgroup.Group
	g.Go(func() error { return srv.serverOn("public server", pub, srv.PublicHandler()).Serve(ctx) })
	g.Go(func() error { return srv.serverOn("control server", ctl, srv.ControlHandler()).Serve(ctx) })
	g.Go(func() error { return srv.declarations.Serve(ctx) })
	var once sync.Once
	r.stop = func() { once.Do(func() { cancel(); g.Wait() }) }
	r.t.Cleanup(r.stop)
	apptest.Eventually(r.t, apptest.Timeout, "the app to be seen as ready", func() bool { ok, _ := r.srv.Ready(); return ok })
}

// declare changes what the fake app's ready route answers, as a new deploy of
// the app would. The poll sees it within its interval.
func (r *srig) declare(decl string) { r.decl.Store(decl) }

func (r *srig) app_(h http.HandlerFunc) { r.handler.Store(&h) }

// commit has the app process commit one row to the shard whose file is path.
func (r *srig) commit(path string) {
	r.t.Helper()
	r.omu.Lock()
	defer r.omu.Unlock()
	if !r.opened[path] {
		r.app.Must(r.t, "open", path, path)
		r.opened[path] = true
	}
	r.app.Must(r.t, "insert", path, "1", "64")
}

// rows is how many rows a restore from blob storage has.
func (r *srig) rows(target string) int {
	r.t.Helper()
	dst := filepath.Join(r.t.TempDir(), "rows.db")
	if _, err := r.n.Files.Restore(context.Background(), shardName(target), dst, shardfile.RestoreOptions{}); err != nil {
		if errors.Is(err, shardfile.ErrNothingToRestore) || errors.Is(err, blob.ErrNotFound) {
			return 0
		}
		r.t.Fatalf("restore %s: %v", target, err)
	}
	return strings.Count(sqlitetest.Dump(r.t, dst), "kv: ")
}

func (r *srig) req(method, base, path string, hdr map[string]string, body string) (*http.Response, string) {
	r.t.Helper()
	q, err := http.NewRequest(method, base+path, strings.NewReader(body))
	if err != nil {
		r.t.Fatal(err)
	}
	for k, v := range hdr {
		q.Header.Set(k, v)
	}
	resp, err := r.client.Do(q)
	if err != nil {
		r.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestServerRouting(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/h", "slow/r1", "slow/q")
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		last.Store(q.Clone(context.Background()))
		w.Header().Set("Shardlite-Secret", "x")
		w.Header().Set("Shardlite-Sync", "none")
		w.Header().Set("X-Plain", "y")
		io.WriteString(w, "hello")
	})

	// The query parameter routes and is stripped; client-sent Shardlite-* headers never reach the app.
	resp, body := r.req("GET", r.pub, "/hello?a=1&shardlite-target=slow/r1&b=2", map[string]string{
		"Shardlite-Caller": "users/9", "Shardlite-File": "/etc/passwd", "Shardlite-Foo": "1", "Shardlite-If-Active": "true", "Shardlite-Sync": "none",
		"Shardlite-Opened": "users/evil", hdrContext: ctxOf("users/evil", 7),
	}, "")
	q := last.Load()
	if resp.StatusCode != 200 || body != "hello" || q == nil {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	// The app is told what the node opened, not what the client named, and a client's own Shardlite-Opened gave way to it.
	if q.Header.Get("Shardlite-Opened") != "slow/r1" || q.Header.Get("Shardlite-Target") != "" || q.URL.RawQuery != "a=1&b=2" || q.URL.Path != "/hello" {
		t.Fatalf("app saw %s %v", q.URL, q.Header)
	}
	// ...and so did a client's Shardlite-Context: the app's is the node's own, for the shard it opened.
	if got := q.Header.Values(hdrContext); len(got) != 1 || got[0] != ctxOf("slow/r1", 0) {
		t.Fatalf("Shardlite-Context = %q, want %q", got, ctxOf("slow/r1", 0))
	}
	if p := q.Header.Get("Shardlite-File"); p == "/etc/passwd" || !strings.HasSuffix(p, ".db") {
		t.Fatalf("Shardlite-File = %q", p)
	}
	// The path is the kind's directory and the id, with no app segment.
	if p := q.Header.Get("Shardlite-File"); filepath.Base(filepath.Dir(p)) != "slow" || !strings.HasPrefix(filepath.Base(p), "r1.") {
		t.Fatalf("Shardlite-File = %q, want .../slow/r1.*.db", p)
	}
	for _, h := range []string{"Shardlite-Caller", "Shardlite-Foo", "Shardlite-If-Active", "Shardlite-Sync"} {
		if q.Header.Get(h) != "" {
			t.Fatalf("%s reached the app: %q", h, q.Header.Get(h))
		}
	}
	if q.Header.Get("X-Forwarded-For") == "" {
		t.Fatal("no X-Forwarded-For")
	}
	for k := range resp.Header {
		if strings.HasPrefix(k, "Shardlite-") {
			t.Fatalf("response header %s reached the client", k)
		}
	}
	if resp.Header.Get("X-Plain") != "y" {
		t.Fatal("ordinary headers must pass")
	}

	// The header wins over the query parameter, which is stripped anyway.
	r.req("GET", r.pub, "/p?shardlite-target=slow/q", map[string]string{"Shardlite-Target": "slow/h"}, "")
	if q := last.Load(); q.Header.Get("Shardlite-Opened") != "slow/h" || q.Header.Get("Shardlite-Target") != "" || q.URL.RawQuery != "" {
		t.Fatalf("%v %q", q.Header, q.URL.RawQuery)
	}

	// No shard: a plain reverse proxy.
	r.req("GET", r.pub, "/news", map[string]string{"Shardlite-Target": "", "Shardlite-Opened": "users/evil"}, "")
	if q := last.Load(); q.Header.Get("Shardlite-Target") != "" || q.Header.Get("Shardlite-Opened") != "" || q.Header.Get("Shardlite-File") != "" || q.URL.Path != "/news" {
		t.Fatalf("shardless request: %v", q.Header)
	}

	// The node's paths are not the clients'.
	for _, p := range []string{"/_shardlite/sync", "/_shardlite/shards/slow/r1", "/_SHARDLITE/sync", "//_shardlite/sync", "/a/../_shardlite/sync", "/_shardlite"} {
		last.Store(nil)
		for _, m := range []string{"GET", "POST", "DELETE"} {
			if resp, _ := r.req(m, r.pub, p, nil, ""); resp.StatusCode != 404 || last.Load() != nil {
				t.Fatalf("%s %s: %d (reached the app: %v)", m, p, resp.StatusCode, last.Load() != nil)
			}
		}
	}
	// ...except the readiness check a load balancer needs.
	if resp, _ := r.req("GET", r.pub, "/_shardlite/ready", nil, ""); resp.StatusCode != 200 {
		t.Fatalf("ready: %d", resp.StatusCode)
	}

	// Kinds.
	if resp, _ := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "typo/1"}, ""); resp.StatusCode != 404 {
		t.Fatalf("undeclared kind: %d", resp.StatusCode)
	}
	if resp, _ := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "users"}, ""); resp.StatusCode != 400 {
		t.Fatalf("no id: %d", resp.StatusCode)
	}
	// The control API has no delete: a shard is deleted with its delete marker.
	if resp, _ := r.req("DELETE", r.ctl, "/_shardlite/shards/users/x", nil, ""); resp.StatusCode != 404 {
		t.Fatalf("delete on the control API: %d", resp.StatusCode)
	}
	// With gRPC the errors the node makes are gRPC statuses.
	resp, _ = r.req("POST", r.pub, "/svc/Method", map[string]string{"Shardlite-Target": "typo/1", "Content-Type": "application/grpc"}, "")
	if resp.StatusCode != 200 || resp.Header.Get("Grpc-Status") != "5" || resp.Header.Get("Grpc-Message") == "" {
		t.Fatalf("grpc error: %d %v", resp.StatusCode, resp.Header)
	}
	resp, _ = r.req("POST", r.ctl, "/svc/Method", map[string]string{"Shardlite-Target": "typo/1", "Content-Type": "application/grpc+proto"}, "")
	if resp.Header.Get("Grpc-Status") != "5" {
		t.Fatalf("grpc error on the control API: %v", resp.Header)
	}
}

func TestServerControlAPI(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		last.Store(q.Clone(context.Background()))
		io.WriteString(w, "ok")
	})

	// A call: the opened shard, the caller (from the context), no X-Forwarded-For.
	resp, body := r.req("POST", r.ctl, "/invite?x=1", map[string]string{"Shardlite-Target": "slow/c1", hdrContext: ctxOf("users/5", 1), "Shardlite-Foo": "1"}, "payload")
	q := last.Load()
	if resp.StatusCode != 200 || body != "ok" || q.Header.Get("Shardlite-Opened") != "slow/c1" || q.Header.Get("Shardlite-Target") != "" || q.Header.Get("Shardlite-Caller") != "users/5" ||
		q.Header.Get(hdrContext) != ctxOf("slow/c1", 2) ||
		q.Header.Get("Shardlite-Foo") != "" || q.Header.Get("X-Forwarded-For") != "" || q.URL.Path != "/invite" || q.Method != "POST" {
		t.Fatalf("%d %q %v", resp.StatusCode, body, q.Header)
	}
	// A call with no shard behind it is from "app", and a Shardlite-Caller the app sends is no caller.
	r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/c1", "Shardlite-Caller": "users/5"}, "")
	if last.Load().Header.Get("Shardlite-Caller") != "app" || last.Load().Header.Get(hdrContext) != ctxOf("slow/c1", 1) {
		t.Fatalf("caller: %v", last.Load().Header)
	}
	// ...and a client's request has none.
	r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "slow/c1", "Shardlite-Caller": "app"}, "")
	if last.Load().Header.Get("Shardlite-Caller") != "" {
		t.Fatalf("a client request carries a caller: %v", last.Load().Header)
	}
	if resp, _ := r.req("GET", r.ctl, "/x", nil, ""); resp.StatusCode != 400 {
		t.Fatalf("call with no target: %d", resp.StatusCode)
	}
	if resp, _ := r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/c1", hdrContext: `shard="nonsense", hops=0`}, ""); resp.StatusCode != 400 {
		t.Fatalf("bad caller: %d", resp.StatusCode)
	}

	// If-Active: a shard that isn't running isn't started.
	last.Store(nil)
	resp, _ = r.req("GET", r.ctl, "/presence", map[string]string{"Shardlite-Target": "slow/c2", "Shardlite-If-Active": "true"}, "")
	if resp.StatusCode != 404 || last.Load() != nil || r.m.IsActive(shardName("slow/c2")) {
		t.Fatalf("If-Active on an inactive shard: %d, activated=%v", resp.StatusCode, r.m.IsActive(shardName("slow/c2")))
	}
	r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/c2"}, "")
	if resp, _ := r.req("GET", r.ctl, "/presence", map[string]string{"Shardlite-Target": "slow/c2", "Shardlite-If-Active": "true"}, ""); resp.StatusCode != 200 {
		t.Fatalf("If-Active on an active shard: %d", resp.StatusCode)
	}
	resp, _ = r.req("POST", r.ctl, "/g", map[string]string{"Shardlite-Target": "slow/c3", "Shardlite-If-Active": "true", "Content-Type": "application/grpc"}, "")
	if resp.Header.Get("Grpc-Status") != "5" {
		t.Fatalf("grpc If-Active: %v", resp.Header)
	}
}

// wantReason checks the Shardlite-Error of a response.
func wantReason(t *testing.T, resp *http.Response, want reason) {
	t.Helper()
	if got := resp.Header.Get(hdrError); got != string(want) {
		t.Errorf("Shardlite-Error is %q, want %q (status %d)", got, want, resp.StatusCode)
	}
}

// Every error the node makes carries its reason in Shardlite-Error, on the public,
// control and peer listeners, and as a header beside grpc-status on a gRPC answer.
func TestServerErrorsCarryTheirReason(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/d1")
	peer := httptest.NewServer(r.srv.PeerHandler())
	t.Cleanup(peer.Close)
	call := map[string]string{"Shardlite-Target": "slow/d1"}
	with := func(extra ...string) map[string]string {
		h := map[string]string{}
		for k, v := range call {
			h[k] = v
		}
		for i := 0; i < len(extra); i += 2 {
			h[extra[i]] = extra[i+1]
		}
		return h
	}
	for _, tc := range []struct {
		name, base, path string
		hdr              map[string]string
		status           int
		reason           reason
	}{
		{"a malformed name, public", r.pub, "/x", map[string]string{"Shardlite-Target": "nokind"}, 400, reasonBadRequest},
		{"a malformed name, control", r.ctl, "/x", map[string]string{"Shardlite-Target": "nokind"}, 400, reasonBadRequest},
		{"a bad caller", r.ctl, "/x", with(hdrContext, `shard="nokind", hops=0`), 400, reasonBadRequest},
		{"a context that does not parse", r.ctl, "/x", with(hdrContext, "shard="), 400, reasonBadRequest},
		{"a context with no hops", r.ctl, "/x", with(hdrContext, `shard="users/1"`), 400, reasonBadRequest},
		{"an undeclared kind, public", r.pub, "/x", map[string]string{"Shardlite-Target": "typo/1"}, 404, reasonUnknownShard},
		{"an undeclared kind, control", r.ctl, "/x", with("Shardlite-Target", "typo/1"), 404, reasonUnknownShard},
		{"a missing shard a client names", r.pub, "/x", map[string]string{"Shardlite-Target": "slow/nobody"}, 404, reasonUnknownShard},
		{"a missing shard a call must not create", r.ctl, "/x", with("Shardlite-Target", "slow/nobody", hdrIfExists, "true"), 404, reasonUnknownShard},
		{"an inactive shard", r.ctl, "/x", with("Shardlite-Target", "slow/nobody", hdrIfActive, "true"), 404, reasonNotActive},
		{"a path the public listener refuses", r.pub, "/_shardlite/sync", nil, 404, reasonNoEndpoint},
		{"a path the control listener lacks", r.ctl, "/_shardlite/nothing", nil, 404, reasonNoEndpoint},
		{"a peer request that is no forward", peer.URL, "/x", map[string]string{"Shardlite-Target": "slow/d1"}, 404, reasonNoEndpoint},
		{"calls chained too far", r.ctl, "/x", with(hdrContext, ctxOf("users/1", 1000)), 508, reasonHops},
	} {
		resp, _ := r.req("GET", tc.base, tc.path, tc.hdr, "")
		if resp.StatusCode != tc.status {
			t.Errorf("%s: %d, want %d", tc.name, resp.StatusCode, tc.status)
		}
		wantReason(t, resp, tc.reason)
	}

	// As gRPC metadata: a response header beside grpc-status.
	resp, _ := r.req("POST", r.pub, "/svc/M", map[string]string{"Shardlite-Target": "typo/1", "Content-Type": "application/grpc"}, "")
	if resp.Header.Get("Grpc-Status") != "5" {
		t.Errorf("gRPC: %v", resp.Header)
	}
	wantReason(t, resp, reasonUnknownShard)
}

// An error the app answers with has no Shardlite-Error, whatever its status, and an
// app that sets the header itself has it removed, since the header means the node
// answered.
func TestServerAppErrorsHaveNoReason(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/d1")
	peer := httptest.NewServer(r.srv.PeerHandler())
	t.Cleanup(peer.Close)
	var status atomic.Int32
	var forge atomic.Bool
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if forge.Load() {
			w.Header().Set("Shardlite-Error", "app-unreachable")
			w.Header().Set("Shardlite_Error", "timeout")
		}
		w.WriteHeader(int(status.Load()))
	})
	for _, forged := range []bool{false, true} {
		forge.Store(forged)
		for _, code := range []int{404, 502, 503} {
			status.Store(int32(code))
			for _, tc := range []struct {
				name, base string
				hdr        map[string]string
			}{
				{"public", r.pub, map[string]string{"Shardlite-Target": "slow/d1"}},
				{"public, shardless", r.pub, nil},
				{"control", r.ctl, map[string]string{"Shardlite-Target": "slow/d1"}},
				{"peer", peer.URL, map[string]string{"Shardlite-Target": "slow/d1", "Shardlite-Origin": originClient}},
			} {
				resp, _ := r.req("GET", tc.base, "/x", tc.hdr, "")
				if resp.StatusCode != code {
					t.Fatalf("%s: the app's %d reached the client as %d", tc.name, code, resp.StatusCode)
				}
				for k := range resp.Header {
					if strings.EqualFold(strings.ReplaceAll(k, "_", "-"), hdrError) {
						t.Errorf("%s, the app's %d (forged %v): header %q reached the client", tc.name, code, forged, k)
					}
				}
			}
		}
	}
}

func TestServerReadiness(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	ready := func() bool { ok, _ := r.srv.Ready(); return ok }
	apptest.Eventually(t, apptest.Timeout, "ready", ready)

	// The app answering 500: not ready, and the reason is on the ready route.
	r.sick.Store(true)
	apptest.Eventually(t, apptest.Timeout, "not ready while the app is sick", func() bool { return !ready() })
	if _, why := r.srv.Ready(); !strings.Contains(why, "500") {
		t.Fatalf("why: %q", why)
	}
	r.sick.Store(false)
	apptest.Eventually(t, apptest.Timeout, "ready again", ready)

	// A declaration that stops being valid: not ready, though the kinds of the
	// last good declaration keep serving.
	r.declare(`{"kinds": {broken`)
	apptest.Eventually(t, apptest.Timeout, "not ready with a bad declaration", func() bool { return !ready() })
	if _, err := r.kinds.Lookup("slow"); err != nil {
		t.Fatal("kinds lost")
	}
	r.declare(testKinds)
	apptest.Eventually(t, apptest.Timeout, "ready after the app declared itself again", ready)
}

// A kind that isn't public is reached by calls from the app only. A client that
// names it gets exactly what it would get for an undeclared kind.
func TestOnlyPublicKindsForClients(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("users/1")
	r.declare(`{"kinds": {"users": {"public": true, "sync": "none"}, "secret": {"sync": "none"}}}`)
	apptest.Eventually(t, apptest.Timeout, "the new declaration", func() bool { _, err := r.kinds.Lookup("secret"); return err == nil })
	r.app_(func(w http.ResponseWriter, q *http.Request) { io.WriteString(w, "ok") })

	hidden, hbody := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "secret/1"}, "")
	missing, mbody := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "nosuch/1"}, "")
	if hidden.StatusCode != missing.StatusCode || strings.ReplaceAll(hbody, "secret", "nosuch") != mbody {
		t.Fatalf("not public: %d %q, undeclared: %d %q", hidden.StatusCode, hbody, missing.StatusCode, mbody)
	}
	if q, _ := r.req("GET", r.pub, "/x?shardlite-target=secret/1", nil, ""); q.StatusCode != missing.StatusCode {
		t.Fatalf("by query parameter: %d", q.StatusCode)
	}
	if r.m.IsActive(shardName("secret/1")) {
		t.Fatal("a client activated a shard of a kind that is not public")
	}
	if resp, body := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "users/1"}, ""); resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("public kind: %d %q", resp.StatusCode, body)
	}
	if resp, body := r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "secret/1"}, ""); resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("call from the app: %d %q", resp.StatusCode, body)
	}
}
