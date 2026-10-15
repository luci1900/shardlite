package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
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
// testapp process (see shards_test.go for why).
type srig struct {
	*mrig
	fake    *httptest.Server
	handler atomic.Pointer[http.HandlerFunc]
	srv     *Server
	stop    func() // stops the public and control servers and the health poll and joins them
	pub     string // http://host:port
	ctl     string
	sock    string
	client  *http.Client
	h2c     *http.Client

	omu    sync.Mutex
	opened map[string]bool

	hook      atomic.Pointer[intakeHook]  // wraps the intake, kept across restarts
	inboxHook atomic.Pointer[inboxHookFn] // wraps the delivery of an inbox row
}

func newSRig(t *testing.T) *srig { return newSRigWith(t, nil) }

func newSRigWith(t *testing.T, mutate func(*ShardsOptions)) *srig {
	t.Helper()
	r := &srig{mrig: newUnstartedMRig(t, nil), opened: map[string]bool{}}
	r.fake = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == "/health" {
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
	r.newTransport = r.serverTransport
	r.startNode()
	r.startShards(mutate)
	r.startServer()

	r.client = &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	tr := &http.Transport{}
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetUnencryptedHTTP2(true)
	r.h2c = &http.Client{Timeout: 20 * time.Second, Transport: tr}
	return r
}

// serverTransport makes the server the shards are built with, and returns its
// transport behind the hooks.
func (r *srig) serverTransport(m *Shards) *transport {
	u, _ := url.Parse(r.fake.URL)
	r.srv = NewServer(m, m.opts.Kinds, m.n.Keeper, ServerOptions{App: u, HealthEvery: 20 * time.Millisecond, Grace: 500 * time.Millisecond})
	tr := r.srv.transport()
	intake, inbox := tr.intake, tr.inbox
	tr.intake = func(ctx context.Context, sender *shard, target string, row *outRow) (int, error) {
		h := r.hook.Load()
		if h == nil {
			return intake(ctx, sender, target, row)
		}
		i := int(h.n.Add(1))
		return h.fn(i, sender, target, row, func() (int, error) { return intake(ctx, sender, target, row) })
	}
	tr.inbox = func(ctx context.Context, a *shard, row *inRow) (int, error) {
		if h := r.inboxHook.Load(); h != nil {
			return (*h)(ctx, func() (int, error) { return inbox(ctx, a, row) })
		}
		return inbox(ctx, a, row)
	}
	return tr
}

// startServer serves the server the shards were built with.
func (r *srig) startServer() {
	srv := r.srv
	pub, _ := net.Listen("tcp", "127.0.0.1:0")
	ctl, _ := net.Listen("tcp", "127.0.0.1:0")
	// Unix socket paths are short (104 bytes on macOS): not t.TempDir().
	dir, err := os.MkdirTemp("", "s")
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { os.RemoveAll(dir) })
	r.sock = dir + "/c.sock"
	sock, err := net.Listen("unix", r.sock)
	if err != nil {
		r.t.Fatal(err)
	}
	r.pub, r.ctl = "http://"+pub.Addr().String(), "http://"+ctl.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	var g errgroup.Group
	g.Go(func() error { return srv.publicServer(pub, nil).Serve(ctx) })
	g.Go(func() error { return srv.controlServer(nil, ctl, sock).Serve(ctx) })
	g.Go(func() error { return srv.health.Serve(ctx) })
	var once sync.Once
	r.stop = func() { once.Do(func() { cancel(); g.Wait() }) }
	r.t.Cleanup(r.stop)
	eventually(r.t, "the app to be seen as healthy", func() bool { ok, _ := r.srv.Ready(); return ok })
}

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
	if _, err := r.n.Files.Restore(context.Background(), target, dst, shardfile.RestoreOptions{}); err != nil {
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
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		last.Store(q.Clone(context.Background()))
		w.Header().Set("Shardlite-Secret", "x")
		w.Header().Set("Shardlite-Sync", "off")
		w.Header().Set("X-Plain", "y")
		io.WriteString(w, "hello")
	})

	// The query parameter routes and is stripped; client-sent Shardlite-* headers never reach the app.
	resp, body := r.req("GET", r.pub, "/hello?a=1&shardlite-target=slow/r1&b=2", map[string]string{
		"Shardlite-Caller": "users/9", "Shardlite-Path": "/etc/passwd", "Shardlite-Foo": "1", "Shardlite-If-Active": "true", "Shardlite-Sync": "on",
	}, "")
	q := last.Load()
	if resp.StatusCode != 200 || body != "hello" || q == nil {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if q.Header.Get("Shardlite-Target") != "slow/r1" || q.URL.RawQuery != "a=1&b=2" || q.URL.Path != "/hello" {
		t.Fatalf("app saw %s %v", q.URL, q.Header)
	}
	if p := q.Header.Get("Shardlite-Path"); p == "/etc/passwd" || !strings.Contains(p, "slow") || !strings.HasSuffix(p, ".db") {
		t.Fatalf("Shardlite-Path = %q", p)
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
	if q := last.Load(); q.Header.Get("Shardlite-Target") != "slow/h" || q.URL.RawQuery != "" {
		t.Fatalf("%v %q", q.Header, q.URL.RawQuery)
	}

	// No shard: a plain reverse proxy.
	r.req("GET", r.pub, "/news", map[string]string{"Shardlite-Target": ""}, "")
	if q := last.Load(); q.Header.Get("Shardlite-Target") != "" || q.Header.Get("Shardlite-Path") != "" || q.URL.Path != "/news" {
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
	if resp, _ := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "typo/1"}, ""); resp.StatusCode != 400 {
		t.Fatalf("undeclared kind: %d", resp.StatusCode)
	}
	if resp, _ := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "users"}, ""); resp.StatusCode != 400 {
		t.Fatalf("no id: %d", resp.StatusCode)
	}
	// With gRPC the errors the node makes are gRPC statuses.
	resp, _ = r.req("POST", r.pub, "/svc/Method", map[string]string{"Shardlite-Target": "typo/1", "Content-Type": "application/grpc"}, "")
	if resp.StatusCode != 200 || resp.Header.Get("Grpc-Status") != "3" || resp.Header.Get("Grpc-Message") == "" {
		t.Fatalf("grpc error: %d %v", resp.StatusCode, resp.Header)
	}
	resp, _ = r.req("POST", r.ctl, "/svc/Method", map[string]string{"Shardlite-Target": "typo/1", "Content-Type": "application/grpc+proto"}, "")
	if resp.Header.Get("Grpc-Status") != "3" {
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

	// A call: the target, the caller (passed on), no X-Forwarded-For.
	resp, body := r.req("POST", r.ctl, "/invite?x=1", map[string]string{"Shardlite-Target": "slow/c1", "Shardlite-Caller": "users/5", "Shardlite-Foo": "1"}, "payload")
	q := last.Load()
	if resp.StatusCode != 200 || body != "ok" || q.Header.Get("Shardlite-Target") != "slow/c1" || q.Header.Get("Shardlite-Caller") != "users/5" ||
		q.Header.Get("Shardlite-Foo") != "" || q.Header.Get("X-Forwarded-For") != "" || q.URL.Path != "/invite" || q.Method != "POST" {
		t.Fatalf("%d %q %v", resp.StatusCode, body, q.Header)
	}
	// A call with no shard behind it is from "app".
	r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/c1"}, "")
	if last.Load().Header.Get("Shardlite-Caller") != "app" {
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
	if resp, _ := r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/c1", "Shardlite-Caller": "nonsense"}, ""); resp.StatusCode != 400 {
		t.Fatalf("bad caller: %d", resp.StatusCode)
	}

	// If-Active: a shard that isn't running isn't started.
	last.Store(nil)
	resp, _ = r.req("GET", r.ctl, "/presence", map[string]string{"Shardlite-Target": "slow/c2", "Shardlite-If-Active": "true"}, "")
	if resp.StatusCode != 404 || last.Load() != nil || r.m.IsActive("slow/c2") {
		t.Fatalf("If-Active on an inactive shard: %d, activated=%v", resp.StatusCode, r.m.IsActive("slow/c2"))
	}
	r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/c2"}, "")
	if resp, _ := r.req("GET", r.ctl, "/presence", map[string]string{"Shardlite-Target": "slow/c2", "Shardlite-If-Active": "true"}, ""); resp.StatusCode != 200 {
		t.Fatalf("If-Active on an active shard: %d", resp.StatusCode)
	}
	resp, _ = r.req("POST", r.ctl, "/g", map[string]string{"Shardlite-Target": "slow/c3", "Shardlite-If-Active": "true", "Content-Type": "application/grpc"}, "")
	if resp.Header.Get("Grpc-Status") != "5" {
		t.Fatalf("grpc If-Active: %v", resp.Header)
	}

	// The control API is also on a Unix socket.
	uc := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", r.sock)
	}}}
	q2, _ := http.NewRequest("POST", "http://unix/_shardlite/sync", nil)
	resp2, err := uc.Do(q2)
	if err != nil || resp2.StatusCode != 204 {
		t.Fatalf("sync over the socket: %v %v", resp2, err)
	}
	resp2.Body.Close()
	q2, _ = http.NewRequest("GET", "http://unix/x", nil)
	q2.Header.Set("Shardlite-Target", "slow/c1")
	if resp2, err = uc.Do(q2); err != nil || resp2.StatusCode != 200 {
		t.Fatalf("call over the socket: %v %v", resp2, err)
	}
	resp2.Body.Close()
}

func TestServerDeleteEndpoint(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		r.commit(q.Header.Get("Shardlite-Path"))
		w.Header().Set("Shardlite-Sync", "on")
	})
	r.req("POST", r.pub, "/w", map[string]string{"Shardlite-Target": "slow/del"}, "")
	// The app still has the file open (its connection is kept).
	if resp, _ := r.req("DELETE", r.ctl, "/_shardlite/shards/slow/del", nil, ""); resp.StatusCode != 409 {
		t.Fatalf("delete of a shard the app has open: %d", resp.StatusCode)
	}
	r.omu.Lock()
	for p := range r.opened {
		r.app.Must(t, "close", p)
	}
	r.omu.Unlock()
	if resp, _ := r.req("DELETE", r.ctl, "/_shardlite/shards/slow/del", nil, ""); resp.StatusCode != 204 {
		t.Fatalf("delete: %d", resp.StatusCode)
	}
	if _, err := r.manifest("slow/del"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v", err)
	}
	if resp, _ := r.req("DELETE", r.ctl, "/_shardlite/shards/typo/x", nil, ""); resp.StatusCode != 400 {
		t.Fatalf("delete of an undeclared kind: %d", resp.StatusCode)
	}
}

// Server-sent events leave the node chunk by chunk.
func TestServerStreamsWithoutBuffering(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	// The app holds back the second event until the test has the first. A proxy
	// that buffers never delivers it, and the safety timer lets go.
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	var buffered atomic.Bool
	safety := time.AfterFunc(10*time.Second, func() { buffered.Store(true); release() })
	defer safety.Stop()
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for i := 1; i <= 3; i++ {
			fmt.Fprintf(w, "data: %d\n\n", i)
			fl.Flush()
			select {
			case <-gate:
			case <-q.Context().Done():
				return
			}
		}
	})
	q, _ := http.NewRequest("GET", r.pub+"/events", nil)
	q.Header.Set("Shardlite-Target", "slow/sse")
	resp, err := r.client.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	line, err := br.ReadString('\n')
	if err != nil || line != "data: 1\n" {
		t.Fatalf("%q %v", line, err)
	}
	if buffered.Load() {
		t.Fatal("the first event came only when the app finished: the proxy buffers")
	}
	release()
	// While the stream is open the shard is held and stays active.
	if !r.m.IsActive("slow/sse") {
		t.Fatal("not active during the stream")
	}
	rest, _ := io.ReadAll(br)
	if !strings.Contains(string(rest), "data: 3") {
		t.Fatalf("rest: %q", rest)
	}
}

// An upgraded connection (a WebSocket) is a byte stream the node copies.
func TestServerUpgrade(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Upgrade") != "websocket" || q.Header.Get("Shardlite-Target") != "slow/ws" {
			http.Error(w, "bad", 400)
			return
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nShardlite-Secret: x\r\n\r\n")
		rw.Flush()
		sc := bufio.NewScanner(rw)
		for sc.Scan() {
			rw.WriteString("echo " + sc.Text() + "\n")
			rw.Flush()
		}
	})
	conn, err := net.Dial("tcp", strings.TrimPrefix(r.pub, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws?shardlite-target=slow/ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil || resp.StatusCode != 101 || resp.Header.Get("Shardlite-Secret") != "" {
		t.Fatalf("%v %v", resp, err)
	}
	for _, m := range []string{"one", "two"} {
		fmt.Fprintf(conn, "%s\n", m)
		line, err := br.ReadString('\n')
		if err != nil || line != "echo "+m+"\n" {
			t.Fatalf("%q %v", line, err)
		}
	}
	if !r.m.IsActive("slow/ws") {
		t.Fatal("an open connection should keep the shard active")
	}
	conn.Close()
	// Once it is closed the shard is free to go.
	eventually(t, "the hold to be released", func() bool { return r.refs("slow/ws") == 0 })
}

// Stopping the servers ends what is left of their requests and joins them: a
// WebSocket, which Shutdown alone would leave open, and a request still running
// after the grace period. A request that finishes within the grace period gets
// its answer. Once the servers have stopped, no request holds a shard.
func TestServerShutdownEndsAndJoinsRequests(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var started, ended apptest.Log[string]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		started.Add(q.URL.Path)
		defer ended.Add(q.URL.Path)
		switch q.URL.Path {
		case "/ws":
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
			rw.Flush()
			sc := bufio.NewScanner(rw)
			for sc.Scan() {
				rw.WriteString("echo " + sc.Text() + "\n")
				rw.Flush()
			}
		case "/slow":
			<-q.Context().Done() // until the node gives up on it
		case "/finish":
			time.Sleep(100 * time.Millisecond) // within the grace period
			io.WriteString(w, "done")
		}
	})
	refs := r.refs

	conn, err := net.Dial("tcp", strings.TrimPrefix(r.pub, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws?shardlite-target=slow/ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	br := bufio.NewReader(conn)
	if resp, err := http.ReadResponse(br, nil); err != nil || resp.StatusCode != 101 {
		t.Fatalf("upgrade: %v %v", resp, err)
	}
	fmt.Fprintf(conn, "one\n")
	if line, err := br.ReadString('\n'); err != nil || line != "echo one\n" {
		t.Fatalf("%q %v", line, err)
	}

	type answer struct {
		status int
		body   string
		err    error
	}
	call := func(base, path, target string) chan answer {
		ch := make(chan answer, 1)
		go func() {
			q, _ := http.NewRequest("GET", base+path, nil)
			q.Header.Set("Shardlite-Target", target)
			resp, err := r.client.Do(q)
			if err != nil {
				ch <- answer{err: err}
				return
			}
			defer resp.Body.Close()
			b, err := io.ReadAll(resp.Body)
			ch <- answer{resp.StatusCode, string(b), err}
		}()
		return ch
	}
	slow := call(r.ctl, "/slow", "slow/call") // through the control server
	finish := call(r.pub, "/finish", "slow/finish")
	started.WaitFor(t, 10*time.Second, "the requests to reach the app", func(p []string) bool {
		return slices.Contains(p, "/ws") && slices.Contains(p, "/slow") && slices.Contains(p, "/finish")
	})
	for _, target := range []string{"slow/ws", "slow/call", "slow/finish"} {
		if n := refs(target); n < 1 {
			t.Fatalf("%s: %d references before the servers stop", target, n)
		}
	}

	r.stop()
	// Joined: nothing is left holding a shard the moment the servers have stopped.
	for _, target := range []string{"slow/ws", "slow/call", "slow/finish"} {
		if n := refs(target); n != 0 {
			t.Fatalf("%s: %d references after the servers stopped", target, n)
		}
	}
	if a := <-finish; a.err != nil || a.status != 200 || a.body != "done" {
		t.Fatalf("a request that finished within the grace period: %+v", a)
	}
	if a := <-slow; a.err == nil && a.status == 200 {
		t.Fatalf("a request still running after the grace period was not ended: %+v", a)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := br.ReadString('\n'); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the WebSocket is still open: %v", err)
	}
	ended.WaitFor(t, 10*time.Second, "the app's handlers to end", func(p []string) bool {
		return slices.Contains(p, "/ws") && slices.Contains(p, "/slow")
	})
}

// gRPC: h2c to the app, trailers passed through, streaming without buffering.
func TestServerGRPC(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var proto atomic.Int32
	// The app holds back its second frame until the test has the first. A node
	// that buffers the response never delivers it, and the safety timer lets go.
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	var buffered atomic.Bool
	safety := time.AfterFunc(10*time.Second, func() { buffered.Store(true); release() })
	defer safety.Stop()
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		proto.Store(int32(q.ProtoMajor))
		if q.Header.Get("Te") != "trailers" || q.Header.Get("Shardlite-Target") != "slow/rpc" {
			http.Error(w, "bad", 400)
			return
		}
		io.Copy(io.Discard, q.Body)
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Trailer", "Grpc-Status, Grpc-Message, Shardlite-Trailer")
		w.Write([]byte("frame1"))
		w.(http.Flusher).Flush()
		<-gate
		w.Write([]byte("frame2"))
		w.Header().Set("Grpc-Status", "0")
		w.Header().Set("Grpc-Message", "fine")
		w.Header().Set("Shardlite-Trailer", "x")
	})
	q, _ := http.NewRequest("POST", r.pub+"/svc/Method", strings.NewReader("request"))
	q.Header.Set("Content-Type", "application/grpc")
	q.Header.Set("Te", "trailers")
	q.Header.Set("Shardlite-Target", "slow/rpc")
	resp, err := r.h2c.Do(q) // the client reaches the node over h2c too
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("client protocol %d", resp.ProtoMajor)
	}
	buf := make([]byte, 6)
	if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "frame1" {
		t.Fatalf("%q %v", buf, err)
	}
	if buffered.Load() {
		t.Fatal("the first frame came only when the app finished: buffered")
	}
	release()
	rest, _ := io.ReadAll(resp.Body)
	if string(rest) != "frame2" {
		t.Fatalf("rest %q", rest)
	}
	if resp.Trailer.Get("Grpc-Status") != "0" || resp.Trailer.Get("Grpc-Message") != "fine" || resp.Trailer.Get("Shardlite-Trailer") != "" {
		t.Fatalf("trailers: %v", resp.Trailer)
	}
	if proto.Load() != 2 {
		t.Fatalf("the app was reached over HTTP/%d, want h2c", proto.Load())
	}

	// An error the node makes, over h2c: a gRPC status, not an HTTP error.
	q, _ = http.NewRequest("POST", r.pub+"/svc/Method", strings.NewReader(""))
	q.Header.Set("Content-Type", "application/grpc")
	q.Header.Set("Shardlite-Target", "typo/1")
	resp2, err := r.h2c.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 || resp2.Header.Get("Grpc-Status") != "3" {
		t.Fatalf("%d %v %v", resp2.StatusCode, resp2.Header, resp2.Trailer)
	}
}

func TestServerAppDown(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.fake.Close()
	if resp, _ := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "slow/d1"}, ""); resp.StatusCode != 502 {
		t.Fatalf("app down: %d", resp.StatusCode)
	}
	if resp, _ := r.req("POST", r.pub, "/x", map[string]string{"Shardlite-Target": "slow/d1", "Content-Type": "application/grpc"}, ""); resp.Header.Get("Grpc-Status") != "14" {
		t.Fatalf("app down, grpc: %v", resp.Header)
	}
	eventually(t, "not ready", func() bool { ok, _ := r.srv.Ready(); return !ok })
	if resp, _ := r.req("GET", r.pub, "/_shardlite/ready", nil, ""); resp.StatusCode != 503 {
		t.Fatalf("ready with the app down: %d", resp.StatusCode)
	}
}

func TestServerReadiness(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var unhealthy atomic.Bool
	h := http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {})
	_ = h
	// The fake app's /health is always 200; use a second server, polling one we can break.
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if unhealthy.Load() {
			w.WriteHeader(500)
		}
	}))
	defer broken.Close()
	u, _ := url.Parse(broken.URL)
	srv := NewServer(r.m, r.kinds, r.m.n.Keeper, ServerOptions{App: u, HealthEvery: 20 * time.Millisecond})
	hctx, stopHealth := context.WithCancel(context.Background())
	var hg errgroup.Group
	hg.Go(func() error { return srv.health.Serve(hctx) })
	defer func() { stopHealth(); hg.Wait() }()
	ready := func() bool { ok, _ := srv.Ready(); return ok }
	eventually(t, "ready", ready)

	unhealthy.Store(true)
	eventually(t, "not ready while the app is unhealthy", func() bool { return !ready() })
	unhealthy.Store(false)
	eventually(t, "ready again", ready)

	// A kinds file that stops being valid: not ready, though traffic goes on with the old kinds.
	if err := writeFile(r.kindsPath, `{"kinds": {broken`); err != nil {
		t.Fatal(err)
	}
	if err := r.kinds.Reload(); err == nil {
		t.Fatal("reload of a bad file succeeded")
	}
	if ready() {
		t.Fatal("ready with an invalid kinds file")
	}
	if _, err := r.kinds.Lookup("slow"); err != nil {
		t.Fatal("kinds lost")
	}
	if err := writeFile(r.kindsPath, testKinds); err != nil {
		t.Fatal(err)
	}
	r.kinds.Reload()
	if !ready() {
		t.Fatal("not ready after the kinds file was fixed")
	}
}

// A kind that isn't public is reached by calls from the app only. A client that
// names it gets exactly what it would get for an undeclared kind.
func TestOnlyPublicKindsForClients(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	if err := writeFile(r.kindsPath, `{"kinds": {"users": {"public": true, "sync": "off"}, "secret": {"sync": "off"}}}`); err != nil {
		t.Fatal(err)
	}
	if err := r.kinds.Reload(); err != nil {
		t.Fatal(err)
	}
	r.app_(func(w http.ResponseWriter, q *http.Request) { io.WriteString(w, "ok") })

	hidden, hbody := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "secret/1"}, "")
	missing, mbody := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "nosuch/1"}, "")
	if hidden.StatusCode != missing.StatusCode || strings.ReplaceAll(hbody, "secret", "nosuch") != mbody {
		t.Fatalf("not public: %d %q, undeclared: %d %q", hidden.StatusCode, hbody, missing.StatusCode, mbody)
	}
	if q, _ := r.req("GET", r.pub, "/x?shardlite-target=secret/1", nil, ""); q.StatusCode != missing.StatusCode {
		t.Fatalf("by query parameter: %d", q.StatusCode)
	}
	if r.m.IsActive("secret/1") {
		t.Fatal("a client activated a shard of a kind that is not public")
	}
	if resp, body := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "users/1"}, ""); resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("public kind: %d %q", resp.StatusCode, body)
	}
	if resp, body := r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "secret/1"}, ""); resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("call from the app: %d %q", resp.StatusCode, body)
	}
}
