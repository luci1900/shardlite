package node

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/http/httputil"
	"net/textproto"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/peers"
	"github.com/luci1900/shardlite/internal/shard"
)

// The load balancer is the trust boundary: the app sees the chain it sent with
// its own address added, and the protocol and host the client used. Without
// them the app sees the node's own view of the request.
func TestServerKeepsForwardedHeadersFromTheLoadBalancer(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/x1")
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		last.Store(q.Clone(context.Background()))
		io.WriteString(w, "ok")
	})
	seen := func(hdr map[string]string) (xff, proto, host string) {
		t.Helper()
		hdr["Shardlite-Target"] = "slow/x1"
		if resp, _ := r.req("GET", r.pub, "/x", hdr, ""); resp.StatusCode != 200 {
			t.Fatalf("status %d", resp.StatusCode)
		}
		q := last.Load()
		return q.Header.Get("X-Forwarded-For"), q.Header.Get("X-Forwarded-Proto"), q.Header.Get("X-Forwarded-Host")
	}

	xff, proto, host := seen(map[string]string{
		"X-Forwarded-For": "203.0.113.9, 198.51.100.7", "X-Forwarded-Proto": "https", "X-Forwarded-Host": "app.example.com",
	})
	if xff != "203.0.113.9, 198.51.100.7, 127.0.0.1" || proto != "https" || host != "app.example.com" {
		t.Fatalf("with the load balancer's headers: %q %q %q", xff, proto, host)
	}
	xff, proto, host = seen(map[string]string{})
	if xff != "127.0.0.1" || proto != "http" || host != strings.TrimPrefix(r.pub, "http://") {
		t.Fatalf("without them: %q %q %q", xff, proto, host)
	}
}

// A proxy passes bodies through as they are: it adds no Accept-Encoding to the
// app's request for a client that sent none, and it hands on a compressed
// answer without decoding it.
func TestServerLeavesCompressionToTheClient(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/z1")
	var asked atomic.Value // the request's Accept-Encoding
	var zipped bytes.Buffer
	zw := gzip.NewWriter(&zipped)
	io.WriteString(zw, "hello")
	zw.Close()
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		asked.Store(q.Header.Get("Accept-Encoding"))
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(zipped.Bytes())
	})

	c := &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{DisableKeepAlives: true, DisableCompression: true}}
	q, _ := http.NewRequest("GET", r.pub+"/z", nil)
	q.Header.Set("Shardlite-Target", "slow/z1")
	resp, err := c.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if ae := asked.Load(); ae != "" {
		t.Fatalf("the app saw Accept-Encoding %v (nil if it saw no request)", ae)
	}
	if resp.Header.Get("Content-Encoding") != "gzip" || !bytes.Equal(body, zipped.Bytes()) {
		t.Fatalf("the answer was changed on the way: %q %x", resp.Header.Get("Content-Encoding"), body)
	}
}

// CGI-style frameworks give Shardlite_Caller the same variable as
// Shardlite-Caller, so the underscore spelling must not get through either way.
func TestServerStripsUnderscoreHeaders(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/u1")
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		last.Store(q.Clone(context.Background()))
		w.Header().Set("Shardlite_Foo", "x")
		w.Header().Set("SHARDLITE_BAR", "y")
		w.Header().Set("X-Plain", "y")
	})

	resp, _ := r.req("GET", r.pub, "/x", map[string]string{
		"Shardlite-Target": "slow/u1", "Shardlite-Opened": "users/evil", "Shardlite_Opened": "users/evil", "Shardlite_Caller": "users/9", "SHARDLITE_FOO": "1", "X-Plain": "z",
	}, "")
	q := last.Load()
	if resp.StatusCode != 200 || q == nil {
		t.Fatalf("%d", resp.StatusCode)
	}
	for k, v := range q.Header {
		if strings.HasPrefix(strings.ToLower(k), "shardlite") && k != "Shardlite-Opened" && k != "Shardlite-File" && k != hdrContext {
			t.Fatalf("%s: %v reached the app", k, v)
		}
	}
	if q.Header.Get("Shardlite-Opened") != "slow/u1" || q.Header.Get("Shardlite-Target") != "" {
		t.Fatalf("app saw Shardlite-Opened %q and Shardlite-Target %q", q.Header.Get("Shardlite-Opened"), q.Header.Get("Shardlite-Target"))
	}
	if q.Header.Get("X-Plain") != "z" {
		t.Fatal("ordinary headers must pass")
	}
	for k, v := range resp.Header {
		if strings.HasPrefix(strings.ToLower(k), "shardlite") {
			t.Fatalf("response header %s: %v reached the client", k, v)
		}
	}
	if resp.Header.Get("X-Plain") != "y" {
		t.Fatal("ordinary headers must pass")
	}
}

// The shard a request is for is told differently depending on who gets it: the
// app is told what the node opened (with its file, and a context of its own),
// and the owner's node, which a forward goes to, is told the target to route by
// and the context the call was made with, unchanged, in place of the caller and
// the hops.
func TestRewriteNamesTheShardByWhereItGoes(t *testing.T) {
	t.Parallel()
	log := slog.New(slog.DiscardHandler)
	m := &Shardset{n: &Node{Config: Config{Name: "n0"}}, tab: newTable(log), log: log}
	m.placer = &peers.Placer{Self: "n0", Draining: &m.draining}
	s := &Server{m: m, opts: ServerOptions{App: &url.URL{Scheme: "http", Host: "app.invalid"}}}
	rewrite := func(info *reqInfo) http.Header {
		in := httptest.NewRequest("GET", "/x", nil)
		in.Header.Set(hdrContext, ctxOf("users/c", 2))
		in.Header.Set("Shardlite-Caller", "users/evil")
		in.Header.Set("Shardlite-Hops", "9")
		in.Header.Set("Shardlite-Opened", "users/evil")
		in.Header.Set(peers.HeaderNode, "n9")
		in.Header.Set(peers.HeaderPlacement, peers.PlacementAvoid)
		in.Header.Set("Shardlite-File", "/etc/passwd")
		in = in.WithContext(context.WithValue(in.Context(), ctxKey{}, info))
		pr := &httputil.ProxyRequest{In: in, Out: in.Clone(in.Context())}
		s.rewrite(pr)
		return pr.Out.Header
	}
	call := func(peer string) *reqInfo {
		return &reqInfo{name: shardName("slow/w1"), path: "/data/w1.db", peer: peer, caller: "users/c", callerName: shardName("users/c"), hops: 3}
	}
	app := rewrite(call(""))
	if got := app.Values(hdrContext); len(got) != 1 || got[0] != ctxOf("slow/w1", 3) || app.Get("Shardlite-Caller") != "users/c" || app.Get("Shardlite-Hops") != "" {
		t.Fatalf("the app was sent %v, want the context of the shard it opened and its caller", app)
	}
	if app.Get("Shardlite-Opened") != "slow/w1" || app.Get("Shardlite-File") != "/data/w1.db" || app.Get("Shardlite-Target") != "" || app.Get(peers.HeaderNode) != "" || app.Get(peers.HeaderPlacement) != "" {
		t.Fatalf("the app was sent %v", app)
	}
	fwd := rewrite(call("n1.invalid:1"))
	if fwd.Get("Shardlite-Target") != "slow/w1" || fwd.Get("Shardlite-Opened") != "" || fwd.Get("Shardlite-File") != "" {
		t.Fatalf("the owner's node was sent %v", fwd)
	}
	if got := fwd.Values(hdrContext); len(got) != 1 || got[0] != ctxOf("users/c", 2) || fwd.Get("Shardlite-Caller") != "" || fwd.Get("Shardlite-Hops") != "" {
		t.Fatalf("the owner's node was sent %v, want the call's context and neither the caller nor the hops", fwd)
	}
	// A client's request has no context to carry, whatever the client sent.
	client := call("n1.invalid:1")
	client.public, client.caller, client.callerName, client.hops = true, "", shard.Name{}, 0
	if h := rewrite(client); h.Get(hdrContext) != "" || h.Get("Shardlite-Caller") != "" || h.Get(hdrOrigin) != originClient {
		t.Fatalf("a client's forward was sent %v", h)
	}
	if fwd.Get(peers.HeaderNode) != "n0" || len(fwd.Values(peers.HeaderPlacement)) != 0 {
		t.Fatalf("the owner's node was sent %v, want n0 as the sender and no placement", fwd)
	}
}

// Server-sent events leave the node chunk by chunk.
func TestServerStreamsWithoutBuffering(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/sse")
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
	if !r.m.IsActive(shardName("slow/sse")) {
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
	r.create("slow/ws")
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Upgrade") != "websocket" || q.Header.Get("Shardlite-Opened") != "slow/ws" {
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
	if !r.m.IsActive(shardName("slow/ws")) {
		t.Fatal("an open connection should keep the shard active")
	}
	conn.Close()
	// Once it is closed the shard is free to go.
	apptest.Eventually(t, apptest.Timeout, "the hold to be released", func() bool { return r.refs("slow/ws") == 0 })
}

// The app's 1xx responses (103 Early Hints) are dropped: ReverseProxy would
// write them before the gate and the stripping of Shardlite-* headers, and a
// shard's output must pass the gate before the client sees any of it.
func TestServerDropsInformationalResponses(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/e1")
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Link", "</style.css>; rel=preload")
		w.Header().Set("Shardlite-Secret", "x")
		w.WriteHeader(http.StatusEarlyHints)
		w.Header().Del("Link")
		w.Header().Del("Shardlite-Secret")
		io.WriteString(w, "done")
	})

	var informational atomic.Int32
	trace := &httptrace.ClientTrace{Got1xxResponse: func(int, textproto.MIMEHeader) error {
		informational.Add(1)
		return nil
	}}
	q, _ := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), "GET", r.pub+"/e", nil)
	q.Header.Set("Shardlite-Target", "slow/e1")
	resp, err := r.client.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "done" {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if n := informational.Load(); n != 0 {
		t.Fatalf("the client got %d informational responses", n)
	}
	if resp.Header.Get("Link") != "" || resp.Header.Get("Shardlite-Secret") != "" {
		t.Fatalf("headers of the dropped response were left behind: %v", resp.Header)
	}
}
