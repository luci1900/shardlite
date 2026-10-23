package node

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/shardfile"
)

var clusterClient = &http.Client{Timeout: 20 * time.Second}

// send sends a request and reads the answer.
func send(t *testing.T, method, url string, hdr map[string]string, body string) (*http.Response, string) {
	t.Helper()
	q, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		q.Header.Set(k, v)
	}
	resp, err := clusterClient.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

// A client's request for a shard another node owns is served by the owner's
// app, which sees what it would have seen had the client sent it to the owner.
func TestForwardIsServedByTheOwner(t *testing.T) {
	c := newCluster(t, 2)
	type seen struct {
		node, uri, body string
		hdr             http.Header
	}
	got := make(chan seen, 1)
	for _, nd := range c.nodes {
		name := nd.env.Name
		nd.serve(func(w http.ResponseWriter, q *http.Request) {
			b, _ := io.ReadAll(q.Body)
			// Host and X-Forwarded-Host name the node the client reached, so they differ.
			hdr := maps.Clone(q.Header)
			hdr.Del("X-Forwarded-Host")
			hdr.Del("Accept-Encoding")
			got <- seen{string(name), q.URL.RequestURI(), string(b), hdr}
			w.Header().Set("X-Served-By", string(name))
			io.WriteString(w, "served")
		})
	}
	hdr := map[string]string{
		"Shardlite-Foo": "1", "Shardlite-Caller": "test.users/9", "Shardlite-Hops": "7", "Shardlite-Deadline": "5",
		"Traceparent":     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"X-Forwarded-For": "203.0.113.9", "X-Forwarded-Proto": "https", "Content-Type": "text/plain", "X-Custom": "a",
	}
	ask := func(nd *clusterNode) seen {
		t.Helper()
		resp, body := send(t, "POST", "http://"+nd.Public+"/items?x=1&shardlite-target=test.users/f1&y=2", hdr, "hello")
		if resp.StatusCode != 200 || body != "served" || resp.Header.Get("X-Served-By") != "n0" {
			t.Fatalf("through %s: %d %q from %q", nd.env.Name, resp.StatusCode, body, resp.Header.Get("X-Served-By"))
		}
		return <-got
	}
	c.nodes[0].create("test.users/f1")
	local := ask(c.nodes[0])
	fwd := ask(c.nodes[1])
	if fwd.node != "n0" {
		t.Fatalf("the forward was served by %s", fwd.node)
	}
	if local.uri != "/items?x=1&y=2" || local.body != "hello" || local.hdr.Get("Shardlite-Target") != "test.users/f1" ||
		local.hdr.Get("Shardlite-Hops") != "0" || local.hdr.Get("Shardlite-Foo") != "" || local.hdr.Get("Shardlite-Caller") != "" ||
		local.hdr.Get("Shardlite-Deadline") != "" || local.hdr.Get("X-Forwarded-For") != "203.0.113.9, 127.0.0.1" {
		t.Fatalf("the local request: %+v", local)
	}
	if fwd.uri != local.uri || fwd.body != local.body || fmt.Sprint(fwd.hdr) != fmt.Sprint(local.hdr) {
		t.Fatalf("forwarded:\n%+v\nlocal:\n%+v", fwd, local)
	}
}

// A path that isn't clean reaches the owner's app as it was sent, whether the
// request is forwarded or not. The peer listener must not answer it with a
// redirect to the cleaned path, as a mux would.
func TestForwardKeepsUncleanPaths(t *testing.T) {
	c := newCluster(t, 2)
	got := make(chan string, 1)
	c.nodes[0].serve(func(w http.ResponseWriter, q *http.Request) {
		got <- q.RequestURI
		io.WriteString(w, "served")
	})
	c.nodes[0].create("test.users/f1") // so the request through n1 is a forward
	for _, p := range []string{"/a//b", "/a/../b"} {
		// http.NewRequest keeps the path as it is given, so the client does not clean it.
		for i, nd := range c.nodes {
			resp, body := send(t, "GET", "http://"+nd.Public+p, map[string]string{"Shardlite-Target": "test.users/f1"}, "")
			if resp.StatusCode != 200 || body != "served" {
				t.Fatalf("%s through n%d: %d %q", p, i, resp.StatusCode, body)
			}
			if uri := <-got; uri != p {
				t.Fatalf("%s through n%d: the app saw %q", p, i, uri)
			}
		}
	}
}

// A streamed response comes through a forward chunk by chunk.
func TestForwardStreams(t *testing.T) {
	c := newCluster(t, 2)
	next := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(next) }) }
	var buffered atomic.Bool
	safety := time.AfterFunc(10*time.Second, func() { buffered.Store(true); release() })
	defer safety.Stop()
	c.nodes[0].serve(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path != "/events" {
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 1; i <= 3; i++ {
			fmt.Fprintf(w, "data: %d\n\n", i)
			w.(http.Flusher).Flush()
			select {
			case <-next:
			case <-q.Context().Done():
				return
			}
		}
	})
	c.nodes[0].create("test.users/sse")

	q, _ := http.NewRequest("GET", "http://"+c.nodes[1].Public+"/events", nil)
	q.Header.Set("Shardlite-Target", "test.users/sse")
	resp, err := clusterClient.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	if line, err := br.ReadString('\n'); err != nil || line != "data: 1\n" {
		t.Fatalf("%q %v", line, err)
	}
	if buffered.Load() {
		t.Fatal("the first event came only when the app finished: the forward buffers")
	}
	release()
	if rest, _ := io.ReadAll(br); !strings.Contains(string(rest), "data: 3") {
		t.Fatalf("rest: %q", rest)
	}
}

// cycle runs one flush cycle on the node (whose flushes are manual).
func (n *clusterNode) cycle() { n.m.Load().flush.cycle(context.Background()) }

func (n *clusterNode) cycles() uint64 { return n.m.Load().Flusher().Completed() }

// cycling runs do, with a flush cycle on each of nodes every 20 ms until it returns.
func cycling(do func(), nodes ...*clusterNode) {
	done := make(chan struct{})
	go func() { do(); close(done) }()
	for {
		select {
		case <-done:
			return
		case <-time.After(20 * time.Millisecond):
		}
		for _, nd := range nodes {
			nd.cycle()
		}
	}
}

// A call from a shard with sync on to a shard on another node leaves only once
// what the caller committed is durable on its node, and its answer comes back
// only once what the callee committed is durable on the callee's.
func TestForwardedCallIsGatedOnBothNodes(t *testing.T) {
	c := newClusterWith(t, 2, manualFlush)
	n0, n1 := c.nodes[0], c.nodes[1]
	// Letting go of the shards waits for cycles too.
	t.Cleanup(func() { cycling(func() { n0.Stop(); n1.Stop() }, n0, n1) })
	app := apptest.Start(t)
	paths := make(chan string, 2)
	type arrival struct{ callerCycles, calleeCycles uint64 }
	arrived := make(chan arrival, 1)
	commit := func(path string) { app.Must(t, "insert", path, "1", "64") }
	n0.serve(func(w http.ResponseWriter, q *http.Request) { paths <- q.Header.Get("Shardlite-Path") })
	n1.serve(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path != "/call" {
			paths <- q.Header.Get("Shardlite-Path")
			return
		}
		a := arrival{callerCycles: n0.cycles()}
		if b, _ := io.ReadAll(q.Body); string(b) != "the call" || q.Header.Get("Shardlite-Caller") != "test.wallet/c" {
			http.Error(w, "bad call", 400)
			return
		}
		commit(q.Header.Get("Shardlite-Path"))
		a.calleeCycles = n1.cycles()
		arrived <- a
		io.WriteString(w, "answer")
	})
	var caller, callee string
	cycling(func() {
		send(t, "GET", "http://"+n0.Control+"/", map[string]string{"Shardlite-Target": "test.wallet/c"}, "")
		caller = <-paths
		send(t, "GET", "http://"+n1.Control+"/", map[string]string{"Shardlite-Target": "test.wallet/d"}, "")
		callee = <-paths
	}, n0, n1)
	for _, p := range []string{caller, callee} {
		app.Must(t, "open", p, p)
		defer app.Must(t, "close", p)
	}

	commit(caller)
	before := n0.cycles()
	answered := make(chan string, 1)
	go func() {
		q, _ := http.NewRequest("POST", "http://"+n0.Control+"/call", strings.NewReader("the call"))
		q.Header.Set("Shardlite-Target", "test.wallet/d")
		q.Header.Set("Shardlite-Caller", "test.wallet/c")
		resp, err := clusterClient.Do(q)
		if err != nil {
			answered <- err.Error()
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		answered <- fmt.Sprintf("%d %s", resp.StatusCode, b)
	}()
	var a arrival
	for got := false; !got; {
		select {
		case a = <-arrived:
			got = true
		case got := <-answered:
			t.Fatalf("answered before the call arrived: %s", got)
		case <-time.After(20 * time.Millisecond):
			n0.cycle()
		}
	}
	if a.callerCycles == before {
		t.Fatal("the call reached the callee before the caller's commit was durable")
	}
	for {
		select {
		case got := <-answered:
			if got != "200 answer" {
				t.Fatalf("the call: %s", got)
			}
			if n1.cycles() == a.calleeCycles {
				t.Fatal("the answer came back before the callee's commit was durable")
			}
			return
		case <-time.After(20 * time.Millisecond):
			n1.cycle()
		}
	}
}

// Cancelling a forwarded request ends the owner's request to its app.
func TestForwardIsCancelled(t *testing.T) {
	c := newCluster(t, 2)
	started, ended := make(chan struct{}), make(chan struct{})
	c.nodes[0].serve(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path != "/wait" {
			return
		}
		close(started)
		<-q.Context().Done()
		close(ended)
	})
	c.nodes[0].create("test.users/w")
	ctx, cancel := context.WithCancel(context.Background())
	q, _ := http.NewRequestWithContext(ctx, "GET", "http://"+c.nodes[1].Public+"/wait", nil)
	q.Header.Set("Shardlite-Target", "test.users/w")
	go func() {
		if resp, err := clusterClient.Do(q); err == nil {
			resp.Body.Close()
		}
	}()
	<-started
	cancel()
	select {
	case <-ended:
	case <-time.After(10 * time.Second):
		t.Fatal("the owner's app went on after the client gave up")
	}
}

var clusterH2C = func() *http.Client {
	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetUnencryptedHTTP2(true)
	return &http.Client{Timeout: 20 * time.Second, Transport: tr}
}()

func grpcRequest(t *testing.T, url, target string, hdr map[string]string) *http.Response {
	t.Helper()
	q, _ := http.NewRequest("POST", url, strings.NewReader("request"))
	q.Header.Set("Content-Type", "application/grpc")
	q.Header.Set("Te", "trailers")
	q.Header.Set("Shardlite-Target", target)
	for k, v := range hdr {
		q.Header.Set(k, v)
	}
	resp, err := clusterH2C.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// gRPC through a forward: a unary call and a server stream keep their frames and
// trailers, and the stream isn't buffered.
func TestForwardGRPC(t *testing.T) {
	c := newCluster(t, 2)
	next := make(chan struct{})
	c.nodes[0].serve(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == "/" {
			return
		}
		if q.Header.Get("Te") != "trailers" || q.ProtoMajor != 2 {
			http.Error(w, "bad", 400)
			return
		}
		io.Copy(io.Discard, q.Body)
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Trailer", "Grpc-Status, Grpc-Message")
		w.Write([]byte("frame1"))
		if q.URL.Path == "/svc/Stream" {
			w.(http.Flusher).Flush()
			select {
			case <-next:
			case <-q.Context().Done():
				return
			}
			w.Write([]byte("frame2"))
		}
		w.Header().Set("Grpc-Status", "0")
		w.Header().Set("Grpc-Message", "fine")
	})
	c.nodes[0].create("test.users/rpc")
	through := "http://" + c.nodes[1].Public

	resp := grpcRequest(t, through+"/svc/Unary", "test.users/rpc", nil)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "frame1" || resp.Trailer.Get("Grpc-Status") != "0" || resp.Trailer.Get("Grpc-Message") != "fine" {
		t.Fatalf("unary: %q %v", body, resp.Trailer)
	}

	resp = grpcRequest(t, through+"/svc/Stream", "test.users/rpc", nil)
	defer resp.Body.Close()
	buf := make([]byte, 6)
	if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "frame1" {
		t.Fatalf("%q %v", buf, err)
	}
	close(next) // only once the first frame is here: a forward that buffers never gets it
	if rest, _ := io.ReadAll(resp.Body); string(rest) != "frame2" || resp.Trailer.Get("Grpc-Status") != "0" {
		t.Fatalf("stream: %q %v", rest, resp.Trailer)
	}
}

// A call with Shardlite-If-Active: true for a shard another node owns goes to
// the owner: it is served while the shard is active there and answered 404 by
// the owner once it is only cached, and isn't activated by it.
func TestForwardIfActive(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	app := apptest.Start(t)
	paths := make(chan string, 1)
	n0.serve(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == "/" {
			paths <- q.Header.Get("Shardlite-Path")
		}
		w.Header().Set("X-Served-By", "n0")
	})
	send(t, "GET", "http://"+n0.Control+"/", map[string]string{"Shardlite-Target": "test.brief/x"}, "")
	p := <-paths
	app.Must(t, "open", p, p)
	app.Must(t, "insert", p, "1", "64") // written, so it is cached when it goes idle
	call := map[string]string{"Shardlite-Target": "test.brief/x", "Shardlite-Caller": "app", "Shardlite-If-Active": "true"}
	if resp, _ := send(t, "GET", "http://"+n1.Control+"/presence", call, ""); resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n0" {
		t.Fatalf("If-Active for a shard active on its owner: %d %v", resp.StatusCode, resp.Header)
	}
	app.Must(t, "close", p)
	eventually(t, "the shard to go idle on its owner", func() bool { return !n0.m.Load().IsActive(shardName("test.brief/x")) })
	if cached, err := n0.m.Load().Cached(); err != nil || !slices.Contains(cached, shardName("test.brief/x")) {
		t.Fatalf("the shard is not cached on its owner: %v %v", cached, err)
	}

	arrived := n0.countReads(shardfile.ManifestKey(shardName("test.brief/x"))) // n0 reads it to find the shard isn't active
	if resp, _ := send(t, "GET", "http://"+n1.Control+"/presence", call, ""); resp.StatusCode != http.StatusNotFound || arrived.Load() == 0 {
		t.Fatalf("If-Active for a shard cached on its owner: %d, %d manifest reads on the owner", resp.StatusCode, arrived.Load())
	}
	if n0.m.Load().IsActive(shardName("test.brief/x")) || n1.m.Load().IsActive(shardName("test.brief/x")) {
		t.Fatal("If-Active activated the shard")
	}
}

// upgrade opens a WebSocket-like upgraded connection through addr, with hdr.
func upgrade(t *testing.T, addr string, hdr map[string]string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	q, _ := http.NewRequest("GET", "http://"+addr+"/ws", nil)
	q.Header.Set("Upgrade", "websocket")
	q.Header.Set("Connection", "Upgrade")
	for k, v := range hdr {
		q.Header.Set(k, v)
	}
	if err := q.Write(conn); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	if resp, err := http.ReadResponse(br, q); err != nil || resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade through %s: %v %v", addr, resp, err)
	}
	return conn, br
}

// echoWS answers an upgrade on /ws by greeting, then echoing each line, and
// says on closed when the connection has ended.
func echoWS(closed chan<- string) http.HandlerFunc {
	return func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path != "/ws" {
			return
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		rw.WriteString("hello " + q.Header.Get("Shardlite-Target") + "\n")
		rw.Flush()
		sc := bufio.NewScanner(rw)
		for sc.Scan() {
			rw.WriteString("echo " + sc.Text() + "\n")
			rw.Flush()
		}
		closed <- q.Header.Get("Shardlite-Target")
	}
}

// A WebSocket to a shard another node owns is served by the owner's app, both
// ways. The owner's shard stays active while it is open (past its idle timeout)
// and the forwarding node holds nothing. Closing the client closes the app's
// side, and the shard then goes idle.
func TestForwardUpgrade(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	closed := make(chan string, 1)
	n0.serve(echoWS(closed))
	// n0 claims both and holds them until the upgrade is open: brief's idle
	// timeout is 100 ms, and a shard that nothing has written is deleted when it
	// goes idle, which leaves the client's request nothing to reach.
	var refs []*Hold
	for _, target := range []string{"test.brief/ws", "test.brief/other"} {
		ref, err := n0.m.Load().Acquire(context.Background(), shardName(target))
		if err != nil {
			t.Fatalf("claim %s: %v", target, err)
		}
		refs = append(refs, ref)
	}
	conn, br := upgrade(t, n1.Public, map[string]string{"Shardlite-Target": "test.brief/ws"})
	if line, err := br.ReadString('\n'); err != nil || line != "hello test.brief/ws\n" {
		t.Fatalf("%q %v", line, err)
	}
	for _, ref := range refs { // the open connection holds brief/ws from here
		ref.Release()
	}
	for _, m := range []string{"one", "two"} {
		fmt.Fprintf(conn, "%s\n", m)
		if line, err := br.ReadString('\n'); err != nil || line != "echo "+m+"\n" {
			t.Fatalf("%q %v", line, err)
		}
	}
	// brief/other went idle with the same timeout: brief/ws would have too.
	eventually(t, "test.brief/other to go idle", func() bool { return !n0.m.Load().IsActive(shardName("test.brief/other")) })
	if !n0.m.Load().IsActive(shardName("test.brief/ws")) {
		t.Fatal("the owner let go of a shard with a connection open")
	}
	if n1.m.Load().IsActive(shardName("test.brief/ws")) {
		t.Fatal("the forwarding node activated the shard")
	}
	conn.Close()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("closing the client did not close the app's side")
	}
	eventually(t, "test.brief/ws to go idle", func() bool { return !n0.m.Load().IsActive(shardName("test.brief/ws")) })
}

// Stopping the forwarding node ends a forwarded WebSocket on both sides, as it
// would a local one, and leaves no goroutines behind.
func TestForwardUpgradeEndsWithTheForwardingNode(t *testing.T) {
	before := nodeGoroutines()
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	closed := make(chan string, 1)
	n0.serve(echoWS(closed))
	n0.create("test.users/ws")
	conn, br := upgrade(t, n1.Public, map[string]string{"Shardlite-Target": "test.users/ws"})
	if line, err := br.ReadString('\n'); err != nil || line != "hello test.users/ws\n" {
		t.Fatalf("%q %v", line, err)
	}
	n1.Stop()
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if rest, err := io.ReadAll(br); err != nil || len(rest) > 0 {
		t.Fatalf("the client's connection after the node stopped: %q %v", rest, err)
	}
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("stopping the forwarding node did not close the app's side")
	}
	n0.Stop()
	if left := startedSince(before, nodeGoroutines()); len(left) > 0 {
		t.Fatalf("%d node goroutines left after the cluster stopped:\n%s", len(left), strings.Join(left, "\n\n"))
	}
}
