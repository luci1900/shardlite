package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/luci1900/shardlite/internal/apptest"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/luci1900/shardlite/internal/blob"
)

// A relay is a client's request the app forwards through the control API,
// marked Shardlite-Relay: true. The node serves it with the client's limits, so
// relaying never widens what a client can reach.

// relayed is the headers of a relay of a client's request for target.
func relayed(target string, more ...string) map[string]string {
	hdr := map[string]string{"Shardlite-Target": target, "Shardlite-Relay": "true"}
	for i := 0; i < len(more); i += 2 {
		hdr[more[i]] = more[i+1]
	}
	return hdr
}

// A relay for a kind clients can't name gets the answer a client gets, and the
// app never sees it. The same request as a call is served.
func TestRelayOfAKindClientsCannotNameIsRefused(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("test.audit/1")
	var served atomic.Int64
	r.app_(func(w http.ResponseWriter, q *http.Request) { served.Add(1); io.WriteString(w, "served") })
	// Creating the shard leaves it active, so the refused relays must not change that.
	wasActive := r.m.IsActive(shardName("test.audit/1"))

	_, undeclared := r.req("GET", r.ctl, "/x", relayed("test.nosuch/1"), "")
	_, byClient := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "test.audit/1"}, "")
	resp, body := r.req("GET", r.ctl, "/x", relayed("test.audit/1"), "")
	if resp.StatusCode != http.StatusNotFound || strings.ReplaceAll(body, "test.audit", "test.nosuch") != undeclared || body != byClient {
		t.Fatalf("a relay for a kind that is not public: %d %q, undeclared: %q, a client's: %q", resp.StatusCode, body, undeclared, byClient)
	}
	// By the query parameter, and as gRPC.
	if resp, _ := r.req("GET", r.ctl, "/x?shardlite-target=test.audit/1", map[string]string{"Shardlite-Relay": "true"}, ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("by query parameter: %d", resp.StatusCode)
	}
	g, _ := r.req("POST", r.ctl, "/svc/M", relayed("test.audit/1", "Content-Type", "application/grpc"), "")
	if g.StatusCode != http.StatusOK || g.Header.Get("Grpc-Status") != "5" {
		t.Fatalf("gRPC: %d grpc-status %q", g.StatusCode, g.Header.Get("Grpc-Status"))
	}
	if served.Load() != 0 || r.m.IsActive(shardName("test.audit/1")) != wasActive {
		t.Fatalf("%d requests reached the app, shard active %v, was %v", served.Load(), r.m.IsActive(shardName("test.audit/1")), wasActive)
	}
	// Without the mark it is a call, and the app serves it.
	if resp, body := r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "test.audit/1"}, ""); resp.StatusCode != http.StatusOK || body != "served" {
		t.Fatalf("a call: %d %q", resp.StatusCode, body)
	}
}

// A relay never creates a shard: for one that doesn't exist the node answers as
// for an undeclared kind, writing nothing and not asking the app. A call creates
// it, and the relay then reaches it.
func TestRelayDoesNotCreate(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var served atomic.Int64
	r.app_(func(w http.ResponseWriter, q *http.Request) { served.Add(1); io.WriteString(w, "served") })
	const target = "test.slow/rn1"

	writes := r.shardWrites()
	_, undeclared := r.req("GET", r.ctl, "/x", relayed("test.nosuch/1"), "")
	resp, body := r.req("GET", r.ctl, "/x", relayed(target), "")
	if resp.StatusCode != http.StatusNotFound || strings.ReplaceAll(body, "test.slow", "test.nosuch") != undeclared {
		t.Fatalf("a relay for a missing shard: %d %q, undeclared: %q", resp.StatusCode, body, undeclared)
	}
	g, _ := r.req("POST", r.ctl, "/svc/M", relayed(target, "Content-Type", "application/grpc"), "")
	if g.Header.Get("Grpc-Status") != "5" {
		t.Fatalf("gRPC: %v", g.Header)
	}
	if got := r.shardWrites() - writes; got != 0 || served.Load() != 0 || r.m.IsActive(shardName(target)) {
		t.Fatalf("%d writes, %d requests to the app, active %v", got, served.Load(), r.m.IsActive(shardName(target)))
	}
	if _, err := r.manifest(target); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest after the relay: %v", err)
	}
	// The options of a call mean nothing to a relay, which never creates.
	if resp, _ := r.req("GET", r.ctl, "/x", relayed(target, "Shardlite-If-Exists", "false"), ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("a relay with Shardlite-If-Exists: false: %d", resp.StatusCode)
	}

	if resp, body := r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": target}, ""); resp.StatusCode != http.StatusOK || body != "served" {
		t.Fatalf("a call: %d %q", resp.StatusCode, body)
	}
	if resp, body := r.req("GET", r.ctl, "/x", relayed(target), ""); resp.StatusCode != http.StatusOK || body != "served" {
		t.Fatalf("a relay after the call: %d %q", resp.StatusCode, body)
	}
}

// The app sees a relay as it would the client's request: opened, with no caller
// and none of the relay's own headers, hops one further on, and the
// X-Forwarded-* headers as the app sent them.
func TestRelayReachesTheAppAsAClientRequest(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("test.slow/rr1")
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		last.Store(q.Clone(context.Background()))
		io.WriteString(w, "served")
	})
	resp, body := r.req("POST", r.ctl, "/items?x=1", relayed("test.slow/rr1",
		"Shardlite-Hops", "4", "Shardlite-If-Active", "true", "Shardlite-Sync", "storage", "Idempotency-Key", "k1",
		"X-Forwarded-For", "203.0.113.9, 198.51.100.7", "X-Forwarded-Proto", "https", "X-Forwarded-Host", "app.example.com"), "hello")
	q := last.Load()
	if resp.StatusCode != http.StatusOK || body != "served" || q == nil {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if q.Header.Get("Shardlite-Opened") != "test.slow/rr1" || q.Header.Get("Shardlite-File") == "" {
		t.Fatalf("the app was not told what the node opened: %v", q.Header)
	}
	if _, has := q.Header["Shardlite-Caller"]; has {
		t.Fatalf("a relay reached the app with Shardlite-Caller %q", q.Header.Get("Shardlite-Caller"))
	}
	for k := range q.Header {
		if strings.HasPrefix(strings.ToLower(k), "shardlite") && k != "Shardlite-Opened" && k != "Shardlite-File" && k != "Shardlite-Hops" {
			t.Fatalf("%s: %v reached the app", k, q.Header[k])
		}
	}
	if q.Header.Get("Shardlite-Hops") != "5" || q.Header.Get("Idempotency-Key") != "k1" || q.URL.RequestURI() != "/items?x=1" {
		t.Fatalf("hops %q, key %q, uri %q", q.Header.Get("Shardlite-Hops"), q.Header.Get("Idempotency-Key"), q.URL.RequestURI())
	}
	// The app is trusted and the control API is local, so the chain is not extended.
	if q.Header.Get("X-Forwarded-For") != "203.0.113.9, 198.51.100.7" || q.Header.Get("X-Forwarded-Proto") != "https" || q.Header.Get("X-Forwarded-Host") != "app.example.com" {
		t.Fatalf("X-Forwarded-*: %q %q %q", q.Header.Get("X-Forwarded-For"), q.Header.Get("X-Forwarded-Proto"), q.Header.Get("X-Forwarded-Host"))
	}
	// With none sent, none is made up.
	r.req("GET", r.ctl, "/x", relayed("test.slow/rr1"), "")
	if q := last.Load(); q.Header.Get("X-Forwarded-For") != "" || q.Header.Get("X-Forwarded-Proto") != "" || q.Header.Get("X-Forwarded-Host") != "" {
		t.Fatalf("X-Forwarded-* with none sent: %v", q.Header)
	}
}

// A relay has no caller, so one that names one is refused before anything is
// activated, and so is a mark that is not "true": reading it as a call would
// widen what the request can reach.
func TestRelayWithACallerOrABadMarkIsRefused(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("test.slow/rc1", "test.users/9")
	var served atomic.Int64
	r.app_(func(w http.ResponseWriter, q *http.Request) { served.Add(1) })
	for _, tc := range []struct {
		name string
		hdr  map[string]string
	}{
		{"caller app", relayed("test.slow/rc1", "Shardlite-Caller", "app")},
		{"caller shard", relayed("test.slow/rc1", "Shardlite-Caller", "test.users/9")},
		{"bad caller", relayed("test.slow/rc1", "Shardlite-Caller", "nokind")},
		{"mark yes", map[string]string{"Shardlite-Target": "test.slow/rc1", "Shardlite-Relay": "yes"}},
		{"mark false", map[string]string{"Shardlite-Target": "test.slow/rc1", "Shardlite-Relay": "false"}},
		{"no target", map[string]string{"Shardlite-Relay": "true"}},
	} {
		if resp, _ := r.req("GET", r.ctl, "/x", tc.hdr, ""); resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", tc.name, resp.StatusCode)
		}
	}
	g, _ := r.req("POST", r.ctl, "/svc/M", relayed("test.slow/rc1", "Shardlite-Caller", "app", "Content-Type", "application/grpc"), "")
	if g.Header.Get("Grpc-Status") != "3" {
		t.Errorf("gRPC: %v", g.Header)
	}
	if served.Load() != 0 {
		t.Fatalf("%d refused relays reached the app", served.Load())
	}
}

// A relay counts hops as a call does: one more than the app says it is serving,
// refused past the limit with 508.
func TestRelayCountsHops(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("test.slow/rh1")
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) { last.Store(q.Clone(context.Background())) })
	for _, tc := range []struct {
		sent, seen string
	}{{"", "0"}, {"0", "1"}, {"4", "5"}, {strconv.Itoa(DefaultMaxHops - 1), strconv.Itoa(DefaultMaxHops)}} {
		hdr := relayed("test.slow/rh1")
		if tc.sent != "" {
			hdr["Shardlite-Hops"] = tc.sent
		}
		resp, _ := r.req("GET", r.ctl, "/x", hdr, "")
		if q := last.Load(); resp.StatusCode != http.StatusOK || q.Header.Get("Shardlite-Hops") != tc.seen {
			t.Fatalf("a relay sent with hops %q: %d, the app saw %q, want %q", tc.sent, resp.StatusCode, q.Header.Get("Shardlite-Hops"), tc.seen)
		}
	}
	last.Store(nil)
	if resp, _ := r.req("GET", r.ctl, "/x", relayed("test.slow/rh1", "Shardlite-Hops", strconv.Itoa(DefaultMaxHops)), ""); resp.StatusCode != 508 {
		t.Fatalf("a relay past the limit: %d", resp.StatusCode)
	}
	g, _ := r.req("POST", r.ctl, "/svc/M", relayed("test.slow/rh1", "Shardlite-Hops", strconv.Itoa(DefaultMaxHops), "Content-Type", "application/grpc"), "")
	if g.Header.Get("Grpc-Status") != "8" {
		t.Fatalf("gRPC: %v", g.Header)
	}
	if last.Load() != nil {
		t.Fatal("a relay past the limit reached the app")
	}
	if resp, _ := r.req("GET", r.ctl, "/x", relayed("test.slow/rh1", "Shardlite-Hops", "x"), ""); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad hops: %d", resp.StatusCode)
	}
}

// Apps relaying to each other stop at the limit, as shards calling each other do.
func TestRelayLoopStopsAtTheHopLimit(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("test.slow/la", "test.slow/lb")
	var invocations, deepest atomic.Int64
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		invocations.Add(1)
		d, _ := strconv.Atoi(q.Header.Get("Shardlite-Hops"))
		deepest.Store(max(deepest.Load(), int64(d)))
		other := map[string]string{"test.slow/la": "test.slow/lb", "test.slow/lb": "test.slow/la"}[q.Header.Get("Shardlite-Opened")]
		c, _ := http.NewRequest("POST", r.ctl+"/ping", nil)
		c.Header.Set("Shardlite-Target", other)
		c.Header.Set("Shardlite-Relay", "true")
		c.Header.Set("Shardlite-Hops", q.Header.Get("Shardlite-Hops"))
		resp, err := r.client.Do(c)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
	})
	resp, _ := r.req("POST", r.pub, "/ping", map[string]string{"Shardlite-Target": "test.slow/la"}, "")
	if resp.StatusCode != 508 {
		t.Fatalf("a relay loop ended with %d, want 508", resp.StatusCode)
	}
	// Hops 0 (the client's request) to the limit ran, and the relay after it was refused.
	if invocations.Load() != DefaultMaxHops+1 || deepest.Load() != DefaultMaxHops {
		t.Fatalf("the app ran %d times, deepest %d", invocations.Load(), deepest.Load())
	}
}

// A client's WebSocket can be relayed: the upgrade goes through the control API
// and the app's side of it sees no caller. The connection keeps the shard active
// while it is open.
func TestRelayedUpgrade(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("test.slow/rw")
	seen := make(chan http.Header, 4)
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		seen <- q.Header.Clone()
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
	})
	open := func(extra string) (net.Conn, *http.Response, *bufio.Reader) {
		t.Helper()
		conn, err := net.Dial("tcp", strings.TrimPrefix(r.ctl, "http://"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nShardlite-Target: test.slow/rw\r\nShardlite-Relay: true\r\n%sUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n", extra)
		br := bufio.NewReader(conn)
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		return conn, resp, br
	}

	// A caller is refused, upgrade or not.
	if _, resp, _ := open("Shardlite-Caller: test.slow/rw\r\n"); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("a relayed upgrade with a caller: %d", resp.StatusCode)
	}
	conn, resp, br := open("")
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("a relayed upgrade: %d", resp.StatusCode)
	}
	h := <-seen
	if h.Get("Shardlite-Opened") != "test.slow/rw" || h.Get("Upgrade") != "websocket" || h.Get("Shardlite-Caller") != "" || h.Get("Shardlite-Relay") != "" {
		t.Fatalf("the app saw %v", h)
	}
	for _, m := range []string{"one", "two"} {
		fmt.Fprintf(conn, "%s\n", m)
		if line, err := br.ReadString('\n'); err != nil || line != "echo "+m+"\n" {
			t.Fatalf("%q %v", line, err)
		}
	}
	if !r.m.IsActive(shardName("test.slow/rw")) {
		t.Fatal("an open connection should keep the shard active")
	}
	conn.Close()
	apptest.Eventually(t, apptest.Timeout, "the hold to be released", func() bool { return r.refs("test.slow/rw") == 0 })
}

// Shardlite-Relay is the app's to send on the control API. From a client it is
// stripped like any Shardlite-* header and changes nothing: the request is the
// client's as it was, and its Shardlite-Hops is still ignored.
func TestClientsRelayHeaderIsStripped(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("test.slow/rs1")
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		last.Store(q.Clone(context.Background()))
		io.WriteString(w, "ok")
	})
	resp, _ := r.req("GET", r.pub, "/x", map[string]string{
		"Shardlite-Target": "test.slow/rs1", "Shardlite-Relay": "true", "Shardlite_Relay": "true", "SHARDLITE-RELAY": "true", "Shardlite-Hops": "20",
	}, "")
	q := last.Load()
	if resp.StatusCode != http.StatusOK || q == nil {
		t.Fatalf("%d", resp.StatusCode)
	}
	for k := range q.Header {
		if strings.Contains(strings.ToLower(k), "relay") {
			t.Fatalf("%s: %v reached the app", k, q.Header[k])
		}
	}
	// A relay would have counted the client's hops.
	if q.Header.Get("Shardlite-Hops") != "0" {
		t.Fatalf("the app saw hops %q", q.Header.Get("Shardlite-Hops"))
	}
	// A bad value from a client is no more an error than any other Shardlite-* header it sends.
	if resp, _ := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "test.slow/rs1", "Shardlite-Relay": "yes"}, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("a client's bad Shardlite-Relay: %d", resp.StatusCode)
	}
	// The mark is no way to create a shard or reach a private kind on the public listener.
	for _, target := range []string{"test.slow/rs-missing", "test.audit/1"} {
		if resp, _ := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": target, "Shardlite-Relay": "true"}, ""); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s with the mark from a client: %d", target, resp.StatusCode)
		}
	}
}

// A relay forwarded to the owner's node is served there as a relay: the owner
// holds it to the client's limits too, sees no caller, takes the hops the first
// node counted and keeps the X-Forwarded-* headers the app sent.
func TestForwardedRelayKeepsTheClientRules(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	type seen struct {
		node string
		hdr  http.Header
	}
	got := make(chan seen, 4)
	for _, nd := range c.nodes {
		name := string(nd.env.Name)
		nd.serve(func(w http.ResponseWriter, q *http.Request) {
			got <- seen{name, q.Header.Clone()}
			io.WriteString(w, "served")
		})
	}
	n0.create("test.users/r1")
	n0.create("test.audit/a1")
	missing := func(node *clusterNode, target string) bool {
		_, _, err := node.m.Load().n.Files.GetManifest(context.Background(), shardName(target))
		return errors.Is(err, blob.ErrNotFound)
	}

	// Through n1 to the owner n0.
	resp, body := send(t, "POST", "http://"+n1.Control+"/items?x=1", relayed("test.users/r1",
		"Shardlite-Hops", "2", "Shardlite-If-Active", "true", "Shardlite-If-Exists", "true", "X-Forwarded-For", "203.0.113.9", "X-Forwarded-Proto", "https"), "hello")
	if resp.StatusCode != http.StatusOK || body != "served" {
		t.Fatalf("a relay through n1: %d %q", resp.StatusCode, body)
	}
	s := <-got
	if s.node != "n0" || s.hdr.Get("Shardlite-Opened") != "test.users/r1" || s.hdr.Get("Shardlite-Hops") != "3" {
		t.Fatalf("the owner's app saw (on %s) %v", s.node, s.hdr)
	}
	if _, has := s.hdr["Shardlite-Caller"]; has || s.hdr.Get("Shardlite-Relay") != "" || s.hdr.Get("Shardlite-Origin") != "" {
		t.Fatalf("the owner's app saw %v", s.hdr)
	}
	if s.hdr.Get("X-Forwarded-For") != "203.0.113.9" || s.hdr.Get("X-Forwarded-Proto") != "https" {
		t.Fatalf("X-Forwarded-*: %q %q", s.hdr.Get("X-Forwarded-For"), s.hdr.Get("X-Forwarded-Proto"))
	}

	// The first node refuses what a client may not reach, and makes no forward.
	if len(got) != 0 {
		t.Fatalf("%d requests queued", len(got))
	}
	for _, target := range []string{"test.audit/a1", "test.users/none"} {
		if resp, _ := send(t, "GET", "http://"+n1.Control+"/x", relayed(target), ""); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("a relay for %s through n1: %d", target, resp.StatusCode)
		}
	}
	if len(got) != 0 {
		t.Fatalf("a refused relay reached an app")
	}

	// The owner holds a forwarded relay to the client rules itself: asked on its
	// peer listener, as the first node would, it refuses the same things, and a
	// forwarded call for the same targets is served (and creates).
	h2c := new(http.Protocols)
	h2c.SetUnencryptedHTTP2(true)
	peer := &http.Client{Transport: &http.Transport{Protocols: h2c}}
	forwardedAs := func(kind, target string, more ...string) int {
		t.Helper()
		q, _ := http.NewRequest(http.MethodGet, "http://"+n0.Peer+"/x", nil)
		q.Header.Set(hdrTarget, target)
		q.Header.Set(hdrOrigin, kind)
		for i := 0; i < len(more); i += 2 {
			q.Header.Set(more[i], more[i+1])
		}
		resp, err := peer.Do(q)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	for _, target := range []string{"test.audit/a1", "test.users/none"} {
		if st := forwardedAs(originRelay, target); st != http.StatusNotFound {
			t.Fatalf("a relay forwarded to the owner for %s: %d", target, st)
		}
	}
	if st := forwardedAs(originRelay, "test.users/r1", hdrCaller, "app"); st != http.StatusBadRequest {
		t.Fatalf("a relay forwarded with a caller: %d", st)
	}
	if st := forwardedAs(originRelay, "test.users/r1"); st != http.StatusOK {
		t.Fatalf("a relay forwarded to the owner for an existing shard: %d", st)
	}
	if len(got) != 1 { // only the last one reached an app
		t.Fatalf("%d requests reached an app", len(got))
	}
	<-got
	if !missing(n0, "test.users/none") {
		t.Fatal("a relay created a shard")
	}
	if st := forwardedAs(originCall, "test.users/none"); st != http.StatusOK || missing(n0, "test.users/none") {
		t.Fatalf("a forwarded call for the same shard: %d, still missing %v", st, missing(n0, "test.users/none"))
	}
}
