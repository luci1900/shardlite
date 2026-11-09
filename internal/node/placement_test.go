package node

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/peers"
	"github.com/luci1900/shardlite/internal/reconcile"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// peerAnswer is what a node's peer listener answered: the 1xx responses it sent
// first and the final one.
type peerAnswer struct {
	status int
	final  http.Header
	early  []http.Header
}

// peerDo sends a request straight to the node's peer listener over h2c, as
// another node would, and reads the whole answer.
func (n *clusterNode) peerDo(method, path string, hdr map[string]string, body string) peerAnswer {
	n.t.Helper()
	protos := new(http.Protocols)
	protos.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: protos}, Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()
	var ans peerAnswer
	var mu sync.Mutex
	ctx := httptrace.WithClientTrace(n.t.Context(), &httptrace.ClientTrace{Got1xxResponse: func(_ int, h textproto.MIMEHeader) error {
		mu.Lock()
		defer mu.Unlock()
		ans.early = append(ans.early, http.Header(h).Clone())
		return nil
	}})
	req, err := http.NewRequestWithContext(ctx, method, "http://"+n.Peer+path, strings.NewReader(body))
	if err != nil {
		n.t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		n.t.Fatal(err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	mu.Lock()
	defer mu.Unlock()
	ans.status, ans.final = resp.StatusCode, resp.Header
	return ans
}

// A draining peer listener says avoid in every answer, the 102 that acknowledges a forward included, and one that isn't draining says nothing about placement. No answer names the node: the forwarder knows whom it forwarded to.
func TestPeerListenerAnswersSayAvoidWhileDraining(t *testing.T) {
	c := newCluster(t, 2)
	n0 := c.nodes[0]
	n0.serve(func(w http.ResponseWriter, q *http.Request) { io.WriteString(w, "served") })
	n0.create("users/a")
	forward := map[string]string{"Shardlite-Origin": originClient, "Shardlite-Target": "users/a"}

	// The routes of the peer listener, and its refusals.
	routes := map[string]struct {
		method, path string
		hdr          map[string]string
		status       int
	}{
		"a forward":                 {"GET", "/x", forward, 200},
		"an activation":             {"POST", reconcile.Activate, map[string]string{"Shardlite-Origin": originNode, "Shardlite-Target": "users/a"}, 204},
		"a client's activation":     {"POST", reconcile.Activate, map[string]string{"Shardlite-Origin": originClient, "Shardlite-Target": "users/a"}, 404},
		"a missing origin":          {"GET", "/x", map[string]string{"Shardlite-Target": "users/a"}, 404},
		"a shard that is not there": {"GET", "/x", map[string]string{"Shardlite-Origin": originClient, "Shardlite-Target": "users/nope"}, 404},
		"a path of the nodes":       {"GET", apiPrefix + "x", map[string]string{"Shardlite-Origin": originClient, "Shardlite-Target": "users/a"}, 404},
	}
	check := func(draining bool) {
		t.Helper()
		for name, tc := range routes {
			got := n0.peerDo(tc.method, tc.path, tc.hdr, "")
			if got.status != tc.status {
				t.Errorf("%s: answered %d, want %d", name, got.status, tc.status)
			}
			answers := map[string]http.Header{"final answer": got.final}
			if name == "a forward" {
				if len(got.early) != 1 {
					t.Fatalf("%d informational answers, want the one 102", len(got.early))
				}
				answers["102"] = got.early[0]
			}
			for which, h := range answers {
				var want []string
				if draining {
					want = []string{peers.PlacementAvoid}
				}
				if got := h.Values(peers.HeaderPlacement); !slices.Equal(got, want) {
					t.Errorf("%s, %s (draining %v): placement %v, want %v", name, which, draining, got, want)
				}
				if v := h.Values(peers.HeaderNode); len(v) != 0 {
					t.Errorf("%s, %s: the answer names its node %v", name, which, v)
				}
			}
		}
	}
	check(false)
	n0.m.Load().Drain()
	check(true)
}

// A header between nodes that does not read is counted and leaves the placer as it was. A correct one in the same traffic is still taken, and so are parameters the node does not know.
func TestPeerListenerCountsBadHeaders(t *testing.T) {
	c := newCluster(t, 2)
	n0 := c.nodes[0]
	st := &n0.m.Load().Stats
	p := n0.m.Load().placer
	// Any request will do: a shard that isn't there is answered 404.
	send := func(hdr map[string]string) {
		t.Helper()
		h := map[string]string{"Shardlite-Origin": originClient, "Shardlite-Target": "users/nope"}
		for k, v := range hdr {
			h[k] = v
		}
		if got := n0.peerDo("GET", "/x", h, ""); got.status != 404 {
			t.Fatalf("%v was answered %d", hdr, got.status)
		}
	}
	send(map[string]string{peers.HeaderNode: "n8", peers.HeaderPlacement: peers.PlacementAvoid + ";for=5;x=?1"})
	if st.BadPeerHeaders.Load() != 0 || !p.Avoids("n8") {
		t.Fatalf("a good pair: %d bad, n8 avoided %v", st.BadPeerHeaders.Load(), p.Avoids("n8"))
	}
	var bad uint64
	for _, hdr := range []map[string]string{
		{peers.HeaderNode: "a b"},
		{peers.HeaderNode: "a/b"},
		{peers.HeaderNode: `"n9"`},
		{peers.HeaderPlacement: "???", peers.HeaderNode: "n9"},
		{peers.HeaderPlacement: "yes", peers.HeaderNode: "n9"},
		{peers.HeaderPlacement: `"avoid"`, peers.HeaderNode: "n9"},
		{peers.HeaderPlacement: "?1", peers.HeaderNode: "n9"},
		{peers.HeaderPlacement: "3", peers.HeaderNode: "n9"},
		{peers.HeaderPlacement: "avoid, avoid", peers.HeaderNode: "n9"},
	} {
		send(hdr)
		bad++
		waitCounter(t, "a bad header", &st.BadPeerHeaders, bad)
	}
	if p.Avoids("n9") {
		t.Fatal("a bad header made the placer avoid n9")
	}
}

// Shardlite-Node and Shardlite-Placement are between nodes. Neither a client's nor the app's request carries one into the placer, the app never sees one and no answer to a client has one, a forwarded answer from a draining node included.
func TestPeerHeadersStayBetweenNodes(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	var mu sync.Mutex
	var appSaw []http.Header
	for _, nd := range c.nodes {
		nd.serve(func(w http.ResponseWriter, q *http.Request) {
			mu.Lock()
			appSaw = append(appSaw, q.Header.Clone())
			mu.Unlock()
			// An app can't pass one on to a client either.
			w.Header().Set(peers.HeaderNode, "n9")
			w.Header().Set(peers.HeaderPlacement, peers.PlacementAvoid)
			io.WriteString(w, "served")
		})
	}
	n0.create("users/a")
	n1.create("users/z")
	n0.m.Load().Drain() // its answers to n1's forwards say avoid
	target := func(shard string) map[string]string {
		return map[string]string{"Shardlite-Target": shard, peers.HeaderNode: "n9", peers.HeaderPlacement: peers.PlacementAvoid}
	}

	type request struct {
		name, url string
		hdr       map[string]string
	}
	for _, rq := range []request{
		{"a client's request served where it arrived", "http://" + n0.Public + "/x", target("users/a")},
		{"a client's request forwarded to a draining node", "http://" + n1.Public + "/x", target("users/a")},
		{"a client's request to a shard of the node", "http://" + n1.Public + "/x", target("users/z")},
		{"a call from the app", "http://" + n1.Control + "/x", target("users/a")},
		{"a client's request with bad headers", "http://" + n0.Public + "/x", map[string]string{"Shardlite-Target": "users/a", peers.HeaderNode: "???", peers.HeaderPlacement: "???"}},
	} {
		resp, body := send(t, "GET", rq.url, rq.hdr, "")
		if resp.StatusCode != 200 || body != "served" {
			t.Fatalf("%s: %d %q", rq.name, resp.StatusCode, body)
		}
		for _, h := range []string{peers.HeaderNode, peers.HeaderPlacement} {
			if _, ok := resp.Header[http.CanonicalHeaderKey(h)]; ok {
				t.Errorf("%s: the answer to the client carries %s %v", rq.name, h, resp.Header.Values(h))
			}
		}
	}
	mu.Lock()
	for i, h := range appSaw {
		for _, name := range []string{peers.HeaderNode, peers.HeaderPlacement} {
			if v := h.Values(name); len(v) > 0 {
				t.Errorf("request %d reached an app with %s %v", i, name, v)
			}
		}
	}
	if len(appSaw) != 5 {
		t.Errorf("the apps saw %d requests, want 5", len(appSaw))
	}
	mu.Unlock()

	// The forwards were answered by a draining node, so n1 avoids it, and no client or app made either node avoid n9.
	if !n1.m.Load().placer.Avoids("n0") {
		t.Error("n1 did not hear that n0 drains")
	}
	for _, nd := range c.nodes {
		if nd.m.Load().placer.Avoids("n9") {
			t.Errorf("%s avoids n9 on a client's or an app's word", nd.env.Name)
		}
		if bad := nd.m.Load().Stats.BadPeerHeaders.Load(); bad != 0 {
			t.Errorf("%s counted %d bad peer headers from a client", nd.env.Name, bad)
		}
	}
}

// serveNamed makes every node's app answer 200 and say which node's it is.
func serveNamed(c *testCluster) {
	for _, nd := range c.nodes {
		name := string(nd.env.Name)
		nd.serve(func(w http.ResponseWriter, q *http.Request) {
			w.Header().Set("X-Served-By", name)
			io.WriteString(w, "served")
		})
	}
}

// newPlacementCluster is a cluster whose lease scans list often, so that every
// node soon knows the others to place on.
func newPlacementCluster(t *testing.T, n int) *testCluster {
	return newClusterWith(t, n, func(o *ShardsetOptions) { o.Leases.Every = 20 * time.Millisecond })
}

// waitForLive waits until the node's lease scan lists n nodes as alive.
func waitForLive(t *testing.T, nd *clusterNode, n int) {
	t.Helper()
	apptest.Eventually(t, apptest.Timeout, "the lease scan to list the cluster", func() bool { return len(nd.m.Load().leases.Nodes().Live()) == n })
}

// callFrom sends a call for target through the node's control API as the app of
// a shard (caller) would. An empty caller is a call with no shard behind it.
func callFrom(t *testing.T, nd *clusterNode, caller, target string) (*http.Response, string) {
	t.Helper()
	hdr := map[string]string{"Shardlite-Target": target}
	if caller != "" {
		hdr[hdrContext] = ctxOf(caller, 0)
	}
	return send(t, "GET", "http://"+nd.Control+"/x", hdr, "")
}

// listOf is a listing with the nodes alive.
func listOf(nodes ...cluster.Node) *peers.List {
	l := &peers.List{}
	for _, n := range nodes {
		l.Nodes = append(l.Nodes, peers.Peer{Listed: lease.Listed{Name: n}})
	}
	return l
}

// placeOn makes the node's placer pick peer, or itself if peer is empty.
func (n *clusterNode) placeOn(peer cluster.Node) {
	p := n.m.Load().placer
	p.Listing = func() *peers.List { return listOf() }
	if peer != "" {
		p.Listing = func() *peers.List { return listOf(peer) }
		p.Draining = new(atomic.Bool) // so that this node is no candidate
		p.Draining.Store(true)
	}
}

// unowned makes a shard that exists and that no node owns or holds: a manifest
// with no owner and no files, as a released shard has.
func (n *clusterNode) unowned(target string) {
	n.t.Helper()
	m := &shardfile.Manifest{Generation: "g", Files: []shardfile.File{}}
	if _, err := shardfile.NewManifestStore(n.m.Load().n.Store).Put(n.t.Context(), shardName(target), m, nil); err != nil {
		n.t.Fatalf("putting the manifest of %s: %v", target, err)
	}
}

// A shard calling a hundred new shards spreads them over the cluster, since a
// call from a shard is placed at random, and nothing is left behind on the calling
// node's account but its share.
func TestCallsFromAShardSpreadNewShards(t *testing.T) {
	c := newPlacementCluster(t, 3)
	serveNamed(c)
	for _, nd := range c.nodes {
		waitForLive(t, nd, 3)
	}
	created := func(nd *clusterNode) int64 { return nd.m.Load().Stats.Creates.Load() }
	before := make([]int64, len(c.nodes))
	for i, nd := range c.nodes {
		before[i] = created(nd)
	}
	served := map[string]int{}
	for i := range 100 {
		resp, body := callFrom(t, c.nodes[0], "users/caller", fmt.Sprintf("users/new%d", i))
		if resp.StatusCode != 200 || body != "served" {
			t.Fatalf("call %d: %d %q", i, resp.StatusCode, body)
		}
		for _, h := range []string{peers.HeaderNode, peers.HeaderPlacement} {
			if _, ok := resp.Header[http.CanonicalHeaderKey(h)]; ok {
				t.Fatalf("call %d: the answer carries %s %v", i, h, resp.Header.Values(h))
			}
		}
		served[resp.Header.Get("X-Served-By")]++
	}
	total := int64(0)
	for i, nd := range c.nodes {
		got := created(nd) - before[i]
		total += got
		if got < 15 || int64(served[string(nd.env.Name)]) != got {
			t.Errorf("%s created %d of 100 shards and its app served %d (an even share is 33)", nd.env.Name, got, served[string(nd.env.Name)])
		}
	}
	if total != 100 {
		t.Errorf("%d shards created, want 100", total)
	}
	if placed := c.nodes[0].m.Load().Stats.Placements.Load(); placed < 15 || placed > 85 {
		t.Errorf("n0 placed %d claims on other nodes", placed)
	}
	for _, nd := range c.nodes[1:] {
		if placed := nd.m.Load().Stats.Placements.Load(); placed != 0 {
			t.Errorf("%s placed %d claims though nothing called through it", nd.env.Name, placed)
		}
	}
}

// What has a session on the node that received it is claimed there: a client's
// request and a call with no shard behind it.
func TestClientRequestsAndShardlessCallsClaimLocally(t *testing.T) {
	c := newPlacementCluster(t, 3)
	serveNamed(c)
	for _, nd := range c.nodes {
		waitForLive(t, nd, 3)
	}
	n0, n1 := c.nodes[0], c.nodes[1]
	m1 := n1.m.Load()
	const each = 15
	for i := range each {
		// Shards that exist and that no node owns: a client's request claims them.
		client, login := fmt.Sprintf("users/c%d", i), fmt.Sprintf("users/l%d", i)
		n0.unowned(client)

		if resp, _ := send(t, "GET", "http://"+n1.Public+"/x", map[string]string{"Shardlite-Target": client}, ""); resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" {
			t.Fatalf("the client's request for %s was served by %q: %d", client, resp.Header.Get("X-Served-By"), resp.StatusCode)
		}
		// A shard that doesn't exist: a login creates the player's account.
		if resp, _ := callFrom(t, n1, "", login); resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" {
			t.Fatalf("the shardless call for %s was served by %q: %d", login, resp.Header.Get("X-Served-By"), resp.StatusCode)
		}
		for _, name := range []string{client, login} {
			if !m1.IsActive(shardName(name)) {
				t.Fatalf("%s is not active on n1", name)
			}
		}
	}
	if placed := m1.Stats.Placements.Load(); placed != 0 {
		t.Fatalf("n1 placed %d claims elsewhere", placed)
	}
}

// A call for a shard another node owns goes to its owner, and is never placed.
func TestCallToAShardOwnedElsewhereIsNotPlaced(t *testing.T) {
	c := newPlacementCluster(t, 3)
	serveNamed(c)
	for _, nd := range c.nodes {
		waitForLive(t, nd, 3)
	}
	n0, n1 := c.nodes[0], c.nodes[1]
	n0.create("users/owned")
	n1.placeOn("n2")   // were it placed, it would go to n2
	for i := range 3 { // the first costs a manifest read, the rest come from the owner cache
		resp, _ := callFrom(t, n1, "users/caller", "users/owned")
		if got := resp.Header.Get("X-Served-By"); resp.StatusCode != 200 || got != "n0" {
			t.Fatalf("call %d was served by %q: %d", i, got, resp.StatusCode)
		}
	}
	if placed := n1.m.Load().Stats.Placements.Load(); placed != 0 {
		t.Fatalf("n1 placed %d claims", placed)
	}
	if n1.m.Load().IsActive(shardName("users/owned")) {
		t.Fatal("n1 activated the shard")
	}
}

// A placement that finds another node has claimed the shard meanwhile is
// answered 503 with Retry-After: 0, naming the owner for the retry.
func TestPlacementThatLosesTheClaimIsRetried(t *testing.T) {
	c := newPlacementCluster(t, 3)
	serveNamed(c)
	for _, nd := range c.nodes {
		waitForLive(t, nd, 3)
	}
	n0, n1, n2 := c.nodes[0], c.nodes[1], c.nodes[2]
	const target = "users/race"

	// B: a call through n2, placed on n0, whose forward waits at n0's read of the manifest.
	n2.placeOn("n0")
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	n0.onStore(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpGet && strings.HasSuffix(key, shardfile.ManifestKey(shardName(target))) {
			once.Do(func() { close(reached); <-release })
		}
		return nil
	})
	type result struct {
		resp *http.Response
		body string
	}
	b := make(chan result, 1)
	go func() {
		resp, body := callFrom(t, n2, "users/caller", target)
		b <- result{resp, body}
	}()
	<-reached

	// A: a call through n1 that claims the shard on n1 while B's forward waits.
	n1.placeOn("")
	if resp, _ := callFrom(t, n1, "users/caller", target); resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" {
		t.Fatalf("A was served by %q: %d", resp.Header.Get("X-Served-By"), resp.StatusCode)
	}
	close(release)

	got := <-b
	if got.resp.StatusCode != http.StatusServiceUnavailable || got.resp.Header.Get("Retry-After") != "0" {
		t.Fatalf("B answered %d with Retry-After %q: %s", got.resp.StatusCode, got.resp.Header.Get("Retry-After"), got.body)
	}
	if owner, ok := n2.m.Load().owners.Get(shardName(target)); !ok || owner != "n1" {
		t.Fatalf("n2 remembers the owner as %q (%v), want n1", owner, ok)
	}
	// The retry reaches the owner.
	if resp, _ := callFrom(t, n2, "users/caller", target); resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" {
		t.Fatalf("the retry was served by %q: %d", resp.Header.Get("X-Served-By"), resp.StatusCode)
	}
	if n0.m.Load().IsActive(shardName(target)) {
		t.Fatal("n0 activated a shard another node had claimed")
	}
}

// The node a call was placed on is remembered, so the next call for the shard
// goes straight to it with no manifest read.
func TestPlacementIsRememberedByTheCaller(t *testing.T) {
	c := newPlacementCluster(t, 2)
	serveNamed(c)
	n0 := c.nodes[0]
	waitForLive(t, n0, 2)
	n0.placeOn("n1")
	if resp, _ := callFrom(t, n0, "users/caller", "users/p"); resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" {
		t.Fatalf("the placed call was served by %q: %d", resp.Header.Get("X-Served-By"), resp.StatusCode)
	}
	if owner, ok := n0.m.Load().owners.Get(shardName("users/p")); !ok || owner != "n1" {
		t.Fatalf("n0 remembers the owner as %q (%v), want n1", owner, ok)
	}
	reads := n0.countReads(shardfile.ManifestPrefix)
	if resp, _ := callFrom(t, n0, "users/caller", "users/p"); resp.Header.Get("X-Served-By") != "n1" {
		t.Fatalf("the next call was served by %q: %d", resp.Header.Get("X-Served-By"), resp.StatusCode)
	}
	if got := reads.Load(); got != 0 {
		t.Fatalf("the next call read %d manifests", got)
	}
}
