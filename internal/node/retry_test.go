package node

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
)

// echoes makes the node's app answer every request with its name and the body it got.
func (n *clusterNode) echoes() *atomic.Int32 {
	var served atomic.Int32
	name := n.env.Name
	n.serve(func(w http.ResponseWriter, q *http.Request) {
		b, _ := io.ReadAll(q.Body)
		served.Add(1)
		w.Header().Set("X-Served-By", string(name))
		fmt.Fprintf(w, "%s got %q", name, b)
	})
	return &served
}

// post sends body to target through nd's public listener.
func post(t *testing.T, nd *clusterNode, target, body string) (*http.Response, string) {
	t.Helper()
	return send(t, "POST", "http://"+nd.Public+"/", map[string]string{"Shardlite-Target": target}, body)
}

// moveShard takes a shard from one node to another: the first lets go of it when
// it goes idle (it holds nothing, so it is deleted then) and the second creates
// it again. The shard's kind needs a short idle timeout.
func moveShard(t *testing.T, from, to *clusterNode, target string) {
	t.Helper()
	eventually(t, target+" to go idle on "+string(from.env.Name), func() bool { return !from.m.Load().IsActive(shardName(target)) })
	to.create(target) // it held nothing, so it was deleted when it went idle
	ask(t, to, target)
}

// peer starts a fake node's peer listener (h2c, as the nodes' are) and returns its address.
func peer(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Start()
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// believe makes the node think node owns target and listens at addr, as it
// would after the shard was there once.
func (n *clusterNode) believe(target string, node cluster.Node, addr string) {
	m := n.m.Load()
	m.addrs.mu.Lock()
	if m.addrs.leases == nil {
		m.addrs.leases = map[cluster.Node]*coord.NodeLease{}
	}
	m.addrs.leases[node] = &coord.NodeLease{NodeInfo: coord.NodeInfo{Name: node, Address: addr}, Renewed: time.Now(), TTL: time.Hour}
	m.addrs.mu.Unlock()
	m.owners.Add(shardName(target), node)
}

// moved421 is a peer that has read the whole request, then says the shard is not
// its own and names owner (and sends the header with no value if owner is empty).
func moved421(t *testing.T, owner string) string {
	return peer(t, func(w http.ResponseWriter, q *http.Request) {
		io.ReadAll(q.Body)
		w.Header().Set(hdrOwner, owner)
		w.WriteHeader(http.StatusMisdirectedRequest)
	})
}

// moved checks that a request was answered as one for a shard that moved: 503,
// to be tried again at once.
func moved(t *testing.T, resp *http.Response, body string) {
	t.Helper()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "0" {
		t.Fatalf("%d (Retry-After %q) %q, want 503 with Retry-After: 0", resp.StatusCode, resp.Header.Get("Retry-After"), body)
	}
}

// A shard that moved: the forward to its old owner gets 421 naming the new one,
// the request is answered 503 and the cache names the new owner, where the next
// attempt goes with its body.
func TestStaleOwnerIsCorrected(t *testing.T) {
	c := newCluster(t, 3)
	n0, n1, n2 := c.nodes[0], c.nodes[1], c.nodes[2]
	n0stale := n0.echoes()
	n2.echoes()
	n0.create("test.brief/m")
	post(t, n0, "test.brief/m", "")
	if resp, _ := post(t, n1, "test.brief/m", ""); resp.Header.Get("X-Served-By") != "n0" {
		t.Fatalf("before the move: %v", resp.Header)
	}
	moveShard(t, n0, n2, "test.brief/m")
	before := n0stale.Load()
	resp, body := post(t, n1, "test.brief/m", "hello")
	moved(t, resp, body)
	if owner, _ := n1.m.Load().owners.Get(shardName("test.brief/m")); owner != "n2" {
		t.Fatalf("the cache names %q", owner)
	}
	if resp, body = post(t, n1, "test.brief/m", "hello"); resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n2" || body != `n2 got "hello"` {
		t.Fatalf("after the move: %d %v %q", resp.StatusCode, resp.Header, body)
	}
	if n0stale.Load() != before {
		t.Fatal("the old owner's app saw a request")
	}
}

// A 421 that names no owner: the entry goes, and the next attempt asks for
// itself, finding the owner the manifest names.
func TestMovedWithoutAnOwner(t *testing.T) {
	c := newCluster(t, 3)
	n1, n2 := c.nodes[1], c.nodes[2]
	n2.echoes()
	n2.create("test.users/w")
	ask(t, n2, "test.users/w")
	n1.believe("test.users/w", "ghost", moved421(t, ""))
	resp, body := post(t, n1, "test.users/w", "hi")
	moved(t, resp, body)
	if owner, ok := n1.m.Load().owners.Get(shardName("test.users/w")); ok {
		t.Fatalf("the cache still names %q", owner)
	}
	if resp, body = post(t, n1, "test.users/w", "hi"); resp.StatusCode != 200 || body != `n2 got "hi"` {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if owner, _ := n1.m.Load().owners.Get(shardName("test.users/w")); owner != "n2" {
		t.Fatalf("the cache names %q", owner)
	}
}

// A stale entry for a shard this node owns: the old owner's 421 names this
// node, the entry goes and the next attempt is served here.
func TestStaleEntryNamingThisNode(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	stale := n0.echoes()
	n1.echoes()
	n1.create("test.users/t")
	ask(t, n1, "test.users/t")
	n1.m.Load().owners.Add(shardName("test.users/t"), "n0")
	resp, body := post(t, n1, "test.users/t", "mine")
	moved(t, resp, body)
	if owner, ok := n1.m.Load().owners.Get(shardName("test.users/t")); ok {
		t.Fatalf("the cache still names %q", owner)
	}
	if resp, body = post(t, n1, "test.users/t", "mine"); resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" || body != `n1 got "mine"` {
		t.Fatalf("%d %v %q", resp.StatusCode, resp.Header, body)
	}
	if stale.Load() != 0 {
		t.Fatal("the other node's app saw the request")
	}
}
