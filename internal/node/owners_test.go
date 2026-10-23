package node

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// countReads counts the node's reads of objects whose keys contain part.
func (n *clusterNode) countReads(part string) *atomic.Int32 {
	var reads atomic.Int32
	n.onStore(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpGet && strings.Contains(key, part) {
			reads.Add(1)
		}
		return nil
	})
	return &reads
}

func ask(t *testing.T, nd *clusterNode, target string) {
	t.Helper()
	if resp, body := send(t, "GET", "http://"+nd.Public+"/", map[string]string{"Shardlite-Target": target}, ""); resp.StatusCode != 200 {
		t.Fatalf("%s through %s: %d %q", target, nd.env.Name, resp.StatusCode, body)
	}
}

// A request for a shard another node owns costs a manifest read and starts no
// activation. The next one is forwarded from the owner cache, reading nothing.
func TestMissForAShardOwnedElsewhereReadsOneManifest(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	n0.create("test.users/o")
	ask(t, n0, "test.users/o")
	reads := n1.countReads(shardfile.ManifestPrefix)

	ask(t, n1, "test.users/o")
	if got := reads.Load(); got != 1 {
		t.Fatalf("the miss read %d manifests", got)
	}
	m := n1.m.Load()
	// (A system shard may be active here: the claimer races the other node's for it.)
	for _, name := range m.Active() {
		if !strings.HasPrefix(name.String(), "_") {
			t.Fatalf("the miss started an activation: %v", m.Active())
		}
	}
	if m.tab.load(shardName("test.users/o")) != nil {
		t.Fatalf("the miss started an activation: %v", m.Active())
	}
	if owner, ok := m.owners.Get(shardName("test.users/o")); !ok || owner != "n0" {
		t.Fatalf("the cache names %q (%v)", owner, ok)
	}
	ask(t, n1, "test.users/o")
	ask(t, n1, "test.users/o")
	if got := reads.Load(); got != 1 {
		t.Fatalf("%d manifest reads after the cache was filled", got)
	}
}

// With room for one owner the cache forgets the other shard's, and both are
// still served.
func TestOwnerCacheOfOne(t *testing.T) {
	c := newClusterWith(t, 2, func(o *CatalogOptions) { o.OwnerCache = 1 })
	n0, n1 := c.nodes[0], c.nodes[1]
	n0.create("test.users/a")
	ask(t, n0, "test.users/a")
	n0.create("test.users/b")
	ask(t, n0, "test.users/b")
	reads := n1.countReads(shardfile.ManifestPrefix)
	for _, target := range []string{"test.users/a", "test.users/b", "test.users/a", "test.users/b"} {
		ask(t, n1, target)
	}
	if got := reads.Load(); got != 4 {
		t.Fatalf("%d manifest reads for four alternating requests with room for one owner", got)
	}
	if got := n1.m.Load().owners.Len(); got != 1 {
		t.Fatalf("the cache holds %d owners", got)
	}
}

// A node that claims a shard forgets who it believed owned it.
func TestClaimingForgetsTheOwner(t *testing.T) {
	c := newCluster(t, 1)
	m := c.nodes[0].m.Load()
	m.owners.Add(shardName("test.users/c"), "n9")
	ref, err := m.Acquire(context.Background(), shardName("test.users/c"))
	if err != nil {
		t.Fatal(err)
	}
	ref.Release()
	if _, ok := m.owners.Get(shardName("test.users/c")); ok {
		t.Fatal("the cache still names an owner for a shard this node claimed")
	}
}

// An owner that stops releases its lease. Once the lease scan sees it, the node
// forgets every owner cache entry naming it, so the next request for one of its
// shards claims the shard here, with no answer from the stopped node first.
func TestStoppedOwnersEntriesAreForgotten(t *testing.T) {
	c := newClusterWith(t, 2, func(o *CatalogOptions) { o.Leases.Every = 50 * time.Millisecond })
	n0, n1 := c.nodes[0], c.nodes[1]
	n1.echoes()
	for _, shard := range []string{"test.users/g", "test.users/h"} {
		n0.create(shard)
		ask(t, n0, shard)
		ask(t, n1, shard) // cached as n0's on n1
	}
	n0.Stop()
	eventually(t, "n1 to forget n0's shards", func() bool { return n1.m.Load().owners.Len() == 0 })
	for _, shard := range []string{"test.users/g", "test.users/h"} {
		n1.create(shard) // n0's held nothing, so they were deleted when it stopped
		if resp, body := post(t, n1, shard, ""); resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" {
			t.Fatalf("%s: %d %q", shard, resp.StatusCode, body)
		}
	}
}

// The address cache reads a lease once, and again when the node is forgotten or
// the listing shows it wrote its lease after the cached one would have expired.
func TestAddressCache(t *testing.T) {
	now := time.Now()
	var reads int
	address := "a:1"
	var list atomic.Pointer[NodeList]
	a := &addresses{
		get: func(_ context.Context, node cluster.Node) (*coord.NodeLease, error) {
			reads++
			if node == "gone" {
				return nil, blob.ErrNotFound
			}
			return &coord.NodeLease{NodeInfo: coord.NodeInfo{Name: node, Address: address}, Renewed: now, TTL: 10 * time.Second}, nil
		},
		nodes: list.Load,
	}
	ctx := context.Background()
	want := func(addr string, reads_ int) {
		t.Helper()
		got, err := a.address(ctx, "n1")
		if err != nil || got != addr || reads != reads_ {
			t.Fatalf("address %q (%v) after %d reads, want %q after %d", got, err, reads, addr, reads_)
		}
	}
	want("a:1", 1)
	want("a:1", 1)
	// A renewal: written before the cached lease would have expired.
	list.Store(&NodeList{Nodes: []NodeView{{Listed: coord.Listed{Name: "n0"}}, {Listed: coord.Listed{Name: "n1", LastModified: now.Add(5 * time.Second)}}}})
	want("a:1", 1)
	// The node was gone for longer than its TTL and came back elsewhere.
	address = "b:2"
	list.Store(&NodeList{Nodes: []NodeView{{Listed: coord.Listed{Name: "n1", LastModified: now.Add(30 * time.Second)}}}})
	want("b:2", 2)
	address = "c:3"
	a.set("n1", nil)
	list.Store(nil)
	want("c:3", 3)
	// A lease read by the owner check replaces the cached one.
	a.set("n1", &coord.NodeLease{NodeInfo: coord.NodeInfo{Name: "n1", Address: "d:4"}, Renewed: now, TTL: 10 * time.Second})
	want("d:4", 3)
	if _, err := a.address(ctx, "gone"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a node with no lease: %v", err)
	}
}
