package node

import (
	"context"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/storage/shardfile"
	"github.com/luci1900/shardlite/internal/test/blobtest"
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
	n0.create("users/o")
	ask(t, n0, "users/o")
	reads := n1.countReads(shardfile.ManifestPrefix)

	ask(t, n1, "users/o")
	if got := reads.Load(); got != 1 {
		t.Fatalf("the miss read %d manifests", got)
	}
	m := n1.m.Load()
	if m.tab.load(shardName("users/o")) != nil {
		t.Fatalf("the miss started an activation: %v", m.Active())
	}
	if owner, ok := m.owners.Get(shardName("users/o")); !ok || owner != "n0" {
		t.Fatalf("the cache names %q (%v)", owner, ok)
	}
	ask(t, n1, "users/o")
	ask(t, n1, "users/o")
	if got := reads.Load(); got != 1 {
		t.Fatalf("%d manifest reads after the cache was filled", got)
	}
}

// With room for one owner the cache forgets the other shard's, and both are
// still served.
func TestOwnerCacheOfOne(t *testing.T) {
	c := newClusterWith(t, 2, func(o *ShardsetOptions) { o.OwnerCache = 1 })
	n0, n1 := c.nodes[0], c.nodes[1]
	n0.create("users/a")
	ask(t, n0, "users/a")
	n0.create("users/b")
	ask(t, n0, "users/b")
	reads := n1.countReads(shardfile.ManifestPrefix)
	for _, target := range []string{"users/a", "users/b", "users/a", "users/b"} {
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
	m.owners.Add(shardName("users/c"), "n9")
	ref, err := m.Acquire(context.Background(), shardName("users/c"))
	if err != nil {
		t.Fatal(err)
	}
	ref.Release()
	if _, ok := m.owners.Get(shardName("users/c")); ok {
		t.Fatal("the cache still names an owner for a shard this node claimed")
	}
}

// An owner that stops releases its lease. Once the lease scan sees it, the node
// forgets every owner cache entry naming it, so the next request for one of its
// shards claims the shard here, with no answer from the stopped node first.
func TestStoppedOwnersEntriesAreForgotten(t *testing.T) {
	c := newClusterWith(t, 2, func(o *ShardsetOptions) { o.Leases.Every = 50 * time.Millisecond })
	n0, n1 := c.nodes[0], c.nodes[1]
	n1.echoes()
	for _, shard := range []string{"users/g", "users/h"} {
		n0.create(shard)
		ask(t, n0, shard)
		ask(t, n1, shard) // cached as n0's on n1
	}
	n0.Stop()
	apptest.Eventually(t, apptest.Timeout, "n1 to forget n0's shards", func() bool { return n1.m.Load().owners.Len() == 0 })
	for _, shard := range []string{"users/g", "users/h"} {
		n1.create(shard) // n0's held nothing, so they were deleted when it stopped
		if resp, body := post(t, n1, shard, ""); resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" {
			t.Fatalf("%s: %d %q", shard, resp.StatusCode, body)
		}
	}
}

// A request forwarded on an owner cache entry that is stale because the owner let the shard go when it went idle is answered 503 with Retry-After 0, and the client's retry claims the shard at the node it asked, where its session is.
func TestStaleOwnerCacheAfterAReleaseCostsARetry(t *testing.T) {
	r := newPairRigOn(t, nil)
	n0, n1 := r.nodes[0], r.nodes[1]
	const target = "users/a"
	r.create(target)
	if got := get(t, n1, target); got != (reply{200, "n0"}) {
		t.Fatalf("the first request through n1: %+v, want 200 served by n0", got)
	}
	if owner, ok := n1.m.Load().owners.Get(shardName(target)); !ok || owner != "n0" {
		t.Fatalf("n1's owner cache names %q (%v), want n0", owner, ok)
	}
	releaseOn(t, n0, target)
	r.releasedFromN0(t, target)

	q, err := http.NewRequest("GET", "http://"+n1.Public+"/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	q.Header.Set("Shardlite-Target", target)
	resp, err := clusterClient.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "0" {
		t.Fatalf("a request on a stale owner cache entry got %d with Retry-After %q, want 503 with 0", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if n0.m.Load().IsActive(shardName(target)) || n1.m.Load().IsActive(shardName(target)) {
		t.Fatal("the shard was claimed by the refused request")
	}
	if got := get(t, n1, target); got != (reply{200, "n1"}) {
		t.Fatalf("the retry: %+v, want 200 served by n1", got)
	}
	if got := owner(t, n1, target); got != "n1" {
		t.Fatalf("the manifest names %q after the retry, want n1", got)
	}
}
