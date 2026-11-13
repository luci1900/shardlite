package peers

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/model/cluster"
	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/storage/lease"
)

// The address cache reads a lease once, and again when the listing shows the node
// wrote its lease after the cached one would have expired.
func TestAddressCache(t *testing.T) {
	now := time.Now()
	var reads int
	address := "a:1"
	var list atomic.Pointer[List]
	a := &Addresses{
		Get: func(_ context.Context, node cluster.Node) (*lease.Record, error) {
			reads++
			if node == "gone" {
				return nil, blob.ErrNotFound
			}
			return &lease.Record{NodeInfo: lease.NodeInfo{Name: node, Address: address}, Renewed: now, TTL: 10 * time.Second}, nil
		},
		Nodes: list.Load,
	}
	ctx := context.Background()
	want := func(addr string, reads_ int) {
		t.Helper()
		got, err := a.Address(ctx, "n1")
		if err != nil || got != addr || reads != reads_ {
			t.Fatalf("address %q (%v) after %d reads, want %q after %d", got, err, reads, addr, reads_)
		}
	}
	want("a:1", 1)
	want("a:1", 1)
	// A renewal: written before the cached lease would have expired.
	list.Store(&List{Nodes: []Peer{{Listed: lease.Listed{Name: "n0"}}, {Listed: lease.Listed{Name: "n1", LastModified: now.Add(5 * time.Second)}}}})
	want("a:1", 1)
	// The node was gone for longer than its TTL and came back elsewhere.
	address = "b:2"
	list.Store(&List{Nodes: []Peer{{Listed: lease.Listed{Name: "n1", LastModified: now.Add(30 * time.Second)}}}})
	want("b:2", 2)
	if _, err := a.Address(ctx, "gone"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a node with no lease: %v", err)
	}
}
