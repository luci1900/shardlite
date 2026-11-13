package node

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

// Two nodes that claim one unowned shard at the same moment: the one whose claim loses the compare-and-swap on the manifest reads it again and sends its request to the winner, as it would for any shard another node owns. Neither client is answered with an error, and the shard has one owner.
func TestTwoNodesClaimingOneShardAtOnceAnswerBothClients(t *testing.T) {
	r := newPairRigOn(t, nil)
	n0, n1 := r.nodes[0], r.nodes[1]
	const target = "users/a"
	r.create(target)
	releaseOn(t, n0, target)
	r.releasedFromN0(t, target)

	// Both claims are held at their update of the manifest until both have got there, so each has read the shard unowned and one of them must lose.
	key := shardfile.ManifestKey(shardName(target))
	var arrived sync.WaitGroup
	arrived.Add(2)
	both := make(chan struct{})
	go func() {
		arrived.Wait()
		close(both)
	}()
	for _, nd := range r.nodes {
		var claimed sync.Once // the first update of the manifest on each node is its claim
		nd.onStore(func(ctx context.Context, op blobtest.Op, k string, after bool) error {
			if op == blobtest.OpUpdate && !after && k == key {
				claimed.Do(func() {
					arrived.Done()
					select {
					case <-both:
					case <-ctx.Done():
					}
				})
			}
			return nil
		})
	}

	answers := make(chan reply, 2)
	for _, nd := range []*clusterNode{n0, n1} {
		go func() {
			got, err := tryGet(nd, target)
			if err != nil {
				t.Error(err)
			}
			answers <- got
		}()
	}
	first, second := <-answers, <-answers
	if first.status != 200 || second.status != 200 || first.by != second.by {
		t.Fatalf("the two clients got %+v and %+v, want both served by the one node that owns the shard", first, second)
	}
	if got := owner(t, n0, target); got != first.by {
		t.Fatalf("the manifest names %q, but %q served the shard", got, first.by)
	}
}

// A claim that loses to a change that leaves the shard unowned (nobody has claimed it) is not a failure either: the client is told to ask again at once, with a 503 and Retry-After 0, and the next request claims the shard.
func TestLostClaimWithNoOwnerYetAsksTheClientToRetry(t *testing.T) {
	r := newPairRigOn(t, nil)
	n0 := r.nodes[0]
	const target = "users/a"
	r.create(target)
	releaseOn(t, n0, target)

	key := shardfile.ManifestKey(shardName(target))
	var changed atomic.Bool
	n0.onStore(func(ctx context.Context, op blobtest.Op, k string, after bool) error {
		if op == blobtest.OpUpdate && !after && k == key && changed.CompareAndSwap(false, true) {
			// Somebody else changes the manifest between the claim's read and its update.
			h, err := n0.m.Load().n.Files.Attach(ctx, shardName(target))
			if err != nil {
				return err
			}
			return h.SetReminder(ctx, shardfile.MinuteAt(time.Now().Add(time.Hour)), false)
		}
		return nil
	})
	q, err := http.NewRequest("GET", "http://"+n0.Public+"/x", nil)
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
		t.Fatalf("a client whose claim lost to a change got %d with Retry-After %q, want 503 with 0", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if got := get(t, n0, target); got != (reply{200, "n0"}) {
		t.Fatalf("the retry got %+v, want 200 served by n0", got)
	}
}
