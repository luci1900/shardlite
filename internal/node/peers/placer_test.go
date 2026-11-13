package peers

import (
	"bytes"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/model/cluster"
	"github.com/luci1900/shardlite/internal/storage/lease"
	"github.com/luci1900/shardlite/internal/syncs/notify"
	"github.com/luci1900/shardlite/internal/test/harness"
)

// syncBuffer is a log buffer the placer can write to from any goroutine.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// listOf is a listing with the nodes alive.
func listOf(nodes ...cluster.Node) *List {
	l := &List{}
	for _, n := range nodes {
		l.Nodes = append(l.Nodes, Peer{Listed: lease.Listed{Name: n}})
	}
	return l
}

// placerRig is a placer for the node "n0" with a listing that tests set.
type placerRig struct {
	t        *testing.T
	p        *Placer
	draining atomic.Bool
	list     atomic.Pointer[List]
	bad      notify.Counter
	logs     syncBuffer
	clock    *harness.FakeClock
}

// placerTTL is the lease TTL the rig's placer runs with.
const placerTTL = 10 * time.Second

func newPlacerRig(t *testing.T, live ...cluster.Node) *placerRig {
	t.Helper()
	r := &placerRig{t: t, clock: harness.NewFakeClock()}
	r.list.Store(listOf(live...))
	log := slog.New(slog.NewTextHandler(&r.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r.p = &Placer{Self: "n0", Draining: &r.draining, Listing: r.list.Load, Clock: r.clock, TTL: placerTTL, Log: log, Bad: &r.bad}
	return r
}

// picked places n times and counts what was picked.
func (r *placerRig) picked(n int) map[cluster.Node]int {
	got := map[cluster.Node]int{}
	for range n {
		got[r.p.Place()]++
	}
	return got
}

// answer gives the placer an answer from peer with the placement header values.
func (r *placerRig) answer(peer cluster.Node, values ...string) {
	h := http.Header{}
	for _, v := range values {
		h.Add(HeaderPlacement, v)
	}
	r.p.Answered(peer, h)
}

// Only live nodes outside the avoid set are picked, this node among them.
func TestPlacerPicksOnlyLiveNodesOutsideTheAvoidSet(t *testing.T) {
	r := newPlacerRig(t, "n0", "n1", "n2", "n3", "n4")
	r.list.Load().Nodes[2].Dead = true
	r.answer("n1", PlacementAvoid)
	r.p.Unanswered("n3")
	for n := range r.picked(500) {
		if n != "n0" && n != "n4" {
			t.Fatalf("picked %s, which is dead, avoided or not listed", n)
		}
	}
}

// Every candidate is picked, each within a bounded number of picks. The source is not seeded (math/rand/v2's top level can't be), so the bound is one a correct pick misses with a chance below 1 in 10^100.
func TestPlacerPicksEveryCandidate(t *testing.T) {
	nodes := []cluster.Node{"n0", "n1", "n2", "n3"}
	r := newPlacerRig(t, nodes...)
	seen := map[cluster.Node]bool{}
	for i := 0; i < 5000 && len(seen) < len(nodes); i++ {
		seen[r.p.Place()] = true
	}
	for _, n := range nodes {
		if !seen[n] {
			t.Errorf("%s was never picked in 5000 picks: %v", n, seen)
		}
	}
}

// A draining node is never a candidate, however many peers it avoids, and its picks are the others.
func TestPlacerNeverPicksThisNodeWhileDraining(t *testing.T) {
	r := newPlacerRig(t, "n0", "n1", "n2")
	r.draining.Store(true)
	got := r.picked(500)
	if got["n0"] != 0 || got["n1"] == 0 || got["n2"] == 0 {
		t.Fatalf("a draining node picked %v", got)
	}
	r.draining.Store(false)
	if got := r.picked(500); got["n0"] == 0 {
		t.Fatalf("a node that stopped draining picked %v, never itself", got)
	}
}

// With nothing else to pick, a pick is this node: no listing yet, no peer left, or every peer avoided, draining or not.
func TestPlacerPicksThisNodeWhenNothingElse(t *testing.T) {
	r := newPlacerRig(t)
	for _, draining := range []bool{false, true} {
		r.draining.Store(draining)
		if got := r.picked(50); got["n0"] != 50 {
			t.Fatalf("with no listing (draining %v) picked %v", draining, got)
		}
	}
	r.list.Store(nil)
	if got := r.picked(50); got["n0"] != 50 {
		t.Fatalf("with a nil listing picked %v", got)
	}

	r = newPlacerRig(t, "n0", "n1", "n2")
	r.answer("n1", PlacementAvoid)
	r.answer("n2", PlacementAvoid)
	for _, draining := range []bool{false, true} {
		r.draining.Store(draining)
		if got := r.picked(50); got["n0"] != 50 {
			t.Fatalf("with every peer avoided (draining %v) picked %v", draining, got)
		}
	}
}

// A peer that said avoid is avoided until an answer from it comes without the header. A header that does not read changes nothing.
func TestPlacerAvoidEndsAtAnAnswerWithoutTheHeader(t *testing.T) {
	r := newPlacerRig(t, "n0", "n1")
	r.answer("n1", PlacementAvoid+";for=5") // a parameter it does not know is left alone
	if got := r.picked(200); got["n1"] != 0 {
		t.Fatalf("picked %v after n1 said avoid", got)
	}
	r.answer("n1", "???")
	if !r.p.Avoids("n1") || r.bad.Load() != 1 {
		t.Fatalf("a bad header changed the entry: avoided %v, %d bad", r.p.Avoids("n1"), r.bad.Load())
	}
	r.answer("n1")
	if r.p.Avoids("n1") {
		t.Fatal("n1 is still avoided after an answer without the header")
	}
	if got := r.picked(500); got["n1"] == 0 {
		t.Fatalf("picked %v, never n1 after it answered without the header", got)
	}
	// Saying it again avoids it again.
	r.answer("n1", PlacementAvoid)
	if got := r.picked(200); got["n1"] != 0 {
		t.Fatalf("picked %v after n1 said avoid again", got)
	}
}

// A peer that said avoid is no longer avoided once the listing doesn't have it live, and an entry made for a node that isn't live is dropped, so a node that comes back live is a candidate.
func TestPlacerAvoidEndsWhenThePeerIsNotLive(t *testing.T) {
	r := newPlacerRig(t, "n0", "n1")
	r.answer("n1", PlacementAvoid)
	r.answer("n7", PlacementAvoid) // never listed
	r.list.Store(listOf("n0"))
	r.picked(1)
	if r.p.Avoids("n1") || r.p.Avoids("n7") {
		t.Fatal("place kept an entry for a node that isn't live")
	}
	r.list.Store(listOf("n0", "n1"))
	if got := r.picked(500); got["n1"] == 0 {
		t.Fatalf("picked %v, never n1 after it came back live", got)
	}

	r.answer("n1", PlacementAvoid)
	r.list.Store(listOf("n0", "n1"))
	r.list.Load().Nodes[1].Dead = true
	r.picked(1)
	if r.p.Avoids("n1") {
		t.Fatal("place kept an entry for a node the listing counts as dead")
	}
}

// A peer that said avoid is avoided for a lease TTL from the last time it said so, and saying it again, in an answer or a forward, starts the TTL again. A forward that gets no answer then avoids it until the next listing.
func TestPlacerAvoidEndsAfterTheTTL(t *testing.T) {
	r := newPlacerRig(t, "n0", "n1")
	r.answer("n1", PlacementAvoid)
	r.clock.Advance(placerTTL - time.Second)
	if got := r.picked(200); got["n1"] != 0 {
		t.Fatalf("picked %v within the TTL", got)
	}
	h := http.Header{}
	h.Set(HeaderNode, "n1")
	h.Set(HeaderPlacement, PlacementAvoid)
	r.p.Received("10.0.0.7", h) // a forward from it says it again
	r.clock.Advance(placerTTL - time.Second)
	if got := r.picked(200); got["n1"] != 0 {
		t.Fatalf("picked %v within the TTL of the forward that said avoid again", got)
	}
	r.clock.Advance(time.Second)
	if got := r.picked(500); got["n1"] == 0 {
		t.Fatalf("picked %v, never n1 a TTL after it last said avoid", got)
	}
	if r.p.Avoids("n1") {
		t.Fatal("the entry outlived its TTL")
	}

	r.answer("n1", PlacementAvoid)
	r.clock.Advance(placerTTL)
	r.p.Unanswered("n1")
	if got := r.picked(200); got["n1"] != 0 {
		t.Fatalf("picked %v, an expired avoid kept the unreachable entry from being made", got)
	}
}

// A peer a forward could not reach is avoided until the next listing, or an answer from it.
func TestPlacerUnreachableEndsAtTheNextListingOrAnAnswer(t *testing.T) {
	r := newPlacerRig(t, "n0", "n1")
	r.p.Unanswered("n1")
	for range 3 {
		if got := r.picked(200); got["n1"] != 0 {
			t.Fatalf("picked %v while n1 is unreachable under the same listing", got)
		}
	}
	r.list.Store(listOf("n0", "n1")) // the next listing
	if got := r.picked(500); got["n1"] == 0 {
		t.Fatalf("picked %v, never n1 after the next listing", got)
	}
	if r.p.Avoids("n1") {
		t.Fatal("the entry outlived the listing it was made under")
	}

	// An answer without the header ends it, and so does one whose header does not read, since the peer answered.
	for _, values := range [][]string{nil, {"???"}} {
		r.p.Unanswered("n1")
		r.answer("n1", values...)
		if r.p.Avoids("n1") {
			t.Fatalf("an answer with %q left n1 avoided as unreachable", values)
		}
	}
	// An answer that says avoid turns it into the other reason.
	r.p.Unanswered("n1")
	r.answer("n1", PlacementAvoid)
	r.list.Store(listOf("n0", "n1"))
	if got := r.picked(200); got["n1"] != 0 {
		t.Fatalf("picked %v after n1 said avoid, which a new listing does not end", got)
	}
}

// A peer that said avoid stays avoided for that reason when a forward to it gets no answer, since that reason lasts longer.
func TestPlacerUnanswerdDoesNotShortenAvoid(t *testing.T) {
	r := newPlacerRig(t, "n0", "n1")
	r.answer("n1", PlacementAvoid)
	r.p.Unanswered("n1")
	r.list.Store(listOf("n0", "n1"))
	if got := r.picked(200); got["n1"] != 0 {
		t.Fatalf("picked %v, the new listing ended an avoid the peer said", got)
	}
}

// A request from a peer names it, may ask to be avoided and leaves a trace in the log when a header is bad. This node's own name is no sender.
func TestPlacerReadsAPeersRequest(t *testing.T) {
	r := newPlacerRig(t, "n0", "n1", "n2")
	read := func(hdr ...string) (cluster.Node, bool) {
		h := http.Header{}
		for i := 0; i < len(hdr); i += 2 {
			h.Add(hdr[i], hdr[i+1])
		}
		return r.p.Received("10.0.0.7", h)
	}
	if node, avoid := read(); node != "" || avoid {
		t.Fatalf("a request with neither header: %q, %v", node, avoid)
	}
	if node, avoid := read(HeaderNode, "n1"); node != "n1" || avoid || r.p.Avoids("n1") {
		t.Fatalf("a request from n1: %q, %v, avoided %v", node, avoid, r.p.Avoids("n1"))
	}
	if node, avoid := read(HeaderNode, "n1", HeaderPlacement, PlacementAvoid); node != "n1" || !avoid || !r.p.Avoids("n1") {
		t.Fatalf("a request from n1 asking to be avoided: %q, %v, avoided %v", node, avoid, r.p.Avoids("n1"))
	}
	// A forward without the header is no answer, so it does not end the entry.
	if node, _ := read(HeaderNode, "n1"); node != "n1" || !r.p.Avoids("n1") {
		t.Fatalf("a request from n1 without the header ended the entry")
	}
	// Asking to be avoided without a name says nothing about whom, but the request still asks.
	if node, avoid := read(HeaderPlacement, PlacementAvoid); node != "" || !avoid {
		t.Fatalf("a request asking to be avoided with no name: %q, %v", node, avoid)
	}
	if node, avoid := read(HeaderNode, "n0", HeaderPlacement, PlacementAvoid); node != "" || !avoid || r.p.Avoids("n0") {
		t.Fatalf("a request in this node's own name: %q, %v, avoided %v", node, avoid, r.p.Avoids("n0"))
	}
	if r.bad.Load() != 0 || r.logs.String() != "" {
		t.Fatalf("good headers counted %d bad, logged %q", r.bad.Load(), r.logs.String())
	}

	// Bad headers are counted and logged at debug, and ignored.
	if node, avoid := read(HeaderNode, "a b", HeaderPlacement, "???"); node != "" || avoid {
		t.Fatalf("a request with bad headers: %q, %v", node, avoid)
	}
	if node, avoid := read(HeaderNode, "n2", HeaderPlacement, "yes"); node != "n2" || avoid || r.p.Avoids("n2") {
		t.Fatalf("a request with a bad placement: %q, %v, avoided %v", node, avoid, r.p.Avoids("n2"))
	}
	if r.bad.Load() != 3 {
		t.Fatalf("%d bad headers counted, want 3", r.bad.Load())
	}
	if lines := strings.Count(r.logs.String(), "level=DEBUG"); lines != 3 {
		t.Fatalf("%d debug lines logged:\n%s", lines, r.logs.String())
	}
}
