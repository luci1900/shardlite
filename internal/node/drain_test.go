package node

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/dispatch"
	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/peers"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// newDrainRig is the pair rig with lease scans that list often, so that n0 and n1 soon know each other to place on.
func newDrainRig(t *testing.T, tune func(*ShardsetOptions)) *pairRig {
	t.Helper()
	r := newPairRigTuned(t, nil, func(o *ShardsetOptions) {
		o.Leases.Every = 20 * time.Millisecond
		if tune != nil {
			tune(o)
		}
	})
	for _, nd := range r.nodes {
		waitForLive(t, nd, 2)
	}
	return r
}

// readiness is what a node's ready route answers.
func readiness(t *testing.T, nd *clusterNode) (int, string) {
	t.Helper()
	resp, err := clusterClient.Get("http://" + nd.Public + readyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// saysAvoid is whether a node's peer traffic says Shardlite-Placement: avoid now.
func saysAvoid(t *testing.T, nd *clusterNode) bool {
	t.Helper()
	h := http.Header{}
	nd.m.Load().placer.StampPlacement(h)
	return h.Get(peers.HeaderPlacement) == peers.PlacementAvoid
}

// holdRequest sends a request for the target through the node and leaves it in the app until the returned func is called, which it also is when the test ends. The answer is read from the channel.
func holdRequest(t *testing.T, nd *clusterNode, target string) (release func(), answer <-chan reply) {
	t.Helper()
	started, finish := make(chan struct{}), make(chan struct{})
	release = sync.OnceFunc(func() { close(finish) })
	t.Cleanup(release) // a failing test must not leave the app's handler blocked
	var once sync.Once
	name := string(nd.env.Name)
	nd.serve(func(w http.ResponseWriter, q *http.Request) {
		once.Do(func() { close(started) })
		<-finish
		w.Header().Set("X-Served-By", name)
		io.WriteString(w, "done")
	})
	got := make(chan reply, 1)
	go func() {
		a, err := tryGet(nd, target)
		if err != nil {
			t.Error(err)
		}
		got <- a
	}()
	<-started
	return release, got
}

// tryCall sends a call with no shard behind it for the target through the node's control API. It returns the error rather than failing the test, so that a goroutine can use it.
func tryCall(nd *clusterNode, target string) (reply, error) {
	q, err := http.NewRequest("GET", "http://"+nd.Control+"/x", nil)
	if err != nil {
		return reply{}, err
	}
	q.Header.Set("Shardlite-Target", target)
	resp, err := clusterClient.Do(q)
	if err != nil {
		return reply{}, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return reply{resp.StatusCode, resp.Header.Get("X-Served-By")}, nil
}

// untilServed repeats a request that is turned away with a 503 and a retry of 0 (a forward to a node that no longer has the shard corrects the forwarding node's hint and asks for a retry) until it is answered by something else.
func untilServed(try func() (reply, error)) (reply, error) {
	for range 50 {
		got, err := try()
		if err != nil || got.status != http.StatusServiceUnavailable {
			return got, err
		}
	}
	return reply{}, errors.New("the request was turned away over and over")
}

// Draining fails readiness (the other node's stays good) and tells the peers to avoid the node, and the other node picks no shard for it though it holds none.
func TestDrainFailsReadinessAndSaysAvoid(t *testing.T) {
	r := newDrainRig(t, nil)
	n0, n1 := r.nodes[0], r.nodes[1]
	for _, target := range []string{"users/a", "users/b", "users/c"} {
		n1.create(target)
	}
	if status, why := readiness(t, n0); status != 200 || why != "ok\n" {
		t.Fatalf("before the drain the node answered %d %q", status, why)
	}
	if saysAvoid(t, n0) {
		t.Fatal("a node that isn't draining says avoid")
	}

	n0.m.Load().Drain()

	if status, why := readiness(t, n0); status != 503 || why != "draining\n" {
		t.Fatalf("the draining node answered %d %q, want 503 draining", status, why)
	}
	if status, _ := readiness(t, n1); status != 200 {
		t.Fatalf("the other node answered %d", status)
	}
	if !saysAvoid(t, n0) {
		t.Fatal("a draining node doesn't say avoid")
	}
	// n0 tells n1 to avoid it with the requests it forwards, and n1 would pick it as often as itself but for that.
	get(t, n0, "users/a")
	apptest.Eventually(t, apptest.Timeout, "n1 to have n0 in its avoid set", func() bool { return n1.m.Load().placer.Avoids("n0") })
	for range 200 {
		if node := n1.m.Load().placer.Place(); node != "n1" {
			t.Fatalf("n1 picked %q, want itself", node)
		}
	}
}

// A client's request or a call with no shard behind it for a shard that no node has is served by another node while this one drains, and this node claims nothing.
func TestDrainPlacesNewClaimsElsewhere(t *testing.T) {
	r := newDrainRig(t, nil)
	n0, n1 := r.nodes[0], r.nodes[1]
	const client, login = "users/client", "users/login"
	n0.unowned(client)
	n0.m.Load().Drain()
	m0 := n0.m.Load()
	placed := m0.Stats.Placements.Load()

	if got := get(t, n0, client); got != (reply{200, "n1"}) {
		t.Fatalf("a client's request for an unowned shard: %+v, want 200 served by n1", got)
	}
	resp, body := callFrom(t, n0, "", login)
	if resp.StatusCode != 200 || body != "served" || resp.Header.Get("X-Served-By") != "n1" {
		t.Fatalf("a call with no shard behind it: %d %q by %q, want 200 served by n1", resp.StatusCode, body, resp.Header.Get("X-Served-By"))
	}
	for _, target := range []string{client, login} {
		if !n1.m.Load().IsActive(shardName(target)) || owner(t, n1, target) != "n1" {
			t.Fatalf("%s was not claimed by n1", target)
		}
		if m0.IsActive(shardName(target)) {
			t.Fatalf("the draining node claimed %s", target)
		}
	}
	if got := m0.Stats.Placements.Load() - placed; got != 2 {
		t.Fatalf("%d claims placed on another node, want 2", got)
	}
}

// A forward or an activation that reaches the draining node for a shard it would claim gets 421 naming the node the claim was placed on.
func TestDrainTurnsForwardsAndActivationsAway(t *testing.T) {
	r := newDrainRig(t, nil)
	n0 := r.nodes[0]
	const forwarded, dispatched = "users/forwarded", "users/dispatched"
	n0.unowned(forwarded)
	n0.unowned(dispatched)
	n0.m.Load().Drain()

	fwd := n0.peerDo("GET", "/x", map[string]string{"Shardlite-Origin": originClient, "Shardlite-Target": forwarded}, "")
	if fwd.status != 421 || fwd.final.Get(hdrOwner) != "n1" {
		t.Errorf("a forward got %d naming %q, want 421 naming n1", fwd.status, fwd.final.Get(hdrOwner))
	}
	if got := fwd.final.Get(hdrError); got != string(reasonNotOwner) {
		t.Errorf("a forward got reason %q, want %q", got, reasonNotOwner)
	}
	dis := n0.peerDo("POST", dispatch.Activate, map[string]string{"Shardlite-Origin": originNode, "Shardlite-Target": dispatched, hdrIfExists: "true"}, "")
	if dis.status != 421 || dis.final.Get(hdrOwner) != "n1" {
		t.Errorf("an activation got %d naming %q, want 421 naming n1", dis.status, dis.final.Get(hdrOwner))
	}
	for _, target := range []string{forwarded, dispatched} {
		if n0.m.Load().IsActive(shardName(target)) {
			t.Errorf("the draining node claimed %s", target)
		}
	}
}

// With no other node to place a claim on, a draining node serves it itself rather than refuse.
func TestDrainOnTheOnlyNodeClaimsLocally(t *testing.T) {
	c := newCluster(t, 1)
	serveNamed(c)
	n0 := c.nodes[0]
	const target = "users/a"
	n0.unowned(target)
	n0.create("users/b")
	n0.m.Load().Drain()
	if !n0.m.Load().IsActive(shardName("users/b")) {
		t.Fatal("a shard was let go of by the drain, which only changes where claims go")
	}
	if status, why := readiness(t, n0); status != 503 || why != "draining\n" {
		t.Fatalf("the node answered %d %q, want 503 draining", status, why)
	}
	if got := get(t, n0, target); got != (reply{200, "n0"}) {
		t.Fatalf("a request for an unowned shard: %+v, want 200 served by n0", got)
	}
}

// A stop lets go of every shard, releasing each so that the next node to need it claims it, and keeps none: nothing is left in the node's table or on its disk. It releases the lease too, so no shard of the node waits for an adoption. A request for one is then served by the other node.
func TestStopLetsGoOfEveryShardAndKeepsNone(t *testing.T) {
	r := newDrainRig(t, nil)
	n0, n1 := r.nodes[0], r.nodes[1]
	targets := []string{"users/a", "users/b", "users/c"}
	keys := map[string]bool{}
	for _, target := range targets {
		r.create(target)
		keys[shardfile.ManifestKey(shardName(target))] = true
	}
	var writes atomic.Int64
	n0.onStore(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpUpdate && after && keys[key] {
			writes.Add(1)
		}
		return nil
	})
	n0.Stop()
	if l := n0.Lease(); !l.Released {
		t.Fatalf("the lease was not released: %+v", l)
	}
	// Each shard's last delta and its release are one update of its manifest.
	if got := writes.Load(); got != int64(len(targets)) {
		t.Fatalf("the stop updated the shards' manifests %d times, want %d (one for each)", got, len(targets))
	}
	if shards := n0.m.Load().Active(); len(shards) != 0 {
		t.Fatalf("shards left on the node after the stop: %v", shards)
	}
	if left := leftBehind(n0.env.Data); len(left) != 0 {
		t.Fatalf("files left on the node after the stop: %v", left)
	}
	for _, target := range targets {
		if got := owner(t, n1, target); got != "" {
			t.Fatalf("the manifest of %s names %q after the stop, want nobody", target, got)
		}
		if got := get(t, n1, target); got != (reply{200, "n1"}) {
			t.Fatalf("a request for %s: %+v, want 200 served by n1", target, got)
		}
		if !n1.m.Load().IsActive(shardName(target)) || owner(t, n1, target) != "n1" {
			t.Fatalf("%s was not claimed by n1", target)
		}
	}
}

// The fault rig kills a node at kill.StopLetGo, so a stop has to reach it once for each shard it lets go of.
func TestStopReachesTheKillPoint(t *testing.T) {
	var points atomic.Int64
	r := newDrainRig(t, func(o *ShardsetOptions) {
		o.Lifecycle.At = func(p kill.Point) {
			if p == kill.StopLetGo {
				points.Add(1)
			}
		}
	})
	targets := []string{"users/a", "users/b", "users/c"}
	for _, target := range targets {
		r.create(target)
	}
	r.nodes[0].Stop()
	if got := points.Load(); got != int64(len(targets)) {
		t.Fatalf("the stop reached the kill point %d times, want %d (once for each shard)", got, len(targets))
	}
}

// While a stop lets go of a shard (its release held back here), a client's request through another node, a call through its control API and a forward straight to the peer listener are all answered by the other node, and none is lost. The stop tells the peers to avoid the node meanwhile.
func TestStopAnswersRequestsForShardsBeingLetGo(t *testing.T) {
	r := newDrainRig(t, nil)
	n0, n1 := r.nodes[0], r.nodes[1]
	const target, other = "users/a", "users/other"
	r.create(target)
	n1.create(other)
	get(t, n1, target) // n1 knows n0 owns it, and forwards there
	blocked, hold := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(unblock) // a failing test must not leave the release blocked
	var armed atomic.Bool
	armed.Store(true)
	n0.onStore(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpUpdate && !after && key == shardfile.ManifestKey(shardName(target)) && armed.CompareAndSwap(true, false) {
			close(blocked)
			<-hold
		}
		return nil
	})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		n0.Stop()
	}()
	<-blocked
	if !n0.m.Load().tab.load(shardName(target)).is(entryBusy) {
		t.Fatal("the shard's entry is not busy while it is let go of")
	}
	if !n0.m.Load().draining.Load() || !saysAvoid(t, n0) {
		t.Fatal("the stop did not drain: readiness would not fail and the node would not say avoid")
	}

	const requests = 4
	waits := n0.m.Load().Stats.Waits.Load()
	answers := make(chan reply, 2*requests+1)
	for range requests {
		go func() {
			got, err := untilServed(func() (reply, error) { return tryGet(n1, target) })
			if err != nil {
				t.Error(err)
			}
			answers <- got
		}()
		go func() {
			got, err := untilServed(func() (reply, error) { return tryCall(n1, target) })
			if err != nil {
				t.Error(err)
			}
			answers <- got
		}()
	}
	// The peer listener is still serving while the shard is let go of: a forward for a shard the other node owns is turned away naming it.
	if fwd := n0.peerDo("GET", "/x", map[string]string{"Shardlite-Origin": originClient, "Shardlite-Target": other}, ""); fwd.status != 421 || fwd.final.Get(hdrOwner) != "n1" {
		t.Errorf("a forward for a shard n1 owns during the stop got %d naming %q, want 421 naming n1", fwd.status, fwd.final.Get(hdrOwner))
	}
	// A forward straight to the peer listener waits for the shard too, finds no entry once it is let go of, and is turned away naming the pick.
	forwarded := make(chan peerAnswer, 1)
	go func() {
		forwarded <- n0.peerDo("GET", "/x", map[string]string{"Shardlite-Origin": originClient, "Shardlite-Target": target}, "")
	}()
	// Every request has reached n0 and waits for the shard being let go of before the release goes on.
	if err := n0.m.Load().Stats.Waits.Wait(t.Context(), waits+2*requests+1); err != nil {
		t.Fatalf("the requests did not all wait for the shard on n0: %v (%d waited)", err, n0.m.Load().Stats.Waits.Load()-waits)
	}
	unblock()
	for range 2 * requests {
		if got := <-answers; got != (reply{200, "n1"}) {
			t.Fatalf("a request during the stop: %+v, want 200 served by n1", got)
		}
	}
	if fwd := <-forwarded; fwd.status != 421 || fwd.final.Get(hdrOwner) != "n1" {
		t.Errorf("a forward during the stop got %d naming %q, want 421 naming n1", fwd.status, fwd.final.Get(hdrOwner))
	}
	<-stopped
	if l := n0.Lease(); !l.Released {
		t.Fatalf("the lease was not released: %+v", l)
	}
	if !n1.m.Load().IsActive(shardName(target)) || owner(t, n1, target) != "n1" {
		t.Fatal("the shard was not claimed by n1")
	}
}

// A stop waits for a request in flight that came through the peer listener, which is still serving, and lets it finish before it lets go of the shard.
func TestStopWaitsForARequestThroughAPeer(t *testing.T) {
	r := newDrainRig(t, nil)
	n0, n1 := r.nodes[0], r.nodes[1]
	const target = "users/a"
	r.create(target)
	get(t, n1, target) // n1 forwards to n0 from now on
	started, finish := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(finish) })
	t.Cleanup(release) // a failing test must not leave the app's handler blocked
	var once sync.Once
	n0.serve(func(w http.ResponseWriter, q *http.Request) {
		once.Do(func() { close(started) })
		<-finish
		w.Header().Set("X-Served-By", "n0")
		io.WriteString(w, "done")
	})
	answer := make(chan reply, 1)
	go func() {
		got, err := tryGet(n1, target)
		if err != nil {
			t.Error(err)
		}
		answer <- got
	}()
	<-started
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		n0.Stop()
	}()
	apptest.Eventually(t, apptest.Timeout, "n0's public server to stop", func() bool {
		return !listens(n0.Public)
	})
	select {
	case <-stopped:
		t.Fatal("the stop ended with a request in flight")
	default:
	}
	if l := n0.Lease(); l.Released {
		t.Fatal("the lease was released with a shard in use")
	}
	release()
	if got := <-answer; got != (reply{200, "n0"}) {
		t.Fatalf("the request in flight: %+v, want 200 from n0", got)
	}
	<-stopped
	if l := n0.Lease(); !l.Released {
		t.Fatalf("the lease was not released: %+v", l)
	}
	if got := owner(t, n1, target); got != "" {
		t.Fatalf("the manifest names %q after the stop, want nobody", got)
	}
}

// A stop keeps the peer and control listeners up until the lease is released: at the release the peer listener still answers, the public server has stopped, and the listeners are closed once it is done.
func TestStopKeepsThePeerAndControlListenersUntilTheLeaseIsReleased(t *testing.T) {
	r := newDrainRig(t, nil)
	n0 := r.nodes[0]
	r.create("users/a")
	leaseKey := lease.Key(n0.env.Name)
	var peerUp, controlUp, publicUp atomic.Bool
	var released atomic.Bool
	n0.onStore(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpUpdate && after && strings.HasSuffix(key, leaseKey) && n0.Lease().Released && released.CompareAndSwap(false, true) {
			err := peerAnswers(n0.Peer)
			peerUp.Store(err == nil)
			controlUp.Store(listens(n0.Control))
			publicUp.Store(listens(n0.Public))
		}
		return nil
	})
	n0.Stop()
	if !released.Load() {
		t.Fatal("the lease was not written during the stop")
	}
	if !peerUp.Load() || !controlUp.Load() {
		t.Errorf("at the release the peer listener answered %v and the control listener %v, want both up", peerUp.Load(), controlUp.Load())
	}
	if publicUp.Load() {
		t.Error("the public server was still up at the release")
	}
	if listens(n0.Peer) || listens(n0.Control) {
		t.Error("a listener outlived the stop")
	}
}

// listens reports whether something accepts connections at addr.
func listens(addr string) bool {
	conn, err := net.Dial("tcp", addr)
	if err == nil {
		conn.Close()
	}
	return err == nil
}
