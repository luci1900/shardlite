package node

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
)

// pairRig is two nodes whose apps say which node they are. clk is the nodes' clock
// (a fake one for the tests that move the idle timer).
type pairRig struct {
	*testCluster
	clk *fakeClock
	app *apptest.App // writes to the shards: one that holds nothing is deleted when it is let go of
}

// newPairRigOn is two nodes on the given fake clock, if there is one.
func newPairRigOn(t *testing.T, clk *fakeClock) *pairRig { return newPairRigTuned(t, clk, nil) }

// newPairRigTuned is newPairRigOn with the nodes' options changed by tune, if it is set.
func newPairRigTuned(t *testing.T, clk *fakeClock, tune func(*ShardsetOptions)) *pairRig {
	r := &pairRig{clk: clk, app: apptest.Start(t)}
	r.testCluster = newClusterWith(t, 2, func(o *ShardsetOptions) {
		if tune != nil {
			tune(o)
		}
		if clk != nil {
			o.Clock = clk
			o.Leases.TTL = 1000 * time.Hour
		}
	})
	serveNamed(r.testCluster)
	return r
}

// create makes the shard on n0 and has the app write a row to it, so that letting go of it keeps it.
func (r *pairRig) create(target string) {
	r.t.Helper()
	hold, err := r.nodes[0].m.Load().Acquire(context.Background(), shardName(target))
	if err != nil {
		r.t.Fatalf("creating %s: %v", target, err)
	}
	defer hold.Release()
	r.app.Must(r.t, "open", target, hold.Path())
	r.app.Must(r.t, "exec", target, `CREATE TABLE t (x)`)
	r.app.Must(r.t, "exec", target, `INSERT INTO t VALUES (1)`)
	r.app.Must(r.t, "close", target)
}

// owner is the node the shard's manifest names, empty if it is unowned.
func owner(t *testing.T, nd *clusterNode, target string) string {
	t.Helper()
	m, _, err := nd.m.Load().n.Files.GetManifest(context.Background(), shardName(target))
	if err != nil {
		t.Fatal(err)
	}
	return string(m.Owner)
}

// releaseOn lets go of an active shard on the node as an idle deactivation does, without waiting for its idle timeout, and returns once the shard is off the node. The app must not have the shard's file open.
func releaseOn(t *testing.T, nd *clusterNode, target string) {
	t.Helper()
	e := nd.m.Load().tab.load(shardName(target))
	if e == nil {
		t.Fatalf("%s is not active on the node", target)
	}
	if ok, err := e.a.deactivateNow(t.Context()); !ok || err != nil {
		t.Fatalf("releasing %s: %v %v", target, ok, err)
	}
}

// reply is what a request through a node's public listener got: the status and the
// node that served it.
type reply struct {
	status int
	by     string
}

// tryGet sends a request for the target through the node's public listener. It
// returns the error rather than failing the test, so that a goroutine can use it.
func tryGet(nd *clusterNode, target string) (reply, error) {
	q, err := http.NewRequest("GET", "http://"+nd.Public+"/x", nil)
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

func get(t *testing.T, nd *clusterNode, target string) reply {
	t.Helper()
	got, err := tryGet(nd, target)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// releasedFromN0 checks that the shard is gone from n0 and unowned: nothing was claimed in its place yet.
func (r *pairRig) releasedFromN0(t *testing.T, target string) {
	t.Helper()
	n0 := r.nodes[0]
	if n0.m.Load().IsActive(shardName(target)) {
		t.Fatal("n0 still has the shard")
	}
	if got := owner(t, n0, target); got != "" {
		t.Fatalf("the manifest names %q, want nobody", got)
	}
}
