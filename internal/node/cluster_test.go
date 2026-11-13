package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

// testCluster is several nodes in one process: Run on one in-memory store, each
// with its own directories, listeners on 127.0.0.1:0 and a fake app (HTTP/1.1
// and h2c) that answers 200 unless a test gives it a handler. The fake app never
// opens a shard, so commits are for tests that start a real one. The fault rig
// (docs/internals.md, Failing loudly) is for real crashes.
type testCluster struct {
	t     *testing.T
	mem   *blob.Memory
	vars  map[string]string // environment variables every node gets, over the defaults
	nodes []*clusterNode
}

// clusterNode is one node of a cluster. Its addresses change when it restarts.
type clusterNode struct {
	t      *testing.T
	mem    *blob.Memory
	env    Env
	hooked *blobtest.Hooked // the node's view of the store, cut off by Kill
	hook   atomic.Pointer[blobtest.Hook]
	app    atomic.Pointer[http.HandlerFunc]
	sick   atomic.Bool // the ready route answers 500, as a failing app would
	killed chan struct{}
	// tune changes the shards' options at each start, and m is the running shards.
	tune   func(*ShardsetOptions)
	m      atomic.Pointer[Shardset]
	cancel context.CancelFunc
	ran    chan error

	Public, Control, Peer string
}

// newCluster starts n nodes named n0, n1 and so on, and stops them when the
// test ends.
func newCluster(t *testing.T, n int) *testCluster { return newClusterWith(t, n, nil) }

// newClusterWith is newCluster with the shards' options changed by tune.
func newClusterWith(t *testing.T, n int, tune func(*ShardsetOptions)) *testCluster {
	t.Helper()
	return newClusterEnv(t, n, nil, tune)
}

// newClusterEnv is newClusterWith with environment variables set on every node.
func newClusterEnv(t *testing.T, n int, vars map[string]string, tune func(*ShardsetOptions)) *testCluster {
	t.Helper()
	c := &testCluster{t: t, mem: blob.NewMemory(), vars: vars}
	for i := range n {
		nd := c.newNode(fmt.Sprintf("n%d", i))
		nd.tune = tune
		c.nodes = append(c.nodes, nd)
	}
	for _, nd := range c.nodes {
		nd.Start()
	}
	return c
}

func (c *testCluster) newNode(name string) *clusterNode {
	t := c.t
	nd := &clusterNode{t: t, mem: c.mem}
	fake := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		// The ready route is the app's own and the fake answers it for the
		// node: a test's handler stands in for the app's shard routes and
		// would not know what to do with the declaration.
		if q.URL.Path == readyPath {
			if nd.sick.Load() {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			io.WriteString(w, clusterKinds)
			return
		}
		if h := nd.app.Load(); h != nil {
			(*h)(w, q)
		}
	}))
	fake.Config.Protocols = new(http.Protocols)
	fake.Config.Protocols.SetHTTP1(true)
	fake.Config.Protocols.SetUnencryptedHTTP2(true)
	fake.Start()
	t.Cleanup(fake.Close)
	dir := t.TempDir()
	vars := map[string]string{
		"SHARDLITE_NODE": name, "SHARDLITE_BLOB": "s3://unused",
		"SHARDLITE_DATA":          filepath.Join(dir, "data"),
		"SHARDLITE_APP":           fake.URL,
		"SHARDLITE_LISTEN_PUBLIC": "127.0.0.1:0", "SHARDLITE_LISTEN_CONTROL": "127.0.0.1:0", "SHARDLITE_LISTEN_PEER": "127.0.0.1:0",
		"SHARDLITE_FLUSH": "50ms",
	}
	maps.Copy(vars, c.vars)
	env, err := ParseEnv(func(k string) string { return vars[k] })
	if err != nil {
		t.Fatal(err)
	}
	nd.env = env
	t.Cleanup(nd.Stop)
	return nd
}

const clusterKinds = `{"kinds": {
	"users":  {"public": true, "sync": "none", "idle_timeout": "1h"},
	"wallet": {"public": true, "idle_timeout": "1h"},
	"audit":  {"idle_timeout": "1h"},
	"brief":  {"public": true, "sync": "none", "idle_timeout": "100ms"}
}}`

// create creates a shard on the node, as an app's call would, without asking the
// app: clients can only reach shards that exist.
func (n *clusterNode) create(target string) {
	n.t.Helper()
	ref, err := n.m.Load().Acquire(context.Background(), shardName(target))
	if err != nil {
		n.t.Fatalf("creating %s: %v", target, err)
	}
	ref.Release()
}

// serve makes h the node's app (but for its ready route, which answers the
// declaration).
func (n *clusterNode) serve(h http.HandlerFunc) { n.app.Store(&h) }

// onStore runs h before and after every operation of the node on the store.
func (n *clusterNode) onStore(h blobtest.Hook) { n.hook.Store(&h) }

// Start runs the node (again, after Stop or Kill) and waits until it serves.
func (n *clusterNode) Start() {
	n.t.Helper()
	n.hooked = blobtest.WithHook(n.mem)
	n.killed = make(chan struct{})
	killed := n.killed
	n.hooked.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		select {
		case <-killed:
			return errors.New("the node was killed")
		default:
		}
		if h := n.hook.Load(); h != nil {
			return (*h)(ctx, op, key, after)
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.ran = make(chan error, 1)
	up := make(chan [3]string, 1)
	go func() {
		n.ran <- Run(ctx, n.env, slog.New(slog.DiscardHandler), func(public, control, peer string) { up <- [3]string{public, control, peer} },
			WithBackend(n.hooked), withGrace(200*time.Millisecond), withShardset(func(o *ShardsetOptions, m *Shardset) {
				if o != nil && n.tune != nil {
					n.tune(o)
				}
				if m != nil {
					n.m.Store(m)
				}
			}))
	}()
	select {
	case a := <-up:
		n.Public, n.Control, n.Peer = a[0], a[1], a[2]
	case err := <-n.ran:
		n.ran <- err
		n.t.Fatalf("%s: Run ended early: %v", n.env.Name, err)
	case <-time.After(20 * time.Second):
		n.t.Fatalf("%s did not come up", n.env.Name)
	}
	// The kinds come from the app's declaration now, not a file read at
	// startup: the first tests to touch a shard wait for the poll to land.
	apptest.Eventually(n.t, apptest.Timeout, "the app's declaration", func() bool {
		m := n.m.Load()
		if m == nil {
			return false
		}
		_, err := m.opts.Kinds.Lookup("users")
		return err == nil
	})
}

// Stop shuts the node down as Run does on a signal: shards are let go of and
// the lease is released. It does nothing if the node is not running.
func (n *clusterNode) Stop() {
	n.t.Helper()
	if n.cancel == nil {
		return
	}
	n.cancel()
	n.cancel = nil
	select {
	case err := <-n.ran:
		if err != nil && !errors.Is(err, context.Canceled) {
			n.t.Errorf("%s: Run: %v", n.env.Name, err)
		}
	case <-time.After(30 * time.Second):
		n.t.Errorf("%s: Run did not return", n.env.Name)
	}
}

// Kill cancels the node with the store cut off from it, and its shards stop as
// in a crash, so nothing is let go of and the lease is not released: to the
// others it is a node that went quiet.
func (n *clusterNode) Kill() {
	n.t.Helper()
	close(n.killed)
	if m := n.m.Load(); m != nil {
		m.abandon.Store(true)
	}
	n.Stop()
}

// Lease is the node's lease as it is in blob storage.
func (n *clusterNode) Lease() *lease.Record {
	n.t.Helper()
	l, err := lease.NewRegistry(blob.WithPrefix(n.mem, n.env.Prefix), n.env.Name, lease.RegistryOptions{}).Get(context.Background(), n.env.Name)
	if err != nil {
		n.t.Fatal(err)
	}
	return l
}

// peerAnswers asks the peer listener at addr, over h2c, for a shard that doesn't
// exist, and fails if it does not answer 404 over HTTP/2.
func peerAnswers(addr string) error {
	protos := new(http.Protocols)
	protos.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: protos}, Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/x", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Shardlite-Origin", originClient)
	req.Header.Set("Shardlite-Target", "users/nope")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound || resp.ProtoMajor != 2 {
		return fmt.Errorf("status %d over HTTP/%d", resp.StatusCode, resp.ProtoMajor)
	}
	return nil
}

// Every node of a cluster answers on its peer listener, the others go on
// answering when one stops, a stopped node can start again, and Run leaves no
// goroutines with the peer listener in place.
func TestClusterNodesAnswerOnTheirPeerListeners(t *testing.T) {
	before := nodeGoroutines()
	c := newCluster(t, 3)
	check := func(nd *clusterNode) {
		t.Helper()
		if err := peerAnswers(nd.Peer); err != nil {
			t.Fatalf("%s: %v", nd.env.Name, err)
		}
		if l := nd.Lease(); l.Address != nd.Peer {
			t.Fatalf("%s listens at %s, its lease is %+v", nd.env.Name, nd.Peer, l)
		}
	}
	for _, nd := range c.nodes {
		check(nd)
	}

	c.nodes[1].Stop()
	if err := peerAnswers(c.nodes[1].Peer); err == nil {
		t.Fatal("a stopped node still answers")
	}
	check(c.nodes[0])
	check(c.nodes[2])

	epoch := c.nodes[1].Lease().Epoch
	c.nodes[1].Start()
	check(c.nodes[1])
	if got := c.nodes[1].Lease().Epoch; got <= epoch {
		t.Fatalf("epoch %d after the restart, was %d", got, epoch)
	}

	c.nodes[2].Kill()
	if l := c.nodes[2].Lease(); l.Released {
		t.Fatal("a killed node released its lease")
	}
	check(c.nodes[0])
	check(c.nodes[1])

	for _, nd := range c.nodes {
		nd.Stop()
	}
	if left := startedSince(before, nodeGoroutines()); len(left) > 0 {
		t.Fatalf("%d node goroutines left after the cluster stopped:\n%s", len(left), strings.Join(left, "\n\n"))
	}
}
