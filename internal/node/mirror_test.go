package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/pos"
)

// The mirrors, on the node: the peer listener's journal endpoints with the
// real epoch view behind them, the choice of the lease's mirrors at a start,
// and the fetch a restart runs before its fence, over the protocol itself.

const testShard = "test.users/42"

// writeShard runs a few app commits and syncs them, as the shardfile tests'
// write does for its harness.
func writeShard(t *testing.T, e *harness.Env, app *apptest.App, sh *capture.Shard, i int) {
	t.Helper()
	app.Must(t, "insert", "a", "2", "300")
	app.Must(t, "exec", "a", fmt.Sprintf("UPDATE kv SET payload = zeroblob(%d) WHERE id = 1", 50+i))
	if err := sh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// The journal endpoints answer on the peer listener, from the node's local
// database, and the epoch check refuses a write id that names a life of the
// writer older than the node's view of its lease.
func TestMirrorEndpointsOnThePeerListener(t *testing.T) {
	c := newCluster(t, 2)
	ctx := context.Background()
	nd := c.nodes[1] // the mirror: it serves n0's journal from its own disk
	m := journal.NewMirror(http.DefaultClient, "http://"+nd.Peer, "n0")

	// n0's lease is at epoch 1: a write id naming epoch 0 is a stale life,
	// and one naming epoch 1 is the writer itself.
	stale := blob.WriteID{Node: "n0", Epoch: 0, N: 1}
	if err := m.Create(ctx, 1, strings.NewReader("one"), stale); err == nil {
		t.Fatal("a stale life's create was taken")
	}
	own := blob.WriteID{Node: "n0", Epoch: 1, N: 1}
	if err := m.Create(ctx, 1, strings.NewReader("one"), own); err != nil {
		t.Fatal(err)
	}
	// The bytes are on the mirror node's disk, under n0's journal.
	back := journal.NewLocal(nd.m.Load().n.LocalDB.DB, "n0")
	if info, err := back.Head(ctx, 1); err != nil || info.Size != 3 || info.WriteID != own {
		t.Fatalf("head on the mirror node's disk: %+v %v", info, err)
	}
	// The listing names what the mirror holds.
	var got []pos.Seg
	for seq, err := range m.List(ctx, 1) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, seq)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("the listing: %v", got)
	}
	// A number taken once stays taken.
	if err := m.Create(ctx, 1, strings.NewReader("two"), blob.WriteID{Node: "n0", Epoch: 1, N: 2}); err == nil {
		t.Fatal("a second create took the number")
	}
	// A read back through the protocol is the bytes the mirror holds.
	rc, err := m.GetRange(ctx, 1, -2, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	buf, err := io.ReadAll(rc)
	if err != nil || string(buf) != "ne" {
		t.Fatalf("the tail read %q %v", buf, err)
	}
	if err := m.Delete(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := back.Head(ctx, 1); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("after the delete: %v", err)
	}
}

// leaseOf is a node's lease as it is in blob storage, read through a registry
// of the test's own.
func nodeLease(t *testing.T, c *testCluster, name cluster.Node) *coord.NodeLease {
	t.Helper()
	l, err := coord.NewRegistry(blob.WithPrefix(c.mem, ""), name, coord.RegistryOptions{}).Get(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// A start with standbys set names its mirrors in the lease, from the listing:
// live nodes that can hold a journal, least loaded first, itself excluded.
// Renewals republish the same set.
func TestMirrorsAreChosenAtTheStartAndStick(t *testing.T) {
	c := newClusterEnv(t, 3, map[string]string{
		"SHARDLITE_STANDBYS":  "1",
		"SHARDLITE_LEASE_TTL": "1s",
	}, nil)
	// Every lease says its node can hold a mirror, and names only other live
	// nodes, never itself. With no load anywhere the tie goes to the lowest
	// name, so the last node to start names n0.
	for _, nd := range c.nodes {
		l := nd.Lease()
		if !l.Standby {
			t.Fatalf("%s's lease says it cannot hold a mirror", l.Name)
		}
		for _, m := range l.Mirrors {
			if m == l.Name {
				t.Fatalf("%s named itself", l.Name)
			}
			if ml := nodeLease(t, c, m); ml.Released || ml.ExpiresAt().Before(time.Now()) {
				t.Fatalf("%s named %s, which does not look alive", l.Name, m)
			}
		}
	}
	last := c.nodes[2].Lease()
	if len(last.Mirrors) != 1 || last.Mirrors[0] != "n0" {
		t.Fatalf("n2 named %v, want n0", last.Mirrors)
	}
	// The set is sticky: renewals republish it, so it is the same once the
	// lease has been renewed.
	eventually(t, "a renewal", func() bool {
		return c.nodes[2].Lease().Renewed.After(last.Renewed)
	})
	if got := c.nodes[2].Lease().Mirrors; len(got) != 1 || got[0] != "n0" {
		t.Fatalf("n2's mirrors changed at a renewal: %v", got)
	}
}

// A start with standbys set names none when nothing else can hold a mirror,
// and says so.
func TestMirrorsNeedNodesThatCanHoldThem(t *testing.T) {
	c := newCluster(t, 1)
	if l := c.nodes[0].Lease(); len(l.Mirrors) != 0 {
		t.Fatalf("a node alone named %v", l.Mirrors)
	}
}

// mirrorServer is a mirror endpoint over HTTP, with the mirror's store of the
// journal and the name its lease carries.
type mirrorServer struct {
	node  cluster.Node
	store *journal.Store
}

// serveMirror starts a mirror endpoint for e's journal on another node, and
// gives the node a lease with the address, so a dialer finds it.
func serveMirror(t *testing.T, e *harness.Env) *mirrorServer {
	t.Helper()
	db, err := localdb.Open(filepath.Join(t.TempDir(), localdb.Name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	src := &testMirrorSource{b: journal.NewLocal(db.DB, e.Node)}
	srv := httptest.NewUnstartedServer(journal.NewMirrorHandler(src, t.TempDir(), 0))
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Start()
	t.Cleanup(srv.Close)
	node := cluster.Node("m1")
	reg := coord.NewRegistry(e.Blob, node, coord.RegistryOptions{TTL: time.Hour})
	if _, err := reg.Acquire(context.Background(), coord.NodeInfo{Name: node,
		Address: strings.TrimPrefix(srv.URL, "http://")}); err != nil {
		t.Fatal(err)
	}
	store := journal.NewStore(journal.NewLocal(db.DB, e.Node), e.Node, 1, journal.Options{Backoff: time.Millisecond})
	return &mirrorServer{node: node, store: store}
}

// testMirrorSource answers the handler's questions: one journal, no view.
type testMirrorSource struct {
	b journal.Backend
}

func (s *testMirrorSource) Journal(node cluster.Node) journal.Backend { return s.b }

func (s *testMirrorSource) Epoch(ctx context.Context, node cluster.Node) pos.Epoch { return 0 }

// takeOutToMirror copies the record's segment seq to the mirror with the write
// id the record's create carried, then deletes the record's copy: the record's
// landed run ends one short, and the mirror is where the tail is.
func takeOutToMirror(t *testing.T, e *harness.Env, mirror *journal.Store, seq pos.Seg) {
	t.Helper()
	ctx := context.Background()
	head, err := e.Journal.Head(ctx, seq)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := e.Journal.Get(ctx, seq)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := mirror.CreateWithID(ctx, seq, bytes.NewReader(data), head.WriteID); err != nil {
		t.Fatal(err)
	}
	if err := e.Blob.Delete(ctx, journal.Key(e.Node, seq), ""); err != nil {
		t.Fatal(err)
	}
}

// A restart fetches its previous life's mirrors before it fences: the segment
// the record never landed comes back from the mirror, and the fence goes above
// it. The restart comes back on a fresh disk, so the local rows hold nothing
// and the mirrors are the only source of the tail.
func TestARestartFetchesItsMirrorsBeforeTheFence(t *testing.T) {
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(testShard)
	_, sh := e.NewShard(testShard)
	defer sh.Close(ctx)
	writeShard(t, e, app, sh, 1)
	writeShard(t, e, app, sh, 2)
	mirr := serveMirror(t, e)
	takeOutToMirror(t, e, mirr.store, 2)

	// The lease names the mirror and the range that may still hold a tail.
	info := e.Lease.NodeInfo
	info.Mirrors = []cluster.Node{mirr.node}
	info.JournalFrom, info.JournalNext = 1, e.W.NextSeq()
	l, err := e.Reg.Renew(ctx, e.Lease, info)
	if err != nil {
		t.Fatal(err)
	}
	e.Lease = l

	n2, err := Start(ctx, Config{Name: e.Node, Address: "127.0.0.1:1", Backend: e.Blob,
		Data: t.TempDir(), TempDir: t.TempDir(), Lease: coord.RegistryOptions{TTL: time.Hour},
		Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	if n2.Adopted.Fetched != 1 || n2.Adopted.Fence != e.W.NextSeq() {
		t.Fatalf("the restart's adoption: %+v", n2.Adopted)
	}
	head, err := n2.Files.Journal(e.Node).Head(ctx, 2)
	if err != nil {
		t.Fatal("the record never got the mirror's copy:", err)
	}
	mhead, err := mirr.store.Head(ctx, 2)
	if err != nil || head.Size != mhead.Size || head.WriteID != mhead.WriteID {
		t.Fatalf("the record's segment %+v is not the mirror's %+v (%v)", head, mhead, err)
	}
}
