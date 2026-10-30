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
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// The mirrors, on the node: the peer listener's journal endpoints, the choice
// of the lease's peer at a start, and the fetch a restart runs before its
// fence, over the protocol itself.

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
// database.
func TestMirrorEndpointsOnThePeerListener(t *testing.T) {
	c := newCluster(t, 2)
	ctx := context.Background()
	nd := c.nodes[1] // the mirror: it serves n0's journal from its own disk
	m := journal.NewMirror(http.DefaultClient, "http://"+nd.Peer, "n0")

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

// A start with standbys set names its peer in the lease, from the listing: a
// live node that can hold a journal, the least loaded, itself excluded.
// Renewals republish the same choice.
func TestThePeerIsChosenAtTheStartAndSticks(t *testing.T) {
	c := newClusterEnv(t, 3, map[string]string{
		"SHARDLITE_STANDBYS":  "1",
		"SHARDLITE_LEASE_TTL": "1s",
	}, nil)
	// Every lease says its node can hold a peer, and names only other live
	// nodes, never itself. With no load anywhere the tie goes to the lowest
	// name: n1 and n2 both name n0, and n0, which started first and had
	// nothing else in the listing, names none until a repair names one.
	for _, nd := range c.nodes {
		l := nd.Lease()
		if !l.Standby {
			t.Fatalf("%s's lease says it cannot hold a peer", l.Name)
		}
		if l.Peer == l.Name {
			t.Fatalf("%s named itself", l.Name)
		}
		if l.Peer == "" {
			continue // it started alone; a repair names a peer later
		}
		if ml := nodeLease(t, c, l.Peer); ml.Released || ml.ExpiresAt().Before(time.Now()) {
			t.Fatalf("%s named %s, which does not look alive", l.Name, l.Peer)
		}
	}
	if got := c.nodes[0].Lease().Peer; got != "" {
		t.Fatalf("n0, which started alone, named %s", got)
	}
	for _, nd := range c.nodes[1:] {
		if got := nd.Lease().Peer; got != "n0" {
			t.Fatalf("%s named %s, want n0", nd.env.Name, got)
		}
	}
	// The choice is sticky: renewals republish it, so it is the same once the
	// lease has been renewed.
	last := c.nodes[2].Lease()
	eventually(t, "a renewal", func() bool {
		return c.nodes[2].Lease().Renewed.After(last.Renewed)
	})
	if got := c.nodes[2].Lease().Peer; got != "n0" {
		t.Fatalf("n2's peer changed at a renewal: %s", got)
	}
}

// A start with standbys set names none when nothing else can hold a peer,
// and says so.
func TestAPeerNeedsANodeThatCanHoldOne(t *testing.T) {
	c := newCluster(t, 1)
	if l := c.nodes[0].Lease(); l.Peer != "" {
		t.Fatalf("a node alone named %s", l.Peer)
	}
}

// mirrorServer is a mirror endpoint over HTTP, with the mirror's store of the
// journal and the name its lease carries.
type mirrorServer struct {
	node  cluster.Node
	store *journal.Store
	srv   *httptest.Server
}

// down stops the endpoint, as the loss of the mirror's host would: the owner's
// sends fail from there on.
func (m *mirrorServer) down() { m.srv.Close() }

// serveMirror starts a mirror endpoint for the owner's journal on the node
// mirror, and gives that node a lease with the address, so a dialer finds it
// and the choice of a peer can name it.
func serveMirror(t *testing.T, backend blob.Backend, mirror, owner cluster.Node) *mirrorServer {
	t.Helper()
	db, err := localdb.Open(filepath.Join(t.TempDir(), localdb.Name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	src := &testMirrorSource{b: journal.NewLocal(db.DB, owner)}
	srv := httptest.NewUnstartedServer(journal.NewMirrorHandler(src, tempfile.NewDir(t.TempDir())))
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Start()
	t.Cleanup(srv.Close)
	reg := coord.NewRegistry(backend, mirror, coord.RegistryOptions{TTL: time.Hour})
	if _, err := reg.Acquire(context.Background(), coord.NodeInfo{Name: mirror,
		Address: strings.TrimPrefix(srv.URL, "http://"), Standby: true}); err != nil {
		t.Fatal(err)
	}
	store := journal.NewStore(journal.NewLocal(db.DB, owner), owner, 1, journal.Options{Backoff: time.Millisecond})
	return &mirrorServer{node: mirror, store: store, srv: srv}
}

// testMirrorSource answers the handler's questions: one journal.
type testMirrorSource struct {
	b journal.Backend
}

func (s *testMirrorSource) Journal(node cluster.Node) journal.Backend { return s.b }

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

// A restart fetches its previous life's peer before it fences: the segment
// the record never landed comes back from the peer, and the fence goes above
// it. The restart comes back on a fresh disk, so the local rows hold nothing
// and the peer is the only source of the tail.
func TestARestartFetchesItsPeerBeforeTheFence(t *testing.T) {
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(testShard)
	_, sh := e.NewShard(testShard)
	defer sh.Close(ctx)
	writeShard(t, e, app, sh, 1)
	writeShard(t, e, app, sh, 2)
	mirr := serveMirror(t, e.Blob, "m1", e.Node)
	takeOutToMirror(t, e, mirr.store, 2)

	// The lease names the peer and the range that may still hold a tail.
	info := e.Lease.NodeInfo
	info.Peer = mirr.node
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
		t.Fatal("the record never got the peer's copy:", err)
	}
	phead, err := mirr.store.Head(ctx, 2)
	if err != nil || head.Size != phead.Size || head.WriteID != phead.WriteID {
		t.Fatalf("the record's segment %+v is not the peer's %+v (%v)", head, phead, err)
	}
}

// A start with standbys set fans out to the peer over the protocol: a flushed
// segment is on the peer's disk under the write id the record's create
// carried, and the writer says the peer is covered.
func TestTheWriterFansOutToItsPeer(t *testing.T) {
	ctx := context.Background()
	mem := blobtest.WithHook(blob.NewMemory())
	mirr := serveMirror(t, mem, "m1", "n1")
	n, err := Start(ctx, Config{Name: "n1", Address: "127.0.0.1:1", Backend: mem,
		Data: t.TempDir(), TempDir: t.TempDir(), Standbys: 1,
		Lease: coord.RegistryOptions{TTL: time.Hour}, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	if n.peer != "m1" {
		t.Fatalf("the start named %s, want m1", n.peer)
	}
	// The writer is served here: a start alone does not run it.
	stop := service.Background(slog.Default(), n.Writer)
	defer stop()

	// A shard, a write, a sync: the sync's flush lands the segment in the
	// record and sends it to the peer.
	sn, err := shard.ParseName(testShard)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(n.Data, "users-42.db")
	// The app creates the shard's file by opening it, as an app's call would;
	// the capture then takes the file over.
	app := apptest.Start(t)
	app.Must(t, "open", "a", path)
	app.Must(t, "insert", "a", "2", "300")
	app.Must(t, "exec", "a", "UPDATE kv SET payload = zeroblob(50) WHERE id = 1")
	h, created, err := n.Files.Open(ctx, sn)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("the shard exists")
	}
	sh, err := capture.New(capture.Config{Name: h.Name, Path: path, Generation: h.Manifest.Generation,
		Writer: n.Writer, TempDir: n.Files.Temp})
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close(ctx)
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	seq := n.Writer.NextSeq() - 1

	head, err := n.Files.Journal(n.Name).Head(ctx, seq)
	if err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, 5*time.Second, "the peer to hold the segment", func() bool {
		_, err := mirr.store.Head(ctx, seq)
		return err == nil
	})
	phead, err := mirr.store.Head(ctx, seq)
	if err != nil {
		t.Fatal(err)
	}
	if phead.WriteID != head.WriteID || phead.Size != head.Size {
		t.Fatalf("the peer's segment %+v is not the record's %+v", phead, head)
	}
	st, ok := n.Writer.Peer()
	if !ok || st.Name != "m1" || st.Frontier != seq {
		t.Fatalf("the peer's state is %+v (ok %v), want m1 at %d", st, ok, seq)
	}
	apptest.Eventually(t, 5*time.Second, "the peer to be covered", n.Writer.PeerCovered)
}

// The peer tier end to end, on a node whose shards are served: a kind whose
// sync names `peer` waits for the node's disk and the peer and not for the
// record — the record's write is stalled and the answer comes anyway — and
// once the peer has fallen out, the answer fails instead of being carried by
// the record.
func TestThePeerTierAcksOnThePeerAndFailsWithoutOne(t *testing.T) {
	r := newUnstartedMRig(t, nil)
	mirr := serveMirror(t, r.local, "m1", "n1")
	r.standbys = 1
	r.startNode()
	// Cycles run on the gate's request alone, so the test decides when the
	// capture that carries a write happens, and where the record stands then.
	r.startShards(func(o *CatalogOptions) { o.Flush = FlushOptions{Interval: time.Hour} })
	if r.n.peer != "m1" {
		t.Fatalf("the node named %s, want m1", r.n.peer)
	}
	ctx := context.Background()
	ref := r.acquire("test.vault/1")
	defer ref.Release()
	if ref.Sync() != kind.SyncPeer {
		t.Fatalf("vault's tier is %v, want peer", ref.Sync())
	}

	// The record's writes are stalled. A `peer` answer comes anyway: it waits
	// for the node's disk and the peer, and the record never sees the segment
	// while the stall holds.
	stall := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(stall) }) }
	defer release()
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpCreate && strings.HasPrefix(key, journal.Root) {
			select {
			case <-stall:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	r.write(ref, "a")
	gated := make(chan error, 1)
	go func() { gated <- r.m.Gate(context.Background(), ref, ref.Sync()) }()
	var seq pos.Seg
	apptest.Eventually(t, 5*time.Second, "the peer to hold the segment", func() bool {
		st, ok := r.n.Writer.Peer()
		if !ok || st.Frontier < 1 {
			return false
		}
		if _, err := mirr.store.Head(ctx, st.Frontier); err != nil {
			return false
		}
		seq = st.Frontier
		return true
	})
	select {
	case err := <-gated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled record held a peer answer")
	}
	if _, err := r.n.Files.Journal("n1").Head(ctx, seq); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the record has segment %d while its writes are stalled (%v), so the answer did not come from the peer", seq, err)
	}
	// The diagnostic says where the peer stands: in step, standing.
	if pi := r.m.PeerInfo(); pi == nil || pi.Node != "m1" || pi.Out || !pi.CaughtUp || pi.Frontier != seq {
		t.Fatalf("peer info %+v, want m1 at %d, in step and standing", pi, seq)
	}
	// Letting the stall go lands the segment in the record as usual.
	release()
	apptest.Eventually(t, 5*time.Second, "the record to take the segment", func() bool {
		_, err := r.n.Files.Journal("n1").Head(ctx, seq)
		return err == nil
	})

	// The peer's host is lost. Its next send exhausts its attempts and it
	// falls out; the answer then fails with the tier unmet, and the node says
	// the peer is out.
	mirr.down()
	r.write(ref, "a")
	err := r.m.Gate(ctx, ref, ref.Sync())
	if err == nil || !strings.Contains(err.Error(), "peer") {
		t.Fatalf("a write on a peer that fell out answered %v, want the tier unmet", err)
	}
	// The write's segment is still the writer's: it lands in the record and
	// on the peer's disk once there is one again.
	apptest.Eventually(t, 5*time.Second, "the record to take the segment", func() bool {
		_, err := r.n.Files.Journal("n1").Head(ctx, r.n.Writer.NextSeq()-1)
		return err == nil
	})
	apptest.Eventually(t, 10*time.Second, "the peer to fall out", func() bool {
		pi := r.m.PeerInfo()
		return pi != nil && pi.Out
	})
}

// A `peer` kind on a node that names no peer fails at the gate: the tier is
// opt-in, and an unmet one is an error, never the record's answer instead.
func TestAPeerKindWithoutAPeerFails(t *testing.T) {
	r := newMRig(t) // no standbys: the node names no peer
	ref := r.acquire("test.vault/1")
	defer ref.Release()
	if ref.Sync() != kind.SyncPeer {
		t.Fatalf("vault's tier is %v, want peer", ref.Sync())
	}
	r.write(ref, "a")
	if err := r.m.Gate(context.Background(), ref, ref.Sync()); err == nil || !strings.Contains(err.Error(), "peer") {
		t.Fatalf("a peer answer without a peer answered %v, want the tier unmet", err)
	}
	if pi := r.m.PeerInfo(); pi != nil {
		t.Fatalf("peer info %+v, want none named", pi)
	}
}

// The repair: a node that named no peer has one named for it, and a peer that
// falls out is replaced — the replacement seeded from the record's landed run,
// the stream continuing where the seed ended, and the next renewal publishing
// the new pairing. Until each repair takes, the answers fail.
func TestTheRepairNamesAndReplacesThePeer(t *testing.T) {
	r := newUnstartedMRig(t, nil)
	r.standbys = 1
	r.startNode()
	r.startShards(func(o *CatalogOptions) { o.Flush = FlushOptions{Interval: time.Hour} })
	ctx := context.Background()
	stop := service.Background(slog.Default(), r.n.repairLoop(20*time.Millisecond))
	defer stop()

	ref := r.acquire("test.vault/1")
	defer ref.Release()
	// The node ran alone: no peer, and the answer fails.
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, ref.Sync()); err == nil || !strings.Contains(err.Error(), "peer") {
		t.Fatalf("a peer answer with no peer named answered %v, want the tier unmet", err)
	}
	// A standby appears; the repair names it, seeds it from the record and
	// swaps it in, and the answer goes to it.
	mirr := serveMirror(t, r.local, "m1", "n1")
	var seq pos.Seg
	apptest.Eventually(t, 10*time.Second, "the repair to name the peer", func() bool {
		st, ok := r.n.Writer.Peer()
		return ok && st.Name == "m1"
	})
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, ref.Sync()); err != nil {
		t.Fatal(err)
	}
	if st, _ := r.n.Writer.Peer(); st.Frontier < 1 {
		t.Fatalf("the peer's frontier is %d, want the writes covered", st.Frontier)
	}
	seq = r.n.Writer.NextSeq() - 1
	if _, err := mirr.store.Head(ctx, seq); err != nil {
		t.Fatalf("the peer lacks segment %d: %v", seq, err)
	}

	// The peer's host is lost. Its answers fail, and the repair names the
	// next standby, seeding what the record has and continuing the stream
	// from there.
	mirr.down()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, ref.Sync()); err == nil || !strings.Contains(err.Error(), "peer") {
		t.Fatalf("a peer answer on a fallen peer answered %v, want the tier unmet", err)
	}
	mirr2 := serveMirror(t, r.local, "m2", "n1")
	apptest.Eventually(t, 10*time.Second, "the repair to replace the peer", func() bool {
		st, ok := r.n.Writer.Peer()
		return ok && st.Name == "m2" && !st.Out
	})
	// The replacement holds the run the record had and the segment the fallen
	// peer never took, and the answers go to it again.
	for s := pos.Seg(1); s <= r.n.Writer.NextSeq()-1; s++ {
		if _, err := mirr2.store.Head(ctx, s); err != nil {
			t.Fatalf("the replacement lacks segment %d: %v", s, err)
		}
	}
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, ref.Sync()); err != nil {
		t.Fatal(err)
	}
	// The next renewal publishes the new pairing.
	apptest.Eventually(t, 10*time.Second, "the lease to name the replacement", func() bool {
		lease, err := coord.NewRegistry(r.local, "n1", coord.RegistryOptions{}).Get(ctx, "n1")
		return err == nil && lease.Peer == "m2"
	})
}

// The repair waits for the record: while the record will not answer the
// seed's reads, the replacement cannot be built and the answers stay failed;
// once it answers again, the repair finishes and the answers go to the peer.
func TestTheRepairWaitsForTheRecord(t *testing.T) {
	r := newUnstartedMRig(t, nil)
	r.standbys = 1
	r.startNode()
	r.startShards(func(o *CatalogOptions) { o.Flush = FlushOptions{Interval: time.Hour} })
	ctx := context.Background()
	stop := service.Background(slog.Default(), r.n.repairLoop(20*time.Millisecond))
	defer stop()

	ref := r.acquire("test.vault/1")
	defer ref.Release()
	// No peer is named: the answer fails, and its cycle lands the entries in
	// the record all the same.
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, ref.Sync()); err == nil || !strings.Contains(err.Error(), "peer") {
		t.Fatalf("a peer answer with no peer named answered %v, want the tier unmet", err)
	}
	mirr := serveMirror(t, r.local, "m1", "n1")
	apptest.Eventually(t, 10*time.Second, "the repair to name the peer", func() bool {
		st, ok := r.n.Writer.Peer()
		return ok && st.Name == "m1"
	})
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, ref.Sync()); err != nil {
		t.Fatal(err)
	}
	// The peer's host is lost, and the record stops answering the reads a
	// seed is made of. The answers stay failed: there is nothing to swap in.
	mirr.down()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, ref.Sync()); err == nil || !strings.Contains(err.Error(), "peer") {
		t.Fatalf("a peer answer on a fallen peer answered %v, want the tier unmet", err)
	}
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && key != "" && strings.HasPrefix(key, journal.Root) &&
			(op == blobtest.OpGet || op == blobtest.OpGetRange || op == blobtest.OpHead) {
			return errors.New("the record is unreachable")
		}
		return nil
	})
	// The seed's reads fail and the writer keeps the fallen peer named.
	eventually(t, "the seed to fail on the record", func() bool {
		st, ok := r.n.Writer.Peer()
		return ok && st.Name == "m1" && st.Out
	})
	// The record answers again: the repair finishes, and the answers go to
	// the replacement.
	r.local.SetHook(nil)
	mirr2 := serveMirror(t, r.local, "m2", "n1")
	apptest.Eventually(t, 10*time.Second, "the repair to replace the peer", func() bool {
		st, ok := r.n.Writer.Peer()
		return ok && st.Name == "m2" && !st.Out
	})
	for s := pos.Seg(1); s <= r.n.Writer.NextSeq()-1; s++ {
		if _, err := mirr2.store.Head(ctx, s); err != nil {
			t.Fatalf("the replacement lacks segment %d: %v", s, err)
		}
	}
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, ref.Sync()); err != nil {
		t.Fatal(err)
	}
}

// The repair on a running cluster: the node whose peer died names the other
// survivor, and the journal moves to it — the lease's pairing moves at the
// next renewal, the replacement is seeded from the record, and the stream
// continues on it.
func TestTheRepairReplacesADeadPeerInTheCluster(t *testing.T) {
	c := newClusterEnv(t, 3, map[string]string{
		"SHARDLITE_STANDBYS":  "1",
		"SHARDLITE_LEASE_TTL": "1s",
	}, nil)
	// n1 and n2 name n0; n0 named none, having started alone. A shard on n1
	// gives its journal a segment, which lands in the record and on n0.
	c.nodes[1].create("test.audit/1")
	// n0 dies. Both repairs name the other survivor: n1's lease moves to n2
	// and n2's to n1.
	c.nodes[0].Kill()
	eventually(t, "the repairs to name replacements", func() bool {
		return nodeLease(t, c, "n1").Peer == "n2" && nodeLease(t, c, "n2").Peer == "n1"
	})
	// The replacement was seeded from the record: n2 holds the segment n0
	// carried, and the next one goes to it as the stream continues there.
	m := journal.NewMirror(http.DefaultClient, "http://"+c.nodes[2].Peer, "n1")
	apptest.Eventually(t, 10*time.Second, "n2 to hold n1's journal", func() bool {
		_, err := m.Head(context.Background(), 1)
		return err == nil
	})
	c.nodes[1].create("test.audit/2")
	apptest.Eventually(t, 10*time.Second, "n2 to hold the next segment", func() bool {
		_, err := m.Head(context.Background(), 2)
		return err == nil
	})
}
