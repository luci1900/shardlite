package segment

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// The peer (fanout.go): every segment goes to the peer beside the record, on
// the writer's own queue and in number order; the frontier is the last number
// it took; and PeerCovered says whether the peer could carry a `peer`
// acknowledgement.

// peerStore is the peer's disk as a home for n1's journal: a local database
// of its own, as another node's would be.
func peerStore(t *testing.T) *journal.Store {
	t.Helper()
	db, err := localdb.Open(filepath.Join(t.TempDir(), localdb.Name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return journal.NewStore(journal.NewLocal(db.DB, "n1"), "n1", 1, journal.Options{Backoff: time.Millisecond})
}

// peerMemory is the peer's store over its own memory backend, within the
// hook's reach: a test holds back or fails its creates through the hook.
type peerMemory struct {
	*journal.Store
	L *blobtest.Hooked
}

func (s peerMemory) SetHook(h blobtest.Hook) { s.L.SetHook(h) }

func newPeerMemory(t *testing.T) peerMemory {
	t.Helper()
	l := blobtest.WithHook(blob.NewMemory())
	return peerMemory{
		Store: journal.NewStore(journal.NewBlob(l, "n1"), "n1", 1, journal.Options{Backoff: time.Millisecond}),
		L:     l,
	}
}

// newPeerMemoryOnce is the peer's store with no retries of its own, so each ask
// the writer makes is one create there.
func newPeerMemoryOnce(t *testing.T) peerMemory {
	t.Helper()
	l := blobtest.WithHook(blob.NewMemory())
	return peerMemory{
		Store: journal.NewStore(journal.NewBlob(l, "n1"), "n1", 1, journal.Options{Backoff: time.Millisecond, Attempts: 1}),
		L:     l,
	}
}

// withPeer is the writer's options with the peer's store.
func withPeer(p *journal.Store) Options {
	return Options{Peer: &Mirror{Node: "m1", Journal: p}}
}

// countingBackend counts the creates the peer was asked for.
type countingBackend struct {
	journal.Backend
	creates *atomic.Int32
}

func (b *countingBackend) Create(ctx context.Context, seq pos.Seg, body blob.Body, id blob.WriteID) error {
	b.creates.Add(1)
	return b.Backend.Create(ctx, seq, body, id)
}

// The peer holds every segment, under the write id the record's create
// carried, and the peer is covered when it is caught up.
func TestSegmentsGoToThePeer(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	peer := peerStore(t)
	w := NewWriter(store.Store, withPeer(peer))
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	head, err := store.Store.Head(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, 5*time.Second, "the peer to hold the segment", func() bool {
		_, err := peer.Head(ctx, 1)
		return err == nil
	})
	info, err := peer.Head(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if info.WriteID != head.WriteID || info.Size != head.Size {
		t.Fatalf("the peer's segment %+v is not the record's %+v", info, head)
	}
	st, ok := w.Peer()
	if !ok || st.Name != "m1" || st.Frontier != 1 || st.Out {
		t.Fatalf("the peer's state is %+v (ok %v), want m1 at 1, in", st, ok)
	}
	// The counter the gate will one day wait for has moved: the entries of
	// the batches the frontier covers.
	if w.peer.written.Load() != 1 {
		t.Fatalf("the peer's counter is %d, want 1", w.peer.written.Load())
	}
	apptest.Eventually(t, 5*time.Second, "the peer to be covered", w.PeerCovered)
}

// The peer is sent in the writer's number order: a held send of one segment
// holds the next, even once the record has it, so a slow send delays later
// sends rather than letting them overtake it.
func TestSendsReachThePeerInNumberOrder(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	peer := newPeerMemory(t)
	release := make(chan struct{})
	started := make(chan struct{})
	var (
		mu   sync.Mutex
		seen []string
	)
	peer.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op != blobtest.OpCreate || after {
			return nil
		}
		mu.Lock()
		seen = append(seen, key)
		mu.Unlock()
		if key == journal.Key("n1", 1) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	w := NewWriter(store.Store, withPeer(peer.Store))
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	go w.Flush(ctx)
	<-started
	// Segment 2 lands in the record while the peer's send of 1 is held. It
	// waits for the peer's turn rather than go ahead of 1.
	mustAppend(t, w, "test.users/1", 2, testLTX(t, 2, 2))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Head(ctx, 2); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the peer took segment 2 ahead of 1 (%v)", err)
	}
	close(release)
	apptest.Eventually(t, 5*time.Second, "the peer to take both segments", func() bool {
		st, _ := w.Peer()
		return st.Frontier == 2
	})
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	want := []string{journal.Key("n1", 1), journal.Key("n1", 2)}
	if !slices.Equal(got, want) {
		t.Fatalf("the peer's creates came in %v, want %v", got, want)
	}
	if got := w.peer.written.Load(); got != 2 {
		t.Fatalf("the peer's counter is %d, want the entries of both segments", got)
	}
	apptest.Eventually(t, 5*time.Second, "the peer to be covered", w.PeerCovered)
}

// Entries count in the order the peer takes them, so a wait for a count is a
// wait for a run and not for a send that happened to land.
func TestPeerReachedFollowsTheTakenRun(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	peer := newPeerMemory(t)
	w := NewWriter(store.Store, withPeer(peer.Store))
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.PeerReached(1):
	case <-time.After(5 * time.Second):
		t.Fatal("the peer never reached the first entry")
	}
	select {
	case <-w.PeerReached(2):
		t.Fatal("the peer reached two entries with one batch built")
	default:
	}
	mustAppend(t, w, "test.users/1", 2, testLTX(t, 2, 2))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.PeerReached(2):
	case <-time.After(5 * time.Second):
		t.Fatal("the peer never reached the second entry")
	}
}

// A peer held mid-send never holds the flush: the record answers it.
func TestASlowPeerNeverHoldsTheFlush(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	held := newPeerMemory(t)
	release := make(chan struct{})
	started := make(chan struct{})
	held.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	w := NewWriter(store.Store, withPeer(held.Store))
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	flushed := make(chan error, 1)
	go func() { flushed <- w.Flush(ctx) }()
	<-started
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a held peer held the flush")
	}
	if w.PeerCovered() {
		t.Fatal("covered while the peer's send is still held")
	}
	close(release)
	apptest.Eventually(t, 5*time.Second, "the peer to be covered", w.PeerCovered)
}

// A peer that refuses a segment falls out at once: nothing more is sent to
// it, its frontier freezes where it was, PeerOut closes, PeerReady turns false
// and the flushes keep landing. The refusal here is a number a stale life's
// segment already holds at the peer, which is what a refusal is on the wire
// (412, a foreign write id).
func TestAPeerThatFallsOutTakesNoMore(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	db, err := localdb.Open(filepath.Join(t.TempDir(), localdb.Name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	// A stale life's segment holds number 1 at the peer: the same journal, a
	// newer epoch's write id, which the writer's create cannot displace.
	foreign := journal.NewStore(journal.NewLocal(db.DB, "n1"), "n1", 2, journal.Options{Backoff: time.Millisecond})
	if err := foreign.Create(ctx, 1, bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	var creates atomic.Int32
	w := NewWriter(store.Store, Options{Peer: &Mirror{Node: "m1", Journal: journal.NewStore(
		&countingBackend{Backend: journal.NewLocal(db.DB, "n1"), creates: &creates}, "n1", 1, journal.Options{Backoff: time.Millisecond})}})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, 5*time.Second, "the failing peer to fall out", func() bool {
		st, _ := w.Peer()
		return st.Out
	})
	st, _ := w.Peer()
	if got := st.Frontier; got != 0 {
		t.Fatalf("the fallen peer's frontier is %d, want 0", got)
	}
	select {
	case <-w.PeerOut():
	default:
		t.Fatal("PeerOut is not closed after the fall-out")
	}
	if w.PeerReady() {
		t.Fatal("a fallen peer is ready to carry an answer")
	}
	// Later segments are not even attempted: nothing more goes to it, and its
	// frontier never rewinds.
	for i := 2; i <= 3; i++ {
		mustAppend(t, w, "test.users/1", pos.Position(i), testLTX(t, ltx.TXID(i), 1))
		if err := w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := creates.Load(); n != 1 {
		t.Fatalf("the peer was asked %d times, want the one refusal", n)
	}
	st, _ = w.Peer()
	if got := st.Frontier; got != 0 {
		t.Fatalf("the fallen peer's frontier is %d, want it frozen at 0", got)
	}
	if w.PeerCovered() {
		t.Fatal("covered with the peer fallen out")
	}
	if _, err := store.Store.Head(ctx, 3); err != nil {
		t.Fatalf("the record stopped when the peer fell out: %v", err)
	}
}

// A peer whose stream keeps failing to the last attempt falls out as a
// refusing one does: the writer's own attempts bound the wait, and an answer
// waiting on the peer fails rather than hang.
func TestAPeerThatNeverAnswersFallsOut(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	peer := newPeerMemoryOnce(t)
	var creates atomic.Int32
	peer.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after {
			creates.Add(1)
			return blob.ErrNotApplied
		}
		return nil
	})
	w := NewWriter(store.Store, Options{Retry: time.Millisecond, Peer: &Mirror{Node: "m1", Journal: peer.Store}})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, 5*time.Second, "the unanswering peer to fall out", func() bool {
		st, _ := w.Peer()
		return st.Out
	})
	if got := creates.Load(); got != peerAttempts {
		t.Fatalf("the peer was asked %d times, want %d", got, peerAttempts)
	}
	select {
	case <-w.PeerOut():
	default:
		t.Fatal("PeerOut is not closed after the fall-out")
	}
	if w.PeerReady() {
		t.Fatal("a fallen peer is ready to carry an answer")
	}
	if st, _ := w.Peer(); st.Frontier != 0 {
		t.Fatalf("the fallen peer's frontier is %d, want 0", st.Frontier)
	}
	if got := w.peer.written.Load(); got != 0 {
		t.Fatalf("the fallen peer's counter is %d, want nothing counted", got)
	}
}

// A writer with no peer is never covered: its acknowledgements wait for the
// record, which is what a deployment that names no standbys gets.
func TestNoPeerIsNeverCovered(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	w := NewWriter(store.Store, Options{})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if w.PeerCovered() {
		t.Fatal("covered with no peer named")
	}
	if _, ok := w.Peer(); ok {
		t.Fatal("a peer state with no peer named")
	}
	select {
	case <-w.PeerOut():
	default:
		t.Fatal("PeerOut is not closed with no peer named")
	}
}

// A batch is dropped, its files closed, once the record, the local store and
// the peer have all settled their side of it.
func TestABatchIsDroppedWhenEverySideSettles(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	peer := newPeerMemory(t)
	local := localStore(t)
	dir := t.TempDir()
	w := NewWriter(store.Store, Options{Local: local, Peer: &Mirror{Node: "m1", Journal: peer.Store}})
	serve(t, w)
	name, err := shard.ParseName("test.users/1")
	if err != nil {
		t.Fatal(err)
	}
	// The entry's bytes live in a log of their own, which the writer reads through
	// a dup, so the batch being dropped is the dup being closed.
	l, f, err := logOf(tempfile.NewDir(dir), testLTX(t, 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendFile(name, "g", 1, f); err != nil {
		t.Fatal(err)
	}
	l.Close() // the writer holds a dup of its own
	w.mu.Lock()
	queued := w.pending[0].body
	w.mu.Unlock()
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, 5*time.Second, "the batch to be dropped", func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return len(w.batches) == 0
	})
	if _, err := peer.Head(ctx, 1); err != nil {
		t.Fatalf("the peer never took the segment: %v", err)
	}
	if got := w.Written(StoreLocal); got != 1 {
		t.Fatalf("the local store has %d entries, want 1", got)
	}
	if _, err := queued.Bytes(); err == nil {
		t.Fatal("the batch's descriptors are still open")
	}
}

// Stopping the writer waits for the peer's send before it closes the batch
// files it reads from, and gives the send up as the writer's own rather than
// blaming the peer.
func TestStopGivesThePeerSendUp(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	held := newPeerMemory(t)
	started := make(chan struct{})
	held.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after {
			close(started)
			<-ctx.Done() // the send ends when the writer's context does
			return ctx.Err()
		}
		return nil
	})
	w := NewWriter(store.Store, withPeer(held.Store))
	stop := serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	go w.Flush(ctx)
	<-started
	stop() // must wait for the send, not close the batch under it
	if st, _ := w.Peer(); st.Out {
		t.Fatal("the peer was blamed for the writer's own stop")
	}
	if got := w.peer.written.Load(); got != 0 {
		t.Fatalf("the peer's counter is %d, want nothing counted for a send given up", got)
	}
}

// Repair swaps a fallen peer for a replacement seeded to the number the seed
// ran to: the next segment goes to the replacement alone, its frontier and
// counter continue where the old peer froze, and a swap offered a stale
// number is refused rather than leave a hole.
func TestRepairSwapsThePeer(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	fallen, fresh := peerStore(t), peerStore(t)
	w := NewWriter(store.Store, withPeer(fallen))
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, 5*time.Second, "the peer to hold the segment", func() bool {
		_, err := fallen.Head(ctx, 1)
		return err == nil
	})
	w.peer.fall() // the seed's premise: the peer is out, the writer owes nothing to it

	// A swap at the number the writer has built takes: the replacement starts
	// at 2 with the seed's entry behind it.
	if !w.Repair(Mirror{Node: "m2", Journal: fresh}, 1, 1) {
		t.Fatal("the repair at the seed's number did not take")
	}
	st, ok := w.Peer()
	if !ok || st.Name != "m2" || st.Frontier != 1 || st.Out {
		t.Fatalf("the replacement's state is %+v (ok %v), want m2 at 1, in", st, ok)
	}
	if w.peer.written.Load() != 1 {
		t.Fatalf("the replacement's counter is %d, want the seed's 1", w.peer.written.Load())
	}
	mustAppend(t, w, "test.users/1", 2, testLTX(t, 2, 2))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, 5*time.Second, "the replacement to hold the segment", func() bool {
		_, err := fresh.Head(ctx, 2)
		return err == nil
	})
	if _, err := fallen.Head(ctx, 2); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the fallen peer got segment 2 (%v), want nothing more sent to it", err)
	}
	st, _ = w.Peer()
	if st.Frontier != 2 {
		t.Fatalf("the replacement's frontier is %d, want 2", st.Frontier)
	}
	if got := w.peer.written.Load(); got != 2 {
		t.Fatalf("the replacement's counter is %d, want 2", got)
	}

	// A swap at a number behind what the writer built is refused: the seed
	// would leave a hole the writer has already filled elsewhere.
	if w.Repair(Mirror{Node: "m3", Journal: fresh}, 1, 1) {
		t.Fatal("a stale repair took")
	}
	if st, _ := w.Peer(); st.Name != "m2" {
		t.Fatalf("the peer is %s after a refused swap, want m2", st.Name)
	}
}

// A repair hands the stream over by draining the writer's queue: the batches
// waiting there are ones the seed copied, so the swap finishes them without
// sending them to the fallen peer, and the replacement takes the batches built
// after it with its frontier and counter continuous.
func TestRepairDrainsTheQueuedBatches(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	fallen := newPeerMemory(t)
	release := make(chan struct{})
	started := make(chan struct{})
	var creates atomic.Int32
	fallen.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op != blobtest.OpCreate || after {
			return nil
		}
		if creates.Add(1) > 1 {
			return errors.New("the peer is gone")
		}
		close(started)
		select {
		case <-release:
			return errors.New("the peer is gone")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	w := NewWriter(store.Store, withPeer(fallen.Store))
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	go w.Flush(ctx)
	<-started
	// Segments 2 and 3 land in the record while the peer's send of 1 is held:
	// they wait in the writer's queue.
	for i := 2; i <= 3; i++ {
		mustAppend(t, w, "test.users/1", pos.Position(i), testLTX(t, ltx.TXID(i), 1))
		if err := w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := w.built.Load(); got != 3 {
		t.Fatalf("the writer built to %d, want 3", got)
	}
	// The peer falls out and a repair swaps in a replacement seeded to the
	// stream's top: the queued batches are ones the seed already copied.
	w.peer.fall()
	fresh := peerStore(t)
	if !w.Repair(Mirror{Node: "m2", Journal: fresh}, 3, 3) {
		t.Fatal("the repair at the seed's number did not take")
	}
	apptest.Eventually(t, 5*time.Second, "the queued batches to finish", func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return len(w.batches) == 1 // only the held send of segment 1 is still open
	})
	if got := creates.Load(); got != 1 {
		t.Fatalf("the fallen peer was asked %d times, want the one held send", got)
	}
	// The next segment goes to the replacement, whose frontier and counter
	// continue from the seed.
	mustAppend(t, w, "test.users/1", 4, testLTX(t, ltx.TXID(4), 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	close(release) // the held send ends, and the queue moves to the replacement
	apptest.Eventually(t, 5*time.Second, "the replacement to take segment 4", func() bool {
		_, err := fresh.Head(ctx, 4)
		return err == nil
	})
	st, ok := w.Peer()
	if !ok || st.Name != "m2" || st.Frontier != 4 || st.Out {
		t.Fatalf("the replacement's state is %+v (ok %v), want m2 at 4, in", st, ok)
	}
	if got := w.peer.written.Load(); got != 4 {
		t.Fatalf("the replacement's counter is %d, want the seed's 3 and the next entry", got)
	}
}
