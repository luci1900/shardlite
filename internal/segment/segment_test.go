package segment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/spool"
)

// serve serves w until the test ends, or until stop is called.
func serve(t testing.TB, w *Writer) (stop func()) {
	stop = service.Background(slog.Default(), w)
	t.Cleanup(stop)
	return stop
}

// testLTX builds a small valid LTX file for txid.
func testLTX(t testing.TB, txid ltx.TXID, fill byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: 4096, Commit: 1, MinTXID: txid, MaxTXID: txid, Timestamp: 1}); err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, bytes.Repeat([]byte{fill}, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// testStore is n1's journal store over a memory backend, with the blob store
// that shapes the record by key and the backend's fault hook within reach.
type testStore struct {
	*journal.Store
	B *blob.Store
	L *blobtest.Hooked
}

func (s testStore) SetHook(h blobtest.Hook) { s.L.SetHook(h) }

func newStore(t *testing.T) testStore {
	t.Helper()
	l := blobtest.WithHook(blob.NewMemory())
	return testStore{
		Store: journal.NewStore(journal.NewBlob(l, "n1"), "n1", 1, journal.Options{Backoff: time.Millisecond}),
		B:     blob.NewStore(l, "n1", 1, blob.Options{Backoff: time.Millisecond}),
		L:     l,
	}
}

// appendBytes queues one LTX file held in memory.
func appendBytes(w *Writer, name, generation string, position pos.Position, ltxBytes []byte) error {
	n, err := shard.ParseName(name)
	if err != nil {
		return err
	}
	f := spool.Bytes(ltxBytes)
	defer f.Close()
	return w.AppendFile(n, shard.Gen(generation), position, f)
}

// mustAppend queues one LTX file of shard (generation "g").
func mustAppend(t testing.TB, w *Writer, shard string, position pos.Position, ltxBytes []byte) {
	t.Helper()
	if err := appendBytes(w, shard, "g", position, ltxBytes); err != nil {
		t.Fatal(err)
	}
}

func TestManyGoroutinesOneSegment(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	w := NewWriter(store.Store, Options{})
	serve(t, w)

	const n = 100
	var wg errgroup.Group
	for i := range n {
		wg.Go(func() error {
			return appendBytes(w, fmt.Sprintf("test.users/%d", i), "g", pos.Position(i+1), testLTX(t, ltx.TXID(i+1), byte(i)))
		})
	}
	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if w.Flushes() != 1 {
		t.Fatalf("flushes=%d, want 1", w.Flushes())
	}
	entries, err := ReadFooter(ctx, store.Store, 1)
	if err != nil || len(entries) != n {
		t.Fatalf("entries=%d err=%v", len(entries), err)
	}
	// Round trip of every entry.
	for _, e := range entries {
		b, err := readRange(ctx, store.Store, 1, e.Offset, e.Length)
		if err != nil {
			t.Fatal(err)
		}
		hdr, _, err := ltx.PeekHeader(bytes.NewReader(b))
		if err != nil || pos.Count(hdr.MaxTXID-hdr.MinTXID)+1 != e.Count {
			t.Fatalf("%s: hdr=%v err=%v", e.Shard, hdr.MinTXID, err)
		}
		var i int
		fmt.Sscanf(e.Shard.String(), "test.users/%d", &i)
		if e.Position != pos.Position(i+1) || e.Count != 1 {
			t.Fatalf("%s: position %d count %d", e.Shard, e.Position, e.Count)
		}
		if !bytes.Equal(b, testLTX(t, ltx.TXID(i+1), byte(i))) {
			t.Fatalf("%s: body differs", e.Shard)
		}
	}
	// Nothing pending: an empty interval writes nothing.
	if err := w.Flush(ctx); err != nil || w.Flushes() != 1 {
		t.Fatalf("empty flush wrote a segment (flushes=%d err=%v)", w.Flushes(), err)
	}
}

// localStore is the node's own disk as a journal's home: the local database
// in a temporary directory.
func localStore(t *testing.T) *journal.Store {
	t.Helper()
	db, err := localdb.Open(filepath.Join(t.TempDir(), localdb.Name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return journal.NewStore(journal.NewLocal(db.DB, "n1"), "n1", 1, journal.Options{Backoff: time.Millisecond})
}

// The local copy is written beside the record's, out of the same body: a slow
// record doesn't hold the local tier's acknowledgement, and the local copy
// goes once the record has the segment.
func TestLocalCopyIsWrittenBesideTheRecord(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	local := localStore(t)
	// The record's write is held back.
	release := make(chan struct{})
	held := make(chan struct{})
	var once atomic.Bool
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after && once.CompareAndSwap(false, true) {
			close(held)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	w := NewWriter(store.Store, Options{Local: local})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	go w.Flush(ctx)
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the record's write did not start")
	}
	// The local store has the segment while the record's write is held, and
	// the counter the gate waits for has moved.
	apptest.Eventually(t, 5*time.Second, "the local copy", func() bool {
		info, err := local.Head(ctx, 1)
		return err == nil && info.Size > 0
	})
	if w.Written(StoreLocal) != 1 {
		t.Fatalf("the local store has %d entries, want 1", w.Written(StoreLocal))
	}
	if w.Written(StoreRecord) != 0 {
		t.Fatalf("the record has %d entries, want none", w.Written(StoreRecord))
	}
	// The bytes are the same in both stores: the body is composed, not copied.
	localBytes, err := io.ReadAll(mustGet(ctx, local, 1))
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	recordBytes, err := io.ReadAll(mustGet(ctx, store.Store, 1))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(localBytes, recordBytes) {
		t.Fatal("the local copy and the record's differ")
	}
	// The local copy goes once the record has the segment.
	apptest.Eventually(t, 5*time.Second, "the local copy to go", func() bool {
		_, err := local.Head(ctx, 1)
		return errors.Is(err, blob.ErrNotFound)
	})
}

// mustGet reads a segment whole.
func mustGet(ctx context.Context, s *journal.Store, seq pos.Seg) io.ReadCloser {
	rc, err := s.Get(ctx, seq)
	if err != nil {
		panic(err)
	}
	return rc
}

func TestFlushReturnsAfterPut(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	release := make(chan struct{})
	var inPut atomic.Bool
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after {
			inPut.Store(true)
			<-release
		}
		return nil
	})
	w := NewWriter(store.Store, Options{})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	flushed := make(chan error, 1)
	go func() { flushed <- w.Flush(ctx) }()
	apptest.Eventually(t, 5*time.Second, "the PUT to start", inPut.Load)
	select { // the PUT is held in the hook, so the flush cannot have returned
	case err := <-flushed:
		t.Fatalf("flush returned (%v) before the PUT finished", err)
	default:
	}
	close(release)
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	if _, err := store.B.Head(ctx, journal.Key("n1", 1)); err != nil {
		t.Fatal(err)
	}
}

func TestSlowPutDelaysNextSegment(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	var cur, max atomic.Int32
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op != blobtest.OpCreate {
			return nil
		}
		if !after {
			n := cur.Add(1)
			for {
				m := max.Load()
				if n <= m || max.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(80 * time.Millisecond) // many intervals
		} else {
			cur.Add(-1)
		}
		return nil
	})
	w := NewWriter(store.Store, Options{})
	serve(t, w)
	// A flush after every append, many of them waiting at once.
	var g errgroup.Group
	for i := range 20 {
		mustAppend(t, w, "test.users/1", pos.Position(i+1), testLTX(t, ltx.TXID(i+1), 1))
		g.Go(func() error { return w.Flush(context.Background()) })
		time.Sleep(10 * time.Millisecond)
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	if max.Load() != 1 {
		t.Fatalf("saw %d concurrent uploads", max.Load())
	}
	// Entries that arrived during an upload went into later segments, not out of order.
	var last pos.Position
	for seq := pos.Seg(1); seq <= pos.Seg(w.Flushes()); seq++ {
		es, err := ReadFooter(context.Background(), store.Store, seq)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range es {
			if e.Position <= last {
				t.Fatalf("segment %d: position %d after %d", seq, e.Position, last)
			}
			last = e.Last()
		}
	}
	if last != 20 {
		t.Fatalf("last txid %d", last)
	}
}

func TestErrExistsFencesWriter(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	// A newer writer already took segment 1 (here, as a fence: a fence of
	// another writer's, so its write id is not this writer's).
	adopter := journal.NewStore(journal.NewBlob(store.L, "n1"), "n2", 1, journal.Options{})
	adopter.Create(ctx, 1, strings.NewReader(""))
	w := NewWriter(store.Store, Options{})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("flush got %v, want ErrFenced", err)
	}
	// Stopped for good: later appends fail at once and nothing is written.
	if err := appendBytes(w, "test.users/1", "g", 1, testLTX(t, 2, 1)); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
	if _, err := store.B.Head(ctx, journal.Key("n1", 2)); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("segment 2 exists: %v", err)
	}
}

func TestFenceRecognised(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	store.B.Create(ctx, journal.Key("n1", 7), strings.NewReader(""))
	if _, err := ReadFooter(ctx, store.Store, 7); !errors.Is(err, ErrFence) {
		t.Fatalf("got %v, want ErrFence", err)
	}
}

// A segment that can't be written is not given up: Flush keeps waiting,
// nothing newer is written ahead of it, and it goes out under the same number
// once storage is back.
func TestFailedSegmentIsRetriedNotDropped(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	var fail atomic.Bool
	fail.Store(true)
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after && fail.Load() {
			return errors.New("storage down")
		}
		return nil
	})
	w := NewWriter(store.Store, Options{})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := w.Flush(short); err == nil {
		t.Fatal("want the caller's timeout")
	}
	// A later commit queues behind it.
	mustAppend(t, w, "test.users/1", 2, testLTX(t, 2, 1))
	fail.Store(false)
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for seq, want := range map[pos.Seg]pos.Position{1: 1, 2: 2} {
		es, err := ReadFooter(ctx, store.Store, seq)
		if err != nil || len(es) != 1 || es[0].Position != want {
			t.Fatalf("segment %d: %v %v, want position %d", seq, es, err, want)
		}
	}
	if _, err := ReadFooter(ctx, store.Store, 3); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("segment 3: %v", err)
	}
}

// A long run of lost responses and failed requests ends with the segment
// written once, under its own number, and no fence.
func TestLongRunOfFaultsWritesSegmentOnce(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	// 60 requests fail before they happen (some as timeouts of the read that checks
	// ownership). Then one is applied and its response lost, and the reads that
	// check ownership fail 10 times before they answer.
	var creates, heads atomic.Int32
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		switch {
		case op == blobtest.OpCreate && !after && creates.Add(1) <= 60:
			return errors.New("request failed")
		case op == blobtest.OpCreate && after && creates.Load() == 61:
			return errors.New("response lost")
		case op == blobtest.OpHead && !after && creates.Load() > 60 && heads.Add(1) <= 10:
			return errors.New("head failed")
		}
		return nil
	})
	w := NewWriter(store.Store, Options{Retry: time.Millisecond})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 7))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if creates.Load() <= 4 {
		t.Fatalf("only %d create calls: journal.Store's own retries were never exhausted", creates.Load())
	}
	if w.Flushes() != 1 || w.NextSeq() != 2 {
		t.Fatalf("flushes=%d next=%d", w.Flushes(), w.NextSeq())
	}
	es, err := ReadFooter(ctx, store.Store, 1)
	if err != nil || len(es) != 1 {
		t.Fatalf("%v %v", es, err)
	}
	if _, err := store.B.Head(ctx, journal.Key("n1", 2)); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("segment 2: %v", err)
	}
	if info, _ := store.B.Head(ctx, journal.Key("n1", 1)); info.WriteID.Node != "n1" || info.WriteID.Epoch != 1 {
		t.Fatalf("segment 1 written by %+v", info.WriteID)
	}
	// Nothing later is fenced either.
	mustAppend(t, w, "test.users/1", 2, testLTX(t, 2, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

// A Flush waiting for a segment that can't be written returns ErrClosed when
// the writer is closed, and so does every Flush and Append after it.
func TestCloseEndsAWaitingFlush(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	inPut := make(chan struct{}, 1)
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after {
			select {
			case inPut <- struct{}{}:
			default:
			}
			return errors.New("storage down")
		}
		return nil
	})
	w := NewWriter(store.Store, Options{})
	stop := serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	flushed := make(chan error, 1)
	go func() { flushed <- w.Flush(context.Background()) }()
	<-inPut
	stop()
	select {
	case err := <-flushed:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("flush got %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the flush still waits after Close")
	}
	if err := w.Flush(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("flush after Close got %v, want ErrClosed", err)
	}
	if err := appendBytes(w, "test.users/1", "g", 2, testLTX(t, 2, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after Close got %v, want ErrClosed", err)
	}
}

// A fence found while retrying fails the flush, of what queued behind it too.
func TestFenceFoundWhileRetrying(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	var down atomic.Bool
	down.Store(true)
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after && down.Load() {
			return errors.New("storage down")
		}
		return nil
	})
	w := NewWriter(store.Store, Options{})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	w.Flush(short)
	mustAppend(t, w, "test.users/1", 2, testLTX(t, 2, 1))
	// An adopter fences the journal while the writer is stuck.
	down.Store(false)
	adopter := journal.NewStore(journal.NewBlob(store.L, "n1"), "n2", 1, journal.Options{})
	if _, err := Fence(ctx, adopter, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
}

// A PUT that succeeds but whose response is lost must not fence the writer:
// journal.Store retries, finds its own write id on the object and succeeds.
func TestLostResponseIsNotAFence(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	var lost, creates atomic.Int32
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after {
			creates.Add(1)
		}
		if op == blobtest.OpCreate && after && lost.Add(1) == 1 {
			return errors.New("response lost")
		}
		return nil
	})
	w := NewWriter(store.Store, Options{})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("lost response: %v", err)
	}
	if creates.Load() != 2 {
		t.Fatalf("%d creates, want the lost one and its retry", creates.Load())
	}
	// And it carries on with the next number.
	mustAppend(t, w, "test.users/1", 2, testLTX(t, 2, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFooter(ctx, store.Store, 2); err != nil {
		t.Fatal(err)
	}
}

// An adopter creates an empty fence at the first free number. If the response
// to that create is lost, the fence is still recognised as its own. If a
// zombie's segment took the number first, the adopter moves on.
func TestAdopterFence(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	zombie := journal.NewStore(journal.NewBlob(store.L, "dead"), "dead", 7, journal.Options{})
	zombie.Create(ctx, 5, strings.NewReader("a zombie's segment"))
	adopter := journal.NewStore(journal.NewBlob(store.L, "dead"), "n2", 1, journal.Options{})

	var lost, creates atomic.Int32
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after && key == journal.Key("dead", 6) {
			creates.Add(1)
		}
		if op == blobtest.OpCreate && after && key == journal.Key("dead", 6) && lost.Add(1) == 1 {
			return errors.New("response lost")
		}
		return nil
	})
	seq, err := Fence(ctx, adopter, 5)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 6 {
		t.Fatalf("fence at %d, want 6 (5 was the zombie's)", seq)
	}
	if creates.Load() != 2 {
		t.Fatalf("%d creates at 6, want the lost one and its retry", creates.Load())
	}
	if _, err := ReadFooter(ctx, adopter, 6); !errors.Is(err, ErrFence) {
		t.Fatalf("got %v, want ErrFence", err)
	}
	// The zombie's own writer now finds its next numbers taken and is fenced.
	store.SetHook(nil)
	w := NewWriter(zombie, Options{StartSeq: 6})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	select {
	case <-w.Fenced():
		t.Fatal("Fenced is closed before the writer was fenced")
	default:
	}
	if err := w.Flush(ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
	select {
	case <-w.Fenced():
	default:
		t.Fatal("Fenced is not closed after the writer was fenced")
	}
	// A number already taken, by anyone, is skipped.
	if _, err := store.B.Create(ctx, journal.Key("dead", 9), strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
	if seq, err := Fence(ctx, adopter, 9); err != nil || seq != 10 {
		t.Fatalf("seq=%d err=%v, want 10", seq, err)
	}
}

func TestBounds(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	if lo, hi, err := Bounds(ctx, store.Store); err != nil || lo != 0 || hi != 0 {
		t.Fatalf("empty: %d %d %v", lo, hi, err)
	}
	store.B.Create(ctx, journal.Key("n1", 5), strings.NewReader("x"))
	store.B.Create(ctx, journal.Key("n1", 7), strings.NewReader(""))
	store.B.Create(ctx, journal.Key("n2", 2), strings.NewReader("x"))
	if lo, hi, err := Bounds(ctx, store.Store); err != nil || lo != 5 || hi != 7 {
		t.Fatalf("got %d %d %v, want 5 7", lo, hi, err)
	}
}

// failCreate makes every Create of a segment fail with a plain error, one that
// is neither "not applied" nor "unknown": retrying can't fix it.
type failCreate struct {
	blob.Backend
	err atomic.Pointer[error]
}

func (f *failCreate) Create(ctx context.Context, key string, body io.ReadSeeker, id blob.WriteID) (blob.ETag, error) {
	if e := f.err.Load(); e != nil && strings.HasPrefix(key, journal.Root) {
		return "", *e
	}
	return f.Backend.Create(ctx, key, body, id)
}

type recorder struct {
	recs apptest.Log[slog.Record]
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.recs.Add(rec)
	return nil
}
func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }
func (r *recorder) errors() int {
	n := 0
	for _, rec := range r.recs.All() {
		if rec.Level >= slog.LevelError {
			n++
		}
	}
	return n
}

func TestErrorsRetryingCannotFixAreLogged(t *testing.T) {
	t.Parallel()
	fc := &failCreate{Backend: blob.NewMemory()}
	denied := errors.New("permission denied")
	fc.err.Store(&denied)
	store := blob.NewStore(fc, "n1", 1, blob.Options{Backoff: time.Millisecond})
	js := journal.NewStore(journal.NewBlob(fc, "n1"), "n1", 1, journal.Options{Backoff: time.Millisecond})
	rec := &recorder{}
	w := NewWriter(js, Options{Retry: time.Millisecond, Logger: slog.New(rec)})
	serve(t, w)

	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	// The caller gives up at once: the uploader goes on alone.
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := w.Flush(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("flush got %v, want the caller's timeout", err)
	}
	apptest.Eventually(t, 5*time.Second, "three error logs", func() bool { return rec.errors() >= 3 })
	// The permissions are fixed: the same segment goes out.
	fc.err.Store(nil)
	if err := w.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Head(context.Background(), journal.Key("n1", 1)); err != nil {
		t.Fatal(err)
	}
}

func TestTransientTroubleIsNotLoggedAsUnfixable(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	var fails atomic.Int32
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after && fails.Add(1) <= 20 {
			return errors.New("storage unavailable")
		}
		return nil
	})
	rec := &recorder{}
	w := NewWriter(store.Store, Options{Retry: time.Millisecond, Logger: slog.New(rec)})
	serve(t, w)
	mustAppend(t, w, "test.users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := rec.errors(); n != 0 {
		t.Fatalf("%d error logs for failures that retrying fixes", n)
	}
}

// The segment's bytes are composed, not spooled: the queued files are read
// where they are, and nothing is left behind when the segment is written, or
// when the writer is closed with files pending.
func TestLargeSegmentsGoThroughTemporaryFiles(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	dir := t.TempDir()
	left := func() int {
		ents, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		return len(ents)
	}
	w := NewWriter(store.Store, Options{})
	stop := serve(t, w)

	var seen atomic.Int32
	store.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after {
			seen.Store(int32(left()))
		}
		return nil
	})
	// Two files, each larger than the limit and held in a file of their own.
	var files []*spool.File
	for i := range 2 {
		f, err := spool.Collect(dir, 100, bytes.NewReader(testLTX(t, ltx.TXID(i+1), byte(i+1))))
		if err != nil {
			t.Fatal(err)
		}
		if !f.InFile() {
			t.Fatal("test file is small")
		}
		files = append(files, f)
	}
	for i, f := range files {
		if err := w.AppendFile(shard.MustParse(fmt.Sprintf("test.users/%d", i+1)), "g", 1, f); err != nil {
			t.Fatal(err)
		}
		f.Close() // the writer holds its own reference
	}
	if left() != 2 {
		t.Fatalf("%d temporary files with two queued, want 2", left())
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// While the upload ran, the segment had no file of its own: the queued
	// files were read where they were.
	if seen.Load() != 2 {
		t.Fatalf("%d temporary files during the upload, want the queued ones", seen.Load())
	}
	if left() != 0 {
		t.Fatalf("%d temporary files after the segment was written", left())
	}
	entries, err := ReadFooter(ctx, store.Store, 1)
	if err != nil || len(entries) != 2 {
		t.Fatalf("%v %v", entries, err)
	}
	for i, e := range entries {
		got, err := ReadEntryFile(ctx, store.Store, 1, e, dir, 100)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := got.Bytes()
		if !bytes.Equal(b, testLTX(t, ltx.TXID(i+1), byte(i+1))) {
			t.Fatalf("entry %d read back differently", i)
		}
		got.Close()
	}

	// Closed with a file still queued.
	f, _ := spool.Collect(dir, 100, bytes.NewReader(testLTX(t, 3, 3)))
	if err := w.AppendFile(shard.MustParse("test.users/3"), "g", 1, f); err != nil {
		t.Fatal(err)
	}
	f.Close()
	stop()
	if err := w.Flush(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
	if left() != 0 {
		t.Fatalf("%d temporary files after the writer was closed", left())
	}
}

// Scan reads footers a window at a time and hands their entries over in order,
// across windows, skipping fences and stopping at the first missing number (or
// failing on it, when it reads through to a given number).
func TestScanReadsWindowsInOrder(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	w := NewWriter(store.Store, Options{})
	serve(t, w)
	const n = 2*scanWindow + 10
	for i := pos.Position(1); i <= n; i++ {
		mustAppend(t, w, "test.users/1", i, testLTX(t, ltx.TXID(i), byte(i)))
		if err := w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Two fences, one of them first in its window.
	fences := map[pos.Seg]bool{10: true, scanWindow + 1: true}
	for seq := range fences {
		if err := store.B.Delete(ctx, journal.Key("n1", seq), ""); err != nil {
			t.Fatal(err)
		}
		if _, err := store.B.Create(ctx, journal.Key("n1", seq), strings.NewReader("")); err != nil {
			t.Fatal(err)
		}
	}
	scan := func(to pos.Seg) ([]pos.Seg, error) {
		var seqs []pos.Seg
		err := Scan(ctx, store.Store, 1, to, func(seq pos.Seg, e Entry) error {
			if e.Position != pos.Position(seq) {
				return fmt.Errorf("segment %d holds position %d", seq, e.Position)
			}
			seqs = append(seqs, seq)
			return nil
		})
		return seqs, err
	}
	want := func(last pos.Seg) (seqs []pos.Seg) {
		for seq := pos.Seg(1); seq <= last; seq++ {
			if !fences[seq] {
				seqs = append(seqs, seq)
			}
		}
		return seqs
	}
	for _, to := range []pos.Seg{0, pos.Seg(n)} {
		if got, err := scan(to); err != nil || !slices.Equal(got, want(n)) {
			t.Fatalf("to %d: %v %v", to, got, err)
		}
	}
	// A number missing below the end.
	if err := store.B.Delete(ctx, journal.Key("n1", 100), ""); err != nil {
		t.Fatal(err)
	}
	if got, err := scan(0); err != nil || !slices.Equal(got, want(99)) {
		t.Fatalf("to 0 with 100 missing: %v %v", got, err)
	}
	if _, err := scan(n); !errors.Is(err, ErrMissing) || !strings.Contains(err.Error(), segName(store.Store, 100)) {
		t.Fatalf("through 100 missing: %v", err)
	}
}

// ReadFooter reads a segment's end once, which holds the footer of all but the
// largest segments, and a large footer with a second read.
func TestFooterReadsTheEndOnce(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	w := NewWriter(store.Store, Options{})
	serve(t, w)
	sizes := []int{1, 1000} // a thousand entries make a footer larger than footerRead
	for _, entries := range sizes {
		for i := range entries {
			mustAppend(t, w, fmt.Sprintf("test.users/%d", i), 1, testLTX(t, 1, byte(i)))
		}
		if err := w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var ranges, heads atomic.Int32
	store.L.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if after {
			return nil
		}
		switch op {
		case blobtest.OpGetRange:
			ranges.Add(1)
		case blobtest.OpHead:
			heads.Add(1)
		case blobtest.OpCreate, blobtest.OpUpdate, blobtest.OpGet, blobtest.OpDelete, blobtest.OpList:
			return nil
		}
		return nil
	})
	for i, entries := range sizes {
		ranges.Store(0)
		heads.Store(0)
		got, err := ReadFooter(ctx, store.Store, pos.Seg(i+1))
		if err != nil || len(got) != entries || int(ranges.Load()) != i+1 {
			t.Fatalf("%d entries: read %d with %d range reads, want %d: %v", entries, len(got), ranges.Load(), i+1, err)
		}
		if heads.Load() != 0 {
			t.Fatalf("%d entries: read the head %d times, want none: a footer is read from the end", entries, heads.Load())
		}
	}
}

// readRange reads a range of a segment's bytes.
func readRange(ctx context.Context, store *journal.Store, seq pos.Seg, off, n int64) ([]byte, error) {
	rc, err := store.GetRange(ctx, seq, off, n)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}
