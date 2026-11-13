package journal

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

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/tempfile"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/blobtest"
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
// that shapes blob storage by key and the backend's fault hook within reach.
type testStore struct {
	Store
	B *blob.Store
	L *blobtest.Hooked
}

func (s testStore) SetHook(h blobtest.Hook) { s.L.SetHook(h) }

func newStore(t *testing.T) testStore {
	t.Helper()
	l := blobtest.WithHook(blob.NewMemory())
	b := blob.NewStore(l, "n1", 1, blob.Options{Backoff: time.Millisecond})
	return testStore{Store: NewBlob(b, "n1"), B: b, L: l}
}

// testJournal is node's journal in b, written by writer at epoch.
func testJournal(b blob.Backend, node, writer cluster.Node, epoch pos.Epoch) *Blob {
	return NewBlob(blob.NewStore(b, writer, epoch, blob.Options{Backoff: time.Millisecond}), node)
}

// logOf makes a log holding b, and returns it with the range b is in. The caller
// closes the log.
func logOf(dir *tempfile.Dir, b []byte) (*tempfile.Log, tempfile.File, error) {
	l, err := dir.Create()
	if err != nil {
		return nil, tempfile.File{}, err
	}
	a, err := l.Append()
	if err != nil {
		l.Close()
		return nil, tempfile.File{}, err
	}
	a.Write(b)
	f, err := a.Finish()
	if err != nil {
		l.Close()
		return nil, tempfile.File{}, err
	}
	return l, f, nil
}

var testDir = tempfile.NewDir(filepath.Join(os.TempDir(), "shardlite-segment-test"))

// appendBytes queues one LTX file, held in a log of its own that is closed at
// once: the writer reads it through a dup.
func appendBytes(w *Writer, name, generation string, position pos.Position, ltxBytes []byte) error {
	n, err := shard.ParseName(name)
	if err != nil {
		return err
	}
	l, f, err := logOf(testDir, ltxBytes)
	if err != nil {
		return err
	}
	defer l.Close()
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
	w := NewWriter(store.Store, WriterOptions{})
	serve(t, w)

	const n = 100
	var wg errgroup.Group
	for i := range n {
		wg.Go(func() error {
			return appendBytes(w, fmt.Sprintf("users/%d", i), "g", pos.Position(i+1), testLTX(t, ltx.TXID(i+1), byte(i)))
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
		fmt.Sscanf(e.Shard.String(), "users/%d", &i)
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
	w := NewWriter(store.Store, WriterOptions{})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
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
	if _, err := store.B.Head(ctx, Key("n1", 1)); err != nil {
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
	w := NewWriter(store.Store, WriterOptions{})
	serve(t, w)
	// A flush after every append, many of them waiting at once.
	var g errgroup.Group
	for i := range 20 {
		mustAppend(t, w, "users/1", pos.Position(i+1), testLTX(t, ltx.TXID(i+1), 1))
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
	adopter := testJournal(store.L, "n1", "n2", 1)
	adopter.Create(ctx, 1, strings.NewReader(""))
	w := NewWriter(store.Store, WriterOptions{})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("flush got %v, want ErrFenced", err)
	}
	// Stopped for good: later appends fail at once and nothing is written.
	if err := appendBytes(w, "users/1", "g", 1, testLTX(t, 2, 1)); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
	if _, err := store.B.Head(ctx, Key("n1", 2)); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("segment 2 exists: %v", err)
	}
}

func TestFenceRecognised(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	store.B.Create(ctx, Key("n1", 7), strings.NewReader(""))
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
	w := NewWriter(store.Store, WriterOptions{})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := w.Flush(short); err == nil {
		t.Fatal("want the caller's timeout")
	}
	// A later commit queues behind it.
	mustAppend(t, w, "users/1", 2, testLTX(t, 2, 1))
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
	w := NewWriter(store.Store, WriterOptions{Retry: time.Millisecond})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 7))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if creates.Load() <= 4 {
		t.Fatalf("only %d create calls: Store's own retries were never exhausted", creates.Load())
	}
	if w.Flushes() != 1 || w.NextSeq() != 2 {
		t.Fatalf("flushes=%d next=%d", w.Flushes(), w.NextSeq())
	}
	es, err := ReadFooter(ctx, store.Store, 1)
	if err != nil || len(es) != 1 {
		t.Fatalf("%v %v", es, err)
	}
	if _, err := store.B.Head(ctx, Key("n1", 2)); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("segment 2: %v", err)
	}
	if info, _ := store.B.Head(ctx, Key("n1", 1)); info.WriteID.Node != "n1" || info.WriteID.Epoch != 1 {
		t.Fatalf("segment 1 written by %+v", info.WriteID)
	}
	// Nothing later is fenced either.
	mustAppend(t, w, "users/1", 2, testLTX(t, 2, 1))
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
	w := NewWriter(store.Store, WriterOptions{})
	stop := serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
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
	if err := appendBytes(w, "users/1", "g", 2, testLTX(t, 2, 1)); !errors.Is(err, ErrClosed) {
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
	w := NewWriter(store.Store, WriterOptions{})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	w.Flush(short)
	mustAppend(t, w, "users/1", 2, testLTX(t, 2, 1))
	// An adopter fences the journal while the writer is stuck.
	down.Store(false)
	adopter := testJournal(store.L, "n1", "n2", 1)
	if _, err := Fence(ctx, adopter, 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
}

// A PUT that succeeds but whose response is lost must not fence the writer:
// Store retries, finds its own write id on the object and succeeds.
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
	w := NewWriter(store.Store, WriterOptions{})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatalf("lost response: %v", err)
	}
	if creates.Load() != 2 {
		t.Fatalf("%d creates, want the lost one and its retry", creates.Load())
	}
	// And it carries on with the next number.
	mustAppend(t, w, "users/1", 2, testLTX(t, 2, 1))
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
	zombie := testJournal(store.L, "dead", "dead", 7)
	zombie.Create(ctx, 5, strings.NewReader("a zombie's segment"))
	adopter := testJournal(store.L, "dead", "n2", 1)

	var lost, creates atomic.Int32
	store.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after && key == Key("dead", 6) {
			creates.Add(1)
		}
		if op == blobtest.OpCreate && after && key == Key("dead", 6) && lost.Add(1) == 1 {
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
	w := NewWriter(zombie, WriterOptions{StartSeq: 6})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
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
	if _, err := store.B.Create(ctx, Key("dead", 9), strings.NewReader("")); err != nil {
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
	store.B.Create(ctx, Key("n1", 5), strings.NewReader("x"))
	store.B.Create(ctx, Key("n1", 7), strings.NewReader(""))
	store.B.Create(ctx, Key("n2", 2), strings.NewReader("x"))
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
	if e := f.err.Load(); e != nil && strings.HasPrefix(key, Root) {
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
	js := testJournal(fc, "n1", "n1", 1)
	rec := &recorder{}
	w := NewWriter(js, WriterOptions{Retry: time.Millisecond, Logger: slog.New(rec)})
	serve(t, w)

	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
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
	if _, err := store.Head(context.Background(), Key("n1", 1)); err != nil {
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
	w := NewWriter(store.Store, WriterOptions{Retry: time.Millisecond, Logger: slog.New(rec)})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := rec.errors(); n != 0 {
		t.Fatalf("%d error logs for failures that retrying fixes", n)
	}
}

// The segment's bytes are composed, not copied: the queued files are read where
// they are, through one dup of each log however many files it holds, which the
// writer closes once the segment is written or the writer is closed. The caller
// closes its own logs whenever it likes.
func TestQueuedFilesAreReadThroughDupsOfTheirLogs(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	dir := tempfile.NewDir(t.TempDir())
	var dups atomic.Int32
	dir.SetHook(func(op tempfile.Op) error {
		if op == tempfile.OpDup {
			dups.Add(1)
		}
		return nil
	})
	w := NewWriter(store.Store, WriterOptions{})
	stop := serve(t, w)

	// Three files in two logs: a log with two files of the same segment is
	// duplicated once.
	l1, err := dir.Create()
	if err != nil {
		t.Fatal(err)
	}
	l2, err := dir.Create()
	if err != nil {
		t.Fatal(err)
	}
	var files []tempfile.File
	for i, l := range []*tempfile.Log{l1, l1, l2} {
		a, err := l.Append()
		if err != nil {
			t.Fatal(err)
		}
		a.Write(testLTX(t, ltx.TXID(i+1), byte(i+1)))
		f, err := a.Finish()
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	for i, f := range files {
		if err := w.AppendFile(shard.MustParse(fmt.Sprintf("users/%d", i+1)), "g", 1, f); err != nil {
			t.Fatal(err)
		}
	}
	l1.Close()
	l2.Close() // the writer holds dups of its own
	if got := dups.Load(); got != 2 {
		t.Fatalf("%d dups for two logs, want 2", got)
	}
	w.mu.Lock()
	queued := w.pending[0].body
	w.mu.Unlock()
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// The writer counts the entries as written only after closing the files.
	if _, err := queued.Bytes(); err == nil {
		t.Fatal("the batch's descriptors are still open after the segment was written")
	}
	entries, err := ReadFooter(ctx, store.Store, 1)
	if err != nil || len(entries) != 3 {
		t.Fatalf("%v %v", entries, err)
	}
	for i, e := range entries {
		out, err := dir.Create()
		if err != nil {
			t.Fatal(err)
		}
		got, err := ReadEntryInto(ctx, store.Store, 1, e, out)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := got.Bytes()
		if !bytes.Equal(b, testLTX(t, ltx.TXID(i+1), byte(i+1))) {
			t.Fatalf("entry %d read back differently", i)
		}
		out.Close()
	}

	// Closed with a file still queued.
	l3, f3, err := logOf(dir, testLTX(t, 4, 4))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendFile(shard.MustParse("users/4"), "g", 1, f3); err != nil {
		t.Fatal(err)
	}
	l3.Close()
	w.mu.Lock()
	queued = w.pending[0].body
	w.mu.Unlock()
	stop()
	if err := w.Flush(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
	if _, err := queued.Bytes(); err == nil {
		t.Fatal("the descriptors of a file never written are still open after the writer was closed")
	}
}

// A log that cannot be duplicated fails the append, and nothing is queued.
func TestAppendFailsWhenTheLogCannotBeDuplicated(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	dir := tempfile.NewDir(t.TempDir())
	w := NewWriter(store.Store, WriterOptions{})
	serve(t, w)
	l, f, err := logOf(dir, testLTX(t, 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	boom := errors.New("too many open files")
	dir.SetHook(func(op tempfile.Op) error {
		if op == tempfile.OpDup {
			return boom
		}
		return nil
	})
	if err := w.AppendFile(shard.MustParse("users/1"), "g", 1, f); !errors.Is(err, boom) {
		t.Fatalf("got %v, want the dup's error", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) != 0 || w.appended != 0 {
		t.Fatalf("%d entries queued after a failed append", len(w.pending))
	}
}

// Scan reads footers a window at a time and hands their entries over in order,
// across windows, skipping fences and stopping at the first missing number (or
// failing on it, when it reads through to a given number).
func TestScanReadsWindowsInOrder(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	w := NewWriter(store.Store, WriterOptions{})
	serve(t, w)
	const n = 2*scanWindow + 10
	for i := pos.Position(1); i <= n; i++ {
		mustAppend(t, w, "users/1", i, testLTX(t, ltx.TXID(i), byte(i)))
		if err := w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// Two fences, one of them first in its window.
	fences := map[pos.Seg]bool{10: true, scanWindow + 1: true}
	for seq := range fences {
		if err := store.B.Delete(ctx, Key("n1", seq), ""); err != nil {
			t.Fatal(err)
		}
		if _, err := store.B.Create(ctx, Key("n1", seq), strings.NewReader("")); err != nil {
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
	if err := store.B.Delete(ctx, Key("n1", 100), ""); err != nil {
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
	w := NewWriter(store.Store, WriterOptions{})
	serve(t, w)
	sizes := []int{1, 1000} // a thousand entries make a footer larger than footerRead
	for _, entries := range sizes {
		for i := range entries {
			mustAppend(t, w, fmt.Sprintf("users/%d", i), 1, testLTX(t, 1, byte(i)))
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
func readRange(ctx context.Context, store Store, seq pos.Seg, off, n int64) ([]byte, error) {
	rc, err := store.GetRange(ctx, seq, off, n)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}
