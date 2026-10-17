package segment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/service"
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

// testStore is a Store over a memory backend, with the backend's fault hook within reach.
type testStore struct {
	*blob.Store
	L *blob.Hooked
}

func (s testStore) SetHook(h blob.Hook) { s.L.SetHook(h) }

func newStore(t *testing.T) testStore {
	t.Helper()
	l := blob.WithHook(blob.NewMemory())
	return testStore{blob.NewStore(l, "n1", 1, blob.Options{Backoff: time.Millisecond}), l}
}

// mustAppend queues one LTX file of shard (generation "g").
func mustAppend(t testing.TB, w *Writer, shard string, position uint64, ltxBytes []byte) {
	t.Helper()
	if err := w.Append(shard, "g", position, ltxBytes); err != nil {
		t.Fatal(err)
	}
}

func TestManyGoroutinesOneSegment(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	w := NewWriter(store.Store, "n1", Options{})
	serve(t, w)

	const n = 100
	var wg errgroup.Group
	for i := range n {
		wg.Go(func() error {
			return w.Append(fmt.Sprintf("users/%d", i), "g", uint64(i+1), testLTX(t, ltx.TXID(i+1), byte(i)))
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
	entries, err := ReadFooter(ctx, store.Store, "n1", 1)
	if err != nil || len(entries) != n {
		t.Fatalf("entries=%d err=%v", len(entries), err)
	}
	// Round trip of every entry.
	for _, e := range entries {
		b, err := ReadEntry(ctx, store.Store, "n1", 1, e)
		if err != nil {
			t.Fatal(err)
		}
		hdr, _, err := ltx.PeekHeader(bytes.NewReader(b))
		if err != nil || uint64(hdr.MaxTXID-hdr.MinTXID)+1 != e.Count {
			t.Fatalf("%s: hdr=%v err=%v", e.Shard, hdr.MinTXID, err)
		}
		var i int
		fmt.Sscanf(e.Shard, "users/%d", &i)
		if e.Position != uint64(i+1) || e.Count != 1 {
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
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after {
			inPut.Store(true)
			<-release
		}
		return nil
	})
	w := NewWriter(store.Store, "n1", Options{})
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
	if _, err := store.Head(ctx, Key("n1", 1)); err != nil {
		t.Fatal(err)
	}
}

func TestSlowPutDelaysNextSegment(t *testing.T) {
	t.Parallel()
	store := newStore(t)
	var cur, max atomic.Int32
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op != blob.OpCreate {
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
	w := NewWriter(store.Store, "n1", Options{})
	serve(t, w)
	// A flush after every append, many of them waiting at once.
	var g errgroup.Group
	for i := range 20 {
		mustAppend(t, w, "users/1", uint64(i+1), testLTX(t, ltx.TXID(i+1), 1))
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
	var last uint64
	for seq := uint64(1); seq <= uint64(w.Flushes()); seq++ {
		es, err := ReadFooter(context.Background(), store.Store, "n1", seq)
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
	// A newer writer already took segment 1 (here, as a fence).
	store.Create(ctx, Key("n1", 1), strings.NewReader(""))
	w := NewWriter(store.Store, "n1", Options{})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("flush got %v, want ErrFenced", err)
	}
	// Stopped for good: later appends fail at once and nothing is written.
	if err := w.Append("users/1", "g", 1, testLTX(t, 2, 1)); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
	if _, err := store.Head(ctx, Key("n1", 2)); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("segment 2 exists: %v", err)
	}
}

func TestFenceRecognised(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	store.Create(ctx, Key("n1", 7), strings.NewReader(""))
	if _, err := ReadFooter(ctx, store.Store, "n1", 7); !errors.Is(err, ErrFence) {
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
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after && fail.Load() {
			return errors.New("storage down")
		}
		return nil
	})
	w := NewWriter(store.Store, "n1", Options{})
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
	for seq, want := range map[uint64]uint64{1: 1, 2: 2} {
		es, err := ReadFooter(ctx, store.Store, "n1", seq)
		if err != nil || len(es) != 1 || es[0].Position != want {
			t.Fatalf("segment %d: %v %v, want position %d", seq, es, err, want)
		}
	}
	if _, err := ReadFooter(ctx, store.Store, "n1", 3); !errors.Is(err, blob.ErrNotFound) {
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
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		switch {
		case op == blob.OpCreate && !after && creates.Add(1) <= 60:
			return errors.New("request failed")
		case op == blob.OpCreate && after && creates.Load() == 61:
			return errors.New("response lost")
		case op == blob.OpHead && !after && creates.Load() > 60 && heads.Add(1) <= 10:
			return errors.New("head failed")
		}
		return nil
	})
	w := NewWriter(store.Store, "n1", Options{Retry: time.Millisecond})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 7))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if creates.Load() <= 4 {
		t.Fatalf("only %d create calls: blob.Store's own retries were never exhausted", creates.Load())
	}
	if w.Flushes() != 1 || w.NextSeq() != 2 {
		t.Fatalf("flushes=%d next=%d", w.Flushes(), w.NextSeq())
	}
	es, err := ReadFooter(ctx, store.Store, "n1", 1)
	if err != nil || len(es) != 1 {
		t.Fatalf("%v %v", es, err)
	}
	if _, err := store.Head(ctx, Key("n1", 2)); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("segment 2: %v", err)
	}
	if info, _ := store.Head(ctx, Key("n1", 1)); info.WriteID.Node != "n1" || info.WriteID.Epoch != 1 {
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
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after {
			select {
			case inPut <- struct{}{}:
			default:
			}
			return errors.New("storage down")
		}
		return nil
	})
	w := NewWriter(store.Store, "n1", Options{})
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
	if err := w.Append("users/1", "g", 2, testLTX(t, 2, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after Close got %v, want ErrClosed", err)
	}
}

// A fence found while retrying fails the flush, of what queued behind it too.
func TestFenceFoundWhileRetrying(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	var down atomic.Bool
	down.Store(true)
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after && down.Load() {
			return errors.New("storage down")
		}
		return nil
	})
	w := NewWriter(store.Store, "n1", Options{})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	w.Flush(short)
	mustAppend(t, w, "users/1", 2, testLTX(t, 2, 1))
	// An adopter fences the journal while the writer is stuck.
	down.Store(false)
	adopter := blob.NewStore(store.L, "n2", 1, blob.Options{})
	if _, err := Fence(ctx, adopter, "n1", 1); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(ctx); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
}

// A PUT that succeeds but whose response is lost must not fence the writer:
// blob.Store retries, finds its own write id on the object and succeeds.
func TestLostResponseIsNotAFence(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	var lost, creates atomic.Int32
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after {
			creates.Add(1)
		}
		if op == blob.OpCreate && after && lost.Add(1) == 1 {
			return errors.New("response lost")
		}
		return nil
	})
	w := NewWriter(store.Store, "n1", Options{})
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
	if _, err := ReadFooter(ctx, store.Store, "n1", 2); err != nil {
		t.Fatal(err)
	}
}

// An adopter creates an empty fence at the first free number. If the response
// to that create is lost, the fence is still recognised as its own. If a
// zombie's segment took the number first, the adopter moves on.
func TestAdopterFence(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	zombie := blob.NewStore(store.L, "dead", 7, blob.Options{})
	zombie.Create(ctx, Key("dead", 5), strings.NewReader("a zombie's segment"))

	var lost, creates atomic.Int32
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after && key == Key("dead", 6) {
			creates.Add(1)
		}
		if op == blob.OpCreate && after && key == Key("dead", 6) && lost.Add(1) == 1 {
			return errors.New("response lost")
		}
		return nil
	})
	seq, err := Fence(ctx, store.Store, "dead", 5)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 6 {
		t.Fatalf("fence at %d, want 6 (5 was the zombie's)", seq)
	}
	if creates.Load() != 2 {
		t.Fatalf("%d creates at 6, want the lost one and its retry", creates.Load())
	}
	if _, err := ReadFooter(ctx, store.Store, "dead", 6); !errors.Is(err, ErrFence) {
		t.Fatalf("got %v, want ErrFence", err)
	}
	// The zombie's own writer now finds its next numbers taken and is fenced.
	store.SetHook(nil)
	w := NewWriter(zombie, "dead", Options{StartSeq: 6})
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
	if _, err := store.Create(ctx, Key("dead", 9), strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
	if seq, err := Fence(ctx, store.Store, "dead", 9); err != nil || seq != 10 {
		t.Fatalf("seq=%d err=%v, want 10", seq, err)
	}
}

func TestBounds(t *testing.T) {
	t.Parallel()
	store, ctx := newStore(t), context.Background()
	if lo, hi, err := Bounds(ctx, store.Store, "n1"); err != nil || lo != 0 || hi != 0 {
		t.Fatalf("empty: %d %d %v", lo, hi, err)
	}
	store.Create(ctx, Key("n1", 5), strings.NewReader("x"))
	store.Create(ctx, Key("n1", 7), strings.NewReader(""))
	store.Create(ctx, Key("n2", 2), strings.NewReader("x"))
	if lo, hi, err := Bounds(ctx, store.Store, "n1"); err != nil || lo != 5 || hi != 7 {
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
	rec := &recorder{}
	w := NewWriter(store, "n1", Options{Retry: time.Millisecond, Logger: slog.New(rec)})
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
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after && fails.Add(1) <= 20 {
			return errors.New("storage unavailable")
		}
		return nil
	})
	rec := &recorder{}
	w := NewWriter(store.Store, "n1", Options{Retry: time.Millisecond, Logger: slog.New(rec)})
	serve(t, w)
	mustAppend(t, w, "users/1", 1, testLTX(t, 1, 1))
	if err := w.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := rec.errors(); n != 0 {
		t.Fatalf("%d error logs for failures that retrying fixes", n)
	}
}

// Segments and the files in them are built in temporary files once they are
// larger than the spool limit, and read back the same. Nothing is left behind
// when the segment is written, or when the writer is closed with files pending.
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
	w := NewWriter(store.Store, "n1", Options{TempDir: dir, SpoolLimit: 100})
	stop := serve(t, w)

	var seen atomic.Int32
	store.SetHook(func(_ context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after {
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
		if err := w.AppendFile(fmt.Sprintf("users/%d", i+1), "g", 1, f); err != nil {
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
	// While the upload ran, the segment had a file of its own.
	if seen.Load() != 1 {
		t.Fatalf("%d temporary files during the upload, want the segment's", seen.Load())
	}
	if left() != 0 {
		t.Fatalf("%d temporary files after the segment was written", left())
	}
	entries, err := ReadFooter(ctx, store.Store, "n1", 1)
	if err != nil || len(entries) != 2 {
		t.Fatalf("%v %v", entries, err)
	}
	for i, e := range entries {
		got, err := ReadEntryFile(ctx, store.Store, "n1", 1, e, dir, 100)
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
	if err := w.AppendFile("users/3", "g", 1, f); err != nil {
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
