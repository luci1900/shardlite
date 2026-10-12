package segment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/blob"
)

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

// testStore is a Store over a local backend, with the backend's fault hook within reach.
type testStore struct {
	*blob.Store
	L *blob.Local
}

func (s testStore) SetHook(h blob.Hook) { s.L.SetHook(h) }

func newStore(t *testing.T) testStore {
	t.Helper()
	l, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return testStore{blob.NewStore(l, "n1", 1, blob.Options{Backoff: time.Millisecond}), l}
}

func wait(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for segment")
		return nil
	}
}

func TestManyGoroutinesOneSegment(t *testing.T) {
	store, ctx := newStore(t), context.Background()
	w := NewWriter(store.Store, "n1", Options{Interval: time.Hour}) // flush by hand
	defer w.Close()

	const n = 100
	var chans [n]<-chan error
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			chans[i] = w.Append(fmt.Sprintf("users/%d", i), "g", uint64(i+1), testLTX(t, ltx.TXID(i+1), byte(i)))
		}()
	}
	wg.Wait()
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	for _, ch := range chans {
		if err := wait(t, ch); err != nil {
			t.Fatal(err)
		}
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

func TestWaitersReleasedAfterPut(t *testing.T) {
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
	w := NewWriter(store.Store, "n1", Options{Interval: 10 * time.Millisecond})
	defer w.Close()
	ch := w.Append("users/1", "g", 1, testLTX(t, 1, 1))
	for !inPut.Load() {
		time.Sleep(time.Millisecond)
	}
	select {
	case <-ch:
		t.Fatal("waiter released before the PUT finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := wait(t, ch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Head(ctx, Key("n1", 1)); err != nil {
		t.Fatal(err)
	}
}

func TestSlowPutDelaysNextSegment(t *testing.T) {
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
	w := NewWriter(store.Store, "n1", Options{Interval: 5 * time.Millisecond})
	defer w.Close()
	var chans []<-chan error
	for i := range 20 {
		chans = append(chans, w.Append("users/1", "g", uint64(i+1), testLTX(t, ltx.TXID(i+1), 1)))
		time.Sleep(10 * time.Millisecond)
	}
	for _, ch := range chans {
		if err := wait(t, ch); err != nil {
			t.Fatal(err)
		}
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
	store, ctx := newStore(t), context.Background()
	// A newer writer already took segment 1 (here, as a fence).
	store.Create(ctx, Key("n1", 1), strings.NewReader(""))
	w := NewWriter(store.Store, "n1", Options{Interval: 5 * time.Millisecond})
	defer w.Close()
	if err := wait(t, w.Append("users/1", "g", 1, testLTX(t, 1, 1))); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
	// Stopped for good: later appends fail at once and nothing is written.
	if err := wait(t, w.Append("users/1", "g", 1, testLTX(t, 2, 1))); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
	if _, err := store.Head(ctx, Key("n1", 2)); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("segment 2 exists: %v", err)
	}
}

func TestFenceRecognised(t *testing.T) {
	store, ctx := newStore(t), context.Background()
	store.Create(ctx, Key("n1", 7), strings.NewReader(""))
	if _, err := ReadFooter(ctx, store.Store, "n1", 7); !errors.Is(err, ErrFence) {
		t.Fatalf("got %v, want ErrFence", err)
	}
}

// A segment that can't be written is not given up: its waiters keep waiting,
// nothing newer is written ahead of it, and it goes out under the same number
// once storage is back.
func TestFailedSegmentIsRetriedNotDropped(t *testing.T) {
	store, ctx := newStore(t), context.Background()
	var fail atomic.Bool
	fail.Store(true)
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after && fail.Load() {
			return errors.New("storage down")
		}
		return nil
	})
	w := NewWriter(store.Store, "n1", Options{Interval: time.Hour})
	defer w.Close()
	first := w.Append("users/1", "g", 1, testLTX(t, 1, 1))
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := w.Flush(short); err == nil {
		t.Fatal("want the caller's timeout")
	}
	select {
	case err := <-first:
		t.Fatalf("waiter released with %v while the segment is unwritten", err)
	default:
	}
	// A later commit queues behind it.
	second := w.Append("users/1", "g", 2, testLTX(t, 2, 1))
	fail.Store(false)
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, first); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, second); err != nil {
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
	w := NewWriter(store.Store, "n1", Options{Interval: time.Hour, Retry: time.Millisecond})
	defer w.Close()
	ch := w.Append("users/1", "g", 1, testLTX(t, 1, 7))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, ch); err != nil {
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
	ch = w.Append("users/1", "g", 2, testLTX(t, 2, 1))
	w.Flush(ctx)
	if err := wait(t, ch); err != nil {
		t.Fatal(err)
	}
}

// Close fails the waiters of a segment still being retried.
func TestCloseFailsWaitersOfUnknownSegment(t *testing.T) {
	store := newStore(t)
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after {
			return errors.New("storage down")
		}
		return nil
	})
	w := NewWriter(store.Store, "n1", Options{Interval: 5 * time.Millisecond})
	ch := w.Append("users/1", "g", 1, testLTX(t, 1, 1))
	select {
	case err := <-ch:
		t.Fatalf("released early: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	w.Close()
	if err := wait(t, ch); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v, want ErrClosed", err)
	}
}

// A fence found while retrying fails the waiters, including those queued behind.
func TestFenceFoundWhileRetrying(t *testing.T) {
	store, ctx := newStore(t), context.Background()
	var down atomic.Bool
	down.Store(true)
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after && down.Load() {
			return errors.New("storage down")
		}
		return nil
	})
	w := NewWriter(store.Store, "n1", Options{Interval: time.Hour})
	defer w.Close()
	first := w.Append("users/1", "g", 1, testLTX(t, 1, 1))
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	w.Flush(short)
	second := w.Append("users/1", "g", 2, testLTX(t, 2, 1))
	// An adopter fences the journal while the writer is stuck.
	down.Store(false)
	adopter := blob.NewStore(store.L, "n2", 1, blob.Options{})
	if _, err := Fence(ctx, adopter, "n1", 1); err != nil {
		t.Fatal(err)
	}
	w.Flush(ctx)
	for _, ch := range []<-chan error{first, second} {
		if err := wait(t, ch); !errors.Is(err, ErrFenced) {
			t.Fatalf("got %v, want ErrFenced", err)
		}
	}
}

// A PUT that succeeds but whose response is lost must not fence the writer:
// blob.Store retries, finds its own write id on the object and succeeds.
func TestLostResponseIsNotAFence(t *testing.T) {
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
	w := NewWriter(store.Store, "n1", Options{Interval: time.Hour})
	defer w.Close()
	ch := w.Append("users/1", "g", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, ch); err != nil {
		t.Fatalf("lost response: %v", err)
	}
	if creates.Load() != 2 {
		t.Fatalf("%d creates, want the lost one and its retry", creates.Load())
	}
	// And it carries on with the next number.
	ch = w.Append("users/1", "g", 2, testLTX(t, 2, 1))
	w.Flush(ctx)
	if err := wait(t, ch); err != nil {
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
	w := NewWriter(zombie, "dead", Options{Interval: time.Hour, StartSeq: 6})
	defer w.Close()
	ch := w.Append("users/1", "g", 1, testLTX(t, 1, 1))
	w.Flush(ctx)
	if err := wait(t, ch); !errors.Is(err, ErrFenced) {
		t.Fatalf("got %v, want ErrFenced", err)
	}
	// A number already taken, by anyone, is skipped.
	if _, err := store.Create(ctx, Key("dead", 9), strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
	if seq, err := Fence(ctx, store.Store, "dead", 9); err != nil || seq != 10 {
		t.Fatalf("seq=%d err=%v, want 10", seq, err)
	}
}

func TestNextSeq(t *testing.T) {
	store, ctx := newStore(t), context.Background()
	if n, err := NextSeq(ctx, store.Store, "n1"); err != nil || n != 1 {
		t.Fatalf("empty: %d %v", n, err)
	}
	store.Create(ctx, Key("n1", 1), strings.NewReader("x"))
	store.Create(ctx, Key("n1", 7), strings.NewReader(""))
	store.Create(ctx, Key("n2", 40), strings.NewReader("x"))
	if n, err := NextSeq(ctx, store.Store, "n1"); err != nil || n != 8 {
		t.Fatalf("got %d %v, want 8", n, err)
	}
}

func TestBounds(t *testing.T) {
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

func TestCheckStopsAcknowledgements(t *testing.T) {
	store, ctx := newStore(t), context.Background()
	var bad atomic.Pointer[error]
	errLease := errors.New("lease expired")
	w := NewWriter(store.Store, "n1", Options{Interval: time.Hour, Check: func() error {
		if e := bad.Load(); e != nil {
			return *e
		}
		return nil
	}})
	defer w.Close()

	ch := w.Append("users/1", "g", 1, testLTX(t, 1, 1))
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, ch); err != nil {
		t.Fatalf("while the lease is good: %v", err)
	}

	// Queued while the lease was good, written after it ran out: nobody is told it is durable.
	ch = w.Append("users/1", "g", 2, testLTX(t, 2, 2))
	bad.Store(&errLease)
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wait(t, ch); !errors.Is(err, errLease) {
		t.Fatalf("waiter of a segment written after expiry got %v", err)
	}
	// And nothing new is accepted.
	if err := wait(t, w.Append("users/1", "g", 3, testLTX(t, 3, 3))); !errors.Is(err, errLease) {
		t.Fatalf("append after expiry got %v", err)
	}
}

// failCreate makes every Create of a segment fail with a plain error, one that
// is neither "not applied" nor "unknown": retrying can't fix it.
type failCreate struct {
	blob.Backend
	err atomic.Pointer[error]
}

func (f *failCreate) Create(ctx context.Context, key string, body io.Reader, id blob.WriteID) (blob.ETag, error) {
	if e := f.err.Load(); e != nil && strings.HasPrefix(key, "journal/") {
		return "", *e
	}
	return f.Backend.Create(ctx, key, body, id)
}

type recorder struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	r.recs = append(r.recs, rec)
	r.mu.Unlock()
	return nil
}
func (r *recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recorder) WithGroup(string) slog.Handler      { return r }
func (r *recorder) errors() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.recs {
		if rec.Level >= slog.LevelError {
			n++
		}
	}
	return n
}

func TestErrorsRetryingCannotFixAreLogged(t *testing.T) {
	l, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fc := &failCreate{Backend: l}
	denied := errors.New("permission denied")
	fc.err.Store(&denied)
	store := blob.NewStore(fc, "n1", 1, blob.Options{Backoff: time.Millisecond})
	rec := &recorder{}
	w := NewWriter(store, "n1", Options{Interval: 5 * time.Millisecond, Retry: time.Millisecond, Logger: slog.New(rec)})
	defer w.Close()

	ch := w.Append("users/1", "g", 1, testLTX(t, 1, 1))
	deadline := time.Now().Add(5 * time.Second)
	for rec.errors() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d error logs", rec.errors())
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-ch:
		t.Fatalf("the segment was given up on: %v", err)
	default:
	}
	// The permissions are fixed: the same segment goes out.
	fc.err.Store(nil)
	if err := wait(t, ch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Head(context.Background(), Key("n1", 1)); err != nil {
		t.Fatal(err)
	}
}

func TestTransientTroubleIsNotLoggedAsUnfixable(t *testing.T) {
	store := newStore(t)
	var fails atomic.Int32
	store.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after && fails.Add(1) <= 20 {
			return errors.New("storage unavailable")
		}
		return nil
	})
	rec := &recorder{}
	w := NewWriter(store.Store, "n1", Options{Interval: 5 * time.Millisecond, Retry: time.Millisecond, Logger: slog.New(rec)})
	defer w.Close()
	if err := wait(t, w.Append("users/1", "g", 1, testLTX(t, 1, 1))); err != nil {
		t.Fatal(err)
	}
	if n := rec.errors(); n != 0 {
		t.Fatalf("%d error logs for failures that retrying fixes", n)
	}
}
