package blob_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

// loseResponse makes the first call of op that has reached the backend and
// succeeded report a failure instead.
func loseResponse(l *blobtest.Hooked, op blobtest.Op) *atomic.Int32 {
	var fired atomic.Int32
	l.SetHook(func(ctx context.Context, o blobtest.Op, key string, after bool) error {
		if o == op && after && fired.Add(1) == 1 {
			return errors.New("response lost")
		}
		return nil
	})
	return &fired
}

func TestCreateLostResponse(t *testing.T) {
	t.Parallel()
	s, l := newStore(t)
	ctx := context.Background()
	fired := loseResponse(l, blobtest.OpCreate)
	etag, err := s.Create(ctx, "k", strings.NewReader("v"))
	if err != nil || fired.Load() != 1 {
		t.Fatalf("etag=%q err=%v fired=%d", etag, err, fired.Load())
	}
	info, _ := s.Head(ctx, "k")
	if etag != info.ETag {
		t.Fatalf("returned ETag %q is not the stored object's %q", etag, info.ETag)
	}
	// The returned ETag is usable.
	if _, err := s.Update(ctx, "k", strings.NewReader("w"), etag); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateLostResponse(t *testing.T) {
	t.Parallel()
	s, l := newStore(t)
	ctx := context.Background()
	e1, _ := s.Create(ctx, "k", strings.NewReader("v1"))
	fired := loseResponse(l, blobtest.OpUpdate)
	e2, err := s.Update(ctx, "k", strings.NewReader("v2"), e1)
	if err != nil || fired.Load() != 1 {
		t.Fatalf("err=%v fired=%d", err, fired.Load())
	}
	if _, err := s.Update(ctx, "k", strings.NewReader("v3"), e2); err != nil {
		t.Fatalf("the ETag returned after a lost response is stale: %v", err)
	}
}

func TestDeleteLostResponse(t *testing.T) {
	t.Parallel()
	s, l := newStrictStore(t)
	ctx := context.Background()
	s.Create(ctx, "k", strings.NewReader("v"))
	fired := loseResponse(l, blobtest.OpDelete)
	if err := s.Delete(ctx, "k", ""); err != nil || fired.Load() != 1 {
		t.Fatalf("err=%v fired=%d", err, fired.Load())
	}
	if _, err := s.Head(ctx, "k"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatal(err)
	}
	// A first attempt that finds nothing is still an error. Only a retry's is done.
	if err := s.Delete(ctx, "k", ""); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

// A conditional delete whose response was lost is recognised as done when the retry
// finds nothing, and a stale ETag is still a conflict.
func TestConditionalDeleteLostResponse(t *testing.T) {
	t.Parallel()
	s, l := newStore(t)
	ctx := context.Background()
	e1, _ := s.Create(ctx, "k", strings.NewReader("v1"))
	if err := s.Delete(ctx, "k", "stale"); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("stale delete: %v", err)
	}
	fired := loseResponse(l, blobtest.OpDelete)
	if err := s.Delete(ctx, "k", e1); err != nil || fired.Load() != 1 {
		t.Fatalf("err=%v fired=%d", err, fired.Load())
	}
	if _, err := s.Head(ctx, "k"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatal(err)
	}
}

// A failed request that certainly changed nothing is no reason to call a
// missing object deleted: only an earlier unknown outcome is.
func TestDeleteNotFoundAfterNotApplied(t *testing.T) {
	t.Parallel()
	s, l := newStrictStore(t)
	ctx := context.Background()
	var calls atomic.Int32
	l.SetHook(func(ctx context.Context, o blobtest.Op, key string, after bool) error {
		if o == blobtest.OpDelete && !after && calls.Add(1) == 1 {
			return errors.New("refused")
		}
		return nil
	})
	if err := s.Delete(ctx, "never-there", ""); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("%d delete calls", calls.Load())
	}
}

// Somebody else's object is a real conflict even after an ambiguous failure.
func TestLostResponseThenSomeoneElsesObject(t *testing.T) {
	t.Parallel()
	s, l := newStore(t)
	other := blob.NewStore(l, "n2", 1, blob.Options{})
	ctx := context.Background()
	// Our create fails with an unknown outcome and the object is not ours: another node got there first.
	var fired atomic.Int32
	l.SetHook(func(ctx context.Context, o blobtest.Op, key string, after bool) error {
		if o == blobtest.OpCreate && !after && fired.Add(1) == 1 {
			other.Create(ctx, "k", strings.NewReader("theirs")) // lands first
			return nil
		}
		if o == blobtest.OpCreate && after && fired.Load() == 1 {
			fired.Add(1)
			return errors.New("response lost")
		}
		return nil
	})
	// Our first attempt hits ErrExists directly (someone else's), which is not ambiguous.
	if _, err := s.Create(ctx, "k", strings.NewReader("mine")); !errors.Is(err, blob.ErrExists) {
		t.Fatalf("got %v, want ErrExists", err)
	}
	// Now with a lost response first: ours is written, response lost, retry finds ours.
	l.SetHook(nil)
	s.Delete(ctx, "k", "")
	fired.Store(0)
	loseResponse(l, blobtest.OpCreate)
	if _, err := s.Create(ctx, "k", strings.NewReader("mine")); err != nil {
		t.Fatal(err)
	}
	// And a different node creating the same key afterwards is a conflict.
	l.SetHook(nil)
	if _, err := other.Create(ctx, "k", strings.NewReader("theirs")); !errors.Is(err, blob.ErrExists) {
		t.Fatalf("got %v, want ErrExists", err)
	}
}

func TestNotAppliedIsRetried(t *testing.T) {
	t.Parallel()
	s, l := newStore(t)
	ctx := context.Background()
	var calls atomic.Int32
	l.SetHook(func(ctx context.Context, o blobtest.Op, key string, after bool) error {
		if o == blobtest.OpCreate && !after && calls.Add(1) <= 2 {
			return errors.New("busy")
		}
		return nil
	})
	if _, err := s.Create(ctx, "k", strings.NewReader("v")); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("%d attempts, want 3", calls.Load())
	}
}

// A write whose first attempt landed unheard, and whose retries and read-backs
// then all fail, has an unknown outcome: never "not applied", which would tell
// the caller that nothing happened.
func TestUnknownStaysUnknownAfterNotApplied(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, op := range []blobtest.Op{blobtest.OpUpdate, blobtest.OpDelete} {
		s, l := newStore(t)
		etag, err := s.Create(ctx, "k", strings.NewReader("v1"))
		if err != nil {
			t.Fatal(err)
		}
		var landed atomic.Bool
		l.SetHook(func(ctx context.Context, o blobtest.Op, key string, after bool) error {
			if o == op && after && !landed.Load() {
				landed.Store(true)
				return errors.New("response lost")
			}
			if landed.Load() && !after {
				return errors.New("unreachable")
			}
			return nil
		})
		if op == blobtest.OpUpdate {
			_, err = s.Update(ctx, "k", strings.NewReader("v2"), etag)
		} else {
			err = s.Delete(ctx, "k", etag)
		}
		if !errors.Is(err, blob.ErrUnknown) || errors.Is(err, blob.ErrNotApplied) {
			t.Fatalf("%s: got %v, want ErrUnknown and not ErrNotApplied", op, err)
		}
	}
}

func TestPersistentFailureIsReturned(t *testing.T) {
	t.Parallel()
	s, l := newStore(t)
	ctx := context.Background()
	l.SetHook(func(ctx context.Context, o blobtest.Op, key string, after bool) error {
		if o == blobtest.OpCreate && !after {
			return errors.New("storage down")
		}
		return nil
	})
	if _, err := s.Create(ctx, "k", strings.NewReader("v")); !errors.Is(err, blob.ErrNotApplied) {
		t.Fatalf("got %v, want ErrNotApplied", err)
	}
	// Reads are retried too.
	var calls atomic.Int32
	l.SetHook(func(ctx context.Context, o blobtest.Op, key string, after bool) error {
		if o == blobtest.OpHead && !after && calls.Add(1) == 1 {
			return errors.New("busy")
		}
		return nil
	})
	l2, _ := s.Create(ctx, "j", strings.NewReader("v"))
	_ = l2
	if _, err := s.Head(ctx, "j"); err != nil || calls.Load() != 2 {
		t.Fatalf("head: err=%v calls=%d", err, calls.Load())
	}
}

// On a store that answers success to a delete of a missing key (S3), a delete
// whose response was lost, and a second delete, are both done.
func TestDeleteOfAMissingKeyOnS3Semantics(t *testing.T) {
	t.Parallel()
	s, l := newStore(t)
	ctx := context.Background()
	s.Create(ctx, "k", strings.NewReader("v"))
	loseResponse(l, blobtest.OpDelete)
	if err := s.Delete(ctx, "k", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "k", ""); err != nil {
		t.Fatalf("second delete: %v", err)
	}
}

// A body that is a file is sent from its start on every attempt, whatever an
// earlier attempt read of it, and the store never holds it in memory.
func TestRetryResendsTheWholeBody(t *testing.T) {
	t.Parallel()
	s, l := newStore(t)
	ctx := context.Background()
	f, err := os.CreateTemp(t.TempDir(), "body")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	want := strings.Repeat("0123456789", 1000)
	f.WriteString(want)
	body := io.NewSectionReader(f, 0, int64(len(want)))

	var attempts atomic.Int32
	l.SetHook(func(ctx context.Context, o blobtest.Op, key string, after bool) error {
		if o == blobtest.OpCreate && !after && attempts.Add(1) == 1 {
			return errors.New("refused") // nothing written
		}
		if o == blobtest.OpCreate && after && attempts.Load() == 2 && attempts.Add(1) == 3 {
			return errors.New("response lost") // written, but the caller isn't told
		}
		return nil
	})
	body.Seek(0, io.SeekEnd) // wherever it was left
	if _, err := s.Create(ctx, "big", body); err != nil {
		t.Fatal(err)
	}
	rc, _, err := s.Get(ctx, "big")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != want {
		t.Fatalf("stored %d bytes, want %d", len(got), len(want))
	}
	if attempts.Load() < 3 {
		t.Fatalf("only %d attempts", attempts.Load())
	}
}

// bodyKeeper is a backend that keeps the reader of its first Create, after
// reading half of it, so a test can see whether a later attempt disturbed it.
type bodyKeeper struct {
	blob.Backend
	first io.ReadSeeker
	half  int
}

func (k *bodyKeeper) Create(ctx context.Context, key string, body io.ReadSeeker, id blob.WriteID) (blob.ETag, error) {
	if k.first == nil {
		k.first = body
		buf := make([]byte, k.half)
		io.ReadFull(body, buf)
		return "", fmt.Errorf("%w: busy", blob.ErrNotApplied)
	}
	return k.Backend.Create(ctx, key, body, id)
}

// Attempts share no offset: the transport can still be reading an earlier
// attempt's body when the next one starts, so a reader an attempt left
// half-way must be untouched by the attempts after it.
func TestAttemptsReadTheBodyThroughReadersOfTheirOwn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	k := &bodyKeeper{Backend: blob.NewMemory(), half: 5}
	s := blob.NewStore(k, "n1", 1, blob.Options{Backoff: time.Millisecond})
	if _, err := s.Create(ctx, "k", strings.NewReader("0123456789")); err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, mustGet(t, s, "k")); got != "0123456789" {
		t.Fatalf("stored %q", got)
	}
	rest, err := io.ReadAll(k.first)
	if err != nil || string(rest) != "56789" {
		t.Fatalf("the first attempt's reader now yields %q (%v), want the half it had left", rest, err)
	}
}

func mustGet(t *testing.T, s *blob.Store, key string) io.ReadCloser {
	t.Helper()
	rc, _, err := s.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return rc
}

// conflictOnMissing is a store that answers a conditional delete of a missing
// key with a failed precondition, as RustFS does.
type conflictOnMissing struct{ blob.Backend }

func (c conflictOnMissing) Delete(ctx context.Context, key string, expected blob.ETag) error {
	if _, err := c.Backend.Head(ctx, key); err != nil && expected != "" {
		return fmt.Errorf("%w: precondition failed", blob.ErrConflict)
	}
	return c.Backend.Delete(ctx, key, expected)
}

// A conditional delete that landed and lost its response is retried into a
// "conflict" on such a store. The object is gone, so the delete is done.
func TestConditionalDeleteLostResponseOnAStoreThatAnswersConflict(t *testing.T) {
	t.Parallel()
	l := blobtest.WithHook(conflictOnMissing{blob.NewMemory()})
	s := blob.NewStore(l, "n1", 1, blob.Options{Backoff: time.Millisecond})
	ctx := context.Background()
	e1, _ := s.Create(ctx, "k", strings.NewReader("v1"))
	fired := loseResponse(l, blobtest.OpDelete)
	if err := s.Delete(ctx, "k", e1); err != nil || fired.Load() != 1 {
		t.Fatalf("err=%v fired=%d", err, fired.Load())
	}
	// Without an unknown outcome before it, the same answer is a conflict the caller sees.
	l.SetHook(nil)
	if err := s.Delete(ctx, "k", e1); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
}

// lostDelete fails its first delete with an unknown outcome after running
// between, which does not apply it.
type lostDelete struct {
	blob.Backend
	between func()
	calls   atomic.Int32
}

func (d *lostDelete) Delete(ctx context.Context, key string, expected blob.ETag) error {
	if d.calls.Add(1) == 1 {
		d.between()
		return fmt.Errorf("%w: response lost", blob.ErrUnknown)
	}
	return d.Backend.Delete(ctx, key, expected)
}

// After an unknown outcome, a conflict with the object still there is a real one.
func TestConditionalDeleteConflictAfterUnknownIsReal(t *testing.T) {
	t.Parallel()
	mem := blob.NewMemory()
	ctx := context.Background()
	e1, _ := blob.NewStore(mem, "n1", 1, blob.Options{}).Create(ctx, "k", strings.NewReader("v1"))
	d := &lostDelete{Backend: mem, between: func() {
		if _, err := blob.NewStore(mem, "n2", 1, blob.Options{}).Update(ctx, "k", strings.NewReader("v2"), e1); err != nil {
			t.Error(err)
		}
	}}
	s := blob.NewStore(d, "n1", 1, blob.Options{Backoff: time.Millisecond})
	err := s.Delete(ctx, "k", e1)
	if !errors.Is(err, blob.ErrConflict) || errors.Is(err, blob.ErrUnknown) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
	if _, err := mem.Head(ctx, "k"); err != nil {
		t.Fatalf("the object should be there: %v", err)
	}
}
