package blob

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

// loseResponse makes the first call of op that has reached the backend and
// succeeded report a failure instead.
func loseResponse(l *Local, op Op) *atomic.Int32 {
	var fired atomic.Int32
	l.SetHook(func(ctx context.Context, o Op, key string, after bool) error {
		if o == op && after && fired.Add(1) == 1 {
			return errors.New("response lost")
		}
		return nil
	})
	return &fired
}

func TestCreateLostResponse(t *testing.T) {
	s, l := newStore(t)
	ctx := context.Background()
	fired := loseResponse(l, OpCreate)
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
	s, l := newStore(t)
	ctx := context.Background()
	e1, _ := s.Create(ctx, "k", strings.NewReader("v1"))
	fired := loseResponse(l, OpUpdate)
	e2, err := s.Update(ctx, "k", strings.NewReader("v2"), e1)
	if err != nil || fired.Load() != 1 {
		t.Fatalf("err=%v fired=%d", err, fired.Load())
	}
	if _, err := s.Update(ctx, "k", strings.NewReader("v3"), e2); err != nil {
		t.Fatalf("the ETag returned after a lost response is stale: %v", err)
	}
}

func TestDeleteLostResponse(t *testing.T) {
	s, l := newStore(t)
	ctx := context.Background()
	s.Create(ctx, "k", strings.NewReader("v"))
	fired := loseResponse(l, OpDelete)
	if err := s.Delete(ctx, "k", ""); err != nil || fired.Load() != 1 {
		t.Fatalf("err=%v fired=%d", err, fired.Load())
	}
	if _, err := s.Head(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	// A first attempt that finds nothing is still an error. Only a retry's is done.
	if err := s.Delete(ctx, "k", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

// A conditional delete whose response was lost is recognised as done when the retry
// finds nothing, and a stale ETag is still a conflict.
func TestConditionalDeleteLostResponse(t *testing.T) {
	s, l := newStore(t)
	ctx := context.Background()
	e1, _ := s.Create(ctx, "k", strings.NewReader("v1"))
	if err := s.Delete(ctx, "k", "stale"); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale delete: %v", err)
	}
	fired := loseResponse(l, OpDelete)
	if err := s.Delete(ctx, "k", e1); err != nil || fired.Load() != 1 {
		t.Fatalf("err=%v fired=%d", err, fired.Load())
	}
	if _, err := s.Head(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

// A failed request that certainly changed nothing is no reason to call a
// missing object deleted: only an earlier unknown outcome is.
func TestDeleteNotFoundAfterNotApplied(t *testing.T) {
	s, l := newStore(t)
	ctx := context.Background()
	var calls atomic.Int32
	l.SetHook(func(ctx context.Context, o Op, key string, after bool) error {
		if o == OpDelete && !after && calls.Add(1) == 1 {
			return errors.New("refused")
		}
		return nil
	})
	if err := s.Delete(ctx, "never-there", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("%d delete calls", calls.Load())
	}
}

// Somebody else's object is a real conflict even after an ambiguous failure.
func TestLostResponseThenSomeoneElsesObject(t *testing.T) {
	s, l := newStore(t)
	other := NewStore(l, "n2", 1, Options{})
	ctx := context.Background()
	// Our create fails with an unknown outcome and the object is not ours: another node got there first.
	var fired atomic.Int32
	l.SetHook(func(ctx context.Context, o Op, key string, after bool) error {
		if o == OpCreate && !after && fired.Add(1) == 1 {
			other.Create(ctx, "k", strings.NewReader("theirs")) // lands first
			return nil
		}
		if o == OpCreate && after && fired.Load() == 1 {
			fired.Add(1)
			return errors.New("response lost")
		}
		return nil
	})
	// Our first attempt hits ErrExists directly (someone else's), which is not ambiguous.
	if _, err := s.Create(ctx, "k", strings.NewReader("mine")); !errors.Is(err, ErrExists) {
		t.Fatalf("got %v, want ErrExists", err)
	}
	// Now with a lost response first: ours is written, response lost, retry finds ours.
	l.SetHook(nil)
	s.Delete(ctx, "k", "")
	fired.Store(0)
	loseResponse(l, OpCreate)
	if _, err := s.Create(ctx, "k", strings.NewReader("mine")); err != nil {
		t.Fatal(err)
	}
	// And a different node creating the same key afterwards is a conflict.
	l.SetHook(nil)
	if _, err := other.Create(ctx, "k", strings.NewReader("theirs")); !errors.Is(err, ErrExists) {
		t.Fatalf("got %v, want ErrExists", err)
	}
}

func TestNotAppliedIsRetried(t *testing.T) {
	s, l := newStore(t)
	ctx := context.Background()
	var calls atomic.Int32
	l.SetHook(func(ctx context.Context, o Op, key string, after bool) error {
		if o == OpCreate && !after && calls.Add(1) <= 2 {
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

func TestPersistentFailureIsReturned(t *testing.T) {
	s, l := newStore(t)
	ctx := context.Background()
	l.SetHook(func(ctx context.Context, o Op, key string, after bool) error {
		if o == OpCreate && !after {
			return errors.New("storage down")
		}
		return nil
	})
	if _, err := s.Create(ctx, "k", strings.NewReader("v")); !errors.Is(err, ErrNotApplied) {
		t.Fatalf("got %v, want ErrNotApplied", err)
	}
	// Reads are retried too.
	var calls atomic.Int32
	l.SetHook(func(ctx context.Context, o Op, key string, after bool) error {
		if o == OpHead && !after && calls.Add(1) == 1 {
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
