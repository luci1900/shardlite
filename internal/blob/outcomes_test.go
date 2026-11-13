package blob_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

// The outcome table in blob.go, as cases. Each backend gets the same faults
// (memory through the hook, S3 through a transport in front of the emulator),
// and each case says which outcome the caller sees and whether the object
// changed.

type fault int

const (
	cancelBefore fault = iota // the context ended before the request was written
	cancelAfter               // the request was written and applied, then the context ended
	dropped                   // the request was applied and the response lost
	slowDown                  // the store answered 503 SlowDown and did nothing
	conflict409               // the store answered 409 ConditionalRequestConflict and did nothing
	tooMany                   // the store answered 429 and did nothing
)

func (f fault) String() string {
	return [...]string{"cancelled before", "cancelled after", "response dropped", "503 SlowDown", "409 ConditionalRequestConflict", "429"}[f]
}

// outcomeRig injects a fault into one call of a backend.
type outcomeRig struct {
	name string
	// backend returns the backend under test, whose next call fails as asked, and
	// the same store without faults for looking at the result.
	backend func(t *testing.T) (faulty blob.Backend, plain blob.Backend, arm func(f fault, cancel context.CancelFunc))
}

func memoryRig() outcomeRig {
	return outcomeRig{name: "memory", backend: func(t *testing.T) (blob.Backend, blob.Backend, func(fault, context.CancelFunc)) {
		mem := blob.NewMemory()
		h := blobtest.WithHook(mem)
		var armed atomic.Pointer[fault]
		var cancel atomic.Pointer[context.CancelFunc]
		h.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
			f := armed.Load()
			if f == nil || (op != blobtest.OpCreate && op != blobtest.OpUpdate && op != blobtest.OpDelete) {
				return nil
			}
			switch {
			case !after && (*f == slowDown || *f == conflict409 || *f == tooMany):
				return errors.New(f.String()) // the hook's before-error is "nothing happened"
			case after && *f == cancelAfter:
				(*cancel.Load())()
				return ctx.Err()
			case after && *f == dropped:
				return errors.New("response lost")
			}
			return nil
		})
		return h, mem, func(f fault, c context.CancelFunc) { armed.Store(&f); cancel.Store(&c) }
	}}
}

// s3Transport sits in front of the emulator.
type s3Transport struct {
	inner  http.RoundTripper
	f      atomic.Pointer[fault]
	cancel atomic.Pointer[context.CancelFunc]
}

func (s *s3Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	f := s.f.Load()
	if f == nil || (r.Method != http.MethodPut && r.Method != http.MethodDelete) {
		return s.inner.RoundTrip(r)
	}
	switch *f {
	case slowDown:
		return s3Error(503, "SlowDown"), nil
	case conflict409:
		return s3Error(409, "ConditionalRequestConflict"), nil
	case tooMany:
		return s3Error(429, "SlowDown"), nil
	case cancelBefore, cancelAfter, dropped:
		// The request goes through: the fault is in what the caller sees of it.
	}
	resp, err := s.inner.RoundTrip(r) // written, and applied
	if err != nil {
		return nil, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if *f == cancelAfter {
		(*s.cancel.Load())()
		return nil, r.Context().Err()
	}
	return nil, &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
}

func s3Rig() outcomeRig {
	return outcomeRig{name: "s3", backend: func(t *testing.T) (blob.Backend, blob.Backend, func(fault, context.CancelFunc)) {
		endpoint := os.Getenv("SHARDLITE_TEST_S3_ENDPOINT")
		if endpoint == "" {
			t.Skip("SHARDLITE_TEST_S3_ENDPOINT is not set")
		}
		tr := &s3Transport{inner: http.DefaultTransport}
		faulty := s3At(t, endpoint, func(o *blob.S3Options) { o.HTTPClient = &http.Client{Transport: tr} })
		// Setup and checks use PUT/DELETE before arming and HEAD/GET after, which the transport leaves alone.
		plain := faulty
		return faulty, plain, func(f fault, c context.CancelFunc) { tr.f.Store(&f); tr.cancel.Store(&c) }
	}}
}

func TestWriteOutcomesFollowTheTable(t *testing.T) {
	t.Parallel()
	for _, rig := range []outcomeRig{memoryRig(), s3Rig()} {
		t.Run(rig.name, func(t *testing.T) {
			for _, op := range []string{"create", "update", "delete"} {
				for _, f := range []fault{cancelBefore, cancelAfter, dropped, slowDown, conflict409, tooMany} {
					t.Run(op+"/"+f.String(), func(t *testing.T) {
						faulty, plain, arm := rig.backend(t)
						bg := context.Background()
						id := blob.WriteID{Node: "n1", Epoch: 1, N: 7}
						etag, err := plain.Create(bg, "k", strings.NewReader("old"), blob.WriteID{Node: "n0", Epoch: 1, N: 1})
						if err != nil {
							t.Fatal(err)
						}
						ctx, cancel := context.WithCancel(bg)
						defer cancel()
						arm(f, cancel)
						if f == cancelBefore {
							cancel()
						}
						var callErr error
						switch op {
						case "create":
							_, callErr = faulty.Create(ctx, "new", strings.NewReader("v"), id)
						case "update":
							_, callErr = faulty.Update(ctx, "k", strings.NewReader("new"), etag, id)
						case "delete":
							callErr = faulty.Delete(ctx, "k", etag)
						}
						applied := f == cancelAfter || f == dropped
						if applied {
							if !errors.Is(callErr, blob.ErrUnknown) || errors.Is(callErr, blob.ErrNotApplied) {
								t.Fatalf("got %v, want ErrUnknown", callErr)
							}
						} else {
							if !errors.Is(callErr, blob.ErrNotApplied) || errors.Is(callErr, blob.ErrUnknown) {
								t.Fatalf("got %v, want ErrNotApplied", callErr)
							}
							if f == cancelBefore && !errors.Is(callErr, context.Canceled) {
								t.Fatalf("the context's error is lost: %v", callErr)
							}
						}
						// Did the object change?
						switch op {
						case "create":
							_, err := plain.Head(bg, "new")
							if exists := err == nil; exists != applied {
								t.Fatalf("object exists = %v, want %v (%v)", exists, applied, err)
							}
						case "update":
							rc, _, err := plain.Get(bg, "k")
							if err != nil {
								t.Fatal(err)
							}
							b, _ := io.ReadAll(rc)
							if changed := string(b) == "new"; changed != applied {
								t.Fatalf("object changed = %v, want %v", changed, applied)
							}
						case "delete":
							_, err := plain.Head(bg, "k")
							if gone := errors.Is(err, blob.ErrNotFound); gone != applied {
								t.Fatalf("object gone = %v, want %v (%v)", gone, applied, err)
							}
						}
					})
				}
			}
		})
	}
}

// Failed preconditions and a wrong setup are not outcomes of the retryable kind.
func TestPreconditionsAndSetupErrorsArePlain(t *testing.T) {
	t.Parallel()
	for _, rig := range []outcomeRig{memoryRig(), s3Rig()} {
		t.Run(rig.name, func(t *testing.T) {
			faulty, _, _ := rig.backend(t)
			ctx := context.Background()
			etag, err := faulty.Create(ctx, "k", strings.NewReader("v"), blob.WriteID{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := faulty.Create(ctx, "k", strings.NewReader("w"), blob.WriteID{}); !errors.Is(err, blob.ErrExists) || blob.Transient(err) {
				t.Fatalf("create over an object: %v", err)
			}
			if _, err := faulty.Update(ctx, "k", strings.NewReader("w"), "stale", blob.WriteID{}); !errors.Is(err, blob.ErrConflict) || blob.Transient(err) {
				t.Fatalf("update with a stale ETag: %v", err)
			}
			if err := faulty.Delete(ctx, "k", "stale"); !errors.Is(err, blob.ErrConflict) || blob.Transient(err) {
				t.Fatalf("delete with a stale ETag: %v", err)
			}
			_ = etag
		})
	}
}

// --- Store ---

// hookedStore is a Store over memory with a hook, and the memory behind it.
func hookedStore(hook blobtest.Hook) (*blob.Store, *blob.Memory) {
	mem := blob.NewMemory()
	h := blobtest.WithHook(mem)
	h.SetHook(hook)
	return blob.NewStore(h, "n1", 1, blob.Options{Backoff: time.Millisecond, Settle: time.Second}), mem
}

// An unknown outcome that the read-back can't settle either, followed by the
// caller's context ending, is ErrUnknown and the context's error, not a bare
// context error.
func TestStoreKeepsAnUnknownOutcomeWhenTheContextEnds(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, mem := hookedStore(func(c context.Context, op blobtest.Op, key string, after bool) error {
		switch {
		case op == blobtest.OpCreate && after:
			cancel()
			return errors.New("response lost")
		case op == blobtest.OpHead:
			return errors.New("storage slow") // even on the store's own context
		}
		return nil
	})
	_, err := s.Create(ctx, "k", strings.NewReader("v"))
	if !errors.Is(err, blob.ErrUnknown) || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want ErrUnknown and context.Canceled", err)
	}
	if _, err := mem.Head(context.Background(), "k"); err != nil {
		t.Fatal("the write should have landed")
	}
	// The same for an update and a delete.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	s2, mem2 := hookedStore(func(c context.Context, op blobtest.Op, key string, after bool) error {
		if (op == blobtest.OpUpdate || op == blobtest.OpDelete) && after {
			cancel2()
			return errors.New("response lost")
		}
		if op == blobtest.OpHead {
			return errors.New("storage slow")
		}
		return nil
	})
	etag, _ := mem2.Create(context.Background(), "k", strings.NewReader("v"), blob.WriteID{})
	if _, err := s2.Update(ctx2, "k", strings.NewReader("w"), etag); !errors.Is(err, blob.ErrUnknown) || !errors.Is(err, context.Canceled) {
		t.Fatalf("update: got %v", err)
	}
}

// A write that landed is reported as a success even if the caller's context
// ended first, because the read-back runs on a context of its own.
func TestStoreSettlesALandedWriteAfterTheCallerGaveUp(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"create", "update", "delete"} {
		t.Run(op, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var readBackCtxEnded atomic.Bool
			var fired atomic.Bool
			s, mem := hookedStore(func(c context.Context, o blobtest.Op, key string, after bool) error {
				if o == blobtest.OpHead && fired.Load() && c.Err() != nil {
					readBackCtxEnded.Store(true)
				}
				if after && (o == blobtest.OpCreate || o == blobtest.OpUpdate || o == blobtest.OpDelete) && fired.CompareAndSwap(false, true) {
					cancel()
					return errors.New("response lost")
				}
				return nil
			})
			var etag blob.ETag
			if op != "create" {
				fired.Store(true) // not for the setup
				etag, _ = s.Create(context.Background(), "k", strings.NewReader("old"))
				fired.Store(false)
			}
			var err error
			switch op {
			case "create":
				etag, err = s.Create(ctx, "k", strings.NewReader("v"))
			case "update":
				etag, err = s.Update(ctx, "k", strings.NewReader("new"), etag)
			case "delete":
				err = s.Delete(ctx, "k", etag)
			}
			if err != nil {
				t.Fatalf("got %v, want success: the write landed", err)
			}
			if readBackCtxEnded.Load() {
				t.Fatal("the read-back ran on the caller's cancelled context")
			}
			info, herr := mem.Head(context.Background(), "k")
			switch op {
			case "delete":
				if !errors.Is(herr, blob.ErrNotFound) {
					t.Fatalf("object still there: %v", herr)
				}
			case "update":
				if herr != nil || info.ETag != etag || info.WriteID.Node != "n1" {
					t.Fatalf("head %+v %v, update returned %q", info, herr, etag)
				}
			default:
				if herr != nil || info.ETag != etag {
					t.Fatalf("head %+v %v, create returned %q", info, herr, etag)
				}
			}
		})
	}
}

// The context ends in the backoff wait after a not-applied attempt: the error
// is the not-applied one with the context's, never claiming an unknown outcome.
func TestStoreKeepsNotAppliedWhenTheContextEnds(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, mem := hookedStore(func(c context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after {
			cancel()
			return errors.New("503 SlowDown")
		}
		return nil
	})
	_, err := s.Create(ctx, "k", strings.NewReader("v"))
	if !errors.Is(err, blob.ErrNotApplied) || errors.Is(err, blob.ErrUnknown) || !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if _, err := mem.Head(context.Background(), "k"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatal("nothing should have been written")
	}
}
