package journal

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

// The conformance suite: what every journal Store has to do, whatever sits
// under it. It holds the semantics the protocol leans on (a number is taken
// once, a fence is an empty segment, a listing is in number order, a lost
// response is told from somebody else's segment) and leaves retries and the
// outcome table to the blob package, which tests them for blob.Store. A new
// journal store runs it by adding a factory.
//
// It is the memory backend by default, and the S3 backend on the emulator when
// SHARDLITE_TEST_S3_ENDPOINT is set (docker/compose.s3.yaml's).

// rig is one test journal: the store of node "n1"'s journal, written by "n1"
// at epoch 1, with the two hooks into whatever is under it that the cases need.
type rig struct {
	js Store
	// foreign writes a segment into the journal as another writer would,
	// bypassing the store.
	foreign func(seq pos.Seg, body string)
	// lose makes the responses of the next n calls of op ("create", "delete" or
	// "list", or any call if op is "") on the journal's segments lost: the call
	// is applied, and the store sees a failure of unknown outcome.
	lose func(op string, n int)
}

type factory func(t *testing.T) rig

// blobFactory builds the factory for a blob backend: the blob store the
// journal delegates to sits over a fault hook, which lose arms.
func blobFactory(b blob.Backend) factory {
	return func(t *testing.T) rig {
		hooked := blobtest.WithHook(b)
		var op atomic.Pointer[blobtest.Op]
		var left atomic.Int64
		hooked.SetHook(func(ctx context.Context, o blobtest.Op, key string, after bool) error {
			if want := op.Load(); after && strings.HasPrefix(key, Prefix("n1")) && (*want == "" || *want == o) && left.Add(-1) >= 0 {
				return errors.New("injected: the response was lost")
			}
			return nil
		})
		none := blobtest.Op("")
		op.Store(&none)
		return rig{
			js: testJournal(hooked, "n1", "n1", 1),
			foreign: func(seq pos.Seg, body string) {
				t.Helper()
				other := blob.NewStore(b, "n9", 1, blob.Options{})
				if _, err := other.Create(context.Background(), Key("n1", seq), strings.NewReader(body)); err != nil {
					t.Fatal(err)
				}
			},
			lose: func(name string, n int) {
				o := blobtest.Op(name)
				op.Store(&o)
				left.Store(int64(n))
			},
		}
	}
}

func memoryFactory(t *testing.T) rig { return blobFactory(blob.NewMemory())(t) }

// s3Factory returns blob storage on the emulator, with the test's keys under a
// prefix of their own.
func s3Factory(t *testing.T) rig {
	t.Helper()
	endpoint := os.Getenv("SHARDLITE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("SHARDLITE_TEST_S3_ENDPOINT is not set: start RustFS with docker/compose.s3.yaml")
	}
	bucket := os.Getenv("SHARDLITE_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "shardlite-test"
	}
	o := blob.S3Options{Bucket: bucket, Endpoint: endpoint}
	if plain, err := blob.NewS3(context.Background(), o); err != nil {
		t.Fatal(err)
	} else if err := plain.EnsureBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, err := blob.NewS3(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	var id [8]byte
	rand.Read(id[:])
	return blobFactory(blob.WithPrefix(b, "journal-conformance/"+hex.EncodeToString(id[:])+"/"))(t)
}

func TestConformanceMemory(t *testing.T) { conformance(t, memoryFactory) }

func TestConformanceS3(t *testing.T) { conformance(t, s3Factory) }

// conformance runs every case against one factory.
func conformance(t *testing.T, mk factory) {
	ctx := context.Background()
	run := func(name string, f func(t *testing.T, r rig)) {
		t.Run(name, func(t *testing.T) { f(t, mk(t)) })
	}

	// A number is taken once: the second create fails with blob.ErrExists and
	// the first segment stays exactly as it was.
	run("CreateOnce", func(t *testing.T, r rig) {
		js := r.js
		if err := js.Create(ctx, 1, body("one")); err != nil {
			t.Fatal(err)
		}
		if err := js.Create(ctx, 1, body("two")); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("got %v, want blob.ErrExists", err)
		}
		if got, err := allOf(ctx, js, 1); err != nil || got != "one" {
			t.Fatalf("the segment changed: %q %v", got, err)
		}
	})

	// A write retried with its own write id succeeds: the store tells an own
	// earlier attempt from a foreign segment. A retry that names another
	// writer's id does not.
	run("OwnRetryIsSettled", func(t *testing.T, r rig) {
		js := r.js
		id := js.NewWriteID()
		if err := js.CreateWithID(ctx, 1, body("one"), id); err != nil {
			t.Fatal(err)
		}
		if err := js.CreateWithID(ctx, 1, body("two"), id); err != nil {
			t.Fatal("a retry of our own create was not recognised as ours:", err)
		}
		if got, err := allOf(ctx, js, 1); err != nil || got != "one" {
			t.Fatalf("the retry rewrote the segment: %q %v", got, err)
		}
		if id2 := js.NewWriteID(); id2 == id {
			t.Fatalf("two write ids are %+v", id)
		}
		if err := js.CreateWithID(ctx, 1, body("three"), js.NewWriteID()); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("a retry under another id of ours: got %v, want blob.ErrExists", err)
		}
	})

	// Somebody else's segment under the number stays there, and a create of ours
	// is told so, retried or not.
	run("ForeignSegmentIsSomebodyElses", func(t *testing.T, r rig) {
		js := r.js
		r.foreign(1, "theirs")
		if err := js.CreateWithID(ctx, 1, body("ours"), js.NewWriteID()); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("got %v, want blob.ErrExists", err)
		}
		if err := js.Create(ctx, 1, body("ours")); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("got %v, want blob.ErrExists", err)
		}
		if got, err := allOf(ctx, js, 1); err != nil || got != "theirs" {
			t.Fatalf("the segment changed: %q %v", got, err)
		}
	})

	// An empty segment is a fence: a real segment whose number is taken, with
	// no bytes and no tail.
	run("FenceIsAnEmptySegment", func(t *testing.T, r rig) {
		js := r.js
		if err := js.CreateWithID(ctx, 1, body(""), js.NewWriteID()); err != nil {
			t.Fatal(err)
		}
		if got, err := allOf(ctx, js, 1); err != nil || got != "" {
			t.Fatalf("a fence's body is %q: %v", got, err)
		}
		if err := js.Create(ctx, 1, body("")); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("second create of a fence: %v, want blob.ErrExists", err)
		}
		rc, err := js.GetRange(ctx, 1, -10, 0)
		if err != nil {
			t.Fatalf("tail of an empty segment: %v", err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil || len(got) != 0 {
			t.Fatalf("tail of an empty segment: got %q: %v", got, err)
		}
	})

	// A listing is in number order, not the order of the keys of unpadded
	// numbers: 10 after 2. List starts at the number it is given, and lists to
	// the end of the journal.
	run("ListingIsInNumberOrder", func(t *testing.T, r rig) {
		js := r.js
		for _, seq := range []pos.Seg{1, 2, 10, 22} {
			if err := js.Create(ctx, seq, body(fmt.Sprint(seq))); err != nil {
				t.Fatal(err)
			}
		}
		assertListing(t, js.List(ctx, 1), 1, 2, 10, 22)
		assertListing(t, js.List(ctx, 2), 2, 10, 22)
		assertListing(t, js.List(ctx, 23))
	})

	// A listing says each segment's size and when the store says it was
	// written, which is what journal expiry decides by: a fence is size 0 (it is
	// never deleted), and an old segment is told by its write time.
	run("ListingSaysSizesAndWriteTimes", func(t *testing.T, r rig) {
		js := r.js
		if err := js.Create(ctx, 1, body("0123456789")); err != nil {
			t.Fatal(err)
		}
		if _, err := Fence(ctx, js, 2); err != nil {
			t.Fatal(err)
		}
		var got []Segment
		for seg, err := range js.List(ctx, 1) {
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, seg)
		}
		if len(got) != 2 || got[0].Seq != 1 || got[0].Size != 10 || got[1].Seq != 2 || got[1].Size != 0 {
			t.Fatalf("listing %+v, want segment 1 of 10 bytes and the fence 2 of 0", got)
		}
		for _, seg := range got {
			if seg.Written.IsZero() {
				t.Fatalf("segment %d lists no write time", seg.Seq)
			}
		}
	})

	run("GetRange", func(t *testing.T, r rig) {
		js := r.js
		if err := js.Create(ctx, 1, body("0123456789")); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			off, n int64
			want   string
		}{{7, 3, "789"}, {0, 4, "0123"}, {4, 0, "456789"}, {8, 100, "89"}} {
			rc, err := js.GetRange(ctx, 1, tc.off, tc.n)
			if err != nil {
				t.Fatalf("range %d+%d: %v", tc.off, tc.n, err)
			}
			if got, err := io.ReadAll(rc); err != nil || string(got) != tc.want {
				t.Fatalf("range %d+%d: got %q, want %q: %v", tc.off, tc.n, got, tc.want, err)
			}
			rc.Close()
		}
		if _, err := js.GetRange(ctx, 2, 0, 1); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("range of a missing segment: %v", err)
		}
	})

	// A tail: a negative offset asks for the last bytes of a segment, which is
	// how a footer is read, and which a reader of the end of a segment cannot
	// ask for by offset because it does not know where the end is.
	run("GetRangeTail", func(t *testing.T, r rig) {
		js := r.js
		if err := js.Create(ctx, 1, body("0123456789")); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			n    int64
			want string
		}{{1, "9"}, {3, "789"}, {10, "0123456789"}, {100, "0123456789"}} {
			rc, err := js.GetRange(ctx, 1, -tc.n, 0)
			if err != nil {
				t.Fatalf("tail of %d bytes: %v", tc.n, err)
			}
			got, err := io.ReadAll(rc)
			rc.Close()
			if err != nil || string(got) != tc.want {
				t.Fatalf("tail of %d bytes: got %q, want %q: %v", tc.n, got, tc.want, err)
			}
		}
		// A tail with a length is a mistake, not a range.
		if _, err := js.GetRange(ctx, 1, -3, 3); err == nil {
			t.Fatal("a tail with a length was accepted")
		}
		if _, err := js.GetRange(ctx, 9, -3, 0); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("tail of a missing segment: %v", err)
		}
	})

	// A lost response is a retry of the same body and the same write id, never
	// a fence: the create lands and its response is lost, and the caller sees
	// success (not blob.ErrExists), with the first body and id kept. Both
	// creates settle: the writer's, with its write id, and the adopter's fence.
	run("LostCreateResponseIsOurs", func(t *testing.T, r rig) {
		js := r.js
		r.lose("create", 1)
		if err := js.CreateWithID(ctx, 1, body("one"), js.NewWriteID()); err != nil {
			t.Fatal("a lost response turned into an error:", err)
		}
		r.lose("create", 1)
		if err := js.Create(ctx, 2, body("")); err != nil {
			t.Fatal("a lost response to a fence turned into an error:", err)
		}
		if got, err := allOf(ctx, js, 1); err != nil || got != "one" {
			t.Fatalf("the retry rewrote the segment: %q %v", got, err)
		}
		if err := js.Create(ctx, 2, body("")); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("the fence is taken: got %v, want blob.ErrExists", err)
		}
	})

	// A lost response over somebody else's segment is still theirs.
	run("LostResponseOverAForeignSegment", func(t *testing.T, r rig) {
		r.foreign(1, "theirs")
		r.lose("create", 1)
		if err := r.js.Create(ctx, 1, body("ours")); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("got %v, want blob.ErrExists", err)
		}
		if got, err := allOf(ctx, r.js, 1); err != nil || got != "theirs" {
			t.Fatalf("the segment changed: %q %v", got, err)
		}
	})

	// When every answer is lost, the read-back included, the caller sees an
	// outcome that stayed unknown, never blob.ErrExists (which would read as
	// "somebody else's").
	run("UnknownStaysUnknown", func(t *testing.T, r rig) {
		r.lose("", 1000)
		if err := r.js.Create(ctx, 1, body("one")); !errors.Is(err, blob.ErrUnknown) {
			t.Fatalf("got %v, want blob.ErrUnknown", err)
		}
	})

	// A delete is garbage collection: gone is gone, and deleting what is gone
	// is done, a lost response included.
	run("Delete", func(t *testing.T, r rig) {
		js := r.js
		for _, seq := range []pos.Seg{1, 2} {
			if err := js.Create(ctx, seq, body("v")); err != nil {
				t.Fatal(err)
			}
		}
		if err := js.Delete(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := js.GetRange(ctx, 1, 0, 0); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("after delete: %v", err)
		}
		if err := js.Delete(ctx, 1); err != nil {
			t.Fatalf("deleting what isn't there: %v", err)
		}
		r.lose("delete", 1)
		if err := js.Delete(ctx, 2); err != nil {
			t.Fatalf("a lost response to a delete: %v", err)
		}
		if _, err := js.GetRange(ctx, 2, 0, 0); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("after the delete with a lost response: %v", err)
		}
	})

	// A listing whose first response is lost is retried, not failed.
	run("LostListResponse", func(t *testing.T, r rig) {
		for _, seq := range []pos.Seg{1, 2} {
			if err := r.js.Create(ctx, seq, body("v")); err != nil {
				t.Fatal(err)
			}
		}
		r.lose("list", 1)
		assertListing(t, r.js.List(ctx, 1), 1, 2)
	})

	// Segments can be as large as a database, and every attempt rereads the
	// body from its start, so a body that is bigger than memory is fine.
	run("BigAndBinary", func(t *testing.T, r rig) {
		js := r.js
		data := make([]byte, 3<<20+17)
		rand.Read(data)
		if err := js.Create(ctx, 1, bodyBytes(data)); err != nil {
			t.Fatal(err)
		}
		rc, err := js.GetRange(ctx, 1, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, data) {
			t.Fatal("contents differ")
		}
		rc, err = js.GetRange(ctx, 1, 1<<20, 1000)
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()
		got, _ = io.ReadAll(rc)
		if !bytes.Equal(got, data[1<<20:1<<20+1000]) {
			t.Fatal("the range differs")
		}
	})
}

// The blob journal's segments are at the keys the other blob callers compose
// (Key), padded so that they list in number order: keys are found by one path,
// not two.
func TestBlobKeysAreTheStorageLayout(t *testing.T) {
	t.Parallel()
	ctx, m := context.Background(), blob.NewMemory()
	js := testJournal(m, "n1", "n1", 1)
	for _, seq := range []pos.Seg{1, 2} {
		if err := js.Create(ctx, seq, body("v")); err != nil {
			t.Fatal(err)
		}
		if info, err := m.Head(ctx, Key("n1", seq)); err != nil || info.Size != 1 {
			t.Fatalf("%s: %+v %v", Key("n1", seq), info, err)
		}
	}
	if got := Key("n1", 129); got != "data/journal/n1/00000000000000000129" {
		t.Fatalf("key %q", got)
	}
}

func assertListing(t *testing.T, it iter.Seq2[Segment, error], want ...pos.Seg) {
	t.Helper()
	var seqs []pos.Seg
	for seg, err := range it {
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seg.Seq)
	}
	if fmt.Sprint(seqs) != fmt.Sprint(want) {
		t.Fatalf("listing: %v, want %v", seqs, want)
	}
}

// body is a body one create writes once (tests don't retry a body of their own).
func body(s string) blob.Body { return bodyBytes([]byte(s)) }

// bodyBytes is b as a body.
func bodyBytes(b []byte) blob.Body { return bytes.NewReader(b) }

// allOf reads a segment whole.
func allOf(ctx context.Context, js Store, seq pos.Seg) (string, error) {
	rc, err := js.GetRange(ctx, seq, 0, 0)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	return string(b), err
}
