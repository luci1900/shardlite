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
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/pos"
)

// The conformance suite: what every journal's store has to do, a bucket or a
// disk alike. It holds the semantics the protocol leans on (a number is taken
// once, a fence is an empty segment, a listing is in number order, a write id
// comes back on a head, a lost response costs a retry and never a fence)
// rather than spelling them per backend, so two very different engines are
// interchangeable behind one interface. It runs through a Store, as everything
// does, and the record's own writes go through the store underneath it, as a
// foreign writer's would.
//
// It is the memory backend by default, and the S3 backend on the emulator when
// SHARDLITE_TEST_S3_ENDPOINT is set (docker/compose.s3.yaml's).

// factory builds one test journal. Its store speaks for the journal of node
// "n1" at epoch 1, over a record whose keys sit under Prefix.
type factory func(t *testing.T) (*Store, record)

// record is the store behind a test journal's adapter (the record).
type record struct {
	Below  blob.Backend
	Prefix string // before the journal's keys in Below, "" when Below is the memory backend's root
	// Foreign writes a segment as another writer would, for the tests that need
	// one. A local store has no record under it, so it writes through a store
	// of its own with another writer's identity.
	Foreign func(t *testing.T, seq pos.Seg, body string)
}

func (r record) key(node cluster.Node, seq pos.Seg) string { return r.Prefix + Key(node, seq) }

// foreign writes a segment under the journal's number as another writer, in
// the record.
func (r record) foreign(t *testing.T, seq pos.Seg, body string) {
	t.Helper()
	if _, err := r.Below.Create(context.Background(), r.key("n1", seq), strings.NewReader(body),
		blob.WriteID{Node: "n9", Epoch: 1, N: 1}); err != nil {
		t.Fatal(err)
	}
}

func memoryFactory(t *testing.T) (*Store, record) {
	t.Helper()
	m := blob.NewMemory()
	rec := record{Below: m}
	rec.Foreign = rec.foreign
	return NewStore(NewBlob(m, "n1"), "n1", 1, Options{Backoff: time.Millisecond}), rec
}

// localFactory returns the node's own disk: the local database in a temporary
// directory, with the journal of node "n1" in it.
func localFactory(t *testing.T) (*Store, record) {
	t.Helper()
	db, err := localdb.Open(filepath.Join(t.TempDir(), localdb.Name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	back := NewLocal(db.DB, "n1")
	rec := record{}
	rec.Foreign = func(t *testing.T, seq pos.Seg, body string) {
		t.Helper()
		// Another writer: another node, another epoch.
		other := NewStore(back, "n9", 1, Options{Backoff: time.Millisecond})
		if err := other.Create(context.Background(), seq, bytes.NewReader([]byte(body))); err != nil {
			t.Fatal(err)
		}
	}
	return NewStore(back, "n1", 1, Options{Backoff: time.Millisecond}), rec
}

// s3Factory returns the record on the emulator, with the test's keys under a
// prefix of their own.
func s3Factory(t *testing.T) (*Store, record) {
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
	// The bucket is made with a plain client: the one configured may have a
	// short timeout or faults, and the endpoint asked for may be unreachable
	// on purpose.
	po := o
	po.Endpoint = endpoint
	if plain, err := blob.NewS3(context.Background(), po); err != nil {
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
	prefix := "journal-conformance/" + hex.EncodeToString(id[:]) + "/"
	prefixed := blob.WithPrefix(b, prefix)
	// The record sees the keys Below does: WithPrefix hides its prefix, so the
	// record's keys are relative to it, like the memory backend's are to its
	// root.
	rec := record{Below: prefixed}
	rec.Foreign = rec.foreign
	return NewStore(NewBlob(prefixed, "n1"), "n1", 1, Options{Backoff: time.Millisecond}), rec
}

func TestConformanceMemory(t *testing.T) { conformance(t, memoryFactory) }

func TestConformanceS3(t *testing.T) { conformance(t, s3Factory) }

func TestConformanceLocal(t *testing.T) { conformance(t, localFactory) }

// conformance runs every case against one factory.
func conformance(t *testing.T, mk factory) {
	ctx := context.Background()
	run := func(name string, f func(t *testing.T, js *Store, rec record)) {
		t.Run(name, func(t *testing.T) {
			js, rec := mk(t)
			f(t, js, rec)
		})
	}

	// A number is taken once: the second create fails with blob.ErrExists and
	// the first segment stays exactly as it was.
	run("CreateOnce", func(t *testing.T, js *Store, rec record) {
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

	// A write retried with its own write id succeeds: the read-back tells an
	// own earlier attempt from a foreign segment. A retry that names another
	// writer's id does not.
	run("OwnRetryIsSettled", func(t *testing.T, js *Store, rec record) {
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
		// Another writer's write id under a number stays there: blob.ErrExists.
		if err := js.Create(ctx, 2, body("mine")); err != nil {
			t.Fatal(err)
		}
		if err := js.CreateWithID(ctx, 2, body("theirs"), blob.WriteID{Node: "n9", Epoch: 1, N: 1}); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("a foreign write id: got %v, want blob.ErrExists", err)
		}
	})

	// The record can hold a foreign segment under the journal's number again
	// only if the write id says so: the store asks, not takes a plain "exists"
	// for its own lost response.
	run("ForeignSegmentIsSomebodyElses", func(t *testing.T, js *Store, rec record) {
		rec.Foreign(t, 1, "theirs")
		if err := js.CreateWithID(ctx, 1, body("ours"), js.NewWriteID()); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("got %v, want blob.ErrExists", err)
		}
		if got, err := allOf(ctx, js, 1); err != nil || got != "theirs" {
			t.Fatalf("the segment changed: %q %v", got, err)
		}
	})

	// An empty segment is a fence: size 0, a real object, and its create and
	// write id behave like any other segment's.
	run("FenceIsAnEmptySegment", func(t *testing.T, js *Store, rec record) {
		id := js.NewWriteID()
		if err := js.CreateWithID(ctx, 1, body(""), id); err != nil {
			t.Fatal(err)
		}
		info, err := js.Head(ctx, 1)
		if err != nil || info.Size != 0 {
			t.Fatalf("head: %+v %v, want size 0", info, err)
		}
		if info.WriteID != id {
			t.Fatalf("head: %+v, want write id %+v", info.WriteID, id)
		}
		if rc, err := js.Get(ctx, 1); err != nil {
			t.Fatal(err)
		} else if b, err := io.ReadAll(rc); err != nil || len(b) != 0 {
			t.Fatalf("a fence's body is %d bytes: %v", len(b), err)
		} else {
			rc.Close()
		}
		if err := js.Create(ctx, 1, body("")); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("second create of a fence: %v, want blob.ErrExists", err)
		}
	})

	// A listing is in number order, not the order of the keys of unpadded
	// numbers: 10 after 2. List starts at the number it is given, and lists to
	// the end of the journal.
	run("ListingIsInNumberOrder", func(t *testing.T, js *Store, rec record) {
		for _, seq := range []pos.Seg{1, 2, 10, 22} {
			if err := js.Create(ctx, seq, body(fmt.Sprint(seq))); err != nil {
				t.Fatal(err)
			}
		}
		assertListing(t, js.List(ctx, 1), 1, 2, 10, 22)
		assertListing(t, js.List(ctx, 2), 2, 10, 22)
		assertListing(t, js.List(ctx, 23))
	})

	// The record's keys are the layout the record's other callers compose: keys
	// are found by one path, not two. A local store has no record under it, so
	// it has no such layout.
	run("KeysAreTheRecordsLayout", func(t *testing.T, js *Store, rec record) {
		if rec.Below == nil {
			t.Skip("a local store has no record's keys")
		}
		for _, seq := range []pos.Seg{1, 2} {
			if err := js.Create(ctx, seq, body("v")); err != nil {
				t.Fatal(err)
			}
		}
		for _, seq := range []pos.Seg{1, 2} {
			key := rec.key("n1", seq)
			info, err := rec.Below.Head(ctx, key)
			if err != nil {
				t.Fatalf("%s: %v", key, err)
			}
			if info.Size != 1 {
				t.Fatalf("%s: %d bytes", key, info.Size)
			}
		}
	})

	run("GetRange", func(t *testing.T, js *Store, rec record) {
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
				t.Fatalf("range %d+%d: got %q, want %q", tc.off, tc.n, got, err)
			}
			rc.Close()
		}
		if _, err := js.GetRange(ctx, 2, 0, 1); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("range of a missing segment: %v", err)
		}
	})

	// A tail: a negative offset asks for the last bytes of a segment, which is
	// how a footer is read, and which a reader of the end of a segment cannot
	// ask for by offset because it does not know where the end is. An empty
	// segment has no tail, which is an empty read and not an error.
	run("GetRangeTail", func(t *testing.T, js *Store, rec record) {
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
		// An empty segment is a fence, and has no last bytes.
		if err := js.Create(ctx, 2, body("")); err != nil {
			t.Fatal(err)
		}
		rc, err := js.GetRange(ctx, 2, -10, 0)
		if err != nil {
			t.Fatalf("tail of an empty segment: %v", err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil || len(got) != 0 {
			t.Fatalf("tail of an empty segment: got %q: %v", got, err)
		}
	})

	// A delete is garbage collection, not an update: gone is gone, and deleting
	// what is gone is done. Nothing of it reads back, not even whole.
	run("Delete", func(t *testing.T, js *Store, rec record) {
		if err := js.Create(ctx, 1, body("v")); err != nil {
			t.Fatal(err)
		}
		if err := js.Delete(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := js.Head(ctx, 1); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("after delete: %v", err)
		}
		if _, err := js.Get(ctx, 1); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("a whole read of a missing segment: %v", err)
		}
		if err := js.Delete(ctx, 1); err != nil {
			t.Fatalf("deleting what isn't there: %v", err)
		}
	})

	// Segments can be as large as a database, and every attempt rereads the
	// body from its start, so a body that is bigger than memory is fine.
	run("BigAndBinary", func(t *testing.T, js *Store, rec record) {
		data := make([]byte, 3<<20+17)
		rand.Read(data)
		if err := js.Create(ctx, 1, bodyBytes(data)); err != nil {
			t.Fatal(err)
		}
		rc, err := js.Get(ctx, 1)
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
		if info, err := js.Head(ctx, 1); err != nil || info.Size != int64(len(data)) {
			t.Fatalf("head: %+v %v", info, err)
		}
	})

	// A lost response is a retry of the same body and the same write id, never
	// a fence: the first create lands and its response is lost, the retry finds
	// it our own and succeeds, and the segment keeps the first body and id.
	// Only the record loses responses: a local store's write is a transaction,
	// which either commits or doesn't.
	t.Run("LostResponseIsARetry", func(t *testing.T) {
		_, rec := mk(t)
		if rec.Below == nil {
			t.Skip("a local store loses no response")
		}
		hooked := blobtest.WithHook(rec.Below)
		store := NewStore(NewBlob(hooked, "n1"), "n1", 1, Options{Backoff: time.Millisecond})
		var lost atomic.Int64
		hooked.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
			if op == blobtest.OpCreate && key == rec.key("n1", 1) && after && lost.Add(1) == 1 {
				return errors.New("injected: the response was lost")
			}
			return nil
		})
		id := store.NewWriteID()
		if err := store.CreateWithID(ctx, 1, body("one"), id); err != nil {
			t.Fatal("a lost response turned into an error:", err)
		}
		info, err := store.Head(ctx, 1)
		if err != nil || info.Size != int64(len("one")) || info.WriteID != id {
			t.Fatalf("after the retry: %+v %v", info, err)
		}
		if got, err := allOf(ctx, store, 1); err != nil || got != "one" {
			t.Fatalf("the retry rewrote the segment: %q %v", got, err)
		}
	})

	// When every answer is lost and the read-back fails too, the caller sees an
	// outcome that stayed unknown, never blob.ErrExists (which would read as
	// "somebody else's").
	t.Run("UnknownStaysUnknown", func(t *testing.T) {
		_, rec := mk(t)
		if rec.Below == nil {
			t.Skip("a local store loses no response")
		}
		hooked := blobtest.WithHook(rec.Below)
		store := NewStore(NewBlob(hooked, "n1"), "n1", 1, Options{Backoff: time.Millisecond, Attempts: 2})
		hooked.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
			// The creates land, and every response of create and head is lost.
			if after && key == rec.key("n1", 1) {
				return errors.New("injected: the response was lost")
			}
			return nil
		})
		if err := store.Create(ctx, 1, body("one")); !errors.Is(err, blob.ErrUnknown) {
			t.Fatalf("got %v, want blob.ErrUnknown", err)
		}
	})
}

func assertListing(t *testing.T, it iter.Seq2[pos.Seg, error], want ...pos.Seg) {
	t.Helper()
	var seqs []pos.Seg
	for seq, err := range it {
		if err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
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
func allOf(ctx context.Context, js *Store, seq pos.Seg) (string, error) {
	rc, err := js.Get(ctx, seq)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	return string(b), err
}
