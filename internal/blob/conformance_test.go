package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

// backendFactory returns a Backend whose keys are private to the test.
type backendFactory func(t *testing.T) Backend

func memoryFactory(t *testing.T) Backend { return NewMemory() }

// s3Factory returns the S3 backend on the emulator, with the test's keys under
// a prefix of their own. The tests that use it are skipped unless
// SHARDLITE_TEST_S3_ENDPOINT is set (docker/compose.s3.yamls one).
func s3Factory(t *testing.T) Backend {
	t.Helper()
	endpoint := os.Getenv("SHARDLITE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("SHARDLITE_TEST_S3_ENDPOINT is not set: start RustFS with docker/compose.s3.yaml")
	}
	return s3At(t, endpoint, nil)
}

func s3At(t *testing.T, endpoint string, mutate func(*S3Options)) Backend {
	t.Helper()
	// The emulator script's credentials, unless the environment has its own (TestMain).
	bucket := os.Getenv("SHARDLITE_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "shardlite-test"
	}
	o := S3Options{Bucket: bucket, Endpoint: endpoint}
	// The bucket is made with a plain client on the emulator: the mutated one may have
	// a short timeout or faults, and the endpoint asked for may be unreachable on purpose.
	po := o
	if e := os.Getenv("SHARDLITE_TEST_S3_ENDPOINT"); e != "" {
		po.Endpoint = e
	}
	if plain, err := NewS3(context.Background(), po); err != nil {
		t.Fatal(err)
	} else if err := plain.EnsureBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	if mutate != nil {
		mutate(&o)
	}
	b, err := NewS3(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	var id [8]byte
	rand.Read(id[:])
	return WithPrefix(b, "conformance/"+hex.EncodeToString(id[:])+"/")
}

func TestConformanceMemory(t *testing.T) { conformance(t, memoryFactory) }

func TestConformanceS3(t *testing.T) { conformance(t, s3Factory) }

func storeOf(b Backend) *Store {
	return NewStore(b, "n1", 1, Options{Backoff: time.Millisecond})
}

// conformance is what every Backend has to do, whatever its store.
func conformance(t *testing.T, mk backendFactory) {
	ctx := context.Background()
	run := func(name string, f func(t *testing.T, b Backend, s *Store)) {
		t.Run(name, func(t *testing.T) {
			b := mk(t)
			f(t, b, storeOf(b))
		})
	}

	run("CreateTwice", func(t *testing.T, b Backend, s *Store) {
		if _, err := s.Create(ctx, "a/b", strings.NewReader("one")); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Create(ctx, "a/b", strings.NewReader("two")); !errors.Is(err, ErrExists) {
			t.Fatalf("got %v, want ErrExists", err)
		}
		rc, _, err := s.Get(ctx, "a/b")
		if err != nil || readAll(t, rc) != "one" {
			t.Fatalf("object changed: %v", err)
		}
	})

	run("UpdateWithAStaleETag", func(t *testing.T, b Backend, s *Store) {
		e1, err := s.Create(ctx, "k", strings.NewReader("v1"))
		if err != nil {
			t.Fatal(err)
		}
		e2, err := s.Update(ctx, "k", strings.NewReader("v2"), e1)
		if err != nil {
			t.Fatal(err)
		}
		if e2 == e1 {
			t.Fatal("different contents, same ETag")
		}
		if _, err := s.Update(ctx, "k", strings.NewReader("v3"), e1); !errors.Is(err, ErrConflict) {
			t.Fatalf("got %v, want ErrConflict", err)
		}
		if _, err := s.Update(ctx, "missing", strings.NewReader("v"), e1); !errors.Is(err, ErrNotFound) {
			t.Fatalf("update of a missing object: got %v, want ErrNotFound", err)
		}
		rc, etag, err := s.Get(ctx, "k")
		if err != nil || readAll(t, rc) != "v2" || etag != e2 {
			t.Fatalf("the stale update changed the object or its ETag: %q %v", etag, err)
		}
		// The ETag Update returned is good for the next update.
		if _, err := s.Update(ctx, "k", strings.NewReader("v3"), e2); err != nil {
			t.Fatal(err)
		}
	})

	run("ETagsAgree", func(t *testing.T, b Backend, s *Store) {
		created, err := s.Create(ctx, "data/shards/users/42/k3x9/1-2.12.ltx", strings.NewReader("contents"))
		if err != nil {
			t.Fatal(err)
		}
		info, err := s.Head(ctx, "data/shards/users/42/k3x9/1-2.12.ltx")
		if err != nil || info.ETag != created || strings.ContainsAny(string(info.ETag), `"`) {
			t.Fatalf("head: %+v %v (create gave %q)", info, err, created)
		}
		rc, got, err := s.Get(ctx, "data/shards/users/42/k3x9/1-2.12.ltx")
		rc.Close()
		if err != nil || got != created {
			t.Fatalf("get: %q %v", got, err)
		}
		for li, err := range s.List(ctx, "data/shards/") {
			if err != nil || li.ETag != created {
				t.Fatalf("list: %+v %v", li, err)
			}
		}
	})

	run("WriteIDsComeBackWithHead", func(t *testing.T, b Backend, s *Store) {
		for i, id := range []WriteID{{Node: "n1", Epoch: 1, N: 1}, {Node: "a:b c/é.d", Epoch: 3, N: 9}, {Node: "host.example.com", Epoch: 1 << 40, N: 1 << 33}, {}} {
			key := fmt.Sprintf("id/%d", i)
			etag, err := b.Create(ctx, key, strings.NewReader("v"), id)
			if err != nil {
				t.Fatal(err)
			}
			info, err := b.Head(ctx, key)
			if err != nil || info.WriteID != id || info.ETag != etag {
				t.Fatalf("id %+v: head %+v %v", id, info, err)
			}
			// An update gets its own id.
			next := WriteID{Node: "n2", Epoch: 7, N: uint64(i + 1)}
			if _, err := b.Update(ctx, key, strings.NewReader("w"), etag, next); err != nil {
				t.Fatal(err)
			}
			if info, err = b.Head(ctx, key); err != nil || info.WriteID != next {
				t.Fatalf("after update: %+v %v", info, err)
			}
		}
		// Through the Store the id is its node, epoch and counter.
		if _, err := s.Create(ctx, "stored", strings.NewReader("one")); err != nil {
			t.Fatal(err)
		}
		if info, err := s.Head(ctx, "stored"); err != nil || info.WriteID != (WriteID{Node: "n1", Epoch: 1, N: 1}) {
			t.Fatalf("head: %+v %v", info, err)
		}
	})

	run("GetRange", func(t *testing.T, b Backend, s *Store) {
		s.Create(ctx, "k", strings.NewReader("0123456789"))
		for _, tc := range []struct {
			off, n int64
			want   string
		}{{7, 3, "789"}, {0, 4, "0123"}, {4, 0, "456789"}, {8, 100, "89"}} {
			rc, err := s.GetRange(ctx, "k", tc.off, tc.n)
			if err != nil {
				t.Fatalf("range %d+%d: %v", tc.off, tc.n, err)
			}
			if got := readAll(t, rc); got != tc.want {
				t.Fatalf("range %d+%d: got %q, want %q", tc.off, tc.n, got, tc.want)
			}
		}
		if _, err := s.GetRange(ctx, "nope", 0, 1); !errors.Is(err, ErrNotFound) {
			t.Fatalf("range of a missing object: %v", err)
		}
		// Past the end is an error that retrying won't fix.
		if _, err := s.GetRange(ctx, "k", 11, 1); err == nil || Transient(err) {
			t.Fatalf("range past the end: %v", err)
		}
	})

	run("HeadAndFences", func(t *testing.T, b Backend, s *Store) {
		s.Create(ctx, "fence", strings.NewReader(""))
		info, err := s.Head(ctx, "fence")
		if err != nil || info.Size != 0 || info.LastModified.IsZero() {
			t.Fatalf("size=%d modified=%v err=%v", info.Size, info.LastModified, err)
		}
		if _, err := s.Head(ctx, "nope"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("got %v", err)
		}
		if _, _, err := s.Get(ctx, "nope"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("get of a missing object: %v", err)
		}
		// An empty object is a real object, which a second create finds.
		if _, err := s.Create(ctx, "fence", strings.NewReader("")); !errors.Is(err, ErrExists) {
			t.Fatalf("second create of a fence: %v", err)
		}
	})

	run("BigAndBinaryObjects", func(t *testing.T, b Backend, s *Store) {
		data := make([]byte, 3<<20+17)
		rand.Read(data)
		if _, err := s.Create(ctx, "big", bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		rc, _, err := s.Get(ctx, "big")
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, data) {
			t.Fatal("contents differ")
		}
		rc, err = s.GetRange(ctx, "big", 1<<20, 1000)
		if err != nil {
			t.Fatal(err)
		}
		got, _ = io.ReadAll(rc)
		rc.Close()
		if !bytes.Equal(got, data[1<<20:1<<20+1000]) {
			t.Fatal("range differs")
		}
		if info, err := s.Head(ctx, "big"); err != nil || info.Size != int64(len(data)) {
			t.Fatalf("size %d %v", info.Size, err)
		}
	})

	run("ListByPrefix", func(t *testing.T, b Backend, s *Store) {
		for _, k := range []string{"data/journal/n1/2", "data/journal/n1/1", "data/journal/n2/1", "state/leases/n1", "data/journal/n10/1"} {
			s.Create(ctx, k, strings.NewReader(k))
		}
		list := func(prefix string) string {
			var got []string
			for info, err := range s.List(ctx, prefix) {
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, fmt.Sprintf("%s:%d", info.Key, info.Size))
			}
			return strings.Join(got, ",")
		}
		if got := list("data/journal/n1/"); got != "data/journal/n1/1:17,data/journal/n1/2:17" {
			t.Fatalf("got %v", got)
		}
		if got := list("nothing/"); got != "" {
			t.Fatalf("got %v", got)
		}
		if got := list(""); strings.Count(got, ",") != 4 {
			t.Fatalf("whole listing: %v", got)
		}
	})

	run("ListPaginates", func(t *testing.T, b Backend, s *Store) {
		const n = 1105 // a listing page is at most 1000 keys
		var wg errgroup.Group
		wg.SetLimit(32)
		for i := range n {
			wg.Go(func() error {
				_, err := s.Create(ctx, fmt.Sprintf("many/%05d", i), strings.NewReader("x"))
				return err
			})
		}
		if err := wg.Wait(); err != nil {
			t.Fatal(err)
		}
		prev, count := "", 0
		for info, err := range s.List(ctx, "many/") {
			if err != nil {
				t.Fatal(err)
			}
			if info.Key <= prev {
				t.Fatalf("out of order: %q after %q", info.Key, prev)
			}
			prev = info.Key
			count++
		}
		if count != n {
			t.Fatalf("listed %d of %d", count, n)
		}
		// From after a key, across the page boundary: the next key is the first.
		after, count := "many/00099", 0
		for info, err := range s.ListAfter(ctx, "many/", after) {
			if err != nil {
				t.Fatal(err)
			}
			if count == 0 && info.Key != "many/00100" {
				t.Fatalf("listing after %q began at %q", after, info.Key)
			}
			count++
		}
		if count != n-100 {
			t.Fatalf("listed %d after %q, want %d", count, after, n-100)
		}
	})

	run("Delete", func(t *testing.T, b Backend, s *Store) {
		s.Create(ctx, "k", strings.NewReader("v"))
		if err := s.Delete(ctx, "k", ""); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Head(ctx, "k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("after delete: %v", err)
		}
		// Deleting what isn't there is ErrNotFound on a store that says so and done
		// on one that doesn't (S3 answers success): callers accept both.
		if err := s.Delete(ctx, "k", ""); err != nil && !errors.Is(err, ErrNotFound) {
			t.Fatalf("second delete: %v", err)
		}
		// The key can be created again.
		if _, err := s.Create(ctx, "k", strings.NewReader("w")); err != nil {
			t.Fatal(err)
		}
	})

	// Conditional deletes are required: a stale ETag is refused and the object
	// stays, the current one deletes it, and a missing key is not found.
	run("ConditionalDelete", func(t *testing.T, b Backend, s *Store) {
		e1, err := s.Create(ctx, "k", strings.NewReader("v"))
		if err != nil {
			t.Fatal(err)
		}
		e2, err := s.Update(ctx, "k", strings.NewReader("w"), e1)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "k", e1); !errors.Is(err, ErrConflict) {
			t.Fatalf("delete with a stale ETag: %v, want ErrConflict", err)
		}
		if _, err := s.Head(ctx, "k"); err != nil {
			t.Fatalf("a refused conditional delete removed the object: %v", err)
		}
		if err := s.Delete(ctx, "k", e2); err != nil {
			t.Fatalf("delete with the current ETag: %v", err)
		}
		if _, err := s.Head(ctx, "k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("after delete: %v", err)
		}
		// A missing key is not found on S3 and a failed precondition on RustFS
		// (412): either way the caller sees that somebody else deleted it.
		if err := s.Delete(ctx, "k", e2); !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) {
			t.Fatalf("conditional delete of a missing key: %v, want ErrNotFound or ErrConflict", err)
		}
	})

	run("WithPrefixesAreSeparate", func(t *testing.T, b Backend, s *Store) {
		x, y := storeOf(WithPrefix(b, "x/")), storeOf(WithPrefix(b, "y/"))
		if _, err := x.Create(ctx, "state/leases/n1", strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := y.Create(ctx, "state/leases/n1", strings.NewReader("y")); err != nil {
			t.Fatal(err)
		}
		var keys []string
		for info, err := range x.List(ctx, "state/leases/") {
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, info.Key)
		}
		if len(keys) != 1 || keys[0] != "state/leases/n1" {
			t.Fatalf("listing: %v", keys)
		}
		if _, err := s.Head(ctx, "state/leases/n1"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("head without the prefix: %v", err)
		}
	})

	// Two conditional creates racing on one key: exactly one wins. Store's retries
	// turn a store's "try again" answers into the real one (exists).
	run("CreateRace", func(t *testing.T, b Backend, s *Store) {
		for round := range 15 {
			key := fmt.Sprintf("race/c%d", round)
			results := make([]error, 4)
			var wg errgroup.Group
			for i := range results {
				wg.Go(func() error {
					st := NewStore(b, fmt.Sprintf("n%d", i), 1, Options{Backoff: time.Millisecond, Attempts: 12})
					_, results[i] = st.Create(ctx, key, strings.NewReader(fmt.Sprint(i)))
					return nil
				})
			}
			wg.Wait()
			assertOneWinner(t, round, results, ErrExists)
			// The winner's contents are there, and its write id names it.
			info, err := b.Head(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			for i, err := range results {
				if err == nil && info.WriteID.Node != fmt.Sprintf("n%d", i) {
					t.Fatalf("round %d: writer %d won but the object says %+v", round, i, info.WriteID)
				}
			}
		}
	})

	// Two (or more) conditional updates on one ETag: exactly one wins.
	run("UpdateRace", func(t *testing.T, b Backend, s *Store) {
		for round := range 15 {
			key := fmt.Sprintf("race/u%d", round)
			etag, err := s.Create(ctx, key, strings.NewReader("start"))
			if err != nil {
				t.Fatal(err)
			}
			results := make([]error, 4)
			var wg errgroup.Group
			for i := range results {
				wg.Go(func() error {
					st := NewStore(b, fmt.Sprintf("n%d", i), 1, Options{Backoff: time.Millisecond, Attempts: 12})
					_, results[i] = st.Update(ctx, key, strings.NewReader(fmt.Sprint("by ", i)), etag)
					return nil
				})
			}
			wg.Wait()
			assertOneWinner(t, round, results, ErrConflict)
		}
	})
}

func assertOneWinner(t *testing.T, round int, results []error, loss error) {
	t.Helper()
	wins := 0
	for i, err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, loss):
		default:
			t.Fatalf("round %d: writer %d got %v, want success or %v", round, i, err, loss)
		}
	}
	if wins != 1 {
		t.Fatalf("round %d: %d winners (%v)", round, wins, results)
	}
}
