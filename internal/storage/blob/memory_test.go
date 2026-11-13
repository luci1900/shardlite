package blob_test

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

// newStore returns a Store over a fresh memory backend behind a fault hook,
// with short backoffs.
func newStore(t *testing.T) (*blob.Store, *blobtest.Hooked) {
	t.Helper()
	h := blobtest.WithHook(blob.NewMemory())
	return blob.NewStore(h, "n1", 1, blob.Options{Backoff: time.Millisecond}), h
}

// notFoundOnDelete is a store that answers a delete of a missing key with
// ErrNotFound, which the Backend contract allows (S3 and the memory backend
// answer success instead). Store has to treat both alike.
type notFoundOnDelete struct{ blob.Backend }

func (n notFoundOnDelete) Delete(ctx context.Context, key string, expected blob.ETag) error {
	if _, err := n.Backend.Head(ctx, key); err != nil {
		return err
	}
	return n.Backend.Delete(ctx, key, expected)
}

// newStrictStore is newStore over a backend that answers ErrNotFound to a
// delete of a missing key.
func newStrictStore(t *testing.T) (*blob.Store, *blobtest.Hooked) {
	t.Helper()
	h := blobtest.WithHook(notFoundOnDelete{blob.NewMemory()})
	return blob.NewStore(h, "n1", 1, blob.Options{Backoff: time.Millisecond}), h
}

func readAll(t *testing.T, rc io.ReadCloser) string {
	t.Helper()
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestWriteIDStoredWithObject(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()
	s.Create(ctx, "a", strings.NewReader("one"))
	info, err := s.Head(ctx, "a")
	if err != nil || info.WriteID != (blob.WriteID{Node: "n1", Epoch: 1, N: 1}) {
		t.Fatalf("head: %+v %v", info, err)
	}
	if _, err := s.Update(ctx, "a", strings.NewReader("two"), info.ETag); err != nil {
		t.Fatal(err)
	}
	if rc, _, err := s.Get(ctx, "a"); err != nil {
		t.Fatalf("get: %v", err)
	} else {
		rc.Close()
	}
	if info, err = s.Head(ctx, "a"); err != nil || info.WriteID.N != 2 {
		t.Fatalf("head after update: %+v %v", info, err)
	}
	// As on S3, listings carry no write ids.
	for info, err := range s.List(ctx, "a") {
		if err != nil || !info.WriteID.IsZero() {
			t.Fatalf("list: %+v %v", info, err)
		}
	}
}

func TestWithPrefix(t *testing.T) {
	t.Parallel()
	m := blob.NewMemory()
	b := blob.WithPrefix(m, "staging/")
	ctx := context.Background()
	if _, err := b.Create(ctx, "state/leases/n1", strings.NewReader("x"), blob.WriteID{}); err != nil {
		t.Fatal(err)
	}
	// It is stored under the prefix, and invisible without it.
	if _, err := m.Head(ctx, "staging/state/leases/n1"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Head(ctx, "state/leases/n1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("head without the prefix: %v", err)
	}
	var keys []string
	for info, err := range b.List(ctx, "state/leases/", "") {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, info.Key)
	}
	if len(keys) != 1 || keys[0] != "state/leases/n1" {
		t.Fatalf("listing: %v", keys)
	}
}

// The ETag is the MD5 of the contents, as on S3: identical bytes keep it
// (callers treat ETags as opaque, see docs/internals.md) and different bytes change it.
func TestETagIsTheContentsMD5(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	ctx := context.Background()
	e1, err := s.Create(ctx, "k", strings.NewReader("same"))
	if err != nil {
		t.Fatal(err)
	}
	sum := md5.Sum([]byte("same"))
	if string(e1) != hex.EncodeToString(sum[:]) {
		t.Fatalf("ETag %q is not the MD5", e1)
	}
	e2, err := s.Update(ctx, "k", strings.NewReader("same"), e1)
	if err != nil || e2 != e1 {
		t.Fatalf("identical bytes: %q %v", e2, err)
	}
	e3, err := s.Update(ctx, "k", strings.NewReader("other"), e1)
	if err != nil || e3 == e1 {
		t.Fatalf("different bytes: %q %v", e3, err)
	}
	if _, err := s.Update(ctx, "k", strings.NewReader("x"), e1); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("update with the old ETag: got %v, want ErrConflict", err)
	}
}

// Contents are copied on the way in and out.
func TestMemoryCopiesContents(t *testing.T) {
	t.Parallel()
	m, ctx := blob.NewMemory(), context.Background()
	in := []byte("hello")
	if _, err := m.Create(ctx, "k", strings.NewReader(string(in)), blob.WriteID{}); err != nil {
		t.Fatal(err)
	}
	rc, _, _ := m.Get(ctx, "k")
	got, _ := io.ReadAll(rc)
	got[0] = 'J' // a caller changing what it was given
	if rc, _, _ = m.Get(ctx, "k"); readAll(t, rc) != "hello" {
		t.Fatal("changing a Get's bytes changed the object")
	}
	r, _ := m.GetRange(ctx, "k", 1, 3)
	part, _ := io.ReadAll(r)
	part[0] = 'X'
	if rc, _, _ = m.Get(ctx, "k"); readAll(t, rc) != "hello" {
		t.Fatal("changing a GetRange's bytes changed the object")
	}
	// A body slice the caller keeps is not the stored object.
	body := []byte("world")
	if _, err := m.Create(ctx, "w", strings.NewReader(string(body)), blob.WriteID{}); err != nil {
		t.Fatal(err)
	}
	body[0] = 'W'
	if rc, _, _ = m.Get(ctx, "w"); readAll(t, rc) != "world" {
		t.Fatal("changing the body after Create changed the object")
	}
}

func TestMemoryClockAndMissingKeys(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m, ctx := blob.NewMemory(blob.WithClock(func() time.Time { return now })), context.Background()
	e, err := m.Create(ctx, "k", strings.NewReader("v"), blob.WriteID{Node: "n", Epoch: 1, N: 1})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Hour)
	m.Create(ctx, "later", strings.NewReader("v"), blob.WriteID{})
	info, _ := m.Head(ctx, "k")
	later, _ := m.Head(ctx, "later")
	if !info.LastModified.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) || later.LastModified.Sub(info.LastModified) != time.Hour {
		t.Fatalf("modified %v and %v", info.LastModified, later.LastModified)
	}
	for li, err := range m.List(ctx, "k", "") {
		if err != nil || !li.LastModified.Equal(info.LastModified) || li.ETag != e {
			t.Fatalf("list: %+v %v", li, err)
		}
	}
	// What a missing key answers, as AWS does.
	if err := m.Delete(ctx, "nope", ""); err != nil {
		t.Fatalf("unconditional delete of a missing key: %v", err)
	}
	if err := m.Delete(ctx, "nope", e); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("conditional delete of a missing key: %v", err)
	}
	if _, err := m.Update(ctx, "nope", strings.NewReader("v"), e, blob.WriteID{}); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("update of a missing key: %v", err)
	}
}
