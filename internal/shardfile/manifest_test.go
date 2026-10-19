package shardfile_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/shardfile"
)

func TestManifestStoreCompareAndSwap(t *testing.T) {
	t.Parallel()
	e, ctx := harness.New(t), context.Background()
	ms := shardfile.NewManifestStore(e.Store)
	m := &shardfile.Manifest{Generation: "g", Owner: "n1", NodeEpoch: 1, Files: []string{}, Journal: "n1"}

	v1, err := ms.Put(ctx, "users/1", m, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Put(ctx, "users/1", m, nil); !errors.Is(err, blob.ErrExists) {
		t.Fatalf("second create: %v", err)
	}
	got, v, err := ms.Get(ctx, "users/1")
	if err != nil || v != v1 || got.Owner != "n1" || got.NodeEpoch != 1 {
		t.Fatalf("get: %+v %v %v", got, v, err)
	}
	m2 := *got
	m2.NodeEpoch = 2
	v2, err := ms.Put(ctx, "users/1", &m2, &v1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Put(ctx, "users/1", &m2, &v1); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("update from an old version: %v", err)
	}
	if err := ms.Delete(ctx, "users/1", v1); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("delete of an old version: %v", err)
	}
	if err := ms.Delete(ctx, "users/1", v2); err != nil {
		t.Fatal(err)
	}
	// A conditional delete removes the object.
	if _, _, err := ms.Get(ctx, "users/1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	if err := ms.Delete(ctx, "users/1", v2); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("delete of a manifest already deleted: %v", err)
	}
}

func TestClaim(t *testing.T) {
	t.Parallel()
	e, ctx := harness.New(t), context.Background()
	h, created, err := e.Files.Open(ctx, "users/1")
	if err != nil || !created {
		t.Fatal(err)
	}
	if m := h.Manifest; m.Owner != "n1" || m.NodeEpoch != e.Lease.Epoch || m.Journal != "n1" {
		t.Fatalf("new manifest: %+v", m)
	}
	// Claiming what is already ours writes nothing.
	etag := h.ETag
	if err := h.Claim(ctx); err != nil || h.ETag != etag {
		t.Fatalf("claim of own shard: %v, etag changed: %v", err, h.ETag != etag)
	}

	// Another node's shard, while its lease is valid, isn't claimed.
	other := coord.NewRegistry(e.Blob, "n2", coord.RegistryOptions{})
	l2, err := other.Acquire(ctx, coord.NodeInfo{Name: "n2"})
	if err != nil {
		t.Fatal(err)
	}
	n2files := &shardfile.Files{Blob: blob.NewStore(e.Blob, "n2", l2.Epoch, blob.Options{}), Node: "n2", Leases: other, TempDir: t.TempDir()}
	h2, _, err := n2files.Open(ctx, "users/2")
	if err != nil {
		t.Fatal(err)
	}
	mine, _, err := e.Files.Open(ctx, "users/2")
	if err != nil {
		t.Fatal(err)
	}
	if err := mine.Claim(ctx); !errors.Is(err, shardfile.ErrOwnedElsewhere) {
		t.Fatalf("claim of a shard owned by a node with a valid lease: %v", err)
	}
	// Once that node has released its lease, the shard is up for taking.
	if err := other.Release(ctx, l2); err != nil {
		t.Fatal(err)
	}
	if err := mine.Claim(ctx); err != nil {
		t.Fatal(err)
	}
	if mine.Manifest.Owner != "n1" || h2.Manifest.Owner != "n2" {
		t.Fatalf("owners: %+v", mine.Manifest)
	}
}

// Creators on two nodes racing to create one shard end with exactly one winner:
// everybody agrees on the winner's generation, and only the winner created it.
func TestCreatorsRacing(t *testing.T) {
	t.Parallel()
	e, ctx := harness.New(t), context.Background()
	other := coord.NewRegistry(e.Blob, "n2", coord.RegistryOptions{})
	l2, err := other.Acquire(ctx, coord.NodeInfo{Name: "n2"})
	if err != nil {
		t.Fatal(err)
	}
	n2 := &shardfile.Files{Blob: blob.NewStore(e.Blob, "n2", l2.Epoch, blob.Options{}), Node: "n2", Leases: other, TempDir: t.TempDir()}

	for round := range 20 {
		shard := fmt.Sprintf("users/race%d", round)
		type result struct {
			h       *shardfile.Handle
			created bool
			err     error
		}
		var wg errgroup.Group
		results := make([]result, 6)
		for i := range results {
			wg.Go(func() error {
				f := e.Files
				if i%2 == 1 {
					f = n2
				}
				h, created, err := f.Create(ctx, shard)
				results[i] = result{h, created, err}
				return nil
			})
		}
		wg.Wait()
		winners := 0
		var gen string
		for _, res := range results {
			if res.err != nil {
				t.Fatal(res.err)
			}
			if res.created {
				winners++
			}
			if gen == "" {
				gen = res.h.Manifest.Generation
			}
			if res.h.Manifest.Generation != gen {
				t.Fatalf("round %d: generations %s and %s", round, gen, res.h.Manifest.Generation)
			}
		}
		if winners != 1 {
			t.Fatalf("round %d: %d winners, generation %s", round, winners, gen)
		}
		m, _, err := e.Files.GetManifest(ctx, shard)
		if err != nil || m.Generation != gen {
			t.Fatalf("manifest %+v: %v", m, err)
		}
	}
}

// Deleting a shard deletes its manifest by conditional delete and its
// generation's files. A delete with a stale version is a conflict that changes
// nothing, and deleting a shard with no manifest is done.
func TestDeleteShard(t *testing.T) {
	t.Parallel()
	e, ctx := harness.New(t), context.Background()
	h, _, err := e.Files.Open(ctx, "users/e1")
	if err != nil {
		t.Fatal(err)
	}
	key := shardfile.FileKey("users/e1", h.Manifest.Generation, shardfile.File{Range: shardfile.Range{Min: 1, Max: 1}, Epoch: 1})
	if _, err := e.Files.Blob.Create(ctx, key, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	other, _, err := e.Files.Open(ctx, "users/e1")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Claim(ctx); err != nil { // same epoch: no change
		t.Fatal(err)
	}
	stale := *other
	stale.ETag = "stale"
	if err := stale.DeleteShard(ctx); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("delete with a stale version: %v", err)
	}
	if _, _, err := e.Files.GetManifest(ctx, "users/e1"); err != nil {
		t.Fatalf("a lost delete removed the manifest: %v", err)
	}
	current, err := e.Files.Attach(ctx, "users/e1")
	if err != nil {
		t.Fatal(err)
	}
	if err := current.DeleteShard(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Files.GetManifest(ctx, "users/e1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	if _, err := e.Files.Blob.Head(ctx, key); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the LTX file is left: %v", err)
	}
	if _, err := e.Files.Attach(ctx, "users/never"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a shard with no manifest: %v", err)
	}
}
