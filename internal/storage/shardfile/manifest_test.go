package shardfile_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/sync/errgroup"

	shardpkg "github.com/luci1900/shardlite/internal/model/shard"
	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/storage/lease"
	"github.com/luci1900/shardlite/internal/storage/shardfile"
	"github.com/luci1900/shardlite/internal/tempfile"
	"github.com/luci1900/shardlite/internal/test/harness"
)

func TestManifestStoreCompareAndSwap(t *testing.T) {
	t.Parallel()
	e, ctx := harness.New(t), context.Background()
	ms := shardfile.NewManifestStore(e.Store)
	m := &shardfile.Manifest{Generation: "g", Owner: "n1", NodeEpoch: 1, Files: []shardfile.File{}, Journal: "n1"}

	v1, err := ms.Put(ctx, shardName("users/1"), m, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Put(ctx, shardName("users/1"), m, nil); !errors.Is(err, blob.ErrExists) {
		t.Fatalf("second create: %v", err)
	}
	got, v, err := ms.Get(ctx, shardName("users/1"))
	if err != nil || v != v1 || got.Owner != "n1" || got.NodeEpoch != 1 {
		t.Fatalf("get: %+v %v %v", got, v, err)
	}
	m2 := *got
	m2.NodeEpoch = 2
	v2, err := ms.Put(ctx, shardName("users/1"), &m2, &v1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Put(ctx, shardName("users/1"), &m2, &v1); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("update from an old version: %v", err)
	}
	if err := ms.Delete(ctx, shardName("users/1"), v1); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("delete of an old version: %v", err)
	}
	if err := ms.Delete(ctx, shardName("users/1"), v2); err != nil {
		t.Fatal(err)
	}
	// A conditional delete removes the object.
	if _, _, err := ms.Get(ctx, shardName("users/1")); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	if err := ms.Delete(ctx, shardName("users/1"), v2); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("delete of a manifest already deleted: %v", err)
	}
}

func TestClaim(t *testing.T) {
	t.Parallel()
	e, ctx := harness.New(t), context.Background()
	h, created, err := e.Files.Open(ctx, shardName("users/1"))
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
	other := lease.NewRegistry(e.Blob, "n2", lease.RegistryOptions{})
	l2, err := other.Acquire(ctx, lease.NodeInfo{Name: "n2"})
	if err != nil {
		t.Fatal(err)
	}
	n2files := &shardfile.Files{Blob: blob.NewStore(e.Blob, "n2", l2.Epoch, blob.Options{}), Node: "n2", Leases: other, Temp: tempfile.NewDir(t.TempDir())}
	h2, _, err := n2files.Open(ctx, shardName("users/2"))
	if err != nil {
		t.Fatal(err)
	}
	mine, _, err := e.Files.Open(ctx, shardName("users/2"))
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
	other := lease.NewRegistry(e.Blob, "n2", lease.RegistryOptions{})
	l2, err := other.Acquire(ctx, lease.NodeInfo{Name: "n2"})
	if err != nil {
		t.Fatal(err)
	}
	n2 := &shardfile.Files{Blob: blob.NewStore(e.Blob, "n2", l2.Epoch, blob.Options{}), Node: "n2", Leases: other, Temp: tempfile.NewDir(t.TempDir())}

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
				h, created, err := f.Create(ctx, shardName(shard))
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
				gen = string(res.h.Manifest.Generation)
			}
			if string(res.h.Manifest.Generation) != gen {
				t.Fatalf("round %d: generations %s and %s", round, gen, res.h.Manifest.Generation)
			}
		}
		if winners != 1 {
			t.Fatalf("round %d: %d winners, generation %s", round, winners, gen)
		}
		m, _, err := e.Files.GetManifest(ctx, shardName(shard))
		if err != nil || string(m.Generation) != gen {
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
	h, _, err := e.Files.Open(ctx, shardName("users/e1"))
	if err != nil {
		t.Fatal(err)
	}
	key := shardfile.FileKey(shardName("users/e1"), h.Manifest.Generation, shardfile.File{Range: shardfile.Range{Min: 1, Max: 1}, By: lease.Incarnation{Node: "n1", Epoch: 1}})
	if _, err := e.Files.Blob.Create(ctx, key, strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	other, _, err := e.Files.Open(ctx, shardName("users/e1"))
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
	if _, _, err := e.Files.GetManifest(ctx, shardName("users/e1")); err != nil {
		t.Fatalf("a lost delete removed the manifest: %v", err)
	}
	current, err := e.Files.Attach(ctx, shardName("users/e1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := current.DeleteShard(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Files.GetManifest(ctx, shardName("users/e1")); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	if _, err := e.Files.Blob.Head(ctx, key); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the LTX file is left: %v", err)
	}
	if _, err := e.Files.Attach(ctx, shardName("users/never")); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a shard with no manifest: %v", err)
	}
}

// A manifest whose file list entry doesn't parse is corrupt, so reading it
// fails: no caller sees it as a list that ends at position 0.
func TestManifestBadFileEntry(t *testing.T) {
	t.Parallel()
	e, ctx := harness.New(t), context.Background()
	name := shardName("users/bad")
	body := `{"generation":"g","owner":"n1","node_epoch":1,"files":["nonsense"],"journal":"n1"}`
	if _, err := e.Store.Create(ctx, shardfile.ManifestKey(name), strings.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Files.GetManifest(ctx, name); err == nil || !strings.Contains(err.Error(), `bad file name "nonsense"`) {
		t.Fatalf("read of a manifest with a bad file entry: %v", err)
	}
}

// A shard's files are keyed lane first, so each lane lists on its own.
func TestFileKeysPutTheLaneFirst(t *testing.T) {
	t.Parallel()
	name := shardName("users/1")
	f := shardfile.File{Range: shardfile.Range{Min: 1, Max: 5}, By: lease.Incarnation{Node: "n1", Epoch: 2}}
	lane := name.Lane().String()
	if got, want := shardfile.FileKey(name, "g1", f), "data/shards/"+lane+"/users/1/g1/"+f.String()+".ltx"; got != want {
		t.Fatalf("file key %q, want %q", got, want)
	}
	if got, want := shardfile.ShardPrefix(name), "data/shards/"+lane+"/users/1/"; got != want {
		t.Fatalf("shard prefix %q, want %q", got, want)
	}
	if got := shardfile.LanePrefix(name.Lane()); !strings.HasPrefix(shardfile.FileKey(name, "g1", f), got) || got != "data/shards/"+lane+"/" {
		t.Fatalf("lane prefix %q", got)
	}
}

func TestManifestKeyPutsTheLaneFirst(t *testing.T) {
	t.Parallel()
	name := shardName("users/42")
	key := shardfile.ManifestKey(name)
	if want := "state/shards/6/users/42"; key != want {
		t.Fatalf("manifest key %q, want %q", key, want)
	}
	if want := shardfile.ManifestPrefix + name.Lane().String() + "/"; !strings.HasPrefix(key, want) {
		t.Fatalf("manifest key %q doesn't start with the shard's lane %q", key, want)
	}
	// Nothing parses a manifest key, so the key never carrying another lane is all there is to check.
	for l := shardpkg.Lane(0); l < shardpkg.Lanes; l++ {
		if l != name.Lane() && strings.HasPrefix(key, shardfile.ManifestPrefix+l.String()+"/") {
			t.Fatalf("manifest key %q starts with lane %s, not the shard's", key, l)
		}
	}
}

// The write ids of a journal store name the node that writes, not the node whose
// journal it is. An adopter's fence in a dead node's journal must never read
// back as that node's own segment, which a zombie of it at the same epoch could
// have written under the same number with the same counter.
func TestJournalWriteIDsNameTheWriter(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	adopter := &shardfile.Files{Blob: blob.NewStore(e.Blob, "n2", e.Store.Epoch(), blob.Options{}), Node: "n2", Temp: e.Files.Temp}
	zombie := e.Files.Journal(e.Node)
	theirs, ours := zombie.NewWriteID(), adopter.Journal(e.Node).NewWriteID()
	if ours.Node != "n2" || ours.Epoch != e.Store.Epoch() {
		t.Fatalf("the adopter writes into %s's journal with write id %+v, want one naming n2 at epoch %d", e.Node, ours, e.Store.Epoch())
	}
	if ours == theirs {
		t.Fatalf("the adopter's first write id %+v is the zombie's", ours)
	}
}
