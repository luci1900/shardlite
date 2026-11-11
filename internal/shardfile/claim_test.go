package shardfile_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// node is another node on the harness's store: its registry, lease and files.
type node struct {
	reg   *lease.Registry
	lease lease.Record
	files *shardfile.Files
}

func otherNode(t *testing.T, e *harness.Env, name cluster.Node, o lease.RegistryOptions) *node {
	t.Helper()
	reg := lease.NewRegistry(e.Blob, name, o)
	l, err := reg.Acquire(context.Background(), lease.NodeInfo{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	return &node{reg, l, &shardfile.Files{Blob: blob.NewStore(e.Blob, name, l.Epoch, blob.Options{}), Node: name, Leases: reg, Temp: tempfile.NewDir(t.TempDir())}}
}

func (n *node) create(t *testing.T, shard string) {
	t.Helper()
	if _, created, err := n.files.Open(context.Background(), shardName(shard)); err != nil || !created {
		t.Fatalf("create %s: %v", shard, err)
	}
}

// claim tries to claim shard on e's node.
func claim(t *testing.T, e *harness.Env, shard string) error {
	t.Helper()
	h, err := e.Files.Attach(context.Background(), shardName(shard))
	if err != nil {
		t.Fatal(err)
	}
	return h.Claim(context.Background())
}

// A shard another node owns is claimed only once nothing of it can be left in
// that node's journal, and no timestamp decides it.
func TestClaimRule(t *testing.T) {
	t.Parallel()
	e, ctx := harness.New(t), context.Background()

	// A live owner, and one whose lease only looks expired (renewed an hour ago
	// by its clock): both are still the owner.
	live := otherNode(t, e, "live", lease.RegistryOptions{})
	live.create(t, "users/live")
	stale := otherNode(t, e, "stale", lease.RegistryOptions{Now: func() time.Time { return time.Now().Add(-time.Hour) }})
	stale.create(t, "users/stale")
	for _, shard := range []string{"users/live", "users/stale"} {
		err := claim(t, e, shard)
		oe, ok := errors.AsType[*shardfile.OwnedElsewhereError](err)
		if !ok || !errors.Is(err, shardfile.ErrOwnedElsewhere) || string(oe.Owner) != shard[len("users/"):] {
			t.Fatalf("claim of %s: %v, want OwnedElsewhereError naming its owner", shard, err)
		}
	}

	// A dead owner (another node marked its lease): its shards wait for the
	// adoption, unless the adopter has already cleared their journal.
	dead := otherNode(t, e, "dead", lease.RegistryOptions{})
	dead.create(t, "users/named")
	dead.create(t, "users/tail")
	adopter := otherNode(t, e, "adopter", lease.RegistryOptions{})
	l, err := adopter.reg.Get(ctx, "dead")
	if err != nil {
		t.Fatal(err)
	}
	marked, err := adopter.reg.Adopt(ctx, *l, adopter.lease.Incarnation())
	if err != nil {
		t.Fatal(err)
	}
	for _, shard := range []string{"users/named", "users/tail"} {
		if err := claim(t, e, shard); !errors.Is(err, shardfile.ErrOwnerNotAdopted) {
			t.Fatalf("claim of %s before the adoption: %v, want ErrOwnerNotAdopted", shard, err)
		}
	}
	// The adopter's delta for users/tail clears its journal.
	ms := shardfile.NewManifestStore(adopter.files.Blob)
	m, v, err := ms.Get(ctx, shardName("users/tail"))
	if err != nil {
		t.Fatal(err)
	}
	m.Journal = ""
	if _, err := ms.Put(ctx, shardName("users/tail"), m, &v); err != nil {
		t.Fatal(err)
	}
	if err := claim(t, e, "users/tail"); err != nil {
		t.Fatalf("claim once its journal is cleared: %v", err)
	}
	if err := claim(t, e, "users/named"); !errors.Is(err, shardfile.ErrOwnerNotAdopted) {
		t.Fatalf("claim of a shard that names the journal before the adoption finished: %v", err)
	}
	if _, err := adopter.reg.FinishAdoption(ctx, marked, marked.Epoch, 1); err != nil {
		t.Fatal(err)
	}
	if err := claim(t, e, "users/named"); err != nil {
		t.Fatalf("claim once the adoption finished: %v", err)
	}

	// An owner that restarted is dead by its newer epoch, and its shards wait
	// for its own adoption of its journal.
	restarted := otherNode(t, e, "restarted", lease.RegistryOptions{})
	restarted.create(t, "users/restarted")
	l2, err := restarted.reg.Acquire(ctx, lease.NodeInfo{Name: "restarted"})
	if err != nil {
		t.Fatal(err)
	}
	if err := claim(t, e, "users/restarted"); !errors.Is(err, shardfile.ErrOwnerNotAdopted) {
		t.Fatalf("claim during the owner's own adoption: %v", err)
	}
	if _, err := restarted.reg.FinishAdoption(ctx, l2, l2.Epoch-1, 1); err != nil {
		t.Fatal(err)
	}
	if err := claim(t, e, "users/restarted"); err != nil {
		t.Fatalf("claim after the owner's own adoption: %v", err)
	}
}
