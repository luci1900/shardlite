package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// rawManifest reads a manifest object as it is in blob storage.
func (r *mrig) rawManifest(target string) (shardfile.Manifest, error) {
	r.t.Helper()
	rc, _, err := r.n.Store.Get(context.Background(), shardfile.ManifestKey(target))
	if err != nil {
		return shardfile.Manifest{}, err
	}
	defer rc.Close()
	var m shardfile.Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		r.t.Fatal(err)
	}
	return m, nil
}

// A shard that is only read leaves no manifest and no local files, and the next
// request creates it again.
func TestReadOnlyShardLeavesNothing(t *testing.T) {
	r := newMRig(t)
	ref := r.acquire("users/e1")
	first, _ := r.manifest("users/e1")
	path := ref.Path()
	r.app.Must(t, "openro", "a", ref.Path())
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("users/e1"))
	if _, err := r.rawManifest("users/e1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the file is still there: %v", err)
	}
	if rows, _ := r.m.idx.all(); len(rows) != 0 {
		t.Fatalf("index rows left: %+v", rows)
	}
	creates := r.m.Stats.Creates.Load()
	ref = r.acquire("users/e1")
	defer ref.Release()
	m, err := r.manifest("users/e1")
	if err != nil || m.Generation == first.Generation || r.m.Stats.Creates.Load() != creates+1 {
		t.Fatalf("manifest %+v (%v): not created again", m, err)
	}
}

// Deleting a shard leaves no manifest and none of its LTX files, and the next
// request gets an empty shard with a new generation.
func TestDeleteLeavesNothing(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("slow/e4")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	old, _ := r.manifest("slow/e4")
	r.app.Must(t, "close", "a")
	ref.Release()
	if err := r.m.Delete(ctx, "slow/e4"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.rawManifest("slow/e4"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	for info := range r.n.Store.List(ctx, "shards/slow/e4/") {
		t.Fatalf("an LTX file is left: %v", info.Key)
	}
	ref = r.acquire("slow/e4")
	defer ref.Release()
	m, err := r.manifest("slow/e4")
	if err != nil || m.Generation == old.Generation {
		t.Fatalf("manifest %+v (%v), want a new generation (old %s)", m, err, old.Generation)
	}
	if dump := r.liveDump(ref.Path()); strings.Contains(dump, "kv: ") {
		t.Fatalf("the new shard has the old shard's data:\n%s", dump)
	}
	r.write(ref, "b")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(r.dumpBlob("slow/e4"), "kv: "); got != 2 { // (write inserts two rows)
		t.Fatalf("%d rows after one write", got)
	}
}

// Deleting a shard that doesn't exist is done and leaves nothing.
func TestDeleteOfAShardWithNoManifest(t *testing.T) {
	r := newMRig(t)
	if err := r.m.Delete(context.Background(), "slow/nothing"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.rawManifest("slow/nothing"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a manifest was created: %v", err)
	}
}

// Another node creates the shard between this node finding no manifest and
// creating it: one wins, the other claims the winner's shard.
func TestCreateRace(t *testing.T) {
	r := newMRig(t)
	raced := false
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && key == "manifests/users/e5" && !after && !raced {
			raced = true
			body, _ := json.Marshal(shardfile.Manifest{Generation: "other", Files: []string{}})
			_, err := r.local.Create(ctx, key, bytes.NewReader(body), blob.WriteID{Node: "n2", Epoch: 1, N: 1})
			return err
		}
		return nil
	})
	creates := r.m.Stats.Creates.Load()
	ref := r.acquire("users/e5")
	defer ref.Release()
	if !raced || !strings.Contains(ref.Path(), ".other.db") {
		t.Fatalf("raced %v, path %s", raced, ref.Path())
	}
	if m, err := r.manifest("users/e5"); err != nil || m.Owner != "n1" || m.Generation != "other" {
		t.Fatalf("manifest %+v: %v", m, err)
	}
	if r.m.Stats.Creates.Load() != creates {
		t.Fatal("counted as created")
	}
}

// A delete that loses a race with a write by the owner (its ETag is stale)
// fails with a conflict and leaves the live manifest, and its files.
func TestDeleteRacingWithAWrite(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("users/e6")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "deactivation", func() bool { return r.cached("users/e6") })
	live, _ := r.manifest("users/e6")
	raced := false
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpDelete && key == "manifests/users/e6" && !after && !raced {
			raced = true // the delete is about to remove the manifest: the owner's own write gets in first
			cur, err := r.local.Head(ctx, key)
			if err != nil {
				return err
			}
			next := *live
			next.NodeEpoch++
			body, _ := json.Marshal(next)
			_, err = r.local.Update(ctx, key, bytes.NewReader(body), cur.ETag, blob.WriteID{Node: "n1", Epoch: 9, N: 1})
			return err
		}
		return nil
	})
	err := r.n.Files.DeleteShard(ctx, "users/e6")
	if !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("delete with a stale ETag: %v, want a conflict", err)
	}
	raw, rerr := r.rawManifest("users/e6")
	if rerr != nil || raw.NodeEpoch != live.NodeEpoch+1 || raw.Generation != live.Generation || len(raw.Files) == 0 {
		t.Fatalf("the live manifest is gone: %+v %v", raw, rerr)
	}
	n := 0
	for range r.n.Store.List(ctx, "shards/users/e6/") {
		n++
	}
	if n == 0 {
		t.Fatal("the shard's LTX files were deleted by a delete that lost")
	}
}

// A never-written shard whose manifest someone else changed meanwhile (the
// conditional delete loses) is kept, with its manifest.
func TestNeverWrittenShardIsKeptWhenItsManifestChanged(t *testing.T) {
	r := newMRig(t)
	ref := r.acquire("users/e7")
	r.app.Must(t, "openro", "a", ref.Path())
	r.app.Must(t, "close", "a")
	var changed atomic.Bool
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpDelete && key == "manifests/users/e7" && !after && changed.CompareAndSwap(false, true) {
			cur, err := r.local.Head(ctx, key)
			if err != nil {
				return err
			}
			body, _ := json.Marshal(shardfile.Manifest{Generation: "theirs", Owner: "n1", NodeEpoch: 9, Files: []string{}})
			_, err = r.local.Update(ctx, key, bytes.NewReader(body), cur.ETag, blob.WriteID{Node: "n2", Epoch: 1, N: 1})
			return err
		}
		return nil
	})
	ref.Release()
	eventually(t, "the delete to lose", changed.Load)
	time.Sleep(100 * time.Millisecond)
	if !r.m.IsActive("users/e7") {
		t.Fatal("the shard was deactivated although its manifest could not be deleted")
	}
	if m, err := r.rawManifest("users/e7"); err != nil || m.Generation != "theirs" {
		t.Fatalf("manifest %+v (%v), want the other writer's kept", m, err)
	}
}

// ---- Ephemeral kinds

func ephemeralFiles(t *testing.T, r *mrig) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(filepath.Join(r.data, ephemeralDir), func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			out = append(out, p)
		}
		return nil
	})
	return out
}

// Writes to an ephemeral shard never reach blob storage, and its responses aren't held.
func TestEphemeralShardShipsNothing(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("lobby/l1")
	if m, err := r.manifest("lobby/l1"); err != nil || !m.Ephemeral || m.Journal != "" || len(m.Files) != 0 || m.Owner != "n1" {
		t.Fatalf("manifest %+v (%v), want an ephemeral one with no journal", m, err)
	}
	if !strings.Contains(ref.Path(), string(filepath.Separator)+ephemeralDir+string(filepath.Separator)) {
		t.Fatalf("path %s is not under %s", ref.Path(), ephemeralDir)
	}
	if ref.Sync() {
		t.Fatal("an ephemeral kind has sync on")
	}
	r.write(ref, "a")
	segments := r.calls("create", "journal")
	flushes := r.m.Flusher().Completed()
	done := make(chan error, 1)
	go func() { done <- r.m.Gate(ctx, ref, true) }() // even with Shardlite-Sync: on
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an ephemeral shard's output was held")
	}
	if r.m.Flusher().Completed() != flushes {
		t.Fatal("the gate waited for a flush cycle")
	}
	if err := r.m.BarrierFor(ctx, "lobby/l1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if r.calls("create", "journal") != segments || r.calls("create", "shards") != 0 || r.calls("update", "manifests") != 0 || r.calls("update", "shards") != 0 {
		t.Fatalf("an ephemeral shard wrote to blob storage: %v", r.counts)
	}
	if r.m.n.Info().Shards != 0 {
		t.Fatal("an ephemeral shard is tracked in the lease")
	}
	r.app.Must(t, "close", "a")
	ref.Release()
}

// Deactivation deletes the file and the manifest; the next activation starts empty.
func TestEphemeralShardLeavesNothingAfterDeactivation(t *testing.T) {
	r := newMRig(t)
	ref := r.acquire("lobby/l2")
	r.write(ref, "a")
	path := ref.Path()
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("lobby/l2"))
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the file is still there: %v", err)
	}
	if _, err := r.rawManifest("lobby/l2"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	if rows, _ := r.m.idx.all(); len(rows) != 0 {
		t.Fatalf("index rows left: %+v", rows)
	}
	ref = r.acquire("lobby/l2")
	defer ref.Release()
	if dump := r.liveDump(ref.Path()); strings.Contains(dump, "kv: ") {
		t.Fatalf("the shard isn't empty:\n%s", dump)
	}
}

// A crash and restart give an empty shard (the manifest stays, owned by the old
// epoch, and is claimed), and data/ephemeral is emptied.
func TestEphemeralShardIsEmptyAfterACrash(t *testing.T) {
	r := newMRig(t)
	ref := r.acquire("lobby/l3")
	first, _ := r.manifest("lobby/l3")
	r.write(ref, "a")
	if _, err := os.Stat(ref.Path()); err != nil {
		t.Fatal(err)
	}
	r.crash()
	r.n.Close(context.Background(), false)
	r.app.Must(t, "close", "a")

	r.startNode()
	r.startManager(nil)
	if files := ephemeralFiles(t, r); len(files) != 0 {
		t.Fatalf("data/ephemeral still has %v", files)
	}
	ref = r.acquire("lobby/l3")
	defer ref.Release()
	m, err := r.manifest("lobby/l3")
	if err != nil || m.Generation != first.Generation || m.NodeEpoch == first.NodeEpoch || !m.Ephemeral {
		t.Fatalf("manifest %+v (%v), want the old one claimed in the new epoch", m, err)
	}
	if dump := r.liveDump(ref.Path()); strings.Contains(dump, "kv: ") {
		t.Fatalf("the shard isn't empty:\n%s", dump)
	}
}

// A durable shard with data whose kind became ephemeral is refused and logged. An
// ephemeral shard whose kind became durable becomes durable, and its later writes
// are shipped.
func TestKindChangedBetweenEphemeralAndDurable(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	reload := func(kinds string) {
		t.Helper()
		if err := os.WriteFile(r.kindsPath, []byte(kinds), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := r.kinds.Reload(); err != nil {
			t.Fatal(err)
		}
	}
	const flipped = `{"kinds": {
		"users":  {"public": true, "sync": "off", "idle_timeout": "300ms"},
		"wallet": {"public": true, "ephemeral": true, "idle_timeout": "300ms"},
		"slow":   {"public": true, "sync": "off", "idle_timeout": "1h"},
		"audit":  {},
		"lobby":  {"public": true, "idle_timeout": "300ms"}
	}}`

	ref := r.acquire("wallet/k1")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "deactivation", func() bool { return r.cached("wallet/k1") })
	ref = r.acquire("lobby/k2")
	r.app.Must(t, "openro", "a", ref.Path())
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("lobby/k2"))

	reload(flipped)
	if _, err := r.m.Acquire(ctx, "wallet/k1"); !errors.Is(err, ErrKindChanged) {
		t.Fatalf("a shard with data whose kind became ephemeral: %v, want ErrKindChanged", err)
	}
	if m, err := r.manifest("wallet/k1"); err != nil || len(m.Files) == 0 || m.Ephemeral {
		t.Fatalf("the refused shard's manifest changed: %+v %v", m, err)
	}

	ref = r.acquire("lobby/k2")
	defer ref.Release()
	m, err := r.manifest("lobby/k2")
	if err != nil || m.Ephemeral || m.Journal != "n1" {
		t.Fatalf("manifest %+v (%v), want a durable one", m, err)
	}
	r.write(ref, "b")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(r.dumpBlob("lobby/k2"), "kv: "); got != 2 {
		t.Fatalf("%d rows shipped after one write", got)
	}
}

// Deleting an ephemeral shard through the control API deletes its file and manifest.
func TestDeleteOfAnEphemeralShard(t *testing.T) {
	r := newMRig(t)
	ref := r.acquire("lobby/l4")
	r.write(ref, "a")
	path := ref.Path()
	r.app.Must(t, "close", "a")
	ref.Release()
	if err := r.m.Delete(context.Background(), "lobby/l4"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the file is still there: %v", err)
	}
	if _, err := r.rawManifest("lobby/l4"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	if r.m.IsActive("lobby/l4") {
		t.Fatal("still active")
	}
}
