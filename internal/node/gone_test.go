package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
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
	t.Parallel()
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
	if rows, _ := r.m.cache.idx.all(); len(rows) != 0 {
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
	t.Parallel()
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
	for info := range r.n.Store.List(ctx, shardfile.ShardPrefix("slow/e4")) {
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
	t.Parallel()
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
	t.Parallel()
	r := newMRig(t)
	raced := false
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && key == shardfile.ManifestKey("users/e5") && !after && !raced {
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
	t.Parallel()
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
		if op == blob.OpDelete && key == shardfile.ManifestKey("users/e6") && !after && !raced {
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
	for range r.n.Store.List(ctx, shardfile.ShardPrefix("users/e6")) {
		n++
	}
	if n == 0 {
		t.Fatal("the shard's LTX files were deleted by a delete that lost")
	}
}

// A never-written shard whose manifest someone else changed meanwhile (the
// conditional delete loses) is kept, with its manifest.
func TestNeverWrittenShardIsKeptWhenItsManifestChanged(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("users/e7")
	r.app.Must(t, "openro", "a", ref.Path())
	r.app.Must(t, "close", "a")
	var changed atomic.Bool
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpDelete && key == shardfile.ManifestKey("users/e7") && !after && changed.CompareAndSwap(false, true) {
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
	r.afterLooks(1, "users/e7") // the idle timer has looked again and kept the shard
	if !r.m.IsActive("users/e7") {
		t.Fatal("the shard was deactivated although its manifest could not be deleted")
	}
	if m, err := r.rawManifest("users/e7"); err != nil || m.Generation != "theirs" {
		t.Fatalf("manifest %+v (%v), want the other writer's kept", m, err)
	}
}

// A shard the app wrote to and later emptied is deleted when it deactivates,
// with the LTX files of its earlier deltas.
func TestEmptiedShardLeavesNothing(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("users/e8")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("users/e8"))
	if len(r.leftInBlob("users/e8")) == 0 {
		t.Fatal("no delta at deactivation")
	}
	ref = r.acquire("users/e8")
	path := ref.Path()
	r.app.Must(t, "openro", "a", path)
	r.app.Must(t, "exec", "a", "DROP TABLE kv")
	r.app.Must(t, "exec", "a", "DROP TABLE acks")
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("users/e8"))
	r.mustBeGone("users/e8", path)
}

// ---- The delete marker

// leftInBlob lists the LTX files that blob storage holds for a shard.
func (r *mrig) leftInBlob(target string) []string {
	r.t.Helper()
	var out []string
	for info, err := range r.n.Store.List(context.Background(), shardfile.ShardPrefix(target)) {
		if err != nil {
			r.t.Fatal(err)
		}
		out = append(out, info.Key)
	}
	return out
}

// mustBeGone checks that a shard has no manifest, LTX files, local files or index row.
func (r *mrig) mustBeGone(target, path string) {
	r.t.Helper()
	if _, err := r.rawManifest(target); !errors.Is(err, blob.ErrNotFound) {
		r.t.Fatalf("manifest: %v, want none", err)
	}
	if left := r.leftInBlob(target); len(left) != 0 {
		r.t.Fatalf("LTX files left: %v", left)
	}
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			r.t.Fatalf("%s is still there: %v", p, err)
		}
	}
	if rows, _ := r.m.cache.idx.all(); len(rows) != 0 {
		r.t.Fatalf("index rows left: %+v", rows)
	}
}

// markForDeletion makes the app create the delete marker in the shard it has open
// as "a" (see r.write).
func (r *mrig) markForDeletion() {
	r.t.Helper()
	r.app.Must(r.t, "exec", "a", "CREATE TABLE _shardlite_delete (x)")
}

// A marked shard is captured and shipped like any other: it has a journal and its
// output is gated.
func TestMarkedShardIsCapturedAndGated(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("lobby/l1")
	defer ref.Release()
	if m, err := r.manifest("lobby/l1"); err != nil || m.Journal != "n1" || m.Owner != "n1" {
		t.Fatalf("manifest %+v (%v), want one with a journal", m, err)
	}
	if !ref.Sync() {
		t.Fatal("the kind's sync setting is ignored")
	}
	r.write(ref, "a")
	r.markForDeletion()
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(r.dumpBlob("lobby/l1"), "kv: "); got != 2 {
		t.Fatalf("%d rows shipped after one write", got)
	}
	if r.m.n.Info().Shards == 0 {
		t.Fatal("the shard is not tracked in the lease")
	}
	r.app.Must(t, "close", "a")
}

// Deactivation deletes the manifest and the local files of a marked shard; the next
// request starts empty with a new generation.
func TestMarkedShardLeavesNothingAfterDeactivation(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("lobby/l2")
	first, _ := r.manifest("lobby/l2")
	r.write(ref, "a")
	r.markForDeletion()
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	path := ref.Path()
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("lobby/l2"))
	r.mustBeGone("lobby/l2", path)
	if r.m.Stats.Deletes.Load() != 1 {
		t.Fatalf("deletes %d", r.m.Stats.Deletes.Load())
	}
	ref = r.acquire("lobby/l2")
	defer ref.Release()
	m, err := r.manifest("lobby/l2")
	if err != nil || m.Generation == first.Generation {
		t.Fatalf("manifest %+v (%v), want a new generation (old %s)", m, err, first.Generation)
	}
	if dump := r.liveDump(ref.Path()); strings.Contains(dump, "kv: ") {
		t.Fatalf("the shard isn't empty:\n%s", dump)
	}
}

// A shard that lived long enough for a background delta has the delta deleted too.
func TestMarkedShardDeletesItsBackgroundDelta(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, func(o *CatalogOptions) { o.Lifecycle.DeltaAge = 30 * time.Millisecond })
	ctx := context.Background()
	ref := r.acquire("lobby/l3")
	r.write(ref, "a")
	r.markForDeletion()
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a delta", func() bool {
		m, err := r.manifest("lobby/l3")
		return err == nil && len(m.Files) > 0
	})
	if len(r.leftInBlob("lobby/l3")) == 0 {
		t.Fatal("the manifest lists a file that blob storage doesn't have")
	}
	path := ref.Path()
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("lobby/l3"))
	r.mustBeGone("lobby/l3", path)
}

// A crash keeps the state of a marked shard: all of it with sync on, and what
// reached a flush with sync off. It is deleted at a later deactivation.
func TestMarkedShardSurvivesACrash(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ kind, name string }{{"lobby", "sync on"}, {"arena", "sync off"}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newMRig(t)
			ctx := context.Background()
			target := tc.kind + "/c1"
			ref := r.acquire(target)
			first, _ := r.manifest(target)
			r.write(ref, "a")
			r.markForDeletion()
			if tc.kind == "lobby" {
				if err := r.m.Gate(ctx, ref, true); err != nil {
					t.Fatal(err)
				}
			} else {
				r.afterCycle() // the write is in a flush cycle: the loss window is one flush interval
				r.afterCycle()
			}
			r.app.Must(t, "close", "a")
			r.crash()
			r.stopJournal()

			r.startNode()
			r.startShards(nil)
			ref = r.acquire(target)
			m, err := r.manifest(target)
			if err != nil || m.Generation != first.Generation {
				t.Fatalf("manifest %+v (%v), want the old shard", m, err)
			}
			dump := r.liveDump(ref.Path())
			if got := strings.Count(dump, "kv: "); got != 2 || !strings.Contains(dump, "_shardlite_delete") {
				t.Fatalf("%d rows after a crash, want the 2 of one write, and the marker:\n%s", got, dump)
			}
			path := ref.Path()
			ref.Release()
			eventually(t, "idle deactivation", r.inactive(target))
			r.mustBeGone(target, path)
		})
	}
}

// writeAndRelease writes to a shard, waits until the manifest lists a delta, and
// closes the app's file, leaving the shard idle.
func (r *mrig) writeAndRelease(target string) (path, generation string) {
	r.t.Helper()
	ref := r.acquire(target)
	r.write(ref, "a")
	r.markForDeletion()
	if err := r.m.Gate(context.Background(), ref, true); err != nil {
		r.t.Fatal(err)
	}
	eventually(r.t, "a delta", func() bool {
		m, err := r.manifest(target)
		return err == nil && len(m.Files) > 0
	})
	path = ref.Path()
	m, _ := r.manifest(target)
	r.app.Must(r.t, "close", "a")
	ref.Release()
	return path, m.Generation
}

func withDeltas(o *CatalogOptions) { o.Lifecycle.DeltaAge = 30 * time.Millisecond }

// The delete after deactivation goes manifest first. If the manifest can't be
// deleted nothing is deleted and the shard stays active, with its data; the next
// pass tries again.
func TestMarkedShardWithAFailingManifestDeleteIsKept(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, withDeltas)
	ref := r.acquire("lobby/f1")
	path := ref.Path()
	var fail atomic.Bool
	fail.Store(true)
	var tries atomic.Int64
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpDelete && key == shardfile.ManifestKey("lobby/f1") && !after && fail.Load() {
			tries.Add(1)
			return errors.New("injected")
		}
		return nil
	})
	r.write(ref, "a")
	r.markForDeletion()
	if err := r.m.Gate(context.Background(), ref, true); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a delta", func() bool {
		m, err := r.manifest("lobby/f1")
		return err == nil && len(m.Files) > 0
	})
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "a failed delete", func() bool { return tries.Load() >= 1 })
	r.afterLooks(1, "lobby/f1")
	if !r.m.IsActive("lobby/f1") {
		t.Fatal("the shard was deactivated although its manifest could not be deleted")
	}
	if m, err := r.rawManifest("lobby/f1"); err != nil || len(m.Files) == 0 || len(r.leftInBlob("lobby/f1")) == 0 {
		t.Fatalf("something was deleted: %+v %v", m, err)
	}
	ref = r.acquire("lobby/f1")
	if got := strings.Count(r.liveDump(ref.Path()), "kv: "); got != 2 {
		t.Fatalf("%d rows, want the shard as it was", got)
	}
	ref.Release()
	fail.Store(false)
	eventually(t, "the delete to go through", r.inactive("lobby/f1"))
	r.mustBeGone("lobby/f1", path)
}

// Once the manifest is deleted the shard is gone. LTX files that couldn't be
// deleted are left in blob storage, which nothing refers to, and a new shard of the
// same id has a new generation.
func TestMarkedShardWithAFailingFileDeleteLeavesOrphans(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, withDeltas)
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpDelete && strings.HasPrefix(key, shardfile.ShardPrefix("lobby/f2")) && !after {
			return errors.New("injected")
		}
		return nil
	})
	path, old := r.writeAndRelease("lobby/f2")
	eventually(t, "idle deactivation", r.inactive("lobby/f2"))
	if _, err := r.rawManifest("lobby/f2"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the local file is still there: %v", err)
	}
	if len(r.leftInBlob("lobby/f2")) == 0 {
		t.Fatal("the orphaned LTX files were deleted although deleting them was made to fail")
	}
	ref := r.acquire("lobby/f2")
	defer ref.Release()
	if m, err := r.manifest("lobby/f2"); err != nil || m.Generation == old {
		t.Fatalf("manifest %+v (%v), want a new generation", m, err)
	}
}

// A crash after the manifest is deleted and before the files are: the shard is gone
// for good, its LTX files are orphans, and its local files (still in the index as
// active) are discarded.
func TestMarkedShardCrashAfterTheManifestDelete(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, withDeltas)
	reached := make(chan struct{})
	var once sync.Once
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpDelete && strings.HasPrefix(key, shardfile.ShardPrefix("lobby/f3")) && !after {
			once.Do(func() { close(reached) })
			<-ctx.Done() // the node dies here
			return ctx.Err()
		}
		return nil
	})
	ref := r.acquire("lobby/f3")
	r.write(ref, "a")
	r.markForDeletion()
	if err := r.m.Gate(context.Background(), ref, true); err != nil {
		t.Fatal(err)
	}
	eventually(t, "a delta", func() bool {
		m, err := r.manifest("lobby/f3")
		return err == nil && len(m.Files) > 0
	})
	old, _ := r.manifest("lobby/f3")
	path := ref.Path()
	r.app.Must(t, "close", "a")
	ref.Release()
	<-reached
	if _, err := r.rawManifest("lobby/f3"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none by now", err)
	}
	r.crash()
	r.stopJournal()
	r.local.SetHook(nil)

	r.startNode()
	r.startShards(nil)
	ref = r.acquire("lobby/f3")
	defer ref.Release()
	if m, err := r.manifest("lobby/f3"); err != nil || m.Generation == old.Generation {
		t.Fatalf("manifest %+v (%v), want a new shard", m, err)
	}
	if dump := r.liveDump(ref.Path()); strings.Contains(dump, "kv: ") {
		t.Fatalf("the new shard has the old shard's data:\n%s", dump)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the old local file is still there: %v", err)
	}
	if len(r.leftInBlob("lobby/f3")) == 0 {
		t.Fatal("the orphaned LTX files are gone: the hook did not stop their deletion")
	}
}
