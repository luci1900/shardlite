package node

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/storage/shardfile"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

// releasedFrom checks that target was let go of by this node: its manifest names no
// owner and no journal, and nothing of it is left here.
func (r *mrig) releasedFrom(target, path string) {
	r.t.Helper()
	man, err := r.manifest(target)
	if err != nil {
		r.t.Fatal(err)
	}
	if man.Owner != "" || man.Journal != "" {
		r.t.Fatalf("the manifest after a release: owner %q, journal %q", man.Owner, man.Journal)
	}
	if r.m.IsActive(shardName(target)) {
		r.t.Fatal("the shard is still on the node")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		r.t.Fatalf("its file is still there: %v", err)
	}
}

// manifestWrites is how many manifest updates the node has made for the shard.
func (r *mrig) manifestWrites(target string) int {
	r.cmu.Lock()
	defer r.cmu.Unlock()
	return r.counts[string(blobtest.OpUpdate)+" "+shardfile.ManifestKey(shardName(target))]
}

// A deactivation is one manifest write: the update that records the shard's last delta also releases it, and a shard with nothing new to record is released by an update of its own. Neither leaves a second write behind.
func TestDeactivationIsOneManifestWrite(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	const target = "slow/g1"
	ref := r.acquire(target)
	r.write(ref, "a")
	r.app.Must(t, "close", "a")
	path := ref.Path()
	ref.Release()
	before := r.manifestWrites(target)
	r.release(target)
	if got := r.manifestWrites(target) - before; got != 1 {
		t.Fatalf("deactivating a shard with a tail wrote the manifest %d times, want 1", got)
	}
	r.releasedFrom(target, path)
	if man, err := r.manifest(target); err != nil || len(man.Files) != 1 {
		t.Fatalf("the manifest after the release: %+v %v, want the one delta", man, err)
	}

	// Activated again and left alone: nothing to record, so the release is the write.
	ref = r.acquire(target)
	path = ref.Path()
	ref.Release()
	before = r.manifestWrites(target)
	r.release(target)
	if got := r.manifestWrites(target) - before; got != 1 {
		t.Fatalf("deactivating a shard with no tail wrote the manifest %d times, want 1", got)
	}
	r.releasedFrom(target, path)
}

// A deactivation's one update records the shard's next reminder with its entry in the reminder index created first, so the dispatcher wakes the released shard.
func TestDeactivationRecordsTheReminderOfAReleasedShard(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	const target = "users/ev1"
	due := time.Now().Add(time.Hour)
	r.remind(target, "k", due.Unix())
	r.closeInApp(target)
	apptest.Eventually(t, apptest.Timeout, "the idle deactivation", func() bool { return r.released(target) })
	man, err := r.manifest(target)
	if err != nil || man.Owner != "" || man.Journal != "" || man.NextReminder != shardfile.MinuteAt(due) {
		t.Fatalf("the manifest after the release: %+v %v, want it released with the reminder's minute %d", man, err, shardfile.MinuteAt(due))
	}
	if n := r.entryCount(target); n != 1 {
		t.Fatalf("%d reminder index entries for the released shard, want the one that backs its manifest", n)
	}
}

// A deactivation whose update has an unknown outcome is asked again until the outcome is learned: the update may have released the shard, which must then not be served, or not, in which case its tail must not be dropped. Here it lands and nothing can be read for a while, then the store answers again, and the shard has been released by that one update.
func TestDeactivationWithAnUnknownOutcomeLearnsItBeforeDeciding(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	const target = "slow/g1"
	ref := r.acquire(target)
	r.write(ref, "a")
	r.app.Must(t, "close", "a")
	path := ref.Path()
	ref.Release()

	key := shardfile.ManifestKey(shardName(target))
	var landed atomic.Bool
	var unreachable, updates atomic.Int64
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, k string, after bool) error {
		if k != key {
			return nil
		}
		if op == blobtest.OpUpdate && after {
			updates.Add(1) // the backend applied it
		}
		if op == blobtest.OpUpdate && after && landed.CompareAndSwap(false, true) {
			return errors.New("response lost")
		}
		if landed.Load() && !after && op != blobtest.OpUpdate && unreachable.Add(1) <= 12 {
			return errors.New("unreachable")
		}
		return nil
	})
	r.release(target)
	if unreachable.Load() <= 12 {
		t.Fatal("the store was never unreachable, so the deactivation did not wait for the outcome")
	}
	r.releasedFrom(target, path)
	if man, err := r.manifest(target); err != nil || len(man.Files) != 1 {
		t.Fatalf("the manifest after the release: %+v %v, want the one delta", man, err)
	}
	if n := updates.Load(); n != 1 {
		t.Fatalf("the backend applied %d manifest updates, want the one that landed", n)
	}
}
