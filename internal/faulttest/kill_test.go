//go:build fault

package faulttest

import (
	"testing"

	"github.com/luci1900/shardlite/internal/kill"
)

// settle gives the shard a few durable commits and a delta, so a crash has history to lose.
func (r *rig) settle(deltas int) {
	r.t.Helper()
	for range 2 {
		if err := r.commit(true); err != nil {
			r.t.Fatal(err)
		}
	}
	for range deltas {
		r.node.Must(r.t, "delta", shardName)
		for range 2 {
			if err := r.commit(true); err != nil {
				r.t.Fatal(err)
			}
		}
	}
}

// after a crash and recovery the shard must keep working and keep its numbering.
func (r *rig) continueAfterRecovery(label string) {
	r.t.Helper()
	how := r.recoverAll()
	r.t.Logf("%s: recovered (%s)", label, how)
	for range 3 {
		if err := r.commit(true); err != nil {
			r.t.Fatalf("%s: commit after recovery: %v", label, err)
		}
	}
	r.node.Must(r.t, "delta", shardName)
	r.verify(label + " after recovery")
}

func TestKillAfterCommitBeforeSync(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.settle(1)
	if err := r.commit(false); err != nil { // committed by the app, never synced
		t.Fatal(err)
	}
	r.crash()
	r.verify("after crash")
	if r.hasCommit(r.n) {
		t.Fatal("an unsynced commit is in blob storage")
	}
	r.continueAfterRecovery("before sync")
}

func TestKillDuringSyncBeforeSegmentWrite(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.settle(1)
	r.node.Must(t, "arm", "segment-before")
	if err := r.commit(true); err == nil {
		t.Fatal("sync succeeded though the node should have died")
	}
	r.crash()
	r.verify("after crash")
	if r.hasCommit(r.n) {
		t.Fatal("a commit whose segment was never written is in blob storage")
	}
	r.continueAfterRecovery("before segment write")
}

func TestKillAfterSegmentWriteBeforeWaitersReleased(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.settle(1)
	r.node.Must(t, "arm", "segment-after")
	if err := r.commit(true); err == nil {
		t.Fatal("sync succeeded though the node should have died")
	}
	r.crash()
	r.verify("after crash")
	// Not acknowledged, but the segment was written, so the commit is durable all the same.
	if !r.hasCommit(r.n) {
		t.Fatal("a commit in a written segment is missing from the restore")
	}
	r.acked[r.n] = true
	r.continueAfterRecovery("after segment write")
}

func TestKillAfterDeltaFileBeforeManifest(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.settle(1)
	r.node.Must(t, "arm", "delta-before-manifest")
	if _, err := r.node.Do("delta", shardName); err == nil {
		t.Fatal("delta succeeded though the node should have died")
	}
	r.crash()
	r.verify("after crash")
	r.continueAfterRecovery("delta before manifest")
}

func TestKillRebase(t *testing.T) {
	t.Parallel()
	for _, point := range []kill.Point{kill.RebaseBeforeManifest, kill.RebaseAfterSwap} {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			r := newRig(t)
			r.settle(3)
			r.node.Must(t, "arm", string(point))
			if _, err := r.node.Do("rebase", shardName); err == nil {
				t.Fatal("rebase succeeded though the node should have died")
			}
			r.crash()
			r.verify("after crash")
			r.continueAfterRecovery(string(point))
		})
	}
}

func TestKillDuringRestoreBeforeRename(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.settle(2)
	r.crash()
	// The first restart dies just before the restored file is renamed into place.
	r.startNode()
	r.node.Must(t, "arm", "restore-before-rename")
	if _, err := r.node.Do("open", shardName, r.path); err == nil {
		t.Fatal("restore succeeded though the node should have died")
	}
	r.node.Kill()
	r.verify("after the interrupted restore")
	// The next restart finds the leftover and starts clean.
	r.continueAfterRecovery("restore before rename")
}

// activationPoints are where an activation of a restored shard can be killed
// between the restore and the first sync: after the restore, as the node folds
// a leftover WAL into the database file, once the node holds its read
// transaction over the database, and once the first sync is done.
var activationPoints = []kill.Point{kill.Restored, kill.Folded, kill.ReadHeld, kill.FirstSyncDone}

func TestKillBetweenRestoreAndFirstSync(t *testing.T) {
	t.Parallel()
	for _, point := range activationPoints {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			r := newRig(t)
			r.settle(2)
			r.crash()
			r.startNode()
			r.node.Must(t, "arm", string(point))
			if _, err := r.node.Do("open", shardName, r.path); err == nil {
				t.Fatal("open succeeded though the node should have died")
			}
			r.node.Kill()
			r.verify("after the interrupted activation")
			// The next restart finds whatever the killed one left and starts clean.
			r.continueAfterRecovery(string(point))
		})
	}
}

// A shard that is activated, written to and activated again keeps its history,
// with a kill at each of the points on the second activation. The first
// activation's WAL is still there when the second begins (as for a cached shard).
func TestKillReactivation(t *testing.T) {
	t.Parallel()
	for _, point := range activationPoints[1:] {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			r := newRig(t)
			r.settle(1)
			r.reactivate(point)
			r.verify("after the interrupted reactivation")
			r.continueAfterRecovery(string(point))
		})
	}
}

// The same from the files a release left, WAL included, which the node folds
// into the database file before it continues from them.
func TestKillCachedReactivation(t *testing.T) {
	t.Parallel()
	for _, point := range activationPoints[1:] {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			r := newRig(t)
			r.settle(1)
			r.reactivateCached(point)
			r.verify("after the interrupted reactivation")
			r.continueAfterRecovery(string(point))
		})
	}
}

// A cached reactivation that isn't killed carries on, and the shard's history
// restores to what the app has.
func TestCachedReactivationCarriesOn(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.settle(1)
	for range 3 {
		r.app.Must(t, "close", "a")
		if v, err := r.node.Do("release", shardName, "delta"); err != nil || v != "released" {
			t.Fatalf("release: %q %v", v, err)
		}
		r.ackPending()
		r.node.Must(t, "cached", shardName, r.path)
		r.app.Must(t, "open", "a", r.path)
		for range 2 {
			if err := r.commit(true); err != nil {
				t.Fatal(err)
			}
		}
	}
	r.verify("after three cached reactivations")
}
