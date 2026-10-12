//go:build fault

package faulttest

import (
	"testing"
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

func TestKillMerge(t *testing.T) {
	t.Parallel()
	for _, point := range []string{"merge-before-manifest", "merge-after-swap"} {
		t.Run(point, func(t *testing.T) {
			t.Parallel()
			r := newRig(t)
			r.settle(3)
			r.node.Must(t, "arm", point)
			if _, err := r.node.Do("merge", shardName); err == nil {
				t.Fatal("merge succeeded though the node should have died")
			}
			r.crash()
			r.verify("after crash")
			r.continueAfterRecovery(point)
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
