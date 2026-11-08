//go:build fault

package e2e

import (
	"testing"

	"github.com/luci1900/shardlite/internal/kill"
)

// The kill scenarios, each on a rig of its own. The random test composes the
// same scenarios in a random order on one rig.

// A write the node acknowledged nothing for, then a kill: what was acknowledged
// before survives, and the shard goes on.
func (r *faultRig) unsynced() {
	r.t.Helper()
	r.settle(1)
	r.mustCommit(faultUsers) // answered by the app, never waited for
	r.recoverAndCheck("after an unsynced write")
}

// A kill when a segment is about to be written, or has just been: the write that
// was waiting is not acknowledged, and everything acknowledged before survives.
func (r *faultRig) segment(point kill.Point) {
	r.t.Helper()
	r.arm(point)
	r.settle(1)
	r.fireNow()
	if err := r.commit(faultWallet); err == nil {
		r.t.Fatalf("the write was acknowledged though the node should have died at %s", point)
	}
	r.dies(point)
	r.recoverAndCheck(string(point))
}

// A kill between the file of a delta (or a rebase's base) and the manifest update
// that adopts it.
func (r *faultRig) snapshot(point kill.Point, deltas int) {
	r.t.Helper()
	r.arm(point)
	r.settle(deltas)
	r.fireNow()
	r.writeUntilDeath(point)
	r.recoverAndCheck(string(point))
}

// A kill during the first activation after a crash: the shard is restored from
// blob storage and the node dies at point on the way.
func (r *faultRig) activation(point kill.Point) {
	r.t.Helper()
	r.settle(2)
	r.arm(point)
	r.fireNow()
	if r.touch(faultWallet) {
		r.t.Fatalf("the shard was served though the node should have died at %s", point)
	}
	r.dies(point)
	r.verify(string(point) + ": after the interrupted activation")
	r.recoverAndCheck(string(point))
}

// A kill when a shard that was let go of when it went idle is activated again:
// it is claimed and restored from blob storage, and the node dies at point on the way.
func (r *faultRig) releasedActivation(point kill.Point) {
	r.t.Helper()
	r.arm(point)
	r.settle(1)
	r.waitReleased(faultWallet)
	r.fireNow()
	if r.touch(faultWallet) {
		r.t.Fatalf("the shard was served though the node should have died at %s", point)
	}
	r.dies(point)
	r.recoverAndCheck("released " + string(point))
}

// Activations of a shard let go of when it went idle that are not killed carry
// on, and the shard's history restores to what the app wrote.
func (r *faultRig) releasedCarriesOn() {
	r.t.Helper()
	r.settle(1)
	for range 3 {
		r.waitReleased(faultWallet)
		r.mustCommit(faultWallet)
		r.mustCommit(faultWallet)
	}
	r.verify("after three activations after a release")
}

func TestFaultKillAfterWriteBeforeSync(t *testing.T) {
	t.Parallel()
	newFaultRig(t).unsynced()
}

func TestFaultKillAtSegment(t *testing.T) {
	t.Parallel()
	for _, point := range []kill.Point{kill.SegmentBefore, kill.SegmentAfter} {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			newFaultRig(t).segment(point)
		})
	}
}

func TestFaultKillAfterDeltaFileBeforeManifest(t *testing.T) {
	t.Parallel()
	newFaultRig(t).snapshot(kill.DeltaBeforeManifest, 1)
}

func TestFaultKillRebase(t *testing.T) {
	t.Parallel()
	for _, point := range []kill.Point{kill.RebaseBeforeManifest, kill.RebaseAfterSwap} {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			newFaultRig(t).snapshot(point, 3)
		})
	}
}

func TestFaultKillDuringRestoreBeforeRename(t *testing.T) {
	t.Parallel()
	newFaultRig(t).activation(kill.RestoreBeforeRename)
}

func TestFaultKillBetweenRestoreAndFirstSync(t *testing.T) {
	t.Parallel()
	for _, point := range activationPoints {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			newFaultRig(t).activation(point)
		})
	}
}

func TestFaultKillReleasedActivation(t *testing.T) {
	t.Parallel()
	for _, point := range activationPoints {
		t.Run(string(point), func(t *testing.T) {
			t.Parallel()
			newFaultRig(t).releasedActivation(point)
		})
	}
}

func TestFaultReleasedActivationCarriesOn(t *testing.T) {
	t.Parallel()
	newFaultRig(t).releasedCarriesOn()
}
