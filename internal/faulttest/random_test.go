//go:build fault

package faulttest

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

var killPoints = []string{"segment-before", "segment-after", "delta-before-manifest", "merge-before-manifest", "merge-after-swap"}

// TestRandomCrashes does random work and kills the node at random points,
// recovering and carrying on each time. Set SHARDLITE_SEED to repeat a run.
func TestRandomCrashes(t *testing.T) {
	t.Parallel()
	rng := newRand(t)
	rounds := 30
	if v := os.Getenv("SHARDLITE_ROUNDS"); v != "" {
		rounds, _ = strconv.Atoi(v)
	}
	r := newRig(t)
	r.settle(1)
	crashes := 0
	for round := range rounds {
		// One operation, maybe with a kill armed for it.
		op := rng.IntN(10)
		arm := ""
		if rng.IntN(4) == 0 {
			arm = killPoints[rng.IntN(len(killPoints))]
		}
		var err error
		desc := ""
		switch {
		case op < 6:
			desc = "commit+sync"
			if arm == "segment-before" || arm == "segment-after" {
				r.node.Must(t, "arm", arm)
			} else {
				arm = ""
			}
			err = r.commit(true)
		case op < 7:
			desc = "commit without sync"
			err = r.commit(false)
			arm = ""
		case op < 9:
			desc = "delta"
			if arm == "delta-before-manifest" {
				r.node.Must(t, "arm", arm)
			} else {
				arm = ""
			}
			_, err = r.node.Do("delta", shardName)
		default:
			desc = "merge"
			if arm == "merge-before-manifest" || arm == "merge-after-swap" {
				r.node.Must(t, "arm", arm)
			} else {
				arm = ""
			}
			_, err = r.node.Do("merge", shardName)
		}
		switch {
		case err != nil && arm == "":
			t.Fatalf("round %d: %s failed with nothing armed: %v", round, desc, err)
		case err != nil:
			crashes++
			label := fmt.Sprintf("round %d: %s killed at %s", round, desc, arm)
			r.crash()
			r.verify(label)
			if arm == "segment-after" {
				// The write reached storage even though the sync was not acknowledged, so it counts.
				if !r.hasCommit(r.n) {
					t.Fatalf("%s: commit in a written segment is missing", label)
				}
				r.acked[r.n] = true
			}
			r.continueAfterRecovery(label)
		case arm != "":
			// The armed point wasn't reached (a delta with an empty tail, say). Disarm by restarting.
			r.crash()
			r.verify(fmt.Sprintf("round %d: armed %s never reached", round, arm))
			r.continueAfterRecovery(fmt.Sprintf("round %d: unreached %s", round, arm))
		}
		// A commit without a sync makes the next sync carry it; a commit's unsynced state is allowed to be lost on a crash.
		if round%5 == 4 {
			r.verify(fmt.Sprintf("round %d", round))
		}
	}
	t.Logf("%d rounds, %d crashes, node started %d times, %d commits acknowledged of %d", rounds, crashes, r.started, len(r.acked), r.n)
}
