//go:build fault

package e2e

import (
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	"github.com/luci1900/shardlite/internal/kill"
)

// TestFaultRandomCrashes runs the kill scenarios in a random order on one
// shard, each recovering and carrying on, so a kill meets a history the others
// left. Set SHARDLITE_SEED to repeat a run and SHARDLITE_ROUNDS for its length.
func TestFaultRandomCrashes(t *testing.T) {
	t.Parallel()
	seed := faultSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	rounds := 8
	if v := os.Getenv("SHARDLITE_ROUNDS"); v != "" {
		rounds, _ = strconv.Atoi(v)
	}
	r := newFaultRig(t)
	type scenario struct {
		name string
		run  func()
	}
	scenarios := []scenario{
		{"unsynced", r.unsynced},
		{"segment-before", func() { r.segment(kill.SegmentBefore) }},
		{"segment-after", func() { r.segment(kill.SegmentAfter) }},
		{"delta", func() { r.snapshot(kill.DeltaBeforeManifest, 1) }},
		{"rebase-before-manifest", func() { r.snapshot(kill.RebaseBeforeManifest, 3) }},
		{"rebase-after-swap", func() { r.snapshot(kill.RebaseAfterSwap, 3) }},
		{"restore-before-rename", func() { r.activation(kill.RestoreBeforeRename) }},
	}
	for _, point := range activationPoints {
		scenarios = append(scenarios, scenario{string(point), func() { r.activation(point) }})
		if point != kill.Restored {
			scenarios = append(scenarios, scenario{"cached " + string(point), func() { r.cachedActivation(point) }})
		}
	}
	for round := range rounds {
		s := scenarios[rng.IntN(len(scenarios))]
		t.Logf("round %d: %s", round, s.name)
		s.run()
	}
	t.Logf("%d rounds, %d writes, %d acknowledged", rounds, r.n, len(r.acked))
}
