//go:build fault

package e2e

import (
	"math/rand/v2"
	"os"
	"strconv"
	"testing"

	"github.com/luci1900/shardlite/internal/test/fault"
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
		{"segment-before", func() { r.segment(fault.SegmentBefore) }},
		{"segment-after", func() { r.segment(fault.SegmentAfter) }},
		{"delta", func() { r.snapshot(fault.DeltaBeforeManifest, 1) }},
		{"rebase-before-manifest", func() { r.snapshot(fault.RebaseBeforeManifest, 3) }},
		{"rebase-after-swap", func() { r.snapshot(fault.RebaseAfterSwap, 3) }},
		{"restore-before-rename", func() { r.activation(fault.RestoreBeforeRename) }},
	}
	for _, point := range activationPoints {
		scenarios = append(scenarios,
			scenario{string(point), func() { r.activation(point) }},
			scenario{"released " + string(point), func() { r.releasedActivation(point) }})
	}
	for round := range rounds {
		s := scenarios[rng.IntN(len(scenarios))]
		t.Logf("round %d: %s", round, s.name)
		s.run()
	}
	t.Logf("%d rounds, %d writes, %d acknowledged", rounds, r.n, len(r.acked))
}
