package dispatch

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/shard"
)

func nodes(n int) []cluster.Node {
	out := make([]cluster.Node, n)
	for i := range out {
		out[i] = cluster.Node(fmt.Sprintf("node-%d", i))
	}
	return out
}

// Every node picks the same runner for a lane from the same listing, whatever
// order the listing is in.
func TestEveryNodePicksTheSameRunner(t *testing.T) {
	t.Parallel()
	live := nodes(7)
	for i := range shard.Lanes {
		lane := shard.Lane(i)
		want := Runner(live, lane)
		for range 20 {
			shuffled := slices.Clone(live)
			rand.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
			if got := Runner(shuffled, lane); got != want {
				t.Fatalf("lane %v: %s in one order, %s in another", lane, want, got)
			}
		}
	}
}

// Each lane has exactly one runner among the nodes' own lanes.
func TestMineIsAPartition(t *testing.T) {
	t.Parallel()
	live := nodes(5)
	owner := map[shard.Lane]cluster.Node{}
	for _, n := range live {
		for _, lane := range Mine(n, live) {
			if prev, ok := owner[lane]; ok {
				t.Fatalf("lane %v is run by %s and %s", lane, prev, n)
			}
			owner[lane] = n
		}
	}
	if len(owner) != shard.Lanes {
		t.Fatalf("%d lanes are run, want %d", len(owner), shard.Lanes)
	}
}

// A node leaving moves only the lanes it ran, and a node joining takes lanes only
// from others, never moving one between the rest.
func TestANodeLeavingMovesOnlyItsLanes(t *testing.T) {
	t.Parallel()
	live := nodes(6)
	gone := live[2]
	rest := slices.DeleteFunc(slices.Clone(live), func(n cluster.Node) bool { return n == gone })
	for i := range shard.Lanes {
		lane := shard.Lane(i)
		before, after := Runner(live, lane), Runner(rest, lane)
		if before != gone && before != after {
			t.Fatalf("lane %v moved from %s to %s though %s left", lane, before, after, gone)
		}
		if after == gone {
			t.Fatalf("lane %v is run by %s, which left", lane, gone)
		}
	}
	// Joining is the same change read the other way.
	for i := range shard.Lanes {
		lane := shard.Lane(i)
		if before, after := Runner(rest, lane), Runner(live, lane); after != gone && after != before {
			t.Fatalf("lane %v moved from %s to %s though only %s joined", lane, before, after, gone)
		}
	}
}

// Before the first listing, and for a node the listing doesn't have, nothing is run.
func TestNobodyRunsWithoutAListing(t *testing.T) {
	t.Parallel()
	for _, live := range [][]cluster.Node{nil, {}} {
		for i := range shard.Lanes {
			if got := Runner(live, shard.Lane(i)); got != "" {
				t.Fatalf("lane %d has runner %q in an empty listing", i, got)
			}
		}
		if got := Mine("node-0", live); len(got) != 0 {
			t.Fatalf("node-0 runs %v in an empty listing", got)
		}
	}
	if got := Mine("stranger", nodes(3)); len(got) != 0 {
		t.Fatalf("a node outside the listing runs %v", got)
	}
}

// A lone node runs every lane.
func TestALoneNodeRunsEveryLane(t *testing.T) {
	t.Parallel()
	if got := Mine("node-0", nodes(1)); len(got) != shard.Lanes {
		t.Fatalf("a lone node runs %d lanes, want %d", len(got), shard.Lanes)
	}
}

// Lanes spread over the nodes: with a few nodes none runs everything, and with
// more nodes than lanes each node runs at most a few.
func TestLanesSpreadOverNodes(t *testing.T) {
	t.Parallel()
	for _, n := range []int{2, 4, 8} {
		live := nodes(n)
		most := 0
		for _, node := range live {
			most = max(most, len(Mine(node, live)))
		}
		if most == shard.Lanes {
			t.Fatalf("with %d nodes one runs every lane", n)
		}
		if most > shard.Lanes*3/4 && n >= 4 {
			t.Fatalf("with %d nodes one runs %d of %d lanes", n, most, shard.Lanes)
		}
	}
	// Of two nodes that differ in the last character of their names, each ranks first for about half of the lanes of many pairs.
	wins := 0
	for i := range 200 {
		live := []cluster.Node{cluster.Node(fmt.Sprintf("node-%da", i)), cluster.Node(fmt.Sprintf("node-%db", i))}
		if Runner(live, shard.Lane(i%shard.Lanes)) == live[0] {
			wins++
		}
	}
	if wins < 70 || wins > 130 {
		t.Fatalf("the first of two similar names won %d of 200 lanes", wins)
	}
}
