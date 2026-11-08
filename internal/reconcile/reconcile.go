// Package reconcile is what the node's reconcilers share (docs/internals.md,
// Reconcilers): the ranking that says which node runs a lane, the names of the
// node's own requests that a reconciler sends through the node, and the loop that
// runs the reconcilers over their due indexes (loop.go). Each reconciler's handler
// is a package of its own below this one. Those packages never import the node or
// each other.
package reconcile

import (
	"hash/fnv"
	"slices"
	"time"

	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/shard"
)

// Target is the header that names the shard a node's own request is for.
const Target = "Shardlite-Target"

// IfExists is the header that makes a call fail with 404 rather than create a
// shard that doesn't exist.
const IfExists = "Shardlite-If-Exists"

// Activate is the path the node answers itself, for a node's own request: it
// makes the target shard active on the node that serves the request and answers
// 200 if the request activated it or 204 if it was active already.
const Activate = "/_shardlite/activate"

// Clock is the time a reconciler runs on: the node's clock, so that tests with a
// fake one drive its loop.
type Clock interface {
	Now() time.Time
	// NewTimer sends the time on the channel once, d from now. The function stops it.
	NewTimer(d time.Duration) (c <-chan time.Time, stop func() bool)
}

// Runner is the node that runs lane among live: the one with the highest
// rendezvous hash of its name and the lane, so every node that has the same
// listing picks the same one, in whatever order the listing is in, and a node
// joining or leaving moves only the lanes it ranks first for. It is empty if
// live is, which is how a node before its first listing runs nothing.
func Runner(live []cluster.Node, lane shard.Lane) cluster.Node {
	var best cluster.Node
	var top uint64
	for _, n := range live {
		w := weight(n, lane)
		// A tie (a repeated name, or a collision) goes to the lower name, so the order of live never decides.
		if best == "" || w > top || (w == top && n < best) {
			best, top = n, w
		}
	}
	return best
}

// Mine is the lanes self runs among live, in lane order.
func Mine(self cluster.Node, live []cluster.Node) []shard.Lane {
	if !slices.Contains(live, self) {
		return nil
	}
	var out []shard.Lane
	for i := range shard.Lanes {
		if lane := shard.Lane(i); Runner(live, lane) == self {
			out = append(out, lane)
		}
	}
	return out
}

// weight is the node's rank for the lane: fnv-1a of the lane and the name, mixed
// so that names that differ in a last character don't rank alike.
func weight(n cluster.Node, lane shard.Lane) uint64 {
	h := fnv.New64a()
	h.Write([]byte{byte(lane)})
	h.Write([]byte(n))
	x := h.Sum64()
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
