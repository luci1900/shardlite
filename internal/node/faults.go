package node

import (
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// Faults are the hooks a fault build of the binary arms (cmd/shardlite/fault.go).
// Every one is nil in production, and nil means no fault.
type Faults struct {
	// At is called at each kill point the node reaches, and may end the process.
	At func(kill.Point)
	// Backend wraps the blob backend (after the prefix) so storage can fail.
	// Run applies it, so a node started with Start takes the wrapped backend as Config.Backend.
	Backend func(blob.Backend) blob.Backend
	// Temp answers for every create, append and duplicate of a temporary file
	// (tempfile.Dir.SetHook): an error fails the operation.
	Temp func(tempfile.Op) error
	// MaxFiles is how many files a manifest may list before the next snapshot
	// rebases the shard (LifecycleOptions.MaxFiles), so a test reaches a rebase
	// with a few deltas. Zero keeps the default.
	MaxFiles int
	// PeerHeader is given the name and the value of every Shardlite-Node and
	// Shardlite-Placement header the node is about to send another node and returns
	// what goes on the wire: the same value, a garbled one, or "" to send none. The
	// receiver ignores a header that does not read, so placement only gets worse
	// for it, never wrong.
	PeerHeader func(name, value string) string
	// NoLeaseExpiry makes the keeper never judge the lease expired by time
	// (lease.KeeperOptions.NoExpiry), so a pause longer than the TTL is one the
	// node cannot see.
	NoLeaseExpiry bool
	// Unconfirmed makes storage answers leave without confirming that the node
	// still owns its shards (Flusher.confirm), as none answers do. It breaks
	// the storage tier's promise, so a test can show its check catches that.
	Unconfirmed bool
}

// WithFaults makes Run arm f.
func WithFaults(f Faults) RunOption {
	return func(o *runOptions) { o.faults = f }
}
