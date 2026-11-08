package node

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/lease"
)

// addresses caches where nodes listen for forwards, from their leases
// (docs/internals.md, Routing and forwarding). A lease is read again when the
// cache has none for the node and when the lease scan's listing shows the node
// wrote its lease after the cached one would have expired, which only a node
// that was gone and came back does: a node that renews writes before its lease
// runs out, so comparing with the last write alone would read again at every
// renewal. A forward that fails does not refresh the address: a node that
// restarted somewhere else within its TTL is reached there once its renewals
// pass the cached lease's expiry (about a TTL, and a failover takes longer),
// and until then its shards answer 503.
type addresses struct {
	get   func(ctx context.Context, node cluster.Node) (*lease.Record, error)
	nodes func() *NodeList // the lease scan's last listing

	mu     sync.Mutex
	leases map[cluster.Node]*lease.Record // by node, as read; their epochs say which life of the node they are
}

// address is where the node listens for forwards.
func (a *addresses) address(ctx context.Context, node cluster.Node) (string, error) {
	a.mu.Lock()
	l := a.leases[node]
	a.mu.Unlock()
	if l == nil || a.stale(l) {
		var err error
		if l, err = a.get(ctx, node); err != nil {
			return "", err
		}
		if l.Address == "" {
			return "", errors.New("its lease has no address")
		}
		a.mu.Lock()
		if a.leases == nil {
			a.leases = map[cluster.Node]*lease.Record{}
		}
		a.leases[node] = l
		a.mu.Unlock()
	}
	return l.Address, nil
}

// stale reports whether the listing shows the node wrote its lease after l
// would have expired.
func (a *addresses) stale(l *lease.Record) bool {
	list := a.nodes()
	if list == nil {
		return false
	}
	i, ok := slices.BinarySearchFunc(list.Nodes, l.Name, func(v NodeView, name cluster.Node) int { return strings.Compare(string(v.Name), string(name)) })
	return ok && list.Nodes[i].LastModified.After(l.ExpiresAt())
}
