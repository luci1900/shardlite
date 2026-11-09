package peers

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/lease"
)

// Addresses caches where nodes listen for forwards, from their leases
// (docs/internals.md, Routing and forwarding). A lease is read again when the
// cache has none for the node and when the lease scan's listing shows the node
// wrote its lease after the cached one would have expired, which only a node
// that was gone and came back does: a node that renews writes before its lease
// runs out, so comparing with the last write alone would read again at every
// renewal. A forward that fails does not refresh the address: a node that
// restarted somewhere else within its TTL is reached there once its renewals
// pass the cached lease's expiry (about a TTL, and a failover takes longer),
// and until then its shards answer 503.
type Addresses struct {
	Get   func(ctx context.Context, node cluster.Node) (*lease.Record, error) // reads a node's lease
	Nodes func() *List                                                        // the lease scan's last listing

	mu     sync.Mutex
	leases map[cluster.Node]*lease.Record // by node, as read; their epochs say which life of the node they are
}

// Address is where the node listens for forwards.
func (a *Addresses) Address(ctx context.Context, node cluster.Node) (string, error) {
	a.mu.Lock()
	l := a.leases[node]
	a.mu.Unlock()
	if l == nil || a.stale(l) {
		var err error
		if l, err = a.Get(ctx, node); err != nil {
			return "", err
		}
		if l.Address == "" {
			return "", errors.New("its lease has no address")
		}
		a.Remember(l)
	}
	return l.Address, nil
}

// Remember caches l as the lease of its node.
func (a *Addresses) Remember(l *lease.Record) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.leases == nil {
		a.leases = map[cluster.Node]*lease.Record{}
	}
	a.leases[l.Name] = l
}

// stale reports whether the listing shows the node wrote its lease after l
// would have expired.
func (a *Addresses) stale(l *lease.Record) bool {
	list := a.Nodes()
	if list == nil {
		return false
	}
	i, ok := slices.BinarySearchFunc(list.Nodes, l.Name, func(v Peer, name cluster.Node) int { return strings.Compare(string(v.Name), string(name)) })
	return ok && list.Nodes[i].LastModified.After(l.ExpiresAt())
}
