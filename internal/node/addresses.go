package node

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"

	"github.com/luci1900/shardlite/internal/coord"
)

// addresses caches where nodes listen for forwards, from their leases
// (docs/internals.md, Routing and forwarding). A lease is read again when the
// cache has none for the node, when a forward to it could not connect (forget)
// and when the lease scan's listing shows the node wrote its lease after the
// cached one would have expired, which only a node that was gone and came back
// does: a node that renews writes before its lease runs out, so comparing with
// the last write alone would read again at every renewal. A node that restarted
// within its TTL is found by its refused connection.
type addresses struct {
	get   func(ctx context.Context, node string) (*coord.NodeLease, error)
	nodes func() *NodeList // the lease scan's last listing

	mu     sync.Mutex
	leases map[string]*coord.NodeLease // by node, as read; their epochs say which life of the node they are
}

// address is where the node listens for forwards.
func (a *addresses) address(ctx context.Context, node string) (string, error) {
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
			a.leases = map[string]*coord.NodeLease{}
		}
		a.leases[node] = l
		a.mu.Unlock()
	}
	return l.Address, nil
}

// stale reports whether the listing shows the node wrote its lease after l
// would have expired.
func (a *addresses) stale(l *coord.NodeLease) bool {
	list := a.nodes()
	if list == nil {
		return false
	}
	i, ok := slices.BinarySearchFunc(list.Nodes, l.Name, func(v NodeView, name string) int { return strings.Compare(v.Name, name) })
	return ok && list.Nodes[i].LastModified.After(l.ExpiresAt())
}

// forget drops the node's address: a forward could not connect to it.
func (a *addresses) forget(node string) {
	a.mu.Lock()
	delete(a.leases, node)
	a.mu.Unlock()
}
