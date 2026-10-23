package node

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/pos"
)

// The mirror's node side: what the peer listener serves the journals it
// mirrors from, the view of the cluster's leases a mirror checks a write id's
// epoch against, and how an adoption dials a dead node's mirrors for the fetch
// (docs/internals.md, Node journals and snapshots).

// newPeerTransport is the transport of the node's requests to other nodes'
// peer listeners: h2c, one multiplexed connection per pair of nodes, a dial
// timeout so a peer that is down fails in seconds, and pings so a peer that
// vanished without a FIN stops swallowing requests on its pooled connection.
func newPeerTransport() *http.Transport {
	t := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: dialTimeout}).DialContext,
		DisableCompression:  true,
		MaxIdleConnsPerHost: 256, IdleConnTimeout: 90 * time.Second,
		HTTP2: &http.HTTP2Config{SendPingTimeout: h2cPingAfter, PingTimeout: h2cPingWait},
	}
	t.Protocols = new(http.Protocols)
	t.Protocols.SetUnencryptedHTTP2(true)
	return t
}

// newPeerClient is a client for other nodes' peer listeners.
func newPeerClient() *http.Client { return &http.Client{Transport: newPeerTransport()} }

// epochReadTimeout bounds one read of a lease on a mirror's create path: a
// slow read leaves the view as it was, and a view that is behind never
// refuses.
const epochReadTimeout = 500 * time.Millisecond

// mirrorEpochs is a mirror's view of the cluster's leases: the epoch of each
// node's lease as of this node's last read of it. A create on a mirrored
// journal carries the writer's epoch in its write id, and the mirror refuses
// only what names a strictly older epoch than its view, so a view that is
// expired, or not yet caught up with a renewal, costs latency and never drops
// a mirror out of the ensemble.
//
// The view follows the lease scan's listing: a lease is read when the listing
// shows an etag the view has not seen, which is a renewal or a new epoch, so
// the reads are a few per renewal and not one per segment. A lease that cannot
// be read leaves the view as it was.
type mirrorEpochs struct {
	reg   *coord.Registry
	nodes func() *NodeList // the lease scan's last listing, nil before its first

	mu   sync.Mutex
	seen map[cluster.Node]mirrorSeen
}

type mirrorSeen struct {
	etag  blob.ETag
	epoch pos.Epoch
}

// Epoch is the view of node's lease epoch, 0 if it has none.
func (v *mirrorEpochs) Epoch(ctx context.Context, node cluster.Node) pos.Epoch {
	var etag blob.ETag
	if list := v.nodes(); list != nil {
		if i, found := slices.BinarySearchFunc(list.Nodes, node, func(view NodeView, name cluster.Node) int {
			return strings.Compare(string(view.Name), string(name))
		}); found {
			etag = list.Nodes[i].ETag
		}
	}
	v.mu.Lock()
	seen := v.seen[node]
	v.mu.Unlock()
	if seen.etag == etag {
		return seen.epoch
	}
	rctx, cancel := context.WithTimeout(ctx, epochReadTimeout)
	l, err := v.reg.Get(rctx, node)
	cancel()
	if err != nil {
		return seen.epoch // not caught up: the old view stands, and it never refuses
	}
	v.mu.Lock()
	if v.seen == nil {
		v.seen = map[cluster.Node]mirrorSeen{}
	}
	v.seen[node] = mirrorSeen{etag: etag, epoch: l.Epoch}
	v.mu.Unlock()
	return l.Epoch
}

// mirrorSource is what this node serves the mirror protocol from: the journals
// in its local database, which holds a mirrored journal beside its own under
// the journal's node name, and the view of the owners' leases.
type mirrorSource struct {
	db     *localdb.DB
	epochs *mirrorEpochs
}

// Journal is the backend of node's mirrored journal, nil if this node has no
// local database to hold one in.
func (s *mirrorSource) Journal(node cluster.Node) journal.Backend {
	if s.db == nil {
		return nil
	}
	return journal.NewLocal(s.db.DB, node)
}

// Epoch is the view of node's lease epoch.
func (s *mirrorSource) Epoch(ctx context.Context, node cluster.Node) pos.Epoch {
	return s.epochs.Epoch(ctx, node)
}

// mirrorDialer dials other nodes' mirror endpoints: the stores an adoption's
// fetch reads a dead node's mirrors through, one per mirror that has a lease
// with an address. A mirror it cannot dial is simply absent, and the fetch
// reports what that cost.
type mirrorDialer struct {
	client *http.Client
	reg    *coord.Registry
	opts   journal.Options
}

// newMirrorDialer returns a dialer over a client of its own.
func newMirrorDialer(reg *coord.Registry, opts journal.Options) *mirrorDialer {
	return &mirrorDialer{client: newPeerClient(), reg: reg, opts: opts}
}

// stores dials the mirrors of the journal whose lease is dead, in the order
// the lease names them. A mirror's lease is read once, here: the fetch asks
// each store a bounded number of questions and the adoption carries on past
// one that cannot be reached.
func (d *mirrorDialer) stores(ctx context.Context, dead coord.NodeLease, log *slog.Logger) []*journal.Store {
	var out []*journal.Store
	for _, name := range dead.Mirrors {
		l, err := d.reg.Get(ctx, name)
		if err != nil {
			if log != nil && ctx.Err() == nil {
				log.Warn("could not read a mirror's lease for the fetch", "node", dead.Name, "mirror", name, "err", err)
			}
			continue
		}
		if l.Address == "" {
			continue
		}
		out = append(out, journal.NewStore(journal.NewMirror(d.client, "http://"+l.Address, dead.Name), dead.Name, dead.Epoch, d.opts))
	}
	return out
}
