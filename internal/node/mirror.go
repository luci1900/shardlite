package node

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/pos"
)

// The mirror's node side: what the peer listener serves the journals it
// mirrors from, and how an adoption dials a dead node's peer for the fetch
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

// mirrorSource is what this node serves the mirror protocol from: the journals
// in its local database, which holds a mirrored journal beside its own under
// the journal's node name.
type mirrorSource struct {
	db *localdb.DB
}

// Journal is the backend of node's mirrored journal, nil if this node has no
// local database to hold one in.
func (s *mirrorSource) Journal(node cluster.Node) journal.Backend {
	if s.db == nil {
		return nil
	}
	return journal.NewLocal(s.db.DB, node)
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

// dial dials one node's mirror endpoint for the journal whose writer is node
// at epoch: the store a fan-out sends segments through, or an adoption's fetch
// reads a dead journal's tail from. The journal is not necessarily this
// node's: an adopter dials a dead node's peer for that node's journal. A peer
// whose lease cannot be read, or that has no address yet, is nil, and the
// caller says what that cost: the writer has no peer, and a fetch's window is
// lost.
func (d *mirrorDialer) dial(ctx context.Context, node cluster.Node, epoch pos.Epoch, peer cluster.Node, log *slog.Logger) *journal.Store {
	if d == nil || peer == "" {
		return nil // a scan that dials nothing, or a journal that named none
	}
	l, err := d.reg.Get(ctx, peer)
	if err != nil {
		if log != nil && ctx.Err() == nil {
			log.Warn("could not read the peer's lease", "node", node, "peer", peer, "err", err)
		}
		return nil
	}
	if l.Address == "" {
		return nil
	}
	return journal.NewStore(journal.NewMirror(d.client, "http://"+l.Address, node), node, epoch, d.opts)
}
