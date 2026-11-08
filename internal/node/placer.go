package node

import (
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/notify"
)

// placer picks the node a claim goes to when it isn't to be made on the node that received the request (docs/internals.md, Activation). It picks at random among the live nodes of the lease scan's last listing that the node doesn't avoid, this node among them unless it is draining. With no candidate at all it picks this node, since it is running.
//
// A peer is avoided for a reason (avoidReason) and only while that reason holds. What the node knows of its peers comes from the traffic it takes part in, so a peer it has not exchanged anything with is never avoided, and a wrong entry only makes placement worse (docs/internals.md, Routing and forwarding). It is hearsay, so no entry outlives the lease TTL without being heard again.
type placer struct {
	self     cluster.Node
	draining *atomic.Bool     // whether this node drains: it is then no candidate, as a peer that says avoid isn't
	listing  func() *NodeList // the lease scan's last listing
	clock    Clock
	ttl      time.Duration // the lease TTL: how long a peer that said avoid stays avoided without saying it again
	log      *slog.Logger
	bad      *notify.Counter // the peer headers ignored, for tests and to see a peer that sends garbage

	mu    sync.Mutex
	avoid map[cluster.Node]avoidEntry // the peers not to place on, only ever live ones once place has looked
}

// avoidReason is why a peer is avoided.
type avoidReason int

const (
	// avoidSaid: an answer from the peer or a forward from it carried Shardlite-Placement: avoid. It ends at an answer from the peer without the header, when the listing no longer has the peer live, or a lease TTL after the peer last said it. The last covers a drainer that restarted before this node's listing saw its lease released: nothing it sends says avoid any more, and this node may send it nothing that would bring an answer.
	avoidSaid avoidReason = iota
	// avoidUnreachable: a forward to the peer got no answer within the answer timeout. It ends at the next listing or at any answer from the peer.
	avoidUnreachable
)

// avoidEntry is why a peer is avoided.
type avoidEntry struct {
	reason avoidReason
	list   *NodeList // the listing an avoidUnreachable entry was made under
	until  time.Time // when an avoidSaid entry ends unless the peer says avoid again
}

// ended reports whether the entry no longer holds under the listing at now.
func (e avoidEntry) ended(list *NodeList, now time.Time) bool {
	switch e.reason {
	case avoidSaid:
		return !now.Before(e.until)
	case avoidUnreachable:
		return e.list != list
	}
	return true
}

func newPlacer(self cluster.Node, draining *atomic.Bool, listing func() *NodeList, clock Clock, ttl time.Duration, log *slog.Logger, bad *notify.Counter) *placer {
	return &placer{self: self, draining: draining, listing: listing, clock: clock, ttl: ttl, log: log, bad: bad, avoid: map[cluster.Node]avoidEntry{}}
}

// said is the entry for a peer that just said avoid.
func (p *placer) said() avoidEntry {
	return avoidEntry{reason: avoidSaid, until: p.clock.Now().Add(p.ttl)}
}

// received reads the peer headers of a request from a peer: the node that sent it, empty if the request named none or the name was bad or this node's own, and whether it asked to be avoided. from says where it connected from, for the log. A peer that asks to be avoided is avoided, if the request named it.
func (p *placer) received(from string, h http.Header) (node cluster.Node, avoid bool) {
	node, err := parseNode(h)
	if err != nil {
		p.ignore(from, err)
	}
	if node == p.self {
		node = ""
	}
	avoid, err = parsePlacement(h)
	if err != nil {
		p.ignore(from, err)
	}
	if node != "" && avoid {
		e := p.said()
		p.mu.Lock()
		p.avoid[node] = e
		p.mu.Unlock()
	}
	return node, avoid
}

// answered reads the placement in an answer from peer, the 102 that acknowledges a forward included. An answer that says avoid makes the peer avoided. One without the header ends the entry, whichever the reason, since the peer answered and takes placements. A header that does not read is ignored, which leaves a peer that said avoid avoided but still ends an unreachable entry, since the peer answered.
func (p *placer) answered(peer cluster.Node, h http.Header) {
	avoid, err := parsePlacement(h)
	if err != nil {
		p.ignore(string(peer), err)
	}
	said := p.said()
	p.mu.Lock()
	defer p.mu.Unlock()
	switch e := p.avoid[peer]; {
	case err != nil:
		if e.reason == avoidUnreachable {
			delete(p.avoid, peer)
		}
	case avoid:
		p.avoid[peer] = said
	default:
		delete(p.avoid, peer)
	}
}

// unanswered notes that a forward to peer got no answer. A peer that said avoid stays avoided for that reason while it holds, since it lasts longer.
func (p *placer) unanswered(peer cluster.Node) {
	list, now := p.listing(), p.clock.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.avoid[peer]; ok && e.reason == avoidSaid && !e.ended(list, now) {
		return
	}
	p.avoid[peer] = avoidEntry{reason: avoidUnreachable, list: list}
}

// ignore counts a peer header that does not read, and logs it at debug since a peer can send one on every request.
func (p *placer) ignore(from string, err error) {
	p.bad.Inc()
	p.log.Debug("a peer header is ignored", "from", from, "err", err)
}

// place picks the node for a claim: at random among the live nodes that aren't avoided, this node included unless it drains, and this node if there is no other candidate. It first drops the entries that have ended: those for a node the listing doesn't have live, the said ones past their TTL and the unreachable ones made under an earlier listing.
func (p *placer) place() cluster.Node {
	list, now := p.listing(), p.clock.Now()
	live := liveOf(list)
	p.mu.Lock()
	defer p.mu.Unlock()
	for n, e := range p.avoid {
		if !slices.Contains(live, n) || e.ended(list, now) {
			delete(p.avoid, n)
		}
	}
	cands := make([]cluster.Node, 0, len(live))
	for _, n := range live {
		if _, avoided := p.avoid[n]; !avoided && n != p.self {
			cands = append(cands, n)
		}
	}
	if !p.draining.Load() {
		cands = append(cands, p.self)
	}
	if len(cands) == 0 {
		return p.self
	}
	return cands[rand.IntN(len(cands))]
}

// liveOf is the names of the nodes a listing doesn't count as dead.
func liveOf(l *NodeList) []cluster.Node {
	var out []cluster.Node
	for _, v := range l.nodes() {
		if !v.Dead {
			out = append(out, v.Name)
		}
	}
	return out
}
