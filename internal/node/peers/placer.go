package peers

import (
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/model/cluster"
	"github.com/luci1900/shardlite/internal/syncs/notify"
)

// Placer picks the node a claim goes to when it isn't to be made on the node that received the request (docs/internals.md, Activation). It picks at random among the live nodes of the lease scan's last listing that the node doesn't avoid, this node among them unless it is draining. With no candidate at all it picks this node, since it is running.
//
// A peer is avoided for a reason (avoidReason) and only while that reason holds. What the node knows of its peers comes from the traffic it takes part in, so a peer it has not exchanged anything with is never avoided, and a wrong entry only makes placement worse (docs/internals.md, Routing and forwarding). It is hearsay, so no entry outlives the lease TTL without being heard again.
//
// It also owns the headers that carry that traffic (Shardlite-Node and Shardlite-Placement): it reads them from requests and answers (Received, Answered) and sets them on what the node sends (StampNode, StampPlacement), since they exist only to feed placement and the first only to say which peer a placement came from. The fields are set before first use and not changed after.
type Placer struct {
	Self     cluster.Node
	Draining *atomic.Bool // whether this node drains: it is then no candidate, as a peer that says avoid isn't, and it stamps avoid
	Listing  func() *List // the lease scan's last listing
	Clock    Clock
	TTL      time.Duration // the lease TTL: how long a peer that said avoid stays avoided without saying it again
	Log      *slog.Logger
	Bad      *notify.Counter // the peer headers ignored, for tests and to see a peer that sends garbage
	// Fault, if set, is given the name and each value of the headers the node stamps (Faults.PeerHeader) and returns what to send: the same, another value or none.
	Fault func(name, value string) string

	mu    sync.Mutex
	avoid map[cluster.Node]avoidEntry // the peers not to place on, only ever live ones once Place has looked
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
	list   *List     // the listing an avoidUnreachable entry was made under
	until  time.Time // when an avoidSaid entry ends unless the peer says avoid again
}

// ended reports whether the entry no longer holds under the listing at now.
func (e avoidEntry) ended(list *List, now time.Time) bool {
	switch e.reason {
	case avoidSaid:
		return !now.Before(e.until)
	case avoidUnreachable:
		return e.list != list
	}
	return true
}

// said is the entry for a peer that just said avoid.
func (p *Placer) said() avoidEntry {
	return avoidEntry{reason: avoidSaid, until: p.Clock.Now().Add(p.TTL)}
}

// Received reads the peer headers of a request from a peer: the node that sent it, empty if the request named none or the name was bad or this node's own, and whether it asked to be avoided. from says where it connected from, for the log. A peer that asks to be avoided is avoided, if the request named it.
func (p *Placer) Received(from string, h http.Header) (node cluster.Node, avoid bool) {
	node, err := parseNode(h)
	if err != nil {
		p.ignore(from, err)
	}
	if node == p.Self {
		node = ""
	}
	avoid, err = parsePlacement(h)
	if err != nil {
		p.ignore(from, err)
	}
	if node != "" && avoid {
		e := p.said()
		p.mu.Lock()
		p.put(node, e)
		p.mu.Unlock()
	}
	return node, avoid
}

// Answered reads the placement in an answer from peer, the 102 that acknowledges a forward included, and takes it out of h, since it is for this node alone and the client never sees it. An answer that says avoid makes the peer avoided. One without the header ends the entry, whichever the reason, since the peer answered and takes placements. A header that does not read is ignored, which leaves a peer that said avoid avoided but still ends an unreachable entry, since the peer answered.
func (p *Placer) Answered(peer cluster.Node, h http.Header) {
	avoid, err := parsePlacement(h)
	h.Del(HeaderPlacement)
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
		p.put(peer, said)
	default:
		delete(p.avoid, peer)
	}
}

// Unanswered notes that a forward to peer got no answer. A peer that said avoid stays avoided for that reason while it holds, since it lasts longer.
func (p *Placer) Unanswered(peer cluster.Node) {
	list, now := p.Listing(), p.Clock.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.avoid[peer]; ok && e.reason == avoidSaid && !e.ended(list, now) {
		return
	}
	p.put(peer, avoidEntry{reason: avoidUnreachable, list: list})
}

// put sets the avoid entry for a peer. The caller holds the lock.
func (p *Placer) put(peer cluster.Node, e avoidEntry) {
	if p.avoid == nil {
		p.avoid = map[cluster.Node]avoidEntry{}
	}
	p.avoid[peer] = e
}

// Avoids reports whether the placer holds an avoid entry for the peer, for tests of what the node learned from its traffic.
func (p *Placer) Avoids(peer cluster.Node) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.avoid[peer]
	return ok
}

// StampNode sets Shardlite-Node on h to this node's name.
func (p *Placer) StampNode(h http.Header) { p.stamp(h, HeaderNode, string(p.Self)) }

// StampPlacement sets Shardlite-Placement on h to avoid while the node drains, and leaves it alone otherwise (absence means the node takes placements).
func (p *Placer) StampPlacement(h http.Header) {
	if p.Draining.Load() {
		p.stamp(h, HeaderPlacement, PlacementAvoid)
	}
}

// stamp sets a header between nodes to v, as the fault hook leaves it: the same, another value or none.
func (p *Placer) stamp(h http.Header, name, v string) {
	if p.Fault != nil {
		v = p.Fault(name, v)
	}
	if v != "" {
		h.Set(name, v)
	}
}

// ignore counts a peer header that does not read, and logs it at debug since a peer can send one on every request.
func (p *Placer) ignore(from string, err error) {
	p.Bad.Inc()
	p.Log.Debug("a peer header is ignored", "from", from, "err", err)
}

// Place picks the node for a claim: at random among the live nodes that aren't avoided, this node included unless it drains, and this node if there is no other candidate. It first drops the entries that have ended: those for a node the listing doesn't have live, the said ones past their TTL and the unreachable ones made under an earlier listing.
func (p *Placer) Place() cluster.Node {
	list, now := p.Listing(), p.Clock.Now()
	live := list.Live()
	p.mu.Lock()
	defer p.mu.Unlock()
	for n, e := range p.avoid {
		if !slices.Contains(live, n) || e.ended(list, now) {
			delete(p.avoid, n)
		}
	}
	cands := make([]cluster.Node, 0, len(live))
	for _, n := range live {
		if _, avoided := p.avoid[n]; !avoided && n != p.Self {
			cands = append(cands, n)
		}
	}
	if !p.Draining.Load() {
		cands = append(cands, p.Self)
	}
	if len(cands) == 0 {
		return p.Self
	}
	return cands[rand.IntN(len(cands))]
}
