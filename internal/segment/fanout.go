package segment

import (
	"context"
	"sync"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/pos"
)

// The peer (docs/internals.md, Node journals and snapshots): every segment the
// writer builds goes to the peer beside the record and the node's own disk, on
// the writer's third queue. The peer is for latency: its copy is what a `peer`
// acknowledgement waits for, and the record lands seconds behind it. A peer
// that is slow, broken or dead never holds the record's write, nor the local
// store's.
//
// What the peer holds is tracked as its frontier: the highest segment number it
// has taken. The sends are the writer's, in the writer's number order, so there
// is no gap below a taken number to bridge. The frontier only ever moves up: a
// peer that has fallen out freezes where it was, and a repair's replacement
// continues from the number the seed ran to.

// peerAttempts is how many times the peer is asked to take a segment before
// it falls out, as the local store's own limit. Each ask is a journal.Store
// create, with its retries and its read-back inside, so the peer is gone
// after tens of unanswers, not after one.
const peerAttempts = 5

// Mirror is the journal's peer, as the writer is given it: the node whose
// disk holds the copy, and that disk through the mirror protocol. The node is
// not the journal's writer, which the write ids name, but the stand-by the
// copy lives on.
type Mirror struct {
	Node    cluster.Node
	Journal *journal.Store
}

// mirrorOut is the writer's side of the peer: the store its segments go to,
// the frontier their outcomes mark and the counter of the entries it holds. A
// peer that refused a segment, or whose stream kept failing, falls out:
// nothing more is sent to it, its frontier freezes, the fall is said at error
// level, and the answers the `peer` tier owes fail until a repair names a
// replacement.
type mirrorOut struct {
	name    cluster.Node   // the node that holds the copy, for the logs
	journal *journal.Store // the peer's disk, through the protocol
	written notify.Counter // the entries of the batches the peer has taken
	out     chan struct{}  // closed once the peer has fallen out

	mu       sync.Mutex
	frontier pos.Seg // the highest number the peer has taken
	fallen   bool    // it fell out: nothing more is sent to it
}

// newMirrorOut is the writer's side of the peer, whose journal starts at
// start: nothing is acknowledged there below it.
func newMirrorOut(m Mirror, start pos.Seg) *mirrorOut {
	return &mirrorOut{name: m.Node, journal: m.Journal, frontier: start - 1, out: make(chan struct{})}
}

// up reports whether the peer still takes segments.
func (m *mirrorOut) up() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.fallen
}

// queuePeer hands the batch to the peer's uploader, unless it has fallen out.
// The push is under peerMu, the swap's lock: a batch queued before a swap is
// one the seed copied into the replacement, and one queued after it is above
// the seed, so the drain leaves neither a duplicate nor a hole.
func (w *Writer) queuePeer(b *batch) {
	w.peerMu.Lock()
	defer w.peerMu.Unlock()
	if m := w.peer; m != nil && m.up() {
		w.peerQueue.push(b)
		return
	}
	b.peerDone.Store(true) // no peer owes it: a repair's seed copies it instead
}

// servePeer is the peer's uploader, the writer's third goroutine: it takes the
// batches the record's uploader made and sends them one at a time, in number
// order. It ends when ctx ends, giving up the batch it is sending.
func (w *Writer) servePeer(ctx context.Context) error {
	for {
		b := w.peerQueue.pop(ctx)
		if b == nil {
			return nil
		}
		w.sendPeer(ctx, b)
	}
}

// sendPeer creates the batch's segment at the peer until the outcome is known
// there: written, or the peer takes no more, or the writer stopped. Every ask
// carries the record create's write id, so what the peer stores is the owner's
// own attempt. A create answered with somebody else's segment under the
// number is not transient: it means this life is not the one the peer
// answers, and the peer falls out rather than retry what will keep failing.
func (w *Writer) sendPeer(ctx context.Context, b *batch) {
	defer w.peerFinished(b)
	w.peerMu.Lock()
	m := w.peer
	w.peerMu.Unlock()
	if m == nil {
		return
	}
	m.mu.Lock()
	fallen, covered := m.fallen, b.seq <= m.frontier
	m.mu.Unlock()
	if fallen {
		return // it takes nothing more
	}
	if covered {
		// A swap happened after this batch was taken from the queue: the
		// number is one the seed already copied into the replacement.
		return
	}
	var (
		acked  bool
		reason error
	)
	backoff := w.opts.Retry
	for attempt := 0; attempt < peerAttempts; attempt++ {
		err := m.journal.CreateWithID(ctx, b.seq, b.body, b.id)
		if err == nil {
			acked = true
			break
		}
		reason = err
		if ctx.Err() != nil {
			break // the writer stopped: the send is given up, the peer is not blamed
		}
		if !blob.Transient(err) {
			break // it takes no more: retrying cannot fix this
		}
		if err := sleep(ctx, backoff); err != nil {
			break
		}
		backoff = min(backoff*2, 40*w.opts.Retry)
	}
	if acked {
		m.take(b.seq, b.count)
		return
	}
	// The peer falls out when it refused a segment, or when its stream kept
	// failing to the last attempt: either way nothing more is sent to it, and
	// the answers the `peer` tier owes fail until a repair names a
	// replacement. A writer that stopped itself blames nobody.
	if ctx.Err() != nil {
		return
	}
	m.fall()
	w.opts.Logger.Error("the peer takes no more of the journal's segments, and answers waiting on it fail until a repair names a replacement",
		"journal", m.journal.Name(), "peer", m.name, "segment", b.seq, "err", reason)
}

// take records that the peer has the batch's segment: with the sends in order
// it is the new frontier, and its entries join the counter the gate waits on.
func (m *mirrorOut) take(seq pos.Seg, count uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frontier = seq
	m.written.Add(count)
}

// fall marks the peer fallen out, once. The frontier stays where it is:
// coverage never rewinds.
func (m *mirrorOut) fall() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fallen {
		return
	}
	m.fallen = true
	close(m.out)
}

// PeerState is the peer's copy of the journal, as the writer sees it.
type PeerState struct {
	Name     cluster.Node // the node that holds the copy
	Frontier pos.Seg      // the highest segment number the peer has taken
	Out      bool         // it fell out: nothing more is sent to it until a repair names a replacement
}

// Peer is the peer's state, ok false when no peer is named.
func (w *Writer) Peer() (PeerState, bool) {
	w.peerMu.Lock()
	m := w.peer
	w.peerMu.Unlock()
	if m == nil {
		return PeerState{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return PeerState{Name: m.name, Frontier: m.frontier, Out: m.fallen}, true
}

// PeerReady reports whether the peer can carry a `peer` answer: one is named
// and it has not fallen out. Where it is false, the tier's answer fails; it is
// never carried by the record instead.
func (w *Writer) PeerReady() bool {
	st, ok := w.Peer()
	return ok && !st.Out
}

// LocalReady reports whether the node's own store can carry a `peer` answer:
// there is a store, and it has not refused a segment. A node without a local
// database cannot meet the tier, and the answers fail rather than be carried by
// the record.
func (w *Writer) LocalReady() bool {
	s := w.stores[StoreLocal]
	if s == nil {
		return false
	}
	select {
	case <-s.down:
		return false
	default:
		return true
	}
}

// LocalDown is a channel closed once the node's own store has refused a
// segment. With no local store it is nil, and a wait on it never wakes: there
// is nothing to fail a wait that could not start.
func (w *Writer) LocalDown() <-chan struct{} {
	if w.stores[StoreLocal] == nil {
		return nil
	}
	return w.stores[StoreLocal].down
}

// PeerCovered reports whether the peer is in step with what the writer has
// built: one is named, it has not fallen out, and its frontier is at the top.
// It is the diagnostic's reading of the peer's state (see Catalog.PeerInfo). A
// `peer` acknowledgement does not depend on it: a batch the peer has not taken
// yet is a wait, not a fault.
func (w *Writer) PeerCovered() bool {
	w.peerMu.Lock()
	m := w.peer
	w.peerMu.Unlock()
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.fallen && uint64(m.frontier) == w.built.Load()
}

// PeerReached is a channel closed once the peer's frontier covers n entries,
// as Written and Reached are for the record and the local store. With no peer
// named it is closed already: nothing waits on it, which is the safe answer.
func (w *Writer) PeerReached(n uint64) <-chan struct{} {
	w.peerMu.Lock()
	m := w.peer
	w.peerMu.Unlock()
	if m == nil {
		return closedChan
	}
	return m.written.Reached(n)
}

// PeerOut is a channel closed once the peer has fallen out, and closed
// already with no peer named.
func (w *Writer) PeerOut() <-chan struct{} {
	w.peerMu.Lock()
	m := w.peer
	w.peerMu.Unlock()
	if m == nil {
		return closedChan
	}
	return m.out
}

// Settled reports whether the record has every segment the writer built: the
// precondition a repair's seed needs, since it seeds the replacement from the
// record, and a segment in flight would leave a hole in the replacement's
// stream that nothing refills. It is the entries the record took against the
// entries appended, so a chunk still waiting to be built holds it off too.
func (w *Writer) Settled() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Written(StoreRecord) == w.appended
}

// StartSeq is the first segment number this writer built: a repair seeds a
// replacement from here, the numbers this life owns, as a peer named at the
// start would have held them.
func (w *Writer) StartSeq() pos.Seg { return w.opts.StartSeq }

// Repair replaces the writer's peer. m is the replacement, last the highest
// segment the seed copied into it, entries the journal entries that seed
// carried: the replacement's frontier starts at last and its counter at
// entries, so `peer` answers keep counting from where the old peer froze. It
// answers whether it swapped: a segment built meanwhile means the seed ran
// behind the writer, and the caller seeds again rather than leave a hole.
//
// The swap hands one queue over: every batch still waiting for the peer is one
// the seed copied, so it is finished here rather than sent to the replacement
// again. Batches built after the swap are queued above the seed.
func (w *Writer) Repair(m Mirror, last pos.Seg, entries uint64) bool {
	w.peerMu.Lock()
	if w.built.Load() != uint64(last) {
		w.peerMu.Unlock()
		return false
	}
	mo := newMirrorOut(m, last+1)
	mo.written.Add(entries)
	w.peer = mo
	held := w.peerQueue.drain()
	w.peerMu.Unlock()
	for _, b := range held {
		w.peerFinished(b)
	}
	return true
}
