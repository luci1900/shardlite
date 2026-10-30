package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/segment"
)

// The repair (docs/internals.md, Node journals and snapshots): the node's
// journal names one peer at its start, and when that peer falls out — or none
// was named, the start having run alone or against a listing it could not
// read — the repair names a replacement, seeds it from the record and swaps it
// into the writer. The choice stays sticky: only a repair changes it, and the
// next renewal publishes it.

// defaultRepairEvery is how often the node looks at its journal's peer when
// nothing is wrong but a peer is missing, and how long it waits between
// passes of a repair that could not finish.
const defaultRepairEvery = 2 * time.Second

// repairLoop is the node's repair, as a service of the journal's stage: it
// shares the writer's and the keeper's lifetime, since it works through both.
type repairLoop struct {
	n      *Node
	every  time.Duration
	warned bool // the no-candidate warn has gone out; repeats are debug
}

// repairLoop makes the node's repair. It runs nothing when the deployment
// names no standbys: there is no peer to name and none to replace.
func (n *Node) repairLoop(every time.Duration) *repairLoop {
	if every <= 0 {
		every = defaultRepairEvery
	}
	return &repairLoop{n: n, every: every}
}

func (r *repairLoop) String() string { return "repair" }

// Serve runs the repair until ctx ends. Each pass asks one question: can the
// writer's peer carry a `peer` answer? While it can, the pass is a lookup and
// ends. When it cannot, the pass names a replacement from the lease listing
// (the fallen one excluded: its lease can look live while its endpoint does
// not answer), seeds it from the record's landed run once the record has
// everything the writer built, and swaps it in. A record that will not answer
// holds the repair, and the `peer` answers with it: the seed is the record's
// copy, and a hole in a seed is a hole in what a fetch gets back.
func (r *repairLoop) Serve(ctx context.Context) error {
	if r.n.Standbys == 0 {
		<-ctx.Done()
		return nil
	}
	tick := time.NewTimer(r.every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		r.pass(ctx)
		tick.Reset(r.every)
	}
}

// pass is one look at the writer's peer, and a repair attempt when the writer
// is without one.
func (r *repairLoop) pass(ctx context.Context) {
	n := r.n
	if n.peerAlive(ctx) {
		r.warned = false
		return // the peer stands: nothing is owed
	}
	if !n.Writer.Settled() {
		return // the record's write is in flight: the seed would run behind it
	}
	name := choosePeer(ctx, n.Registry, n.Name, n.Standbys, n.currentPeer(), n.Logger)
	if name == "" {
		if !r.warned {
			r.warned = true
			n.Logger.Warn("the repair found no candidate for the journal's peer, the answers on it stay failed")
		}
		return
	}
	d := newMirrorDialer(n.Registry, n.Files.JournalOptions)
	store := d.dial(ctx, n.Name, n.Keeper.Lease().Epoch, name, n.Logger)
	if store == nil {
		n.Logger.Warn("the repair could not open the replacement's store", "peer", name)
		return
	}
	last, entries, err := r.seed(ctx, store)
	if err != nil {
		n.Logger.Warn("the repair could not seed the replacement from the record, the peer answers stay failed",
			"peer", name, "segment", last, "err", err)
		return
	}
	if !n.Writer.Repair(segment.Mirror{Node: name, Journal: store}, last, entries) {
		return // the writer built a segment meanwhile: the next pass seeds it too
	}
	n.setPeer(name)
	n.Logger.Info("repair named a replacement for the journal's peer", "peer", name, "segment", last, "entries", entries)
}

// peerAlive reports whether the writer's peer can carry the journal's next
// segment: named, standing, and holding a lease that looks alive. A peer
// whose lease is gone is replaced whether it fell out or not — an idle
// journal never falls out, and the next write should not pay for finding out.
func (n *Node) peerAlive(ctx context.Context) bool {
	if st, ok := n.Writer.Peer(); !ok || st.Out {
		return false
	}
	peer := n.currentPeer()
	if peer == "" {
		return false
	}
	l, err := n.Registry.Get(ctx, peer)
	if err != nil {
		return false
	}
	return !l.Released && !l.ExpiresAt().Before(time.Now())
}

// seed copies this life's landed run from the record into the replacement,
// checked as the bytes leave and created under the write ids the record
// holds, so what the replacement stores is the owner's own attempt and a
// fetch copies it back as one. It answers the number the seed ran to and the
// entries it carries, which the swap starts the replacement's frontier and
// counter at. Segments the replacement already holds are skipped and counted,
// so a pass cut short is continued, not repeated.
func (r *repairLoop) seed(ctx context.Context, store *journal.Store) (last pos.Seg, entries uint64, err error) {
	n := r.n
	w := n.Writer
	record := n.Files.Journal(n.Name)
	last, entries = w.StartSeq()-1, 0
	for seq := w.StartSeq(); seq <= w.NextSeq()-1; seq++ {
		foot, err := segment.ReadFooter(ctx, record, seq)
		if err != nil {
			return last, entries, fmt.Errorf("the record's segment %d: %w", seq, err)
		}
		_, err = store.Head(ctx, seq)
		switch {
		case err == nil:
			// The replacement already holds it.
		case errors.Is(err, blob.ErrNotFound):
			id, err := record.Head(ctx, seq)
			if err != nil {
				return last, entries, fmt.Errorf("the record's segment %d: %w", seq, err)
			}
			if err := segment.Land(ctx, store, record, seq, id.WriteID, n.Files.Temp); err != nil {
				return last, entries, fmt.Errorf("seeding the replacement's segment %d: %w", seq, err)
			}
		default:
			return last, entries, fmt.Errorf("the replacement's segment %d: %w", seq, err)
		}
		entries += uint64(len(foot))
		last = seq
	}
	return last, entries, nil
}
