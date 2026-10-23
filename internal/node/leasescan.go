package node

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// LeaseScanOptions configure the lease scan.
type LeaseScanOptions struct {
	// Every is how often the cluster as a whole lists state/leases/: each node lists
	// every N × Every on average, N being the nodes in its last listing. 2 s if zero.
	Every time.Duration
	// TTL is the lease TTL the cluster runs with, coord.DefaultTTL if zero. A
	// lease last written longer ago than that may have expired.
	TTL time.Duration
}

// NodeView is what the lease scan last saw of one node.
type NodeView struct {
	coord.Listed
	// Dead is true once the node's lease is released, or this node has seen it
	// unchanged for its TTL on its own clock (docs/internals.md, Noticing dead
	// nodes). That is a decision, not a hint: the holder's own deadline has passed.
	Dead bool
}

// NodeList is the lease scan's last listing, published whole: nothing changes it
// once it is out.
type NodeList struct {
	At    time.Time // when it was listed, by the scan's clock
	Nodes []NodeView
}

// leaseScan is the part of the Catalog that lists state/leases/, finds dead nodes and
// adopts them (docs/internals.md, Takeover). A
// lease whose last write (the store's clock) is older than the TTL by the scan's
// clock may have expired: the scan reads it, notes its ETag and looks again once
// the lease's TTL has passed on its own clock. If the ETag is unchanged the
// holder renewed nothing in that time, so its deadline has passed. The second
// look has a timer of its own, so it doesn't wait for the scan's next listing,
// which in a large cluster is far apart.
//
// A dead node is adopted at once, on the loop, one at a time: mark, the
// journal's adoption (fence, read, and for each tail an entry due now in the
// reminder index before its delta), finish and the sweep of its segments below the
// fence. A node that stopped (its lease released) has nothing to adopt: its shards
// are covered by their entries. Every step is safe to redo, so an adoption cut
// short (an error, the scan stopping, this node dying) is simply made again, by
// this node or another.
//
// Its loop owns the watches. What others read is the published NodeList.
type leaseScan struct {
	reg     *coord.Registry
	files   *shardfile.Files
	store   *blob.Store
	self    func() coord.Incarnation // this node's own incarnation
	nodeErr func() error             // nil while this node may write (Node.Err)
	clock   Clock
	opts    LeaseScanOptions
	log     *slog.Logger
	scans   *notify.Counter // listings made, for tests
	adopted *notify.Counter // adoptions finished, for tests
	mirror  *mirrorDialer   // dials the dead node's mirrors, for the fetch before the fence
	// died, if set, is told each node a listing first shows dead (Catalog.died).
	died func(node cluster.Node)

	view    atomic.Pointer[NodeList]
	watches map[cluster.Node]*leaseWatch // by node, the loop's own
}

// leaseWatch is a lease that may have expired, as of its last read.
type leaseWatch struct {
	lease *coord.NodeLease
	due   time.Time // when to look again: a TTL after the read, by the scan's clock
	dead  bool
	done  bool // dead and needing nothing more from this node: adopted, or nothing to adopt
}

// newWatch watches l, read at read. read is taken once the read has returned,
// never before it: a renewal that lands while the read is in flight gives its
// holder a TTL from then, so the second look must come a full TTL after the
// read returned.
func newWatch(l *coord.NodeLease, read time.Time) *leaseWatch {
	return &leaseWatch{lease: l, due: read.Add(l.TTL), dead: l.Released}
}

func (*leaseScan) String() string { return "lease scan" }

// Nodes returns the last listing, nil before the first.
func (s *leaseScan) Nodes() *NodeList { return s.view.Load() }

// Serve lists at once, then on its schedule, and makes each watch's second look
// when it is due.
func (s *leaseScan) Serve(ctx context.Context) error {
	s.watches = map[cluster.Node]*leaseWatch{}
	next := s.clock.Now()
	for {
		wake := next
		for _, w := range s.watches {
			if !w.dead && w.due.Before(wake) {
				wake = w.due
			}
		}
		t, stop := s.clock.NewTimer(wake.Sub(s.clock.Now()))
		select {
		case <-ctx.Done():
			stop()
			return nil
		case <-t:
		}
		now := s.clock.Now()
		if !now.Before(next) {
			n := s.scan(ctx, now)
			// On average N × Every, so the cluster lists about every Every.
			next = now.Add(time.Duration((0.5 + rand.Float64()) * float64(max(n, 1)) * float64(s.opts.Every)))
		}
		s.confirm(ctx, now)
		s.adoptDead(ctx)
	}
}

// scan lists state/leases/, starts or ends watches and publishes the listing. It
// returns how many nodes it listed.
func (s *leaseScan) scan(ctx context.Context, now time.Time) int {
	listed, err := s.reg.List(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn("could not list the node leases", "err", err)
		}
		return len(s.Nodes().nodes())
	}
	s.scans.Inc()
	seen := make(map[cluster.Node]bool, len(listed))
	out := &NodeList{At: now, Nodes: make([]NodeView, 0, len(listed))}
	for _, e := range listed {
		seen[e.Name] = true
		w := s.watches[e.Name]
		switch {
		case now.Before(e.LastModified.Add(s.opts.TTL)):
			delete(s.watches, e.Name) // written lately: renewed
			w = nil
		case w != nil && w.lease.ETag == e.ETag:
			// Unchanged since the watch's read.
		default:
			w = s.watch(ctx, e.Name)
		}
		out.Nodes = append(out.Nodes, NodeView{Listed: e, Dead: w != nil && w.dead})
	}
	for name := range s.watches {
		if !seen[name] {
			delete(s.watches, name)
		}
	}
	s.publish(out)
	return len(listed)
}

// publish makes out the last listing, and tells died about each node it shows
// dead that the listing before it didn't.
func (s *leaseScan) publish(out *NodeList) {
	was := map[cluster.Node]bool{}
	for _, v := range s.Nodes().nodes() {
		was[v.Name] = v.Dead
	}
	s.view.Store(out)
	for _, v := range out.Nodes {
		if v.Dead && !was[v.Name] && s.died != nil {
			s.died(v.Name)
		}
	}
}

// watch reads a lease that may have expired and starts its watch. A released
// lease is dead at once. It returns the watch, nil if the lease couldn't be read.
func (s *leaseScan) watch(ctx context.Context, node cluster.Node) *leaseWatch {
	l, err := s.reg.Get(ctx, node)
	if err != nil {
		if !errors.Is(err, blob.ErrNotFound) && ctx.Err() == nil {
			s.log.Warn("could not read a node lease", "node", node, "err", err)
		}
		delete(s.watches, node)
		return nil
	}
	w := newWatch(l, s.clock.Now())
	s.watches[node] = w
	return w
}

// confirm makes the second look of every watch that is due: a lease unchanged
// for its TTL is dead, and a changed one is watched again from now.
func (s *leaseScan) confirm(ctx context.Context, now time.Time) {
	for node, w := range s.watches {
		if w.dead || now.Before(w.due) {
			continue
		}
		l, err := s.reg.Get(ctx, node)
		switch {
		case err != nil:
			if !errors.Is(err, blob.ErrNotFound) && ctx.Err() == nil {
				s.log.Warn("could not read a node lease", "node", node, "err", err)
			}
			delete(s.watches, node)
		case l.ETag == w.lease.ETag:
			w.lease, w.dead = l, true
			s.log.Debug("a node lease went unrenewed for its TTL", "node", node, "epoch", l.Epoch)
			s.publishDead(node)
		default:
			*w = *newWatch(l, s.clock.Now())
		}
	}
}

// publishDead republishes the last listing with node dead.
func (s *leaseScan) publishDead(node cluster.Node) {
	cur := s.Nodes()
	if cur == nil {
		return
	}
	out := &NodeList{At: cur.At, Nodes: append([]NodeView(nil), cur.Nodes...)}
	for i := range out.Nodes {
		if out.Nodes[i].Name == node {
			out.Nodes[i].Dead = true
		}
	}
	s.publish(out)
}

// adoptDead adopts every dead node not adopted yet, one after the other, in
// name order.
func (s *leaseScan) adoptDead(ctx context.Context) {
	names := make([]cluster.Node, 0, len(s.watches))
	for name, w := range s.watches {
		if w.dead && !w.done {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for _, name := range names {
		if ctx.Err() != nil {
			return
		}
		s.adopt(ctx, name, s.watches[name])
	}
}

// adopt adopts the dead node x, whose lease is as the watch last read it. A
// lease another node is already adopting is left to it while that node isn't
// known to be dead. When anything changed under it, the watch goes: the next
// listing looks at the node afresh.
func (s *leaseScan) adopt(ctx context.Context, x cluster.Node, w *leaseWatch) {
	l, me := w.lease, s.self()
	if x == me.Node {
		w.done = true
		return
	}
	if l.Released || l.Adopted >= l.Epoch { // a release also marks the lease adopted: there is no tail
		w.done = true
		return
	}
	if by := l.AdoptedBy; by != nil && by.Node != x && *by != me && !s.incarnationDead(ctx, *by) {
		return // a live adopter has it: look again later
	}
	if s.nodeErr() != nil {
		return // this node may not write
	}
	marked, err := s.reg.Adopt(ctx, *l, me)
	switch {
	case errors.Is(err, coord.ErrNotAdoptable):
		delete(s.watches, x) // it renewed, restarted or someone else marked it
		return
	case err != nil:
		if ctx.Err() == nil {
			s.log.Warn("could not mark a dead node's lease, trying again later", "node", x, "err", err)
		}
		return
	}
	s.log.Info("adopting a dead node", "node", x, "epoch", l.Epoch, "readopted", l.AdoptedBy != nil)
	// The fetch (docs/internals.md, Takeover): the mirrors are asked before
	// the fence is written, inside AdoptJournal, since a fence over a number a
	// mirror holds would strand what the cluster still has.
	rep, err := s.files.AdoptJournal(ctx, x, l.JournalFrom, l.JournalNext, s.mirror.stores(ctx, *l, s.log), s.log)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Error("adopting a dead node failed, it will be adopted again", "node", x, "epoch", l.Epoch, "err", err)
		}
		delete(s.watches, x)
		return
	}
	if s.nodeErr() != nil {
		return
	}
	if _, err := s.reg.FinishAdoption(ctx, marked, l.Epoch, rep.Fence); err != nil {
		if ctx.Err() == nil {
			s.log.Warn("could not finish adopting a dead node, it will be adopted again", "node", x, "epoch", l.Epoch, "err", err)
		}
		delete(s.watches, x)
		return
	}
	// Segments from the fence on may be the node's next life's by now.
	sw := &expiry{store: s.store, node: x, below: rep.Fence, log: s.log}
	if _, err := sw.expire(ctx, s.clock.Now()); err != nil && ctx.Err() == nil {
		s.log.Warn("could not sweep a dead node's journal", "node", x, "err", err)
	}
	s.log.Info("adopted a dead node", "node", x, "epoch", l.Epoch, "deltas", rep.Deltas, "lost", rep.Lost, "fence", rep.Fence)
	w.done = true
	s.adopted.Inc()
}

// incarnationDead reports whether an adopter is known to be dead: its node has a
// newer epoch or released its lease, or this scan has confirmed its lease
// unrenewed. A watch on it starts with the scan's next listing.
func (s *leaseScan) incarnationDead(ctx context.Context, inc coord.Incarnation) bool {
	if w := s.watches[inc.Node]; w != nil && w.dead && w.lease.Epoch == inc.Epoch {
		return true
	}
	l, err := s.reg.Get(ctx, inc.Node)
	if err != nil {
		return errors.Is(err, blob.ErrNotFound)
	}
	return l.Epoch > inc.Epoch || (l.Epoch == inc.Epoch && l.Released)
}

func (l *NodeList) nodes() []NodeView {
	if l == nil {
		return nil
	}
	return l.Nodes
}
