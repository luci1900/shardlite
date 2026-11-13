// Package peers is what a node knows of the other nodes. Scan lists the
// leases in turn to learn which nodes are live and has dead ones adopted,
// Addresses finds a peer's address from its lease, and Placer chooses where a
// new shard goes and reads and writes the peer headers, avoiding the peers that
// asked to be avoided or did not answer.
package peers

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/model/cluster"
	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/storage/lease"
	"github.com/luci1900/shardlite/internal/syncs/notify"
)

// ScanOptions configure the lease scan.
type ScanOptions struct {
	// Every is how often the cluster as a whole lists state/leases/: each node lists
	// every N × Every on average, N being the nodes in its last listing. 2 s if zero.
	Every time.Duration
	// TTL is the lease TTL the cluster runs with, lease.DefaultTTL if zero. A
	// lease last written longer ago than that may have expired.
	TTL time.Duration
}

// Peer is what the lease scan last saw of one node.
type Peer struct {
	lease.Listed
	// Dead is true once the node's lease is released, or this node has seen it
	// unchanged for its TTL on its own clock (docs/internals.md, Noticing dead
	// nodes). That is a decision, not a hint: the holder's own deadline has passed.
	// It stays true through the writes that adopting the node makes to its lease,
	// and ends only when the lease shows a new epoch, which is the node's own
	// restart.
	Dead bool
}

// List is the lease scan's last listing, published whole: nothing changes it
// once it is out.
type List struct {
	At    time.Time // when it was listed, by the scan's clock
	Nodes []Peer
}

// Clock is the time the scan, the addresses and the placer run on: the node's
// clock, so that tests with a fake one drive them.
type Clock interface {
	Now() time.Time
	// NewTimer sends the time on the channel once, d from now. The function stops it.
	NewTimer(d time.Duration) (c <-chan time.Time, stop func() bool)
}

// Scan lists state/leases/, publishes the listing (what this node knows of the
// others), finds dead nodes and has each one adopted (docs/internals.md,
// Takeover). A lease whose last write (the store's clock) is older than the TTL by the scan's
// clock may have expired: the scan reads it, notes its ETag and looks again once
// the lease's TTL has passed on its own clock. If the ETag is unchanged the
// holder renewed nothing in that time, so its deadline has passed. The second
// look has a timer of its own, so it doesn't wait for the scan's next listing,
// which in a large cluster is far apart.
//
// A dead node is adopted at once, on the loop, one at a time, in name order. The
// scan decides whether a lease needs adopting and Adopt does it, answering only
// whether the node is adopted. Adoption is takeover, which belongs to the shard
// runtime, so the scan knows nothing of it beyond that answer. A node not adopted
// is tried again after the scan's next wait, with the lease as the watch has it:
// a lease that changed under the adoption (the node renewed or restarted, another
// node marked it, or this node marked it and a later step failed) is read again
// at the next listing, which shows its ETag changed, and an attempt before then
// fails its conditional write and changes nothing. A node that stopped (its lease released) has nothing to adopt: its
// shards are covered by their entries.
//
// The watch of a node found dead is kept: the writes to its lease that follow
// (the adopter's mark and finish) are not renewals, so the node stays dead in
// every listing until its lease shows a new epoch, when it restarted. The scan
// reads the lease again whenever the listing shows a dead node's ETag changed.
//
// Its loop owns the watches. What others read is the published List. The fields
// are set before Serve runs and not changed after.
type Scan struct {
	Registry *lease.Registry
	Self     func() lease.Incarnation // this node's own incarnation
	Clock    Clock
	Options  ScanOptions
	Log      *slog.Logger
	Scans    *notify.Counter // listings made, for tests
	// Adopt adopts the dead node whose lease is l, which needs it, and reports
	// whether it did. It runs on the scan's loop.
	Adopt func(ctx context.Context, l *lease.Record) (done bool)
	// Died, if set, is told each node a listing first shows dead.
	Died func(node cluster.Node)

	view    atomic.Pointer[List]
	watches map[cluster.Node]*leaseWatch // by node, the loop's own
}

// leaseWatch is a lease that may have expired, as of its last read.
type leaseWatch struct {
	lease *lease.Record
	due   time.Time // when to look again: a TTL after the read, by the scan's clock
	dead  bool
	done  bool // dead and needing nothing more from this node: adopted, or nothing to adopt
}

// newWatch watches l, read at read. read is taken once the read has returned,
// never before it: a renewal that lands while the read is in flight gives its
// holder a TTL from then, so the second look must come a full TTL after the
// read returned.
func newWatch(l *lease.Record, read time.Time) *leaseWatch {
	return &leaseWatch{lease: l, due: read.Add(l.TTL), dead: l.Released}
}

func (*Scan) String() string { return "lease scan" }

// Nodes returns the last listing, nil before the first.
func (s *Scan) Nodes() *List { return s.view.Load() }

// Serve lists at once, then on its schedule, and makes each watch's second look
// when it is due.
func (s *Scan) Serve(ctx context.Context) error {
	s.watches = map[cluster.Node]*leaseWatch{}
	next := s.Clock.Now()
	for {
		wake := next
		for _, w := range s.watches {
			if !w.dead && w.due.Before(wake) {
				wake = w.due
			}
		}
		t, stop := s.Clock.NewTimer(wake.Sub(s.Clock.Now()))
		select {
		case <-ctx.Done():
			stop()
			return nil
		case <-t:
		}
		now := s.Clock.Now()
		if !now.Before(next) {
			n := s.scan(ctx, now)
			// On average N × Every, so the cluster lists about every Every.
			next = now.Add(time.Duration((0.5 + rand.Float64()) * float64(max(n, 1)) * float64(s.Options.Every)))
		}
		s.confirm(ctx, now)
		s.adoptDead(ctx)
	}
}

// scan lists state/leases/, starts or ends watches and publishes the listing. It
// returns how many nodes it listed.
func (s *Scan) scan(ctx context.Context, now time.Time) int {
	listed, err := s.Registry.List(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.Log.Warn("could not list the node leases", "err", err)
		}
		return len(s.Nodes().nodes())
	}
	s.Scans.Inc()
	seen := make(map[cluster.Node]bool, len(listed))
	out := &List{At: now, Nodes: make([]Peer, 0, len(listed))}
	for _, e := range listed {
		seen[e.Name] = true
		w := s.watches[e.Name]
		if w != nil && w.dead && w.lease.ETag != e.ETag {
			w = s.refresh(ctx, e.Name, w)
		}
		switch {
		case w != nil && w.dead:
			// Stays dead, whatever was written to its lease since: only a new epoch ends it.
		case now.Before(e.LastModified.Add(s.Options.TTL)):
			delete(s.watches, e.Name) // written lately: renewed
			w = nil
		case w != nil && w.lease.ETag == e.ETag:
			// Unchanged since the watch's read.
		default:
			w = s.watch(ctx, e.Name)
		}
		out.Nodes = append(out.Nodes, Peer{Listed: e, Dead: w != nil && w.dead})
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
func (s *Scan) publish(out *List) {
	was := map[cluster.Node]bool{}
	for _, v := range s.Nodes().nodes() {
		was[v.Name] = v.Dead
	}
	s.view.Store(out)
	for _, v := range out.Nodes {
		if v.Dead && !was[v.Name] && s.Died != nil {
			s.Died(v.Name)
		}
	}
}

// watch reads a lease that may have expired and starts its watch. A released
// lease is dead at once. It returns the watch, nil if the lease couldn't be read.
func (s *Scan) watch(ctx context.Context, node cluster.Node) *leaseWatch {
	l, err := s.Registry.Get(ctx, node)
	if err != nil {
		if !errors.Is(err, blob.ErrNotFound) && ctx.Err() == nil {
			s.Log.Warn("could not read a node lease", "node", node, "err", err)
		}
		delete(s.watches, node)
		return nil
	}
	w := newWatch(l, s.Clock.Now())
	s.watches[node] = w
	return w
}

// refresh reads the lease of a node found dead after it was written to. The
// same epoch is the adoption's writes (or a holder that came back too late), so
// the node stays dead and the watch takes the new lease. A newer epoch is the
// node's own restart, which ends the watch (nil). A lease that can't be read now
// leaves the watch as it is, to be refreshed at the next listing, and one that is
// gone ends it.
func (s *Scan) refresh(ctx context.Context, node cluster.Node, w *leaseWatch) *leaseWatch {
	l, err := s.Registry.Get(ctx, node)
	switch {
	case errors.Is(err, blob.ErrNotFound):
		delete(s.watches, node)
		return nil
	case err != nil:
		if ctx.Err() == nil {
			s.Log.Warn("could not read a node lease", "node", node, "err", err)
		}
		return w
	case l.Epoch > w.lease.Epoch:
		delete(s.watches, node)
		return nil
	}
	w.lease = l
	return w
}

// confirm makes the second look of every watch that is due: a lease unchanged
// for its TTL is dead, and a changed one is watched again from now.
func (s *Scan) confirm(ctx context.Context, now time.Time) {
	for node, w := range s.watches {
		if w.dead || now.Before(w.due) {
			continue
		}
		l, err := s.Registry.Get(ctx, node)
		switch {
		case err != nil:
			if !errors.Is(err, blob.ErrNotFound) && ctx.Err() == nil {
				s.Log.Warn("could not read a node lease", "node", node, "err", err)
			}
			delete(s.watches, node)
		case l.ETag == w.lease.ETag:
			w.lease, w.dead = l, true
			s.Log.Debug("a node lease went unrenewed for its TTL", "node", node, "epoch", l.Epoch)
			s.publishDead(node)
		default:
			*w = *newWatch(l, s.Clock.Now())
		}
	}
}

// publishDead republishes the last listing with node dead.
func (s *Scan) publishDead(node cluster.Node) {
	cur := s.Nodes()
	if cur == nil {
		return
	}
	out := &List{At: cur.At, Nodes: append([]Peer(nil), cur.Nodes...)}
	for i := range out.Nodes {
		if out.Nodes[i].Name == node {
			out.Nodes[i].Dead = true
		}
	}
	s.publish(out)
}

// adoptDead adopts every dead node not adopted yet, one after the other, in
// name order.
func (s *Scan) adoptDead(ctx context.Context) {
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
		s.adoptOne(ctx, name, s.watches[name])
	}
}

// adoptOne has the dead node x adopted if its lease, as the watch last read it,
// needs that. A lease another node is already adopting is left to it while that
// node isn't known to be dead.
func (s *Scan) adoptOne(ctx context.Context, x cluster.Node, w *leaseWatch) {
	l, me := w.lease, s.Self()
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
	if s.Adopt(ctx, l) {
		w.done = true
	}
}

// incarnationDead reports whether an adopter is known to be dead: its node has a
// newer epoch or released its lease, or this scan has confirmed its lease
// unrenewed. A watch on it starts with the scan's next listing.
func (s *Scan) incarnationDead(ctx context.Context, inc lease.Incarnation) bool {
	if w := s.watches[inc.Node]; w != nil && w.dead && w.lease.Epoch == inc.Epoch {
		return true
	}
	l, err := s.Registry.Get(ctx, inc.Node)
	if err != nil {
		return errors.Is(err, blob.ErrNotFound)
	}
	return l.Epoch > inc.Epoch || (l.Epoch == inc.Epoch && l.Released)
}

func (l *List) nodes() []Peer {
	if l == nil {
		return nil
	}
	return l.Nodes
}

// Live is the names of the nodes the listing doesn't count as dead. It is safe to
// call on the nil list before the first listing, which has none.
func (l *List) Live() []cluster.Node {
	var out []cluster.Node
	for _, v := range l.nodes() {
		if !v.Dead {
			out = append(out, v.Name)
		}
	}
	return out
}
