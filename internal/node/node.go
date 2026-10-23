// Package node is the shardlite node: the pieces that make one process the
// owner of its shards. This file starts it: it takes the node's lease, adopts
// the journal the previous process left behind and makes the journal's writer.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/migrate"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// Config describes a node.
type Config struct {
	Name    cluster.Node
	Address string
	Backend blob.Backend
	// Data is the node's data directory: the shard files, the local database
	// and the temporary files. Without it the node has no local database, and
	// so no local tier and no cache index.
	Data string

	Retry  time.Duration         // first wait before a segment upload is retried, see segment.Options
	Blob   blob.Options          // retries of the node's blob.Store
	Lease  coord.RegistryOptions // lease TTL
	Keeper coord.KeeperOptions   // renewal interval and safety margin (Info is the node's own)
	Logger *slog.Logger
	// TempDir is where deltas, snapshots and downloads too large for memory are
	// built (in the data directory). It is made when it is first needed. The
	// catalog empties it of what a crash left.
	TempDir string
	// SpoolLimit is how large a commit file, segment or delta gets before it is
	// held in a temporary file in TempDir instead of memory (spool.DefaultLimit if
	// zero).
	SpoolLimit int
	// Standbys is how many mirrors the node names for its journal, 0 for none.
	// The mirrors are chosen once, at this start, from the lease listing, and
	// the set is sticky: only a repair names different ones.
	Standbys int
}

// Node is a started node.
type Node struct {
	Config
	Registry *coord.Registry
	Keeper   *coord.Keeper
	Store    *blob.Store
	Files    *shardfile.Files
	Journal  *journal.Store
	// Local is the node's own disk as a home for its journal: a copy of every
	// segment, which the local tier acknowledges. It is nil when the node has
	// no local database.
	Local *journal.Store
	// LocalDB is the node's local database: the journal's segments and the
	// cache index's rows. It is nil when the node has no data directory.
	LocalDB *localdb.DB
	Writer  *segment.Writer
	// mirrors is the set chosen for this life, published in every renewal. It
	// is chosen once, at Start, and changed by repair, not by a re-choice at
	// renewal. A start that could not choose names none, and the record keeps
	// the acknowledgements (the tiers fall back upward).
	mirrors []cluster.Node
	// Adopted is what adopting the previous process's journal did (zero if the node
	// had no previous lease).
	Adopted shardfile.AdoptReport

	tails atomic.Pointer[Tails] // what the lease counts, nil until SetTails
}

// Close closes the node's local database. Run does it once the journal's
// writer has ended, since the writer writes to it.
func (n *Node) Close() error {
	if n.LocalDB == nil {
		return nil
	}
	return n.LocalDB.Close()
}

// Err returns nil while the node may write: its lease is good and its journal's
// writer is not fenced. Otherwise it returns the reason, which is fatal.
func (n *Node) Err() error {
	select {
	case <-n.Writer.Fenced():
		return fmt.Errorf("the node's journal writer is fenced: %w", segment.ErrFenced)
	default:
	}
	if err := n.Keeper.Check(); err != nil {
		return fmt.Errorf("the node's lease is gone: %w", err)
	}
	return nil
}

// Start takes the node's lease with a new epoch and recovers from whatever the
// previous process left (docs/internals.md, Takeover: Restart): it fences its own journal from
// the range recorded in the lease, writes a delta for every shard with a tail
// there and records the adoption with the new range. A restart is treated as a
// crash, so this is the one recovery path. If the previous life released its
// lease, or another node finished adopting it, there is nothing to adopt: the
// node only fences, to find where its journal starts.
//
// If it fails after taking the lease, the lease keeps the previous range, so
// starting again repeats the adoption under a new epoch.
func Start(ctx context.Context, cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	reg := coord.NewRegistry(cfg.Backend, cfg.Name, cfg.Lease)
	info := coord.NodeInfo{Name: cfg.Name, Address: cfg.Address}
	lease, err := reg.Acquire(ctx, info)
	if err != nil {
		return nil, fmt.Errorf("acquire lease: %w", err)
	}
	store := blob.NewStore(cfg.Backend, cfg.Name, lease.Epoch, cfg.Blob)
	n := &Node{
		Config:   cfg,
		Registry: reg,
		Store:    store,
		Files: &shardfile.Files{Blob: store, Backend: cfg.Backend, Node: cfg.Name, Leases: reg,
			JournalOptions: journal.Options(cfg.Blob), TempDir: cfg.TempDir, SpoolLimit: cfg.SpoolLimit},
	}
	// The node's local database: the journal's segments and the cache index's
	// rows. A database that cannot be opened costs the local tier and the
	// cache and nothing else: the record still holds the data.
	n.LocalDB = openLocalDB(cfg)
	if n.LocalDB != nil && n.LocalDB.Persistent() {
		n.Local = journal.NewStore(journal.NewLocal(n.LocalDB.DB, cfg.Name), cfg.Name, lease.Epoch, journal.Options(cfg.Blob))
	}
	// The mirrors of this life, before the writer is built, and what the lease
	// says of this node beside them: a node whose local database is not
	// persistent holds no mirrors for others, so none picks it. The choice is
	// the lease's from its first renewal, and it is sticky: repair changes it,
	// renewals republish it. A start that cannot read the listing names none
	// and logs why.
	n.mirrors = chooseMirrors(ctx, reg, cfg.Name, cfg.Standbys, cfg.Logger)
	info.Mirrors = n.mirrors
	info.Standby = n.LocalDB != nil && n.LocalDB.Persistent()
	// The store of the node's own journal. One of it: the fence, the writer and
	// a restore's reads all go through it, so their write ids share a counter
	// and never repeat (see Files.Journal).
	n.Journal = n.Files.Journal(cfg.Name)

	// Record the range before anything is written to it. Until now the lease
	// carried the previous process's range.
	start := pos.Seg(1)
	if lease.Previous != nil {
		// The restart's own tail lands in the record before the fence, which
		// the adoption reads it up to (docs/internals.md, Takeover: Restart).
		// A landing that fails fails the start, whose next life retries it.
		if err := n.landTail(ctx, lease); err != nil {
			return nil, err
		}
	}
	switch prev := lease.Previous; {
	case prev != nil && prev.Adopted < prev.Epoch:
		var rep shardfile.AdoptReport
		rep, lease, start, err = n.adoptOwn(ctx, lease)
		n.Adopted = rep
		if err != nil {
			return nil, err
		}
		if lease, err = reg.FinishAdoption(ctx, lease, prev.Epoch, rep.Fence); err != nil {
			return nil, fmt.Errorf("record the adoption of the journal: %w", err)
		}
	case prev != nil:
		// The range starts where the previous life (or its adopter) left it. The
		// fence finds the first free number in case anything was written since.
		fence, err := segment.Fence(ctx, n.Journal, lease.JournalNext)
		if err != nil {
			return nil, fmt.Errorf("fence the journal: %w", err)
		}
		n.Adopted.Fence = fence
		start = fence + 1
		fallthrough
	default:
		info.JournalFrom, info.JournalNext = start, start
		if lease, err = reg.Renew(ctx, lease, info); err != nil {
			return nil, fmt.Errorf("record journal range: %w", err)
		}
	}

	n.Writer = segment.NewWriter(n.Journal, segment.Options{
		StartSeq: start,
		Retry:    cfg.Retry,
		Logger:   cfg.Logger,
		Local:    n.Local,
	})
	kopts := cfg.Keeper
	kopts.Info = n.Info
	kopts.Logger = cfg.Logger
	n.Keeper = coord.NewKeeper(reg, lease, kopts)
	return n, nil
}

// chooseMirrors names the node's mirrors from the lease listing: live nodes
// that can hold a journal (a persistent local database), least loaded first,
// this node excluded. Chosen once, at the start of a life: the set changes by
// repair, not by a re-choice at renewal, since nothing in correctness depends
// on who the mirrors are and churn would cost the streams nothing but trouble
// (docs/internals.md, Node leases). A listing that cannot be read, or a
// candidate that cannot, leaves the set short: the acknowledgements fall back
// upward to the record until a repair names a replacement.
func chooseMirrors(ctx context.Context, reg *coord.Registry, self cluster.Node, count int, log *slog.Logger) []cluster.Node {
	if count == 0 {
		return nil
	}
	listed, err := reg.List(ctx)
	if err != nil {
		log.Warn("could not list the leases to choose the mirrors, this life names none", "err", err)
		return nil
	}
	type candidate struct {
		node   cluster.Node
		shards int
	}
	var candidates []candidate
	for _, e := range listed {
		if e.Name == self {
			continue
		}
		l, err := reg.Get(ctx, e.Name)
		if err != nil {
			continue // not nameable now; a repair names replacements later
		}
		if !l.Standby || l.Released || l.ExpiresAt().Before(time.Now()) {
			continue
		}
		candidates = append(candidates, candidate{l.Name, l.Shards})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].shards != candidates[j].shards {
			return candidates[i].shards < candidates[j].shards
		}
		return candidates[i].node < candidates[j].node
	})
	out := make([]cluster.Node, 0, min(count, len(candidates)))
	for _, c := range candidates[:min(count, len(candidates))] {
		out = append(out, c.node)
	}
	if len(out) < count {
		log.Warn("the cluster has fewer nodes that can hold mirrors than the standbys setting names", "standbys", count, "chosen", len(out))
	}
	if len(out) > 0 {
		log.Info("chose the mirrors of this node's journal", "mirrors", out)
	}
	return out
}

// openLocalDB opens the node's local database. Without a data directory there
// is none, and the node has no local tier and no cache index. A database that
// cannot be opened is given up on: the cache index then lives in memory and
// the local tier is empty, and the record still holds the data.
func openLocalDB(cfg Config) *localdb.DB {
	if cfg.Data == "" {
		return nil
	}
	db, err := localdb.Open(filepath.Join(cfg.Data, localdb.Name))
	if err == nil {
		return db
	}
	if errors.Is(err, migrate.ErrNewer) {
		// The file was written by a node whose schema is newer. This node
		// cannot read its chunks, so the local store is treated as empty and
		// the file is left alone: a newer node's tail may be in it. The local
		// tier's answers fall back to the record.
		cfg.Logger.Error("the local database is of a newer schema, the local tier is empty and the cache index is not kept", "err", err)
	} else {
		cfg.Logger.Error("the local database could not be opened, the local tier is empty and the cache index is not kept", "err", err)
	}
	db, err = localdb.Memory()
	if err != nil {
		cfg.Logger.Error("the cache index could not be opened either", "err", err)
		return nil
	}
	return db
}

// landTail lands the previous life's un-landed segments in the record (Start),
// checked as the bytes leave (internal/segment, Land), before the fence the
// start writes next: the adoption reads up to the fence, and a fence over an
// un-landed tail would lose what the node's local tier sits on. Each row
// decides by the record's head against its own, and a landing's error fails
// the start, whose next life retries it with the row as it is.
func (n *Node) landTail(ctx context.Context, lease coord.NodeLease) error {
	if n.Local == nil {
		return nil
	}
	prev := lease.Previous
	from := prev.JournalFrom
	if from < 1 {
		from = 1
	}
	landed := 0
	for seq, err := range n.Local.List(ctx, 1) {
		if err != nil {
			return fmt.Errorf("list the previous life's segments: %w", err)
		}
		lhead, err := n.Local.Head(ctx, seq)
		if err != nil {
			return fmt.Errorf("head the local store's segment %d: %w", seq, err)
		}
		rhead, err := n.Journal.Head(ctx, seq)
		switch {
		case err != nil && !errors.Is(err, blob.ErrNotFound):
			return fmt.Errorf("head the record's segment %d: %w", seq, err)
		case rhead.Size == lhead.Size:
			// The record has the segment (the write landed and the response was
			// lost, or this landing's earlier attempt did): the copy is done.
		case errors.Is(err, blob.ErrNotFound) && seq >= from:
			// The record never got it: land it, checked as the bytes leave.
			// Numbers above it standing in the record is a hole in the range,
			// not a tail: a hole fails the start, and its copy fixes nothing
			// into the record on its way out.
			hole := false
			for _, lerr := range n.Journal.List(ctx, seq+1) {
				if lerr != nil {
					return fmt.Errorf("list the record's journal above the local store's segment %d: %w", seq, lerr)
				}
				hole = true
				break
			}
			if hole {
				n.Logger.Warn("a segment missing from the middle of the record's range keeps its local copy out of the landing", "segment", seq)
				continue
			}
			if err := segment.Land(ctx, n.Journal, n.Local, seq, lhead.WriteID, n.TempDir, n.SpoolLimit); err != nil {
				return fmt.Errorf("land the previous life's segment %d: %w", seq, err)
			}
			landed++
		case errors.Is(err, blob.ErrNotFound):
			// Below the lease's range and gone from the record: leftovers
			// whose copy's purpose is over.
		default:
			// Somebody else's segment holds the number: an adoption's cutoff
			// passed it, and its commits are in no one's copy. One line at
			// error level, and the row goes so the store carries no ghost.
			n.Logger.Error("the record's segment at a number the local store holds is not the local copy's, an adoption's cutoff passed the segment and its commits are in no one's store",
				"node", n.Name, "segment", seq)
		}
		if err := n.Local.Delete(ctx, seq); err != nil && ctx.Err() == nil {
			n.Logger.Warn("could not drop a local copy of a segment in the restart's landing", "segment", seq, "err", err)
		}
	}
	if landed > 0 {
		n.Logger.Info("the previous life's un-landed segments landed in the record", "node", n.Name, "segments", landed)
	}
	return nil
}

// adoptOwn adopts the journal the node's previous life left (Start), as a peer
// adopts a dead node's (docs/internals.md, Takeover: Restart). It writes a delta
// for every shard with a tail there, which can take longer than the lease's
// TTL, so a keeper of its own renews the lease meanwhile: other nodes would
// otherwise take the node for dead and adopt it themselves. The keeper
// publishes the lease as Acquire wrote it, with the previous life's journal
// range, so a crash before the adoption finishes leaves that range for the next
// life.
//
// Each delta is written after an entry due now for the shard is in the reminder
// index, as a peer's adoption does. The node's own writer starts just after the
// fence, once the adoption is done. The adoption stops if the lease is lost. It
// returns the lease as the keeper last renewed it, for FinishAdoption, and where
// the node's writer starts.
func (n *Node) adoptOwn(ctx context.Context, lease coord.NodeLease) (shardfile.AdoptReport, coord.NodeLease, pos.Seg, error) {
	kopts := n.Config.Keeper
	kopts.Info = nil // the lease's own
	kopts.Logger = n.Logger
	k := coord.NewKeeper(n.Registry, lease, kopts)
	kctx, stop := context.WithCancel(ctx)
	g, actx := errgroup.WithContext(kctx)
	g.Go(func() error {
		k.Serve(actx)
		return k.Check() // nil unless the lease was lost or ran out
	})
	// The previous life's mirrors are asked before the fence, inside
	// AdoptJournal: they may hold segments the process died before landing,
	// beside the local rows the landing above put in the record
	// (docs/internals.md, Takeover).
	var mirrors []*journal.Store
	if prev := lease.Previous; prev != nil && len(prev.Mirrors) > 0 {
		mirrors = newMirrorDialer(n.Registry, journal.Options(n.Blob)).stores(actx, *prev, n.Logger)
	}
	rep, err := n.Files.AdoptJournal(actx, n.Name, lease.JournalFrom, lease.JournalNext, mirrors, n.Logger)
	stop()
	if kerr := g.Wait(); kerr != nil {
		return rep, lease, 0, fmt.Errorf("adopting the journal: %w", kerr)
	}
	return rep, k.Lease(), rep.Fence + 1, err
}

// Tails is where the lease's shard count and journal range come from: whatever
// runs the node's shards (the Catalog, or a TailSet for programs that capture shards
// themselves).
type Tails interface {
	// Tails returns how many shards are captured and the lowest segment number
	// their tails still need (0 if none does). A shard is counted from before its
	// capture's first sync until it is let go of.
	Tails() (shards int, from pos.Seg)
}

// SetTails makes t what the lease counts.
func (n *Node) SetTails(t Tails) { n.tails.Store(&t) }

// Info is what the node publishes in its lease.
func (n *Node) Info() coord.NodeInfo {
	// The writer's next number comes first: a file queued after it is read has
	// a segment at or above it, so the look at the tails below can't miss one
	// that sits below it.
	next := n.Writer.NextSeq()
	from := next
	shards := 0
	if t := n.tails.Load(); t != nil {
		var f pos.Seg
		shards, f = (*t).Tails()
		if f != 0 && f < from {
			from = f
		}
	}
	return coord.NodeInfo{Name: n.Name, Address: n.Address, Shards: shards, JournalFrom: from, JournalNext: next,
		Mirrors: n.mirrors, Standby: n.LocalDB != nil && n.LocalDB.Persistent()}
}

// TailSet is a set of captured shards as Tails, for programs that capture
// shards without the Catalog (the test node, and tests of the node's journal).
type TailSet struct {
	mu sync.Mutex
	m  map[*capture.Shard]struct{}
}

// Track counts a shard, before its capture's first sync.
func (s *TailSet) Track(sh *capture.Shard) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[*capture.Shard]struct{}{}
	}
	s.m[sh] = struct{}{}
}

// Untrack forgets a shard that is no longer active.
func (s *TailSet) Untrack(sh *capture.Shard) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, sh)
}

func (s *TailSet) Tails() (shards int, from pos.Seg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for sh := range s.m {
		if f := sh.TailFrom(); f != 0 && (from == 0 || f < from) {
			from = f
		}
	}
	return len(s.m), from
}
