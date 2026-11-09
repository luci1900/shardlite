// Package node is the shardlite node: the pieces that make one process the
// owner of its shards. This file starts it: it takes the node's lease, adopts
// the journal the previous process left behind and makes the journal's writer.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// Config describes a node.
type Config struct {
	Name    cluster.Node
	Address string
	Backend blob.Backend
	// Data is the node's data directory: the active shards' files and the
	// temporary files.
	Data string

	Retry  time.Duration         // first wait before a segment upload is retried, see journal.WriterOptions
	Blob   blob.Options          // retries of the node's blob.Store
	Lease  lease.RegistryOptions // lease TTL
	Keeper lease.KeeperOptions   // renewal interval and safety margin (Info is the node's own)
	Logger *slog.Logger
	// TempDir is where tails, deltas, snapshots and downloads are kept (in the
	// data directory). It is made when it is first needed. The
	// The shardset empties it of what a crash left.
	TempDir string
	// OrphanAge is the age rule (shardfile.Files.AgeRule), the one value the writer's
	// orphan entries and the collector share. shardfile.DefaultAgeRule if zero.
	OrphanAge time.Duration
	// Faults is nil in production (see Faults).
	Faults Faults
}

// Node is a started node.
type Node struct {
	Config
	Registry *lease.Registry
	Keeper   *lease.Keeper
	// Confirmer reads the lease for the gates (Flusher.confirm). It is served beside the keeper.
	Confirmer *lease.Confirmer
	Store     *blob.Store
	Files     *shardfile.Files
	Journal   *journal.Store
	Writer    *journal.Writer
	// Adopted is what adopting the previous process's journal did (zero if the node
	// had no previous lease).
	Adopted shardfile.AdoptReport

	tails atomic.Pointer[Tails] // what the lease counts, nil until SetTails
}

// Err returns nil while the node may write: its lease is good and its journal's
// writer is not fenced. Otherwise it returns the reason, which is fatal.
func (n *Node) Err() error {
	select {
	case <-n.Writer.Fenced():
		return fmt.Errorf("the node's journal writer is fenced: %w", journal.ErrFenced)
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
	reg := lease.NewRegistry(cfg.Backend, cfg.Name, cfg.Lease)
	info := lease.NodeInfo{Name: cfg.Name, Address: cfg.Address}
	rec, err := reg.Acquire(ctx, info)
	if err != nil {
		return nil, fmt.Errorf("acquire lease: %w", err)
	}
	// The lease is left to run out, as after any failure past this point: it
	// carries the previous life's range, which the next start adopts.
	store := blob.NewStore(cfg.Backend, cfg.Name, rec.Epoch, cfg.Blob)
	n := &Node{
		Config:   cfg,
		Registry: reg,
		Store:    store,
		Files: &shardfile.Files{Blob: store, Backend: cfg.Backend, Node: cfg.Name, Leases: reg,
			JournalOptions: journal.Options(cfg.Blob), Temp: tempfile.NewDir(cfg.TempDir), OrphanAge: cfg.OrphanAge},
	}
	if cfg.Faults.Temp != nil {
		n.Files.Temp.SetHook(cfg.Faults.Temp)
	}
	// The store of the node's own journal. One of it: the fence, the writer and
	// a restore's reads all go through it, so their write ids share a counter
	// and never repeat (see Files.Journal).
	n.Journal = n.Files.Journal(cfg.Name)

	// Record the range before anything is written to it. Until now the lease
	// carried the previous process's range.
	start := pos.Seg(1)
	switch prev := rec.Previous; {
	case prev != nil && prev.Adopted < prev.Epoch:
		var rep shardfile.AdoptReport
		rep, rec, start, err = n.adoptOwn(ctx, rec)
		n.Adopted = rep
		if err != nil {
			return nil, err
		}
		if rec, err = reg.FinishAdoption(ctx, rec, prev.Epoch, rep.Fence); err != nil {
			return nil, fmt.Errorf("record the adoption of the journal: %w", err)
		}
		// The lease records the range before the writer is built, as in the
		// branches below.
		info.JournalFrom, info.JournalNext = rec.JournalFrom, rec.JournalNext
		if rec, err = reg.Renew(ctx, rec, info); err != nil {
			return nil, fmt.Errorf("record journal range: %w", err)
		}
	case prev != nil:
		// The range starts where the previous life (or its adopter) left it. The
		// fence finds the first free number in case anything was written since.
		fence, err := journal.Fence(ctx, n.Journal, rec.JournalNext)
		if err != nil {
			return nil, fmt.Errorf("fence the journal: %w", err)
		}
		n.Adopted.Fence = fence
		start = fence + 1
		fallthrough
	default:
		info.JournalFrom, info.JournalNext = start, start
		if rec, err = reg.Renew(ctx, rec, info); err != nil {
			return nil, fmt.Errorf("record journal range: %w", err)
		}
	}

	n.Writer = journal.NewWriter(n.Journal, journal.WriterOptions{
		StartSeq: start,
		Retry:    cfg.Retry,
		Logger:   cfg.Logger,
	})
	kopts := cfg.Keeper
	kopts.Info = n.Info
	kopts.Logger = cfg.Logger
	n.Keeper = lease.NewKeeper(reg, rec, kopts)
	n.Confirmer = lease.NewConfirmer(reg, n.Keeper)
	return n, nil
}

// adoptOwn adopts the journal the node's previous life left (Start), as another node
// adopts a dead node's (docs/internals.md, Takeover: Restart). It writes a delta
// for every shard with a tail there, which can take longer than the lease's
// TTL, so a keeper of its own renews the lease meanwhile: other nodes would
// otherwise take the node for dead and adopt it themselves. The keeper
// publishes the lease as Acquire wrote it, with the previous life's journal
// range, so a crash before the adoption finishes leaves that range for the next
// life.
//
// Each delta is written after an entry due now for the shard is in the reminder
// index, as another node's adoption does. The node's own writer starts just after the
// fence, once the adoption is done. The adoption stops if the lease is lost. It
// returns the lease as the keeper last renewed it, for FinishAdoption, and where
// the node's writer starts.
func (n *Node) adoptOwn(ctx context.Context, rec lease.Record) (shardfile.AdoptReport, lease.Record, pos.Seg, error) {
	kopts := n.Config.Keeper
	kopts.Info = nil // the lease's own
	kopts.Logger = n.Logger
	k := lease.NewKeeper(n.Registry, rec, kopts)
	kctx, stop := context.WithCancel(ctx)
	g, actx := errgroup.WithContext(kctx)
	g.Go(func() error {
		k.Serve(actx)
		return k.Check() // nil unless the lease was lost or ran out
	})
	rep, err := n.Files.AdoptJournal(actx, n.Name, rec.JournalFrom, rec.JournalNext, n.Logger)
	stop()
	if kerr := g.Wait(); kerr != nil {
		return rep, rec, 0, fmt.Errorf("adopting the journal: %w", kerr)
	}
	return rep, k.Lease(), rep.Fence + 1, err
}

// Tails is where the lease's journal range comes from: whatever runs the
// node's shards (the shardset, or a TailSet for programs that capture shards
// themselves).
type Tails interface {
	// Tails returns the lowest segment number a captured shard's tail still
	// needs (0 if none does).
	Tails() pos.Seg
}

// SetTails makes t what the lease's journal range is read from.
func (n *Node) SetTails(t Tails) { n.tails.Store(&t) }

// Info is what the node publishes in its lease.
func (n *Node) Info() lease.NodeInfo {
	// The writer's next number comes first: a file queued after it is read has
	// a segment at or above it, so the look at the tails below can't miss one
	// that sits below it.
	next := n.Writer.NextSeq()
	from := next
	if t := n.tails.Load(); t != nil {
		if f := (*t).Tails(); f != 0 && f < from {
			from = f
		}
	}
	return lease.NodeInfo{Name: n.Name, Address: n.Address, JournalFrom: from, JournalNext: next}
}

// TailSet is a set of captured shards as Tails, for programs that capture
// shards without the shardset (tests of the node's journal).
type TailSet struct {
	mu sync.Mutex
	m  map[*capture.Shard]struct{}
}

// Track remembers a shard, before its capture's first sync.
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

func (s *TailSet) Tails() pos.Seg {
	var from pos.Seg
	s.mu.Lock()
	defer s.mu.Unlock()
	for sh := range s.m {
		if f := sh.TailFrom(); f != 0 && (from == 0 || f < from) {
			from = f
		}
	}
	return from
}
