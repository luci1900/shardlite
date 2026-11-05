// Package node is the shardlite node: the pieces that make one process the
// owner of its shards. This file starts it: it takes the node's lease, adopts
// the journal the previous process left behind and makes the journal's writer.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// Config describes a node.
type Config struct {
	Name    cluster.Node
	Address string
	Backend blob.Backend
	// Data is the node's data directory: the shard files, the local database
	// and the temporary files. Without it the node has no local database, and
	// so no cache index.
	Data string

	Retry  time.Duration         // first wait before a segment upload is retried, see segment.Options
	Blob   blob.Options          // retries of the node's blob.Store
	Lease  coord.RegistryOptions // lease TTL
	Keeper coord.KeeperOptions   // renewal interval and safety margin (Info is the node's own)
	Logger *slog.Logger
	// Clock, if set, checks the node's clock against the store's. The backend
	// must feed it (blob.S3Options.ClockSink). Start refuses a clock more than a
	// lease TTL off, and the control API reports the offset.
	Clock *coord.ClockWatch
	// TempDir is where tails, deltas, snapshots and downloads are kept (in the
	// data directory). It is made when it is first needed. The
	// catalog empties it of what a crash left.
	TempDir string
	// Faults is nil in production (see Faults).
	Faults Faults
}

// Node is a started node.
type Node struct {
	Config
	Registry *coord.Registry
	Keeper   *coord.Keeper
	// Confirmer reads the lease for the gates (Flusher.confirm). It is served beside the keeper.
	Confirmer *coord.Confirmer
	Store     *blob.Store
	Files     *shardfile.Files
	Journal   *journal.Store
	// LocalDB is the node's local database: the cache index's rows. It is nil when the node has no data directory.
	LocalDB *LocalDB
	Writer  *segment.Writer
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
	// A clock too far off refuses the start before the lease is taken, by what the
	// requests so far showed, and again once the lease's own response has.
	checkClock := func() error {
		if cfg.Clock == nil {
			return nil
		}
		return cfg.Clock.CheckStart(reg.TTL())
	}
	if err := checkClock(); err != nil {
		return nil, err
	}
	lease, err := reg.Acquire(ctx, info)
	if err != nil {
		return nil, fmt.Errorf("acquire lease: %w", err)
	}
	// The lease is left to run out, as after any failure past this point: it
	// carries the previous life's range, which the next start adopts.
	if err := checkClock(); err != nil {
		return nil, err
	}
	store := blob.NewStore(cfg.Backend, cfg.Name, lease.Epoch, cfg.Blob)
	n := &Node{
		Config:   cfg,
		Registry: reg,
		Store:    store,
		Files: &shardfile.Files{Blob: store, Backend: cfg.Backend, Node: cfg.Name, Leases: reg,
			JournalOptions: journal.Options(cfg.Blob), Temp: tempfile.NewDir(cfg.TempDir)},
	}
	if cfg.Faults.Temp != nil {
		n.Files.Temp.SetHook(cfg.Faults.Temp)
	}
	// The node's local database: the cache index's rows, made fresh at every
	// start. A database that cannot be made costs the cache's persistence and
	// nothing else: blob storage still holds the data.
	n.LocalDB = openLocalDB(cfg)
	// The store of the node's own journal. One of it: the fence, the writer and
	// a restore's reads all go through it, so their write ids share a counter
	// and never repeat (see Files.Journal).
	n.Journal = n.Files.Journal(cfg.Name)

	// Record the range before anything is written to it. Until now the lease
	// carried the previous process's range.
	start := pos.Seg(1)
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
		// The lease records the range before the writer is built, as in the
		// branches below.
		info.JournalFrom, info.JournalNext = lease.JournalFrom, lease.JournalNext
		if lease, err = reg.Renew(ctx, lease, info); err != nil {
			return nil, fmt.Errorf("record journal range: %w", err)
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
	})
	kopts := cfg.Keeper
	kopts.Info = n.Info
	kopts.Logger = cfg.Logger
	n.Keeper = coord.NewKeeper(reg, lease, kopts)
	n.Confirmer = coord.NewConfirmer(reg, n.Keeper)
	return n, nil
}

// openLocalDB makes the node's local database. Without a data directory there
// is none, and the node has no cache index. A database that cannot be made is
// given up on: the cache index then lives in memory, and blob storage still
// holds the data.
func openLocalDB(cfg Config) *LocalDB {
	if cfg.Data == "" {
		return nil
	}
	db, err := makeLocalDB(filepath.Join(cfg.Data, LocalDBName))
	if err == nil {
		return db
	}
	cfg.Logger.Error("the local database could not be made, the cache index is not kept", "err", err)
	db, err = memoryLocalDB()
	if err != nil {
		cfg.Logger.Error("the cache index could not be opened either", "err", err)
		return nil
	}
	return db
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
	rep, err := n.Files.AdoptJournal(actx, n.Name, lease.JournalFrom, lease.JournalNext, n.Logger)
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
	return coord.NodeInfo{Name: n.Name, Address: n.Address, Shards: shards, JournalFrom: from, JournalNext: next}
}

// TailSet is a set of captured shards as Tails, for programs that capture
// shards without the Catalog (tests of the node's journal).
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
