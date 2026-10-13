// Package node is the shardlite node: the pieces that make one process the
// owner of its shards. This file starts it: it takes the node's lease, adopts
// the journal the previous process left behind and starts the journal's writer.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// Config describes a node.
type Config struct {
	Name    string
	Address string
	Backend blob.Backend

	Flush time.Duration // segment flush interval, segment.DefaultFlush if zero
	// ManualFlush leaves flushing the segment writer to the caller (the shard
	// manager's flush loop flushes it right after capturing, so a segment holds
	// what the capture just queued) instead of the writer's own timer.
	ManualFlush bool
	Retry       time.Duration         // first wait before a segment upload is retried, see segment.Options
	Blob        blob.Options          // retries of the node's blob.Store
	Lease       coord.RegistryOptions // lease TTL
	Keeper      coord.KeeperOptions   // renewal interval and safety margin (Info is the node's own)
	Logger      *slog.Logger
	// TempDir is where deltas, snapshots and downloads too large for memory are
	// built (in the data directory, never the scratch directory, which is tmpfs).
	// The caller clears what a crash left in it before Start.
	TempDir string
	// SpoolLimit is how large a commit file, segment or delta gets before it is
	// held in a temporary file in TempDir instead of memory (spool.DefaultLimit if
	// zero).
	SpoolLimit int
}

// Node is a started node.
type Node struct {
	Config
	Registry *coord.Registry
	Keeper   *coord.Keeper
	Store    *blob.Store
	Files    *shardfile.Files
	Writer   *segment.Writer
	// Adopted is what adopting the previous process's journal did (zero if the node
	// had no previous lease).
	Adopted shardfile.AdoptReport

	mu      sync.Mutex
	tracked map[*capture.Shard]struct{}

	sweepCancel context.CancelFunc // set once the journal sweeper runs (guarded by mu)
	sweepGroup  errgroup.Group     // owns the journal sweeper; Close joins it
}

// Start takes the node's lease with a new epoch and recovers from whatever the
// previous process left (docs/internals.md, Restarting): it fences its own journal from
// the range recorded in the lease, writes a delta for every shard with a tail
// there and records the new range. A restart is treated as a crash, so this is
// the one recovery path.
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
		Files:    &shardfile.Files{Blob: store, Node: cfg.Name, Leases: reg, TempDir: cfg.TempDir, SpoolLimit: cfg.SpoolLimit},
		tracked:  map[*capture.Shard]struct{}{},
	}

	start := uint64(1)
	if lease.Previous != nil {
		rep, err := n.Files.AdoptJournal(ctx, cfg.Name, lease.JournalFrom, lease.JournalNext, cfg.Logger)
		n.Adopted = rep
		if err != nil {
			return nil, err
		}
		start = rep.Fence + 1
	}

	// Record the range before anything is written to it. Until now the lease
	// carried the previous process's range.
	info.JournalFrom, info.JournalNext = start, start
	if lease, err = reg.Renew(ctx, lease, info); err != nil {
		return nil, fmt.Errorf("record journal range: %w", err)
	}

	interval := cfg.Flush
	if cfg.ManualFlush {
		interval = 24 * time.Hour
	}
	n.Writer = segment.NewWriter(store, cfg.Name, segment.Options{
		Interval: interval,
		StartSeq: start,
		Retry:    cfg.Retry,
		Check:    func() error { return n.Keeper.Check() },
		Logger:   cfg.Logger,
		TempDir:  cfg.TempDir, SpoolLimit: cfg.SpoolLimit,
	})
	kopts := cfg.Keeper
	kopts.Info = n.Info
	kopts.Logger = cfg.Logger
	n.Keeper = coord.StartKeeper(reg, lease, kopts)
	return n, nil
}

// Track registers an active shard, so the lease counts it and records its
// tail as part of the journal range still needed.
func (n *Node) Track(sh *capture.Shard) {
	n.mu.Lock()
	n.tracked[sh] = struct{}{}
	n.mu.Unlock()
}

// Untrack forgets a shard that is no longer active.
func (n *Node) Untrack(sh *capture.Shard) {
	n.mu.Lock()
	delete(n.tracked, sh)
	n.mu.Unlock()
}

// Info is what the node publishes in its lease.
func (n *Node) Info() coord.NodeInfo {
	// The writer's next number comes first: a file queued after it is read has
	// a segment at or above it, so the scan below can't miss one that sits
	// below it.
	next := n.Writer.NextSeq()
	from := next
	n.mu.Lock()
	defer n.mu.Unlock()
	for sh := range n.tracked {
		if f := sh.TailFrom(); f != 0 && f < from {
			from = f
		}
	}
	return coord.NodeInfo{Name: n.Name, Address: n.Address, Shards: len(n.tracked), JournalFrom: from, JournalNext: next}
}

// stopSweeper stops the journal sweeper, if one runs, and waits for it.
func (n *Node) stopSweeper() {
	n.mu.Lock()
	cancel := n.sweepCancel
	n.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	n.sweepGroup.Wait()
}

// Close stops the node's writer and its lease. With release, the lease is
// marked released (a graceful shutdown, after the node's shards were let go).
func (n *Node) Close(ctx context.Context, release bool) error {
	n.stopSweeper()
	n.Writer.Close()
	return n.Keeper.Close(ctx, release)
}
