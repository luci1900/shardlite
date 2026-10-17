package shardfile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/segment"
)

// AdoptReport says what AdoptJournal did.
type AdoptReport struct {
	Fence   uint64 // the number of the fence the adoption wrote: the journal's writer continues after it
	From    uint64 // the first segment read (later than asked if the segments before it had expired)
	Shards  int    // shards with entries in the range
	Deltas  int    // deltas written
	Expired bool   // the range began with segments that no longer exist
	// Lost lists shards whose tail has a hole, which is only tolerated when the
	// journal's first segments had expired (Expired). Their history up to the hole is
	// kept and what follows is gone. It is logged as an error and does not stop
	// the start. Any other hole is a bug and fails the adoption.
	Lost     []string
	Problems []error
}

// adoptWorkers is how many shards are turned into deltas at once. Each is blob
// storage requests, so the limit is about requests in flight: a restart or an
// adoption holds up its node's start, and a busy node can leave thousands of
// tails.
const adoptWorkers = 64

// AdoptJournal is what a node does with a journal that may hold commits no file has:
// its own, left by the process it replaces (docs/internals.md, Restarting), or a dead
// node's.
//
//  1. It fences the journal: an empty segment at the first free number from next,
//     the next segment number the lease recorded, so the old writer can't add
//     anything below it.
//  2. It reads the journal's footers from from, the oldest segment the lease says
//     is still needed.
//  3. For every shard with entries the manifest's files don't cover, it writes
//     one delta and adds it to the manifest, clearing the manifest's journal.
//     A wake hint for the current minute is written (by wake, which writes the
//     hint and counts an existing one as success) before the delta: the shard
//     may hold messages that were never delivered, and the scan for hints is what
//     wakes it. The delta sets the manifest's wake time to now to match.
//
// A tail with a hole in it is reported in Lost, not as a failure, only if the
// journal's first segments had expired: refusing to start would not bring those
// commits back. A hole anywhere else means the range in the lease was wrong or
// the journal is damaged, and fails the adoption so the bug is seen.
//
// Every shard is tried, so one bad shard doesn't leave the others' tails in
// the journal. If any failed the error says so (and Problems lists them): the
// journal's range must not be moved on, since the tails are still there.
func (f *Files) AdoptJournal(ctx context.Context, node string, from, next uint64, wake func(ctx context.Context, shard string) error, logger *slog.Logger) (AdoptReport, error) {
	if logger == nil {
		logger = slog.Default()
	}
	from, next = max(from, 1), max(next, 1)
	rep := AdoptReport{}
	fence, err := segment.Fence(ctx, f.Blob, node, next)
	if err != nil {
		return rep, fmt.Errorf("adopt %s: fence: %w", node, err)
	}
	rep.Fence = fence

	// Segments expire. A lease that has been idle for longer than they live can
	// name ones that are gone, and the journal then starts where the listing says.
	if from < fence {
		if _, err := segment.ReadFooter(ctx, f.Blob, node, from); errors.Is(err, blob.ErrNotFound) {
			low, _, err := segment.Bounds(ctx, f.Blob, node)
			if err != nil {
				return rep, fmt.Errorf("adopt %s: %w", node, err)
			}
			logger.Warn("the journal's first segments have expired", "node", node, "lease_from", from, "first_present", low)
			rep.Expired = true
			from = max(low, from)
		}
	}
	rep.From = from

	// Every number from the start of the range to the fence must be there. A
	// missing one in the middle would hide the segments after it from a plain
	// scan, and with them commits an acknowledgement was given for.
	idx, err := f.LoadJournal(ctx, node, from, fence)
	if err != nil {
		return rep, fmt.Errorf("adopt %s: reading the journal: %w", node, err)
	}
	rep.Shards = len(idx.byShard)

	// Each worker fills its own slot, and they are folded after Wait.
	names := make([]string, 0, len(idx.byShard))
	for name := range idx.byShard {
		names = append(names, name)
	}
	sort.Strings(names)
	type result struct {
		wrote bool
		err   error
	}
	results := make([]result, len(names))
	var g errgroup.Group
	g.SetLimit(adoptWorkers)
	for i, name := range names {
		g.Go(func() error {
			results[i].wrote, results[i].err = f.adoptShard(ctx, node, name, idx, wake)
			return nil // each shard's problem is collected below, the others carry on
		})
	}
	g.Wait()
	for i, name := range names {
		r := results[i]
		if _, gap := errors.AsType[*gapError](r.err); gap && rep.Expired {
			logger.Error("a shard's tail has a hole, commits after it are lost", "node", node, "shard", name, "err", r.err)
			rep.Lost = append(rep.Lost, name)
		} else if r.err != nil {
			err := r.err
			if gap {
				err = fmt.Errorf("%w (the journal's range was not expired, so this is a bug)", err)
			}
			rep.Problems = append(rep.Problems, fmt.Errorf("%s: %w", name, err))
		} else if r.wrote {
			rep.Deltas++
		}
	}
	sort.Slice(rep.Problems, func(i, j int) bool { return rep.Problems[i].Error() < rep.Problems[j].Error() })
	if len(rep.Problems) > 0 {
		return rep, fmt.Errorf("adopt %s: %d of %d shards failed: %w", node, len(rep.Problems), rep.Shards, errors.Join(rep.Problems...))
	}
	return rep, nil
}

type gapError struct{ from, to uint64 }

func (g *gapError) Error() string {
	return fmt.Sprintf("the journal has nothing for positions %d to %d", g.from, g.to)
}

// adoptShard writes the delta for one shard's uncovered entries, if it has any.
func (f *Files) adoptShard(ctx context.Context, node, name string, idx *JournalIndex, wake func(ctx context.Context, shard string) error) (bool, error) {
	for range 5 {
		h, err := f.Attach(ctx, name)
		if errors.Is(err, blob.ErrNotFound) {
			return false, nil // deleted since: its tail is never read again
		} else if err != nil {
			return false, err
		}
		m := h.Manifest
		if m.Journal != node {
			return false, nil // its commits go to another journal now
		}
		tail, err := idx.tail(ctx, f, name, m.Generation, m.LastPosition(), time.Time{})
		if err != nil {
			return false, err
		}
		if len(tail) == 0 {
			return false, nil
		}
		// The hint comes first: if this node dies before the delta, a repeated
		// adoption finds the tail in the journal and writes the hint again (an
		// existing one counts as success). A hint that is found before the delta only
		// means "look": a wake that fails while the shard isn't released leaves it, and
		// the next scan gets it.
		if err := wake(ctx, name); err != nil {
			capture.CloseTail(tail)
			return false, fmt.Errorf("writing the wake hint: %w", err)
		}
		err = h.putTail(ctx, tail, Hooks{}, func(n *Manifest) {
			n.Journal = ""
			n.Wake = time.Now().Unix() // it can't read the shard's rows, so the shard is woken to look
		})
		capture.CloseTail(tail)
		if errors.Is(err, blob.ErrConflict) {
			continue // somebody changed the manifest: look again
		}
		return err == nil, err
	}
	return false, errors.New("the manifest keeps changing")
}
