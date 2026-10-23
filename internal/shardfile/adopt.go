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
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/remindex"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shard"
)

// AdoptReport says what AdoptJournal (or an Adoption) did.
type AdoptReport struct {
	Fence   pos.Seg // the number of the fence the adoption wrote: the journal's writer continues after it
	From    pos.Seg // the first segment read (later than asked if the segments before it had expired)
	Shards  int     // shards with entries in the range
	Deltas  int     // deltas written
	Fetched int     // segments the mirrors held above the record's landed run, copied into it before the fence
	Expired bool    // the range began with segments that no longer exist
	// Lost lists shards whose tail has a hole, which is only tolerated when the
	// journal's first segments had expired (Expired). Their history up to the hole is
	// kept and what follows is gone. It is logged as an error and does not stop
	// the start. Any other hole is a bug and fails the adoption.
	Lost     []shard.Name
	Problems []error
}

// adoptWorkers is how many shards are turned into deltas at once. Each is blob
// storage requests, so the limit is about requests in flight: a restart or an
// adoption holds up its node's start, and a busy node can leave thousands of
// tails.
const adoptWorkers = 64

// AdoptJournal is what a node does with a journal that may hold commits no file has:
// its own, left by the process it replaces (docs/internals.md, Takeover: Restart), or a dead
// node's.
//
//  0. It fetches the node's mirrors: the segments they hold above the run the
//     record landed are copied into the record first, since a fence written
//     over their numbers would strand them (docs/internals.md, Takeover). The
//     mirrors are the caller's: stores over the mirror protocol, empty when the
//     node named none.
//  1. It fences the journal: an empty segment at the first free number from next,
//     the next segment number the lease recorded, so the old writer can't add
//     anything below it.
//  2. It reads the journal's footers from from, the oldest segment the lease says
//     is still needed.
//  3. For every shard with entries the manifest's files don't cover, it writes
//     one delta and adds it to the manifest, clearing the manifest's journal. The
//     delta records a reminder due now, with its entry in the reminder index
//     created first (Handle.setReminder): the shard may hold reminders that were
//     never delivered and the adopter can't read them (docs/internals.md,
//     Takeover). A failed entry leaves the delta unwritten.
//
// It is FetchTail (step 0), ReadJournal (steps 1 and 2) and Adopt (step 3).
func (f *Files) AdoptJournal(ctx context.Context, node cluster.Node, from, next pos.Seg, mirrors []*journal.Store, logger *slog.Logger) (AdoptReport, error) {
	a, err := f.ReadJournal(ctx, node, from, next, mirrors, logger)
	if err != nil {
		return a.Report, err
	}
	err = a.Adopt(ctx)
	return a.Report, err
}

// Adoption is a journal being adopted: fetched, fenced and read (ReadJournal),
// with its deltas still to write (Adopt).
type Adoption struct {
	f      *Files
	node   cluster.Node
	idx    *JournalIndex
	log    *slog.Logger
	Report AdoptReport
}

// ReadJournal fetches the mirrors (step 0) and fences the journal and reads
// its footers: steps 0 to 2 of AdoptJournal. On an error the Adoption still
// has the Report so far. The fetch comes first, since the fence decides what
// may be read at all: a number the mirrors hold that the record lacks has to
// be in the record before the fence takes the first free number above it. A
// fetch that fails is logged and carried past (the window is the ensemble's
// documented bound), never a failed adoption.
func (f *Files) ReadJournal(ctx context.Context, node cluster.Node, from, next pos.Seg, mirrors []*journal.Store, logger *slog.Logger) (*Adoption, error) {
	if logger == nil {
		logger = slog.Default()
	}
	a := &Adoption{f: f, node: node, log: logger}
	fetched, err := f.FetchTail(ctx, node, from, next, mirrors, logger)
	a.Report.Fetched = fetched
	if err != nil && ctx.Err() == nil {
		logger.Warn("the fetch of the mirrors failed, what they hold is lost", "node", node, "err", err)
	}
	from, next = max(from, 1), max(next, 1)
	js := f.Journal(node)
	fence, err := segment.Fence(ctx, js, next)
	if err != nil {
		return a, fmt.Errorf("adopt %s: fence: %w", node, err)
	}
	a.Report.Fence = fence

	// Segments expire. A lease that has been idle for longer than they live can
	// name ones that are gone, and the journal then starts where the listing says.
	if from < fence {
		if _, err := segment.ReadFooter(ctx, js, from); errors.Is(err, blob.ErrNotFound) {
			low, _, err := segment.Bounds(ctx, js)
			if err != nil {
				return a, fmt.Errorf("adopt %s: %w", node, err)
			}
			logger.Warn("the journal's first segments have expired", "node", node, "lease_from", from, "first_present", low)
			a.Report.Expired = true
			from = max(low, from)
		}
	}
	a.Report.From = from

	// Every number from the start of the range to the fence must be there. A
	// missing one in the middle would hide the segments after it from a plain
	// scan, and with them commits an acknowledgement was given for.
	idx, err := f.LoadJournal(ctx, node, from, fence)
	if err != nil {
		return a, fmt.Errorf("adopt %s: reading the journal: %w", node, err)
	}
	a.idx = idx
	a.Report.Shards = len(idx.byShard)
	return a, nil
}

// Adopt writes the deltas of step 3 for the shards with entries in the journal,
// adding to Report.
//
// A tail with a hole in it is reported in Lost, not as a failure, only if the
// journal's first segments had expired: refusing to start would not bring those
// commits back. A hole anywhere else means the range in the lease was wrong or
// the journal is damaged, and fails the adoption so the bug is seen.
//
// Every shard is tried, so one bad one doesn't leave the others' tails in
// the journal. If any failed the error says so (and Problems lists them): the
// journal's range must not be moved on, since the tails are still there.
func (a *Adoption) Adopt(ctx context.Context) error {
	names := make([]shard.Name, 0, len(a.idx.byShard))
	for name := range a.idx.byShard {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i].String() < names[j].String() })
	type result struct {
		wrote bool
		err   error
	}
	results := make([]result, len(names))
	var g errgroup.Group
	g.SetLimit(adoptWorkers)
	for i, name := range names {
		g.Go(func() error {
			results[i].wrote, results[i].err = a.f.adoptShard(ctx, a.node, name, a.idx)
			return nil // each shard's problem is collected below, the others carry on
		})
	}
	g.Wait()
	rep := &a.Report
	failed := 0
	for i, name := range names {
		r := results[i]
		if _, gap := errors.AsType[*gapError](r.err); gap && rep.Expired {
			a.log.Error("a shard's tail has a hole, commits after it are lost", "node", a.node, "shard", name, "err", r.err)
			rep.Lost = append(rep.Lost, name)
		} else if r.err != nil {
			err := r.err
			if gap {
				err = fmt.Errorf("%w (the journal's range was not expired, so this is a bug)", err)
			}
			rep.Problems = append(rep.Problems, fmt.Errorf("%s: %w", name, err))
			failed++
		} else if r.wrote {
			rep.Deltas++
		}
	}
	sort.Slice(rep.Problems, func(i, j int) bool { return rep.Problems[i].Error() < rep.Problems[j].Error() })
	if failed > 0 {
		return fmt.Errorf("adopt %s: %d of %d shards failed: %w", a.node, failed, len(names), errors.Join(rep.Problems...))
	}
	return nil
}

type gapError struct{ from, to pos.Position }

func (g *gapError) Error() string {
	return fmt.Sprintf("the journal has nothing for positions %d to %d", g.from, g.to)
}

// adoptShard writes the delta for one shard's uncovered entries, if it has any.
func (f *Files) adoptShard(ctx context.Context, node cluster.Node, n shard.Name, idx *JournalIndex) (bool, error) {
	for range 5 {
		h, err := f.Attach(ctx, n)
		if errors.Is(err, blob.ErrNotFound) {
			return false, nil // deleted since: its tail is never read again
		} else if err != nil {
			return false, err
		}
		m := h.Manifest
		if m.Journal != node {
			return false, nil // its commits go to another journal now
		}
		tail, err := idx.tail(ctx, f, n, m.Generation, m.LastPosition(), time.Time{})
		if err != nil {
			return false, err
		}
		if len(tail) == 0 {
			return false, nil
		}
		// The entry comes first: journal expiry checks no lease, so once the delta
		// covers the tail a zombie of the dead node may delete the segments holding
		// it, and then nothing but the entry says the shard may have a reminder. If
		// this node dies before the delta, a repeated adoption finds the tail and
		// creates an entry again. The delta records a reminder due now, since the
		// shard may hold reminders that were never delivered and the adopter can't
		// read them. A system shard has none: the claimer brings it back.
		var hooks Hooks
		if !n.System() {
			now := remindex.MinuteAt(f.now())
			hooks.BeforeUpdate = func(context.Context, pos.Position) (remindex.Minute, error) { return now, nil }
		}
		err = h.putTail(ctx, tail, hooks, func(n *Manifest) { n.Journal = "" })
		tail.Close()
		if errors.Is(err, blob.ErrConflict) {
			continue // somebody changed the manifest: look again
		}
		return err == nil, err
	}
	return false, errors.New("the manifest keeps changing")
}
