package node

import (
	"context"
	"errors"
	"log/slog"

	"github.com/luci1900/shardlite/internal/storage/lease"
	"github.com/luci1900/shardlite/internal/storage/shardfile"
	"github.com/luci1900/shardlite/internal/syncs/notify"
)

// adopter adopts a dead node's journal on behalf of the lease scan, which finds
// the dead nodes, owns the watches and calls it on its own loop, one node at a
// time (docs/internals.md, Takeover). It has no goroutine of its own and no state
// between calls. The steps are the lease's mark, the journal's adoption (fence,
// read, and for each tail an entry due now in the reminder index before its
// delta), the lease's finish and the sweep of the node's segments below the
// fence. Every step is safe to redo, so an adoption cut short (an error, the scan
// stopping, this node dying) is simply made again, by this node or another.
type adopter struct {
	reg     *lease.Registry
	files   *shardfile.Files
	self    func() lease.Incarnation // this node's own incarnation
	nodeErr func() error             // nil while this node may write (Node.Err)
	clock   Clock
	log     *slog.Logger
	adopted *notify.Counter // adoptions finished, for tests
}

// adopt adopts the dead node whose lease is l, as the scan last read it. The scan
// has checked that the lease needs adopting and that no live node is adopting it.
// It reports whether the node is adopted. Every step is safe to redo, so one that
// fails leaves the adoption to be made again.
func (a *adopter) adopt(ctx context.Context, l *lease.Record) (done bool) {
	x, me := l.Name, a.self()
	if a.nodeErr() != nil {
		return false // this node may not write
	}
	marked, err := a.reg.Adopt(ctx, *l, me)
	switch {
	case errors.Is(err, lease.ErrNotAdoptable):
		return false // it renewed, restarted or someone else marked it
	case err != nil:
		if ctx.Err() == nil {
			a.log.Warn("could not mark a dead node's lease, trying again later", "node", x, "err", err)
		}
		return false
	}
	a.log.Info("adopting a dead node", "node", x, "epoch", l.Epoch, "readopted", l.AdoptedBy != nil)
	rep, err := a.files.AdoptJournal(ctx, x, l.JournalFrom, l.JournalNext, a.log)
	if err != nil {
		if ctx.Err() == nil {
			a.log.Error("adopting a dead node failed, it will be adopted again", "node", x, "epoch", l.Epoch, "err", err)
		}
		return false
	}
	if a.nodeErr() != nil {
		return false
	}
	if _, err := a.reg.FinishAdoption(ctx, marked, l.Epoch, rep.Fence); err != nil {
		if ctx.Err() == nil {
			a.log.Warn("could not finish adopting a dead node, it will be adopted again", "node", x, "epoch", l.Epoch, "err", err)
		}
		return false
	}
	// Segments from the fence on may be the node's next life's by now.
	sw := &expiry{journal: a.files.Journal(x), below: rep.Fence, log: a.log}
	if _, err := sw.expire(ctx, a.clock.Now()); err != nil && ctx.Err() == nil {
		a.log.Warn("could not sweep a dead node's journal", "node", x, "err", err)
	}
	a.log.Info("adopted a dead node", "node", x, "epoch", l.Epoch, "deltas", rep.Deltas, "lost", rep.Lost, "fence", rep.Fence)
	a.adopted.Inc()
	return true
}
