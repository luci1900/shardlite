package shardfile

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/spool"
)

type journalRef struct {
	seq   pos.Seg
	entry segment.Entry
}

// JournalIndex lists which segment entries belong to which shard. It carries
// the store of the journal it read, so its tail is read from the same place.
type JournalIndex struct {
	store   *journal.Store
	byShard map[shard.Name][]journalRef
}

// LoadLeasedJournal reads the footers of a node's segments from the start of the
// range its lease says is still needed. A node with no lease has no journal.
func (f *Files) LoadLeasedJournal(ctx context.Context, node cluster.Node) (*JournalIndex, error) {
	if f.Leases == nil {
		return nil, errors.New("shardfile: no node registry to find the journal's range")
	}
	l, err := f.Leases.Get(ctx, node)
	if errors.Is(err, blob.ErrNotFound) {
		return &JournalIndex{}, nil
	} else if err != nil {
		return nil, err
	}
	return f.LoadJournal(ctx, node, max(l.JournalFrom, 1), 0)
}

// LoadJournal reads the footers of a node's segments from number from on. With
// to zero it stops at the first number that doesn't exist, and otherwise it
// reads through to (the fence of an adoption) and fails on a missing number
// (segment.Scan).
func (f *Files) LoadJournal(ctx context.Context, node cluster.Node, from, to pos.Seg) (*JournalIndex, error) {
	idx := &JournalIndex{store: f.Journal(node), byShard: map[shard.Name][]journalRef{}}
	err := segment.Scan(ctx, idx.store, from, to, func(seq pos.Seg, e segment.Entry) error {
		idx.byShard[e.Shard] = append(idx.byShard[e.Shard], journalRef{seq, e})
		return nil
	})
	return idx, err
}

// tail reads the shard's entries of generation that follow position from, in
// position order, as tail files stamped at. Positions were assigned when the
// commits were captured, so they say what is covered. Copies of an entry
// already taken (a retried segment write) are skipped. A hole between the
// entries is a *gapError. The caller closes the files.
func (idx *JournalIndex) tail(ctx context.Context, f *Files, n shard.Name, generation shard.Gen, from pos.Position, at time.Time) (capture.TailFiles, error) {
	var refs []journalRef
	for _, ref := range idx.byShard[n] {
		if ref.entry.Generation == generation {
			refs = append(refs, ref)
		}
	}
	sort.SliceStable(refs, func(i, j int) bool { return refs[i].entry.Position < refs[j].entry.Position })
	var tail capture.TailFiles
	pos := from
	for _, ref := range refs {
		e := ref.entry
		if e.Last() <= pos {
			continue // covered by the files, or a copy of an entry already taken
		}
		if e.Position != pos+1 {
			tail.Close()
			return nil, &gapError{from: pos + 1, to: e.Position - 1}
		}
		// An entry can be as large as the database, so it is read into a temporary
		// file when it is large.
		body, err := segment.ReadEntryFile(ctx, idx.store, ref.seq, e, f.TempDir, f.spoolLimit())
		if err != nil {
			tail.Close()
			return nil, err
		}
		tail = append(tail, capture.TailFile{Position: e.Position, Count: e.Count, Body: body, At: at, Seg: ref.seq})
		pos = e.Last()
	}
	return tail, nil
}

// ErrNothingToRestore means a shard has a manifest but no commits in its files or journal.
var ErrNothingToRestore = errors.New("shardfile: no commits to restore")

// RestoreOptions tune Restore.
type RestoreOptions struct {
	Journal      *JournalIndex // the journal to read the tail from, loaded from the manifest's journal if nil
	BeforeRename func() error  // called with the file complete and synced, for tests
	TailAge      time.Time     // age given to tail files read from the journal (now if zero)
	NoJournal    bool          // the caller knows the shard has no tail in any journal (an activation, which has just claimed it), so none is read
}

// Restored is the result of Restore.
type Restored struct {
	Handle *Handle
	State  State
	// Capture is what capture.Config needs to continue the shard.
	Capture *capture.Restored
}

// Restore rebuilds a shard's SQLite file at dst from its manifest files and,
// for commits after them, from its node journal. Every file's checksums are
// verified, and the file only appears at dst when complete. A file that fails
// its checksums leaves the shard unserved and every file in place.
func (f *Files) Restore(ctx context.Context, n shard.Name, dst string, opt RestoreOptions) (*Restored, error) {
	h, err := f.Attach(ctx, n)
	if err != nil {
		return nil, err
	}
	m := h.Manifest

	// Download the manifest's files in parallel, each to a temporary file: a
	// shard's files can add up to far more than the node has memory for.
	spools := make([]*spool.File, len(m.Files))
	defer func() {
		for _, sp := range spools {
			if sp != nil {
				sp.Close()
			}
		}
	}()
	errs := make([]error, len(m.Files))
	var g errgroup.Group
	for i, mf := range m.Files {
		g.Go(func() error {
			spools[i], errs[i] = h.getFile(ctx, mf)
			return nil // each file keeps its own error, reported in order below
		})
	}
	g.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("restore %s: file %s: %w", n, m.Files[i], err)
		}
	}

	tmp := dst + ".restoring"
	ap, err := NewApplier(tmp)
	if err != nil {
		return nil, err
	}
	var tail capture.TailFiles
	fail := func(err error) (*Restored, error) {
		ap.Abort()
		os.Remove(tmp)
		tail.Close()
		return nil, fmt.Errorf("restore %s: %w", n, err)
	}
	for i, sp := range spools {
		if err := ap.Apply(bufio.NewReaderSize(sp.Reader(), 1<<20), m.Files[i].Range); err != nil {
			return fail(err)
		}
		sp.Close() // applied: its disk is free again
		spools[i] = nil
	}

	// Then the commits that only reached the node journal.
	journal := opt.Journal
	if journal == nil && m.Journal != "" && !opt.NoJournal {
		// The journal's lease says where its live range starts: old segments expire.
		if journal, err = f.LoadLeasedJournal(ctx, m.Journal); err != nil {
			return fail(err)
		}
	}
	if journal == nil {
		journal = &JournalIndex{}
	}
	at := opt.TailAge
	if at.IsZero() {
		at = time.Now()
	}
	if tail, err = journal.tail(ctx, f, n, m.Generation, ap.State().Position, at); err != nil {
		return fail(err)
	}
	for _, tf := range tail {
		if err := ap.Apply(bufio.NewReaderSize(tf.Body.Reader(), 1<<20), Range{tf.Position, tf.Position.Add(tf.Count)}); err != nil {
			return fail(err)
		}
	}

	st, err := ap.Finish()
	if err != nil {
		return fail(err)
	}
	if st.Position == 0 {
		os.Remove(tmp)
		return nil, fmt.Errorf("restore %s: %w", n, ErrNothingToRestore)
	}
	if opt.BeforeRename != nil {
		if err := opt.BeforeRename(); err != nil {
			return nil, err // leaves the .restoring file, as a crash would
		}
	}
	// A restored file starts clean: no WAL sidecars left by an earlier life of
	// the file.
	if err := capture.WALSidecarsOf(dst).Remove(); err != nil {
		return fail(err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fail(err)
	}
	if d, err := os.Open(filepath.Dir(dst)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return &Restored{Handle: h, State: st, Capture: &capture.Restored{Position: st.Position, Tail: tail, Checksums: ap.Checksums()}}, nil
}
