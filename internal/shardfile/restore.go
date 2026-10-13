package shardfile

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/spool"
)

type journalRef struct {
	seq   uint64
	entry segment.Entry
}

// JournalIndex lists which segment entries belong to which shard.
type JournalIndex struct {
	node    string
	byShard map[string][]journalRef
}

// LoadLeasedJournal reads the footers of a node's segments from the start of the
// range its lease says is still needed. A node with no lease has no journal.
func (f *Files) LoadLeasedJournal(ctx context.Context, node string) (*JournalIndex, error) {
	if f.Leases == nil {
		return nil, errors.New("shardfile: no node registry to find the journal's range")
	}
	l, err := f.Leases.Get(ctx, node)
	if errors.Is(err, blob.ErrNotFound) {
		return &JournalIndex{node: node, byShard: map[string][]journalRef{}}, nil
	} else if err != nil {
		return nil, err
	}
	return f.LoadJournal(ctx, node, max(l.JournalFrom, 1))
}

// LoadJournal reads the footers of a node's segments from number from on, up to
// the first number that doesn't exist.
func (f *Files) LoadJournal(ctx context.Context, node string, from uint64) (*JournalIndex, error) {
	idx := &JournalIndex{node: node, byShard: map[string][]journalRef{}}
	err := segment.Scan(ctx, f.Blob, node, from, func(seq uint64, e segment.Entry) error {
		idx.byShard[e.Shard] = append(idx.byShard[e.Shard], journalRef{seq, e})
		return nil
	})
	return idx, err
}

// ErrNothingToRestore means a shard has a manifest but no commits in its files or journal.
var ErrNothingToRestore = errors.New("shardfile: no commits to restore")

// RestoreOptions tune Restore.
type RestoreOptions struct {
	Journal      *JournalIndex // the journal to read the tail from, loaded from the manifest's journal if nil
	BeforeRename func() error  // called with the file complete and synced, for tests
	TailAge      time.Time     // age given to tail files read from the journal (now if zero)
	NoJournal    bool          // the caller knows the shard has no tail (its manifest named no journal before it was claimed)
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
// verified, and the file only appears at dst when complete.
func (f *Files) Restore(ctx context.Context, shard, dst string, opt RestoreOptions) (*Restored, error) {
	m, etag, err := f.GetManifest(ctx, shard)
	if err != nil {
		return nil, err
	}
	h := &Handle{F: f, Name: shard, Manifest: m, ETag: etag}
	files, err := m.List()
	if err != nil {
		return nil, err
	}

	// Download the manifest's files in parallel, each to a temporary file: a
	// shard's files can add up to far more than the node has memory for.
	spools := make([]*spool.File, len(files))
	defer func() {
		for _, sp := range spools {
			if sp != nil {
				sp.Close()
			}
		}
	}()
	errs := make([]error, len(files))
	var g errgroup.Group
	for i, mf := range files {
		g.Go(func() error {
			spools[i], errs[i] = h.getFile(ctx, mf)
			return nil // each file keeps its own error, reported in order below
		})
	}
	g.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("restore %s: file %s: %w", shard, files[i], err)
		}
	}

	tmp := dst + ".restoring"
	ap, err := NewApplier(tmp)
	if err != nil {
		return nil, err
	}
	var tail []capture.TailFile
	fail := func(err error) (*Restored, error) {
		ap.Abort()
		os.Remove(tmp)
		capture.CloseTail(tail)
		return nil, fmt.Errorf("restore %s: %w", shard, err)
	}
	for i, sp := range spools {
		if err := ap.Apply(bufio.NewReaderSize(sp.Reader(), 1<<20), files[i].Range); err != nil {
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
		journal = &JournalIndex{byShard: map[string][]journalRef{}}
	}
	at := opt.TailAge
	if at.IsZero() {
		at = time.Now()
	}
	// Entries of this generation that the manifest's files don't cover, in
	// position order. Positions were assigned when the commits were captured, so
	// they say what is covered without anything of Litestream's in the manifest.
	var refs []journalRef
	for _, ref := range journal.byShard[shard] {
		if ref.entry.Generation == m.Generation && ref.entry.Last() > ap.State().Position {
			refs = append(refs, ref)
		}
	}
	sort.SliceStable(refs, func(i, j int) bool { return refs[i].entry.Position < refs[j].entry.Position })
	for _, ref := range refs {
		e := ref.entry
		if e.Last() <= ap.State().Position {
			continue // a copy of an entry already applied (a retried segment write)
		}
		// An entry can be as large as the database, so it is read into a temporary
		// file when it is large.
		body, err := segment.ReadEntryFile(ctx, f.Blob, journal.node, ref.seq, e, f.TempDir, f.spoolLimit())
		if err != nil {
			return fail(err)
		}
		if err := ap.Apply(bufio.NewReaderSize(body.Reader(), 1<<20), Range{e.Position, e.Last()}); err != nil {
			body.Close()
			return fail(err)
		}
		tail = append(tail, capture.TailFile{Position: e.Position, Count: e.Count, Body: body, At: at, Seg: ref.seq})
	}

	st, err := ap.Finish()
	if err != nil {
		return fail(err)
	}
	if st.Position == 0 {
		os.Remove(tmp)
		return nil, fmt.Errorf("restore %s: %w", shard, ErrNothingToRestore)
	}
	if opt.BeforeRename != nil {
		if err := opt.BeforeRename(); err != nil {
			return nil, err // leaves the .restoring file, as a crash would
		}
	}
	// A restored file starts clean: no WAL and no shared memory left by an earlier life of the file.
	for _, p := range []string{dst + "-wal", dst + "-shm"} {
		if err := os.RemoveAll(p); err != nil {
			return fail(err)
		}
	}
	if err := os.Rename(tmp, dst); err != nil {
		return fail(err)
	}
	if d, err := os.Open(filepath.Dir(dst)); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return &Restored{Handle: h, State: st, Capture: &capture.Restored{Position: st.Position, Tail: tail}}, nil
}
