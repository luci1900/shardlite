package shardfile_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

// entries returns every segment entry of a shard.
func entries(t *testing.T, e *harness.Env, name string) (out []segment.Entry) {
	t.Helper()
	err := segment.Scan(context.Background(), e.Journal, 1, 0, func(_ pos.Seg, en segment.Entry) error {
		if en.Shard.String() == name {
			out = append(out, en)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

type variant struct {
	deltas      int  // deltas after the base
	rebase      bool // rebase onto one base before letting go
	journalTail bool // commits after the last file, only in the node journal
	activations int  // further activations after the first, each restoring and capturing again, without deltas in between
}

// letGo ends an activation the way the node does: the app closes the shard,
// then the node releases it and finds nothing else has it open.
func letGo(t *testing.T, e *harness.Env, app *apptest.App, sh *capture.Shard) {
	t.Helper()
	app.Must(t, "close", "a")
	app.Exit()
	released, _, err := sh.Release(context.Background(), nil)
	if err != nil || !released {
		t.Fatalf("release: released=%v err=%v", released, err)
	}
}

// restoreAndCapture follows the sequence from docs/internals.md: restore,
// capture continuing from the restored position, sync once before any app
// opens the file (which writes nothing), then let the app write more.
func restoreAndCapture(t *testing.T, e *harness.Env, v variant) {
	t.Helper()
	ctx := context.Background()
	path := e.Path(shard)

	app := e.StartApp(shard)
	app.Must(t, "insert", "a", "100", "1000") // about 100 KB
	h, sh := e.NewShard(shard)
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		write(t, e, app, func() error { return sh.Sync(ctx) }, i)
	}
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	for i := range v.deltas {
		write(t, e, app, func() error { return sh.Sync(ctx) }, 10+i)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	if v.rebase {
		if _, err := h.Rebase(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	if !v.rebase && len(h.Manifest.Files) != 1+v.deltas || v.rebase && len(h.Manifest.Files) != 1 {
		t.Fatalf("manifest files %v", h.Manifest.Files)
	}
	if v.journalTail {
		write(t, e, app, func() error { return sh.Sync(ctx) }, 15)
		write(t, e, app, func() error { return sh.Sync(ctx) }, 16)
	}
	position := sh.Position()

	for act := 2; act <= 2+v.activations; act++ {
		letGo(t, e, app, sh)
		removeLocal(t, path)

		// Restore, register, and sync before any app has the file open.
		res, err := e.Files.Restore(ctx, shardName(shard), path, shardfile.RestoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if res.State.Position != position {
			t.Fatalf("activation %d: restored to position %d, the shard was at %d", act, res.State.Position, position)
		}
		before := len(entries(t, e, shard))
		sh = e.Activate(res.Handle, res.Capture)
		if err := sh.Sync(ctx); err != nil {
			t.Fatalf("sync of restored shard: %v", err)
		}
		if n := len(entries(t, e, shard)); n != before {
			t.Fatalf("the first sync of a restored shard added %d journal entries: a snapshot was taken and uploaded", n-before)
		}

		app = e.StartApp(shard)
		for i := range 3 {
			write(t, e, app, func() error { return sh.Sync(ctx) }, 20+i)
		}
		all := entries(t, e, shard)[before:]
		if len(all) < 3 {
			t.Fatalf("expected the three commits, got %d entries", len(all))
		}

		// The app's next commit continues the shard's own positions: no gap, no overlap.
		next := position + 1
		for i, en := range all {
			if en.Position != next {
				t.Fatalf("activation %d: entry %d is at position %d, want %d", act, i, en.Position, next)
			}
			next += pos.Position(en.Count)
		}
		position = next - 1
		if position != sh.Position() {
			t.Fatalf("shard position %d, entries end at %d", sh.Position(), position)
		}
		// Nothing but the three commits was uploaded: no entry holds more than a fraction of the database's pages.
		for _, en := range all {
			b, err := harness.ReadEntry(ctx, e.Journal, entrySeq(t, e, en), en)
			if err != nil {
				t.Fatal(err)
			}
			if pages, commit := pageCount(t, b); pages*4 > commit {
				t.Fatalf("entry at position %d holds %d of %d pages: a snapshot was uploaded", en.Position, pages, commit)
			}
		}
		t.Logf("activation %d: restored to %d; %d new entries from position %d", act, res.State.Position, len(all), all[0].Position)
	}

	// Restoring again from manifest plus tails of every activation gives the app's final state.
	want := apptest.Dump(t, path)
	again := filepath.Join(t.TempDir(), "again.db")
	res, err := e.Files.Restore(ctx, shardName(shard), again, shardfile.RestoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.State.Position != position {
		t.Fatalf("second restore reached position %d, want %d", res.State.Position, position)
	}
	if got := sqlitetest.Dump(t, again); got != want {
		t.Fatal("second restore differs from the app's final state")
	}
	// And so does a delta written now (carried tails and this activation's), then a restore from it alone.
	if err := res.Handle.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if last := res.Handle.Manifest.LastPosition(); last != position {
		t.Fatalf("delta ends at position %d, want %d", last, position)
	}
	third := filepath.Join(t.TempDir(), "third.db")
	if _, err := e.Files.Restore(ctx, shardName(shard), third, shardfile.RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := sqlitetest.Dump(t, third); got != want {
		t.Fatal("restore after a delta differs from the app's final state")
	}
	// Manifests contain the shard's own positions: their ranges start at 1.
	files := res.Handle.Manifest.Files
	if files[0].Min != 1 {
		t.Fatalf("first file %s does not start at position 1", files[0])
	}
	sh.Close(ctx)
}

func TestRestoreThenCapture(t *testing.T) {
	t.Parallel()
	for name, v := range map[string]variant{
		"base only":                         {},
		"base and deltas":                   {deltas: 3},
		"rebased base":                      {deltas: 4, rebase: true},
		"with journal tail":                 {deltas: 1, journalTail: true},
		"rebased, journal tail":             {deltas: 2, rebase: true, journalTail: true},
		"tails of several activations":      {deltas: 1, activations: 2},
		"several activations, journal tail": {journalTail: true, activations: 3},
	} {
		t.Run(name, func(t *testing.T) {
			restoreAndCapture(t, harness.New(t), v)
		})
	}
}

// entrySeq finds the segment holding an entry.
func entrySeq(t *testing.T, e *harness.Env, want segment.Entry) pos.Seg {
	t.Helper()
	var found pos.Seg
	err := segment.Scan(context.Background(), e.Journal, 1, 0, func(seq pos.Seg, en segment.Entry) error {
		if en == want {
			found = seq
		}
		return nil
	})
	if err != nil || found == 0 {
		t.Fatalf("entry not found: %v", err)
	}
	return found
}

// pageCount returns how many pages an LTX file holds and the database size in pages.
func pageCount(t *testing.T, b []byte) (pages, commit int) {
	t.Helper()
	dec := ltx.NewDecoder(bytes.NewReader(b))
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, dec.Header().PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); err != nil {
			break
		}
		pages++
	}
	return pages, int(dec.Header().Commit)
}

// Segments expire a day after they are written, oldest first, so restore
// can't start at segment 1: it starts where the node's lease says.
func TestRestoreFindsTailAfterOldSegmentsExpire(t *testing.T) {
	t.Parallel()
	e, ctx := harness.New(t), context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	for i := range 4 {
		write(t, e, app, func() error { return sh.Sync(ctx) }, i)
	}
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	covered := len(entries(t, e, shard))
	for i := range 3 {
		write(t, e, app, func() error { return sh.Sync(ctx) }, 10+i)
	}
	tailEntries := entries(t, e, shard)[covered:]
	firstTail := entrySeq(t, e, tailEntries[0])
	if firstTail < 3 {
		t.Fatalf("tail starts in segment %d, test needs earlier segments to expire", firstTail)
	}
	want := apptest.Dump(t, e.Path(shard))

	// The lifecycle rule has removed every segment before the tail's.
	for seq := pos.Seg(1); seq < firstTail; seq++ {
		if err := e.Store.Delete(ctx, journal.Key(e.Node, seq), ""); err != nil {
			t.Fatal(err)
		}
	}
	// The lease records where the live range starts, so the expired segments
	// before it are never looked for.
	e.SetJournalRange(firstTail)
	if got := restoreDump(t, e, "expired"); got != want {
		t.Fatal("restore after the journal's first segments expired lost the tail")
	}
	// And a caller that loaded the journal itself can pass it.
	idx, err := e.Files.LoadJournal(ctx, e.Node, firstTail, 0)
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "explicit.db")
	if _, err := e.Files.Restore(ctx, shardName(shard), dst, shardfile.RestoreOptions{Journal: idx}); err != nil {
		t.Fatal(err)
	}
	if got := sqlitetest.Dump(t, dst); got != want {
		t.Fatal("restore from an explicit starting segment differs")
	}
	sh.Close(ctx)
}
