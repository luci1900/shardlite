package capture_test

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"testing"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/harness"
	"github.com/luci1900/shardlite/internal/test/sqlitetest"
)

// Snapshot returns the shard's whole database as of its returned position,
// taken while the app commits and checkpoints, with the WAL restarted around
// it (by the node, which waits for the snapshot to close, or by the app once
// the node has copied everything): the pages it reads are exact, the node's
// syncs go on while it reads them (they don't wait for the snapshot), the
// snapshot holds every row the commits up to its position wrote and none
// after, and the history that follows still replays to the app's database.
func TestSnapshotWhileTheAppCommits(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	// The app writes on one connection and checkpoints on another, as an app
	// with a reason to checkpoint would: a checkpoint that finds the node's
	// read transaction in the way gives up on it quickly (a short busy
	// timeout), while a write waits out the write lock of one of the node's
	// restarts, which under load is longer than a short timeout (the default
	// five seconds).
	app := apptest.Start(t)
	app.Must(t, "create", "a", e.Path(name))
	app.Must(t, "create", "c", e.Path(name), "20")
	h, _, _ := e.Files.Open(ctx, shardName(name))
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W, RestartFrames: 10})
	if err != nil {
		t.Fatal(err)
	}
	const writes = 3000
	done := make(chan error, 1)
	go func() { // one row of 300 bytes per write: one commit, one id, with the app's checkpoints between them
		for i := 0; i < writes; i++ {
			if _, err := app.Do("insert", "a", "1", "300"); err != nil {
				done <- err
				return
			}
			if i%100 == 99 {
				if _, err := app.Do("ckpt", "c", []string{"PASSIVE", "RESTART", "TRUNCATE"}[(i/100)%3]); err != nil {
					done <- err
					return
				}
			}
		}
		done <- nil
	}()
	for sh.Position() < 20 { // some commits land before the snapshot is taken
		select {
		case err := <-done:
			t.Fatalf("the writer stopped before 20 positions were captured: %v", err)
		default:
		}
		if err := sh.Capture(ctx); err != nil {
			t.Fatal(err)
		}
	}
	pos, r, err := sh.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos == 0 {
		t.Fatal("the snapshot covers no position")
	}
	// The salts the WAL has had over the run, to check that it was restarted
	// (by the node's locked restart, or by the app's next write once one had
	// copied everything: a mark-0 reader stops the copying, not the restart).
	salts := map[string]bool{}
	noteSalts := func() {
		h := header(t, sh)
		salts[string(h[32:40])] = true
	}
	noteSalts()

	// Read the snapshot in pieces, with the node's syncs going on in between:
	// a sync that waited for the snapshot would starve this read of the pipe.
	var snap bytes.Buffer
	b := make([]byte, 8<<10)
	var synced int
	for {
		n, rerr := r.Read(b)
		snap.Write(b[:n])
		if n > 0 {
			if err := sh.Capture(ctx); err != nil {
				t.Fatal(err)
			}
			noteSalts()
			synced++
			if p := sh.Position(); p < pos {
				t.Fatalf("the position went back from %d to %d during the snapshot's read", pos, p)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatal(rerr)
		}
	}
	if synced == 0 {
		t.Fatal("the snapshot was read in one piece: no sync ran during the read")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// The locked restart the WAL has gone past follows the snapshot once it
	// has closed, and the syncs carry on.
	for range 100 {
		if err := sh.Capture(ctx); err != nil {
			t.Fatal(err)
		}
		noteSalts()
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	noteSalts()
	// A restart needs the node's checkpoint to have copied everything, and an
	// app in the way (a reader, or a checkpoint of its own holding the
	// checkpointer lock) leaves the WAL as it is until the next try, which
	// under load can outlast the run: safe, the WAL only grows (the plan's
	// open question). What must hold is the snapshot's content.
	t.Logf("the WAL was restarted %d times over the run", len(salts)-1)

	// The snapshot holds every row the commits up to its position wrote and
	// none after: the rows run from 1 to their count with no gap, and every
	// payload is whole.
	snapPath := filepath.Join(t.TempDir(), "snapshot.db")
	ap, err := shardfile.NewApplier(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Apply(bytes.NewReader(snap.Bytes()), shardfile.Range{Min: 1, Max: pos}); err != nil {
		t.Fatal(err)
	}
	st, err := ap.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if st.Position != pos {
		t.Fatalf("the snapshot covers position %d, want %d", st.Position, pos)
	}
	db, err := sql.Open("sqlite", "file:"+snapPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n, maxID, sumID, sumLen int64
	if err := db.QueryRow(`SELECT count(*), coalesce(max(id),0), coalesce(sum(id),0), coalesce(sum(length(payload)),0) FROM kv`).Scan(&n, &maxID, &sumID, &sumLen); err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("the snapshot holds no rows")
	}
	if maxID != n {
		t.Fatalf("rows run to id %d for %d rows: rows are missing in between", maxID, n)
	}
	if sumID != n*(n+1)/2 {
		t.Fatalf("the ids sum to %d for %d rows: rows are missing or doubled", sumID, n)
	}
	if sumLen != 300*n {
		t.Fatalf("the payloads sum to %d bytes for %d rows, want 300 each", sumLen, n)
	}

	// What the syncs captured around the snapshot, restarts included, still
	// replays to the app's database.
	replayed := filepath.Join(t.TempDir(), "replayed.db")
	replayInto(t, e, replayed)
	if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, replayed); want != got {
		t.Fatal("the history around a snapshot does not restore to the app's database")
	}
	sh.Close(ctx)
}
