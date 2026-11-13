package capture_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/model/pos"
	"github.com/luci1900/shardlite/internal/storage/journal"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/harness"
)

// dbChecksum is the checksum of the database at path, which holds nPages pages
// of pageSize bytes.
func dbChecksum(t testing.TB, path string, pageSize, nPages uint32) ltx.Checksum {
	t.Helper()
	if nPages == 0 {
		return ltx.ChecksumFlag
	}
	crcs := make([]ltx.Checksum, nPages)
	if _, err := ltx.ChecksumPages(path, pageSize, nPages, 0, crcs); err != nil {
		t.Fatal(err)
	}
	sum := ltx.Checksum(0)
	for _, c := range crcs {
		sum ^= c &^ ltx.ChecksumFlag
	}
	return ltx.ChecksumFlag | sum
}

// fileChecksum is the checksum of the database at path, from its size.
func fileChecksum(t testing.TB, path string, pageSize uint32) ltx.Checksum {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return dbChecksum(t, path, pageSize, uint32(fi.Size()/int64(pageSize)))
}

// replayChecksum is the checksum of the database the shard's captured files
// produce.
func replayChecksum(t testing.TB, e *harness.Env, pageSize uint32) ltx.Checksum {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replayed.db")
	replayInto(t, e, path)
	return fileChecksum(t, path, pageSize)
}

// The capture keeps the checksum of the database it has captured, and every
// file it writes carries it: the checksum before the file and the one after.
// The capture's checksum is the checksum of the database its files produce, and
// at the end the checksum of the app's own database.
//
// It holds across a WAL restart, a commit that shrinks the database and one
// that grows it.
func TestCaptureChecksumMatchesTheDatabase(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, err := e.Files.Open(ctx, shardName(name))
	if err != nil {
		t.Fatal(err)
	}
	// A small restart threshold, so the WAL is restarted during the test.
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W, RestartFrames: 20})
	if err != nil {
		t.Fatal(err)
	}
	pageSize := uint32(4096)
	if v := apptest.Query(t, e.Path(name), `PRAGMA page_size`); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatal(err)
		}
		pageSize = uint32(n)
	}
	for _, step := range []struct {
		what string
		run  func()
	}{
		{"the database grows", func() { app.Must(t, "insert", "a", "5", "1000") }},
		{"the database grows again", func() { app.Must(t, "insert", "a", "40", "1000") }},
		{"the WAL is restarted", func() { app.Must(t, "insert", "a", "40", "1000") }},
		{"the database shrinks", func() { app.Must(t, "exec", "a", "VACUUM") }},
		{"the database grows once more", func() { app.Must(t, "insert", "a", "10", "1000") }},
	} {
		step.run()
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		want := replayChecksum(t, e, pageSize)
		if got := sh.Checksums().Checksum(); got != want {
			t.Fatalf("after %s: the capture's checksum is %s, the database it captured is %s", step.what, got, want)
		}
	}
	// The app closes, then the capture: the last connection to close checkpoints
	// the WAL into the database file, so the file holds the app's database.
	app.Must(t, "close", "a")
	if err := sh.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := sh.Checksums().Checksum(), fileChecksum(t, e.Path(name), pageSize); got != want {
		t.Fatalf("the capture's checksum is %s, the app's database is %s", got, want)
	}
}

// entriesAndSeqs returns the shard's journal entries and the segments holding
// them.
func entriesAndSeqs(t *testing.T, e *harness.Env, shard string) (out []journal.Entry, seqs []pos.Seg) {
	t.Helper()
	err := journal.Scan(context.Background(), e.Journal, 1, 0, func(seq pos.Seg, en journal.Entry) error {
		if en.Shard.String() == shard {
			out, seqs = append(out, en), append(seqs, seq)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out, seqs
}

// preApplyChecksum is the checksum of the database before an LTX file is
// applied, which is in its header.
func preApplyChecksum(t testing.TB, b []byte) ltx.Checksum {
	t.Helper()
	hdr, _, err := ltx.PeekHeader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	return hdr.PreApplyChecksum
}

// A restored shard continues from the restore's checksum: the first file it
// writes has that checksum before it, so the shard's files are a chain from
// the restore on.
func TestRestoredShardContinuesFromTheRestoresChecksum(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	res := stored(t, e, 2)
	restored := res.Capture.Checksums.Checksum() // New takes the checksums over
	sh := e.Activate(res.Handle, res.Capture)
	defer sh.Close(ctx)
	app := e.StartApp(name)
	app.Must(t, "insert", "a", "2", "300")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	entries, seqs := entriesAndSeqs(t, e, name)
	if len(entries) == 0 {
		t.Fatal("no files captured")
	}
	b, err := harness.ReadEntry(ctx, e.Journal, seqs[len(seqs)-1], entries[len(entries)-1])
	if err != nil {
		t.Fatal(err)
	}
	if got, want := preApplyChecksum(t, b), restored; got != want {
		t.Fatalf("the file after the restore has %s before it, the restore's is %s", got, want)
	}
}

// trailerChecksum is the checksum of the database after an LTX file is
// applied, which is in its trailer.
func trailerChecksum(t testing.TB, b []byte) ltx.Checksum {
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
	}
	if err := dec.Close(); err != nil {
		t.Fatal(err)
	}
	return dec.Trailer().PostApplyChecksum
}

// The files the capture writes carry the checksum of the database before and
// after them, so a restore can check that the files are the ones that were
// written.
func TestCapturedFilesCarryTheChecksums(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	_, sh := e.NewShard(name)
	for range 3 {
		app.Must(t, "insert", "a", "2", "500")
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	entries, seqs := entriesAndSeqs(t, e, name)
	if len(entries) == 0 {
		t.Fatal("no files captured")
	}
	var pre, post ltx.Checksum
	for i, en := range entries {
		b, err := harness.ReadEntry(ctx, e.Journal, seqs[i], en)
		if err != nil {
			t.Fatal(err)
		}
		hdr, _, err := ltx.PeekHeader(bytesReader(b))
		if err != nil {
			t.Fatal(err)
		}
		if hdr.NoChecksum() {
			t.Fatalf("the file at position %d has no checksums", en.Position)
		}
		if hdr.PreApplyChecksum != pre {
			t.Fatalf("the file at position %d has %s before it, want %s", en.Position, hdr.PreApplyChecksum, pre)
		}
		post = trailerChecksum(t, b)
		pre = post
	}
	// The last file's checksum is the capture's.
	if got, want := post, sh.Checksums().Checksum(); got != want {
		t.Fatalf("the last file's checksum is %s, the capture's is %s", got, want)
	}
}
