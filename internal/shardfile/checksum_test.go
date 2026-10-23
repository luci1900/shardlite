package shardfile_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// shardWithDeltas is a shard with a base and a few deltas, and the manifest
// that names them.
func shardWithDeltas(t *testing.T, e *harness.Env) (*shardfile.Handle, *capture.Shard) {
	t.Helper()
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	for i := range 4 {
		write(t, e, app, func() error { return sh.Sync(ctx) }, i)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	return h, sh
}

// filesOf are the bytes of a shard's files, in the order the manifest lists
// them.
func filesOf(t *testing.T, e *harness.Env, h *shardfile.Handle) [][]byte {
	t.Helper()
	ctx := context.Background()
	list := h.Manifest.Files
	out := make([][]byte, len(list))
	for i, f := range list {
		rc, _, err := e.Store.Get(ctx, shardfile.FileKey(shardName(shard), h.Manifest.Generation, f))
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		out[i] = b
	}
	return out
}

// putFile writes a file of a shard back, as a damaged one would be.
func putFile(t *testing.T, e *harness.Env, h *shardfile.Handle, i int, b []byte) {
	t.Helper()
	list := h.Manifest.Files
	ctx := context.Background()
	key := shardfile.FileKey(shardName(shard), h.Manifest.Generation, list[i])
	if err := e.Store.Delete(ctx, key, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Store.Create(ctx, key, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
}

// preApply is the checksum of the database before an LTX file is applied.
func preApply(t *testing.T, b []byte) ltx.Checksum {
	t.Helper()
	hdr, _, err := ltx.PeekHeader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	return hdr.PreApplyChecksum
}

// postApply is the checksum of the database after an LTX file is applied.
func postApply(t *testing.T, b []byte) ltx.Checksum {
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

// A restore checks every file: a file whose page is wrong, a file that is not
// the one the manifest names and a file whose trailer is wrong all fail the
// restore, with the file named.
func TestRestoreChecksEveryFile(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, e *harness.Env, h *shardfile.Handle, files [][]byte)
	}{
		{"a page flipped", func(t *testing.T, e *harness.Env, h *shardfile.Handle, files [][]byte) {
			b := append([]byte(nil), files[1]...)
			b[len(b)/2] ^= 0xff // a byte in the middle of the file
			putFile(t, e, h, 1, b)
		}},
		{"a delta left out", func(t *testing.T, e *harness.Env, h *shardfile.Handle, files [][]byte) {
			// The delta is left out and the one after it renumbered to close the
			// gap, so the positions are right and the pages are not.
			putFile(t, e, h, 1, renumber(t, files[2], files[1]))
		}},
		{"a trailer changed", func(t *testing.T, e *harness.Env, h *shardfile.Handle, files [][]byte) {
			b := append([]byte(nil), files[1]...)
			b[len(b)-1] ^= 0xff // the last byte of the file checksum
			putFile(t, e, h, 1, b)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := harness.New(t)
			h, sh := shardWithDeltas(t, e)
			defer sh.Close(context.Background())
			files := filesOf(t, e, h)
			tc.damage(t, e, h, files)
			dst := filepath.Join(t.TempDir(), "restored.db")
			_, err := e.Files.Restore(context.Background(), shardName(shard), dst, shardfile.RestoreOptions{})
			if err == nil {
				t.Fatal("the restore of a damaged file succeeded")
			}
			if !strings.Contains(err.Error(), "checksum") {
				t.Fatalf("the restore failed with %q, want it to name the checksum", err)
			}
			if !strings.Contains(err.Error(), shard) {
				t.Fatalf("the restore failed with %q, want it to name the shard", err)
			}
			// Nothing is deleted: the operator has every file.
			if n := countFiles(t, e); n != len(files) {
				t.Fatalf("%d files left, want %d", n, len(files))
			}
		})
	}
}

// A file whose header says another range from the one the manifest gives it
// fails the restore, with the file named.
func TestRestoreChecksTheFilesOwnRange(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	h, sh := shardWithDeltas(t, e)
	defer sh.Close(context.Background())
	files := filesOf(t, e, h)
	// The second file's header says it covers another range.
	b := append([]byte(nil), files[1]...)
	hdr, _, err := ltx.PeekHeader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	b = rewriteHeader(t, b, ltx.Header{
		Version: ltx.Version, PageSize: hdr.PageSize, Commit: hdr.Commit,
		MinTXID: hdr.MinTXID + 1, MaxTXID: hdr.MaxTXID + 1, Timestamp: hdr.Timestamp,
		PreApplyChecksum: hdr.PreApplyChecksum,
	})
	putFile(t, e, h, 1, b)
	dst := filepath.Join(t.TempDir(), "restored.db")
	_, err = e.Files.Restore(context.Background(), shardName(shard), dst, shardfile.RestoreOptions{})
	if err == nil {
		t.Fatal("the restore of a file with another range succeeded")
	}
	if !strings.Contains(err.Error(), "positions") {
		t.Fatalf("the restore failed with %q, want it to name the positions", err)
	}
}

// rewriteHeader writes an LTX file with another header, keeping its pages.
func rewriteHeader(t *testing.T, b []byte, hdr ltx.Header) []byte {
	t.Helper()
	dec := ltx.NewDecoder(bytes.NewReader(b))
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	enc, err := ltx.NewEncoder(&out)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, dec.Header().PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); err != nil {
			break
		}
		if err := enc.EncodePage(ph, buf); err != nil {
			t.Fatal(err)
		}
	}
	if err := dec.Close(); err != nil {
		t.Fatal(err)
	}
	enc.SetPostApplyChecksum(dec.Trailer().PostApplyChecksum)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// renumber is an LTX file with the range of another one, so that a file put in
// its place says the right positions.
func renumber(t *testing.T, b []byte, other []byte) []byte {
	t.Helper()
	hdr, _, err := ltx.PeekHeader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	ohdr, _, err := ltx.PeekHeader(bytes.NewReader(other))
	if err != nil {
		t.Fatal(err)
	}
	return rewriteHeader(t, b, ltx.Header{
		Version: ltx.Version, PageSize: hdr.PageSize, Commit: hdr.Commit,
		MinTXID: ohdr.MinTXID, MaxTXID: ohdr.MaxTXID, Timestamp: hdr.Timestamp,
		PreApplyChecksum: hdr.PreApplyChecksum,
	})
}

// panics reports whether f panics.
func panics(f func()) (p bool) {
	defer func() {
		if recover() != nil {
			p = true
		}
	}()
	f()
	return false
}

// decodeAll decodes an LTX file, as the applier does.
func decodeAll(b []byte) {
	dec := ltx.NewDecoder(bytes.NewReader(b))
	if err := dec.DecodeHeader(); err != nil {
		return
	}
	buf := make([]byte, dec.Header().PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); err != nil {
			break
		}
	}
	dec.Close()
}

// A file the LTX decoder panics on fails the restore instead of taking the
// node down: the decoder slices what is left of the file without checking its
// length, and a file cut short at the end of its page block makes it panic.
func TestRestoreOfATruncatedFileFails(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	h, sh := shardWithDeltas(t, e)
	defer sh.Close(context.Background())
	files := filesOf(t, e, h)
	// The length the file can be cut to that leaves the decoder less than
	// eight bytes to slice: the window it panics in.
	cut := 0
	for n := range len(files[1]) {
		if panics(func() { decodeAll(files[1][:n]) }) {
			cut = n
			break
		}
	}
	if cut == 0 {
		t.Fatal("no length of the file makes the decoder panic")
	}
	putFile(t, e, h, 1, files[1][:cut])

	done := make(chan error, 1)
	go func() {
		dst := filepath.Join(t.TempDir(), "restored.db")
		_, err := e.Files.Restore(context.Background(), shardName(shard), dst, shardfile.RestoreOptions{})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the restore of a truncated file succeeded")
		}
		if !strings.Contains(err.Error(), "malformed") {
			t.Fatalf("the restore failed with %q, want it to name the malformed file", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the restore of a truncated file did not finish")
	}
}

// A shard whose files were written before the checksums existed restores, and
// the first file with checksums is checked from there on.
func TestRestoreOfFilesWithoutChecksums(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	h, sh := shardWithDeltas(t, e)
	defer sh.Close(ctx)
	files := filesOf(t, e, h)
	// The first two files are written as they were before the checksums: no
	// checksum in the header and none in the trailer.
	for i := range 2 {
		putFile(t, e, h, i, withoutChecksums(t, files[i]))
	}
	// The restore still works: the files without checksums are applied
	// unchecked, and the third file's checksum is checked against them.
	dst := filepath.Join(t.TempDir(), "restored.db")
	if _, err := e.Files.Restore(ctx, shardName(shard), dst, shardfile.RestoreOptions{}); err != nil {
		t.Fatalf("restore of files without checksums: %v", err)
	}
	// A file with checksums that does not follow the ones without them fails.
	bad := append([]byte(nil), files[2]...)
	bad[len(bad)/3] ^= 0xff
	putFile(t, e, h, 2, bad)
	if _, err := e.Files.Restore(ctx, shardName(shard), dst, shardfile.RestoreOptions{}); err == nil {
		t.Fatal("the restore of a file with a wrong page succeeded")
	}
}

// withoutChecksums is an LTX file with no checksums in it, as the files
// written before this stage were.
func withoutChecksums(t *testing.T, b []byte) []byte {
	t.Helper()
	dec := ltx.NewDecoder(bytes.NewReader(b))
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	hdr := dec.Header()
	var out bytes.Buffer
	enc, err := ltx.NewEncoder(&out)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: hdr.PageSize,
		Commit: hdr.Commit, MinTXID: hdr.MinTXID, MaxTXID: hdr.MaxTXID, Timestamp: hdr.Timestamp,
	}); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, hdr.PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); err != nil {
			break
		}
		if err := enc.EncodePage(ph, buf); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// A delta carries the checksums of the chain it is built from: the first
// file's before and the last file's after.
func TestDeltaCarriesTheChecksums(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	for i := range 3 {
		write(t, e, app, func() error { return sh.Sync(ctx) }, i)
	}
	tail := sh.Tail()
	if len(tail) < 2 {
		t.Fatalf("%d files in the tail", len(tail))
	}
	first, last := tail[0], tail[len(tail)-1]
	firstBytes, err := first.Body.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	lastBytes, err := last.Body.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	files := filesOf(t, e, h)
	if len(files) != 1 {
		t.Fatalf("%d files in the manifest", len(files))
	}
	dec := ltx.NewDecoder(bytes.NewReader(files[0]))
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	hdr := dec.Header()
	if hdr.NoChecksum() {
		t.Fatal("the delta has no checksums")
	}
	if want := preApply(t, firstBytes); hdr.PreApplyChecksum != want {
		t.Fatalf("the delta's checksum before it is %s, want %s", hdr.PreApplyChecksum, want)
	}
	buf := make([]byte, hdr.PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); err != nil {
			break
		}
	}
	if err := dec.Close(); err != nil {
		t.Fatal(err)
	}
	if want := postApply(t, lastBytes); dec.Trailer().PostApplyChecksum != want {
		t.Fatalf("the delta's checksum after it is %s, want %s", dec.Trailer().PostApplyChecksum, want)
	}
}

// A rebase whose snapshot does not match the capture's checksum is logged,
// counted and written anyway, and the capture carries on from the snapshot's
// CRCs.
func TestRebaseWithASkewedChecksum(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	var logged strings.Builder
	var mismatches int
	logger := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelError}))
	app := e.StartApp(shard)
	sh, err := capture.New(capture.Config{
		Name: shardName(shard), Path: e.Path(shard), Generation: "g1", Writer: e.W,
		Logger: logger, SkewChecksum: true,
		ChecksumMismatch: func(want, got ltx.Checksum) { mismatches++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close(ctx)
	app.Must(t, "insert", "a", "20", "1000")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	h, _, err := e.Files.Open(ctx, shardName(shard))
	if err != nil {
		t.Fatal(err)
	}
	// The rebase is written, and the mismatch is logged and counted.
	if _, err := h.Rebase(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if mismatches != 1 {
		t.Fatalf("%d mismatches counted, want 1", mismatches)
	}
	if !strings.Contains(logged.String(), "checksum") {
		t.Fatalf("the log says %q, want it to name the checksum", logged.String())
	}
	// The capture carries on from the snapshot's CRCs: its checksum is the one
	// the snapshot computed, not the skewed one.
	files := filesOf(t, e, h)
	if len(files) != 1 {
		t.Fatalf("%d files in the manifest", len(files))
	}
	if got, want := sh.Checksums().Checksum(), postApply(t, files[0]); got != want {
		t.Fatalf("the capture's checksum is %s, the base's is %s", got, want)
	}
}
