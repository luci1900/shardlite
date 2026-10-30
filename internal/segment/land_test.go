package segment

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// bytesBody is bytes as a body, re-readable as many times as a create retries.
type bytesBody []byte

func (b bytesBody) Size() int64 { return int64(len(b)) }

func (b bytesBody) ReadAt(p []byte, off int64) (int, error) { return bytes.NewReader(b).ReadAt(p, off) }

// segDisk is a segment the landing is to read: its bytes, its store and the
// write id that made it.
type segDisk struct {
	store *journal.Store
	id    blob.WriteID
	body  []byte
}

// segFile is one LTX file of a segment, with the checksums the chain needs:
// the database's before the file is applied and after, as the files the
// capture builds carry them. noCheck says a file without checksums instead.
type segFile struct {
	shard       string
	min, max    ltx.TXID
	pre, post   ltx.Checksum
	fill, fill2 byte
	noCheck     bool
}

// csum is a checksum in the format the files carry.
func csum(n uint64) ltx.Checksum { return ltx.ChecksumFlag | ltx.Checksum(n) }

// segLTX encodes one file of a segFile.
func segLTX(t *testing.T, f segFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	hdr := ltx.Header{Version: ltx.Version, PageSize: 4096, Commit: 2,
		MinTXID: f.min, MaxTXID: f.max, PreApplyChecksum: f.pre, Timestamp: 1}
	if f.noCheck {
		hdr.Flags, hdr.PreApplyChecksum = ltx.HeaderFlagNoChecksum, 0
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 1}, bytes.Repeat([]byte{f.fill}, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodePage(ltx.PageHeader{Pgno: 2}, bytes.Repeat([]byte{f.fill2}, 4096)); err != nil {
		t.Fatal(err)
	}
	if !f.noCheck {
		enc.SetPostApplyChecksum(f.post)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// segEntries are the footer's entries for one pass of segFiles.
func segEntries(t *testing.T, files []segFile) []Entry {
	t.Helper()
	entries := make([]Entry, 0, len(files))
	var off int64
	for _, f := range files {
		n := int64(len(segLTX(t, f)))
		entries = append(entries, Entry{
			Shard: shard.MustParse(f.shard), Generation: "g",
			Position: pos.Position(f.min), Count: pos.Count(f.max-f.min) + 1,
			Offset: off, Length: n,
		})
		off += n
	}
	return entries
}

// segBytes lays out a segment the way the writer does: the files back to back,
// then the footer that names them and the trailer. entries, when not nil,
// replaces the footer's: it is how the test gives the footer a shape the
// layout cannot have.
func segBytes(t *testing.T, files []segFile, entries []Entry) []byte {
	t.Helper()
	var body bytes.Buffer
	for _, f := range files {
		if _, err := body.Write(segLTX(t, f)); err != nil {
			t.Fatal(err)
		}
	}
	if entries == nil {
		entries = segEntries(t, files)
	}
	fb, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(fb)))
	b := append(body.Bytes(), fb...)
	b = append(b, n[:]...)
	return append(b, magic...)
}

// segSource stores a laid-out segment in its own store under number 1.
func segSource(t *testing.T, files []segFile, entries []Entry) segDisk {
	t.Helper()
	return segHeld(t, segBytes(t, files, entries))
}

// segHeld puts body in its own store under number 1, whatever the bytes are.
func segHeld(t *testing.T, body []byte) segDisk {
	t.Helper()
	store := journal.NewStore(journal.NewBlob(blob.NewMemory(), "n1"), "n1", 1, journal.Options{Backoff: time.Millisecond})
	id := store.NewWriteID()
	if err := store.CreateWithID(context.Background(), 1, bytesBody(body), id); err != nil {
		t.Fatal(err)
	}
	return segDisk{store: store, id: id, body: body}
}

func newLandRecord(t *testing.T) *journal.Store {
	t.Helper()
	return journal.NewStore(journal.NewBlob(blob.NewMemory(), "n1"), "n1", 1, journal.Options{Backoff: time.Millisecond})
}

// readAll is a store's whole segment as bytes.
func readAll(t *testing.T, store *journal.Store, seq pos.Seg) []byte {
	t.Helper()
	rc, err := store.Get(context.Background(), seq)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// refused says the landing said no and put nothing: the error is the
// segment's own (ErrCorrupt) and the record's number is still free.
func refused(t *testing.T, err error, record *journal.Store) {
	t.Helper()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("land of a segment that cannot be what it says: %v, want ErrCorrupt", err)
	}
	if _, err := record.Head(context.Background(), 1); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the record's number: %v, want nothing there", err)
	}
}

// The landing copies a segment from one store to another once the segment is
// what the formats say it is: each file's own checksum with the decoder, the
// chain of the files of one shard, the footer's entries laying out the bytes
// they sit on, and the trailer under them. It is refused as corrupt
// otherwise, and it lands nothing.
func TestLand(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// Every landing closes its log, whether the segment passed or not.
	dir := tempfile.NewDir(t.TempDir())
	t.Cleanup(func() {
		if n := dir.Open(); n != 0 {
			t.Errorf("%d descriptors left open by the landings", n)
		}
	})

	// One shard's two files chained, and another shard's file resting beside
	// them. Two pages in each file, so a mid-file byte is page bytes and not
	// the header's. No file sits at TXID 1, which is a snapshot's, and every
	// file says the two pages it covers.
	chain := []segFile{
		{shard: "test.users/1", min: 2, max: 2, pre: csum(1), post: csum(2), fill: 'a', fill2: 'p'},
		{shard: "test.users/1", min: 3, max: 3, pre: csum(2), post: csum(3), fill: 'b', fill2: 'q'},
		{shard: "test.users/2", min: 2, max: 2, pre: csum(4), post: csum(5), fill: 'c', fill2: 'r'},
	}

	t.Run("lands a segment of chained files", func(t *testing.T) {
		record := newLandRecord(t)
		src := segSource(t, chain, nil)
		if err := Land(ctx, record, src.store, 1, src.id, dir); err != nil {
			t.Fatal(err)
		}
		if got, want := readAll(t, record, 1), src.body; !bytes.Equal(got, want) {
			t.Fatal("the record's segment is not the source's bytes")
		}
		if h, err := record.Head(ctx, 1); err != nil || h.WriteID != src.id {
			t.Fatalf("the record's segment carries %v, want the source's write id", h)
		}
	})

	t.Run("lands files without checksums", func(t *testing.T) {
		files := slices.Clone(chain)
		for i := range files {
			files[i].noCheck = true
		}
		record := newLandRecord(t)
		src := segSource(t, files, nil)
		if err := Land(ctx, record, src.store, 1, src.id, dir); err != nil {
			t.Fatal(err)
		}
		if got, want := readAll(t, record, 1), src.body; !bytes.Equal(got, want) {
			t.Fatal("the record's segment is not the source's bytes")
		}
	})

	t.Run("a byte out of place is refused", func(t *testing.T) {
		body := segBytes(t, chain, nil)
		mid := len(body) / (3 * 2) // the first file is a third of the segment: half of it is page bytes
		body[mid] ^= 0x5a
		record := newLandRecord(t)
		src := segHeld(t, body)
		if err := Land(ctx, record, src.store, 1, src.id, dir); err != nil {
			refused(t, err, record)
			return
		}
		t.Fatal("a segment with page bytes of no one's truth landed")
	})

	t.Run("a footer off its layout is refused", func(t *testing.T) {
		entries := segEntries(t, chain)
		entries[1].Offset = 1 // the first file's bytes do not end at the second's
		record := newLandRecord(t)
		src := segSource(t, chain, entries)
		refused(t, Land(ctx, record, src.store, 1, src.id, dir), record)
	})

	t.Run("a chain that does not follow is refused", func(t *testing.T) {
		files := slices.Clone(chain)
		files[1].pre = csum(99) // not what the one before left
		record := newLandRecord(t)
		src := segSource(t, files, nil)
		refused(t, Land(ctx, record, src.store, 1, src.id, dir), record)
	})

	t.Run("an empty segment is refused", func(t *testing.T) {
		record := newLandRecord(t)
		src := segHeld(t, []byte{})
		refused(t, Land(ctx, record, src.store, 1, src.id, dir), record)
	})
}
