package shardfile

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/superfly/ltx"
	"pgregory.net/rapid"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/spool"
)

// Property tests of the delta arithmetic: reencode (tail files into one file
// over a range of the shard's positions) is a pure function over LTX bytes.

func reencodeBytes(files []capture.TailFile, rg Range) ([]byte, error) {
	var buf bytes.Buffer
	err := reencode(&buf, files, rg)
	return buf.Bytes(), err
}

// fataler is what the helpers need of *testing.T and *rapid.T alike.
type fataler interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// commitSpec is the state a captured file leaves: the database size after it
// and the pages it carries.
type commitSpec struct {
	size  uint32
	pages map[uint32][]byte
}

// model applies specs in order: the latest version of each page, and the
// pages past the final size gone.
func model(specs []commitSpec) (size uint32, pages map[uint32][]byte) {
	pages = map[uint32][]byte{}
	for _, s := range specs {
		maps.Copy(pages, s.pages)
		size = s.size
		for pgno := range pages {
			if pgno > size {
				delete(pages, pgno)
			}
		}
	}
	return size, pages
}

func encodeSpec(t fataler, pageSize int, s commitSpec, minTXID, maxTXID ltx.TXID, ts int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := enc.EncodeHeader(ltx.Header{Version: ltx.Version, Flags: ltx.HeaderFlagNoChecksum, PageSize: uint32(pageSize), Commit: s.size, MinTXID: minTXID, MaxTXID: maxTXID, Timestamp: ts}); err != nil {
		t.Fatal(err)
	}
	for _, pgno := range slices.Sorted(maps.Keys(s.pages)) {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, s.pages[pgno]); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// decoded is an LTX file's header and pages, in file order.
type decoded struct {
	hdr   ltx.Header
	order []uint32
	pages map[uint32][]byte
}

func decode(t fataler, b []byte) decoded {
	t.Helper()
	dec := ltx.NewDecoder(bytes.NewReader(b))
	if err := dec.DecodeHeader(); err != nil {
		t.Fatalf("decode header: %v", err)
	}
	d := decoded{hdr: dec.Header(), pages: map[uint32][]byte{}}
	for {
		var ph ltx.PageHeader
		buf := make([]byte, d.hdr.PageSize)
		if err := dec.DecodePage(&ph, buf); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("decode page: %v", err)
		}
		d.order = append(d.order, ph.Pgno)
		d.pages[ph.Pgno] = buf
	}
	if err := dec.Close(); err != nil {
		t.Fatalf("file checksum: %v", err)
	}
	return d
}

// pageContents fails the test unless d holds exactly the model's pages.
func (d decoded) matches(t *rapid.T, size uint32, pages map[uint32][]byte) {
	t.Helper()
	if d.hdr.Commit != size {
		t.Fatalf("database size %d pages, want %d", d.hdr.Commit, size)
	}
	if !slices.IsSorted(d.order) {
		t.Fatalf("pages not in order: %v", d.order)
	}
	if len(d.pages) != len(pages) {
		t.Fatalf("pages %v, want %v", slices.Sorted(maps.Keys(d.pages)), slices.Sorted(maps.Keys(pages)))
	}
	for pgno, want := range pages {
		if !bytes.Equal(d.pages[pgno], want) {
			t.Fatalf("page %d differs", pgno)
		}
	}
}

// run is a generated sequence of captured files starting at a position.
type run struct {
	pageSize int
	tails    []capture.TailFile
	specs    []commitSpec // one per tail file
	start    pos.Position
}

func (r run) rangeOf(i, j int) Range { // tail files i..j-1
	last := r.tails[j-1]
	return Range{r.tails[i].Position, last.Position.Add(last.Count)}
}

// genRun draws a database size and page size and a sequence of commits (each a
// set of pages, some repeated across commits, the database growing and
// shrinking), encoded with transaction numbers of their own in the header and
// the shard's positions beside them, to check that the numbers inside the
// files are never used.
func genRun(t *rapid.T) run {
	r := run{pageSize: rapid.SampledFrom([]int{512, 1024, 4096}).Draw(t, "pageSize")}
	r.start = pos.Position(rapid.SampledFrom([]uint64{1, 1, 1, 2, 17}).Draw(t, "startPosition"))
	n := rapid.IntRange(1, 10).Draw(t, "files")
	var prev uint32
	if r.start > 1 { // a delta run after a restore: the database already has pages
		prev = uint32(rapid.IntRange(1, 12).Draw(t, "sizeBefore"))
	}
	// A file that starts at position 1 is a snapshot, so its numbers start
	// at 1; a delta run's start elsewhere.
	at, txid := r.start, ltx.TXID(1)
	if r.start > 1 {
		txid = ltx.TXID(rapid.IntRange(2, 1000).Draw(t, "firstTXID"))
	}
	for i := range n {
		size := prev
		snapshot := prev == 0
		switch {
		case snapshot:
			size = uint32(rapid.IntRange(1, 12).Draw(t, "initialSize"))
		default:
			size = uint32(max(1, min(16, int(prev)+rapid.IntRange(-4, 4).Draw(t, "sizeChange"))))
		}
		fill := rapid.Byte().Draw(t, "fill")
		spec := commitSpec{size: size, pages: map[uint32][]byte{}}
		for pgno := uint32(1); pgno <= size; pgno++ {
			if snapshot || pgno > prev || rapid.Bool().Draw(t, "writePage") {
				page := bytes.Repeat([]byte{fill ^ byte(pgno)}, r.pageSize)
				copy(page, []byte{byte(i), byte(pgno), fill, byte(at)})
				spec.pages[pgno] = page
			}
		}
		count := pos.Count(rapid.IntRange(1, 3).Draw(t, "commits"))
		data := encodeSpec(t, r.pageSize, spec, txid, txid+ltx.TXID(count)-1, int64(i+1))
		r.tails = append(r.tails, capture.TailFile{Position: at, Count: count, Body: spool.Bytes(data)})
		r.specs = append(r.specs, spec)
		at += pos.Position(count)
		txid += ltx.TXID(count)
		prev = size
	}
	return r
}

var applyCases atomic.Int64

// Reencoding tail files over a range keeps only that range and gives the pages
// that applying the tail files one by one gives.
func TestReencodeEqualsApplyingTheTail(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		r := genRun(t)
		rg := r.rangeOf(0, len(r.tails))
		data, err := reencodeBytes(r.tails, rg)
		if err != nil {
			t.Fatal(err)
		}
		d := decode(t, data)
		if d.hdr.MinTXID != ltx.TXID(rg.Min) || d.hdr.MaxTXID != ltx.TXID(rg.Max) {
			t.Fatalf("range %d-%d in the header, want %s", d.hdr.MinTXID, d.hdr.MaxTXID, rg)
		}
		if d.hdr.PageSize != uint32(r.pageSize) {
			t.Fatalf("page size %d", d.hdr.PageSize)
		}
		size, pages := model(r.specs)
		d.matches(t, size, pages)

		// Applying goes through a file and an fsync each time (a full fsync on macOS),
		// so only every eighth case does it.
		if r.start != 1 || applyCases.Add(1)%8 != 0 {
			return
		}
		// From position 1 the files make a whole database: applying the tail files
		// one by one and applying the result give the same file.
		dir, err := os.MkdirTemp("", "arith")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		applyAll := func(name string, files [][]byte, ranges []Range) []byte {
			a, err := NewApplier(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			for i, f := range files {
				if err := a.Apply(bytes.NewReader(f), ranges[i]); err != nil {
					a.Abort()
					t.Fatalf("apply %s: %v", ranges[i], err)
				}
			}
			st, err := a.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if st.Position != rg.Max {
				t.Fatalf("position %d, want %d", st.Position, rg.Max)
			}
			b, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			return b
		}
		var files [][]byte
		var ranges []Range
		for i, tf := range r.tails {
			b, err := tf.Body.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			files, ranges = append(files, b), append(ranges, r.rangeOf(i, i+1))
		}
		oneByOne := applyAll("one", files, ranges)
		merged := applyAll("merged", [][]byte{data}, []Range{rg})
		if !bytes.Equal(oneByOne, merged) {
			t.Fatalf("applying the merged file differs from applying %d files one by one", len(files))
		}
	})
}

// manifestFiles groups a run's tail files into the files of a manifest (deltas
// written one after another), each from reencode over its own range.
func manifestFiles(t *rapid.T, r run) (files []File, data [][]byte, groups [][]commitSpec) {
	epoch := pos.Epoch(rapid.IntRange(1, 1<<30).Draw(t, "epoch"))
	for i := 0; i < len(r.tails); {
		j := i + rapid.IntRange(1, len(r.tails)-i).Draw(t, "filesInDelta")
		rg := r.rangeOf(i, j)
		b, err := reencodeBytes(r.tails[i:j], rg)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, File{rg, epoch + pos.Epoch(len(files))})
		data, groups = append(data, b), append(groups, r.specs[i:j])
		i = j
	}
	return files, data, groups
}

// The manifest's file list built from these is contiguous and round-trips
// through the manifest's JSON.
func TestManifestListFromADeltaRun(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		r := genRun(t)
		files, _, _ := manifestFiles(t, r)
		m := &Manifest{Files: files}
		b, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		var stored Manifest
		if err := json.Unmarshal(b, &stored); err != nil {
			t.Fatal(err)
		}
		got := stored.Files
		if !slices.Equal(got, files) {
			t.Fatalf("list %v, want %v", got, files)
		}
		for i, f := range got {
			if i == 0 && f.Min != r.start || i > 0 && f.Min != got[i-1].Max+1 {
				t.Fatalf("file %v does not follow %v (run starts at %d)", f, got[:i], r.start)
			}
			again, err := ParseFile(f.String())
			if err != nil || again != f {
				t.Fatalf("%q parses as %v, %v", f.String(), again, err)
			}
		}
		if m.LastPosition() != got[len(got)-1].Max {
			t.Fatalf("last position %d", m.LastPosition())
		}
	})
}

func TestParseFileRoundTrip(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		min := pos.Position(rapid.Uint64Range(1, 1<<62).Draw(t, "min"))
		f := File{Range{min, min + pos.Position(rapid.Uint64Range(0, 1<<40).Draw(t, "extra"))}, pos.Epoch(rapid.Uint64().Draw(t, "epoch"))}
		got, err := ParseFile(f.String())
		if err != nil || got != f {
			t.Fatalf("%q parses as %v, %v", f.String(), got, err)
		}
	})
}
