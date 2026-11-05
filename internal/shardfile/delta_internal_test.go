package shardfile

import (
	"bytes"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// encoded is an LTX file with the checksum of the database before it, the one
// after it, and the pages it holds. A file with neither checksum has none.
func encoded(t *testing.T, pageSize, commit uint32, pre, post ltx.Checksum, pages map[uint32][]byte, minTXID, maxTXID ltx.TXID) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := ltx.NewEncoder(&buf)
	if err != nil {
		t.Fatal(err)
	}
	hdr := ltx.Header{Version: ltx.Version, PageSize: pageSize, Commit: commit, MinTXID: minTXID, MaxTXID: maxTXID, Timestamp: 1}
	if pre != 0 {
		hdr.PreApplyChecksum = pre
	}
	if pre == 0 && post == 0 {
		hdr.Flags = ltx.HeaderFlagNoChecksum
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		t.Fatal(err)
	}
	for _, pgno := range slices.Sorted(maps.Keys(pages)) {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pages[pgno]); err != nil {
			t.Fatal(err)
		}
	}
	enc.SetPostApplyChecksum(post)
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

var (
	bodiesMu  sync.Mutex
	bodiesLog *tempfile.Log
)

// bodyOf is b as a range of a log shared by the whole test process (appended to
// under a lock, read by anyone), the way a tail file is one of its shard's log.
func bodyOf(b []byte) tempfile.File {
	bodiesMu.Lock()
	defer bodiesMu.Unlock()
	if bodiesLog == nil {
		l, err := tempfile.NewDir(os.TempDir()).Create()
		if err != nil {
			panic(err)
		}
		bodiesLog = l
	}
	a, err := bodiesLog.Append()
	if err != nil {
		panic(err)
	}
	a.Write(b)
	f, err := a.Finish()
	if err != nil {
		panic(err)
	}
	return f
}

// tailFiles are the captured files a delta is built from.
func tailFiles(t *testing.T, bodies ...[]byte) []capture.TailFile {
	t.Helper()
	out := make([]capture.TailFile, len(bodies))
	at := pos.Position(1)
	for i, b := range bodies {
		out[i] = capture.TailFile{Position: at, Count: 1, Body: bodyOf(b)}
		at++
	}
	return out
}

// A delta built from tail files that do not chain fails: a file that does not
// follow the one before it is a bug in what was written, and the delta is not
// written.
func TestDeltaFromFilesThatDoNotChainFails(t *testing.T) {
	t.Parallel()
	const pageSize = 4096
	page := make([]byte, pageSize)
	first := encoded(t, pageSize, 1, ltx.ChecksumFlag|1, ltx.ChecksumFlag|2, map[uint32][]byte{1: page}, 2, 2)
	// The second file's checksum before it is not the first one's after it.
	second := encoded(t, pageSize, 1, ltx.ChecksumFlag|3, ltx.ChecksumFlag|4, map[uint32][]byte{1: page}, 3, 3)
	files := tailFiles(t, first, second)
	var out bytes.Buffer
	err := reencode(&out, files, Range{2, 3})
	if err == nil {
		t.Fatal("the delta of files that do not chain was written")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("does not follow")) {
		t.Fatalf("the delta failed with %q, want it to say the file does not follow the one before it", err)
	}
}

// A delta built from tail files that chain carries their checksums: the first
// file's before and the last file's after.
func TestDeltaOfAChainCarriesItsChecksums(t *testing.T) {
	t.Parallel()
	const pageSize = 4096
	page := make([]byte, pageSize)
	first := encoded(t, pageSize, 1, ltx.ChecksumFlag|1, ltx.ChecksumFlag|2, map[uint32][]byte{1: page}, 2, 2)
	second := encoded(t, pageSize, 1, ltx.ChecksumFlag|2, ltx.ChecksumFlag|3, map[uint32][]byte{1: page}, 3, 3)
	files := tailFiles(t, first, second)
	var out bytes.Buffer
	if err := reencode(&out, files, Range{2, 3}); err != nil {
		t.Fatal(err)
	}
	dec := ltx.NewDecoder(bytes.NewReader(out.Bytes()))
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	hdr := dec.Header()
	if hdr.NoChecksum() {
		t.Fatal("the delta has no checksums")
	}
	if want := ltx.ChecksumFlag | 1; hdr.PreApplyChecksum != want {
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
	if want := ltx.ChecksumFlag | 3; dec.Trailer().PostApplyChecksum != want {
		t.Fatalf("the delta's checksum after it is %s, want %s", dec.Trailer().PostApplyChecksum, want)
	}
}

// A delta over a tail file without checksums fails, naming the file.
func TestDeltaOfFilesWithoutChecksumsFails(t *testing.T) {
	t.Parallel()
	const pageSize = 4096
	page := make([]byte, pageSize)
	first := encoded(t, pageSize, 1, ltx.ChecksumFlag|1, ltx.ChecksumFlag|2, map[uint32][]byte{1: page}, 2, 2)
	second := encoded(t, pageSize, 1, 0, 0, map[uint32][]byte{1: page}, 3, 3)
	files := tailFiles(t, first, second)
	var out bytes.Buffer
	err := reencode(&out, files, Range{2, 3})
	if err == nil || !strings.Contains(err.Error(), "position 2 has no checksums") {
		t.Fatalf("reencode of files without checksums: %v, want it to name the file at position 2", err)
	}
}
