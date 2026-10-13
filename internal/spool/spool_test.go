package spool_test

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/luci1900/shardlite/internal/spool"
)

func entries(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return len(ents)
}

func TestSmallStaysInMemoryAndLargeSpills(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, tc := range []struct {
		n      int
		inFile bool
	}{{10, false}, {100, false}, {101, true}, {1000, true}} {
		in := bytes.Repeat([]byte{7}, tc.n)
		f, err := spool.Collect(dir, 100, bytes.NewReader(in))
		if err != nil {
			t.Fatal(err)
		}
		if f.InFile() != tc.inFile || f.Size() != int64(tc.n) {
			t.Fatalf("%d bytes: in file %v, size %d", tc.n, f.InFile(), f.Size())
		}
		// Every reader starts at the start and is its own.
		for range 2 {
			got, err := io.ReadAll(f.Reader())
			if err != nil || !bytes.Equal(got, in) {
				t.Fatalf("read back %d bytes, %v", len(got), err)
			}
		}
		if want := map[bool]int{false: 0, true: 1}[tc.inFile]; entries(t, dir) != want {
			t.Fatalf("%d bytes: %d files in the directory", tc.n, entries(t, dir))
		}
		f.Close()
		if entries(t, dir) != 0 {
			t.Fatalf("%d bytes: file left behind", tc.n)
		}
	}
}

func TestZeroLimitIsAlwaysAFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f, err := spool.Collect(dir, 0, strings.NewReader("x"))
	if err != nil || !f.InFile() {
		t.Fatalf("%v %v", f, err)
	}
	f.Close()
}

func TestTheLastReferenceRemovesTheFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f, _ := spool.Collect(dir, 0, strings.NewReader("hello"))
	g := f.Retain()
	f.Close()
	if entries(t, dir) != 1 {
		t.Fatal("removed while a reference was held")
	}
	if b, err := io.ReadAll(g.Reader()); err != nil || string(b) != "hello" {
		t.Fatalf("%q %v", b, err)
	}
	g.Close()
	if entries(t, dir) != 0 {
		t.Fatal("file left behind")
	}
}

func TestAbortRemovesTheFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	w := spool.NewWriter(dir, 4)
	w.Write(bytes.Repeat([]byte{1}, 100))
	if !w.InFile() || entries(t, dir) != 1 {
		t.Fatal("expected a file")
	}
	w.Abort()
	if entries(t, dir) != 0 {
		t.Fatal("file left behind")
	}
}

func TestNoDirectoryIsAnErrorOnlyOnceItIsNeeded(t *testing.T) {
	t.Parallel()
	w := spool.NewWriter("", 10)
	if _, err := w.Write([]byte("small")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(bytes.Repeat([]byte{1}, 100)); err == nil {
		t.Fatal("wrote past the limit with nowhere to spill")
	}
}
