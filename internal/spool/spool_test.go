package spool_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"testing/iotest"

	"golang.org/x/sync/errgroup"

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

func mustPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("did not panic, want %q", want)
		}
		if got := fmt.Sprint(r); !strings.Contains(got, want) {
			t.Fatalf("panicked with %q, want %q", got, want)
		}
	}()
	fn()
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

func TestRetainAfterLastClosePanics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		limit int
	}{{"memory", 100}, {"file", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := spool.Collect(t.TempDir(), tc.limit, strings.NewReader("hello"))
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			mustPanic(t, "retain after the last close", func() { f.Retain() })
		})
	}
}

func TestReadAfterLastClosePanics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		limit int
	}{{"memory", 100}, {"file", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := spool.Collect(t.TempDir(), tc.limit, strings.NewReader("hello"))
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			mustPanic(t, "read after the last close", func() { f.Reader() })
			mustPanic(t, "read after the last close", func() { _, _ = f.Bytes() })
		})
	}
}

func TestCloseMoreOftenThanRetainedPanics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		limit int
	}{{"memory", 100}, {"file", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := spool.Collect(t.TempDir(), tc.limit, strings.NewReader("hello"))
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			mustPanic(t, "closed more often than retained", func() { _ = f.Close() })
		})
	}
}

func TestConcurrentReadersAndCloses(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const readers = 8
	want := bytes.Repeat([]byte("hello"), 1000)
	f, err := spool.Collect(dir, 0, bytes.NewReader(want))
	if err != nil {
		t.Fatal(err)
	}
	refs := make([]*spool.File, readers)
	for i := range refs {
		refs[i] = f.Retain()
	}
	start := make(chan struct{})
	var g errgroup.Group
	for _, ref := range refs {
		g.Go(func() error {
			defer ref.Close()
			<-start
			got, err := io.ReadAll(ref.Reader())
			if err != nil {
				return err
			}
			if !bytes.Equal(got, want) {
				return fmt.Errorf("read %d bytes, want %d", len(got), len(want))
			}
			return nil
		})
	}
	close(start)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	if entries(t, dir) != 0 {
		t.Fatal("file left behind")
	}
}

func TestBytesReturnsCopy(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		limit int
	}{{"memory", 100}, {"file", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := spool.Collect(t.TempDir(), tc.limit, strings.NewReader("hello"))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			first, err := f.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			first[0] = 'x'
			second, err := f.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if string(second) != "hello" {
				t.Fatalf("Bytes returned %q after the first copy was changed", second)
			}
		})
	}
}

func TestWriterIsDoneAfterFinish(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		limit  int
		inFile bool
	}{{"memory", 100, false}, {"file", 0, true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			w := spool.NewWriter(dir, tc.limit)
			in := []byte("hello")
			if _, err := w.Write(in); err != nil {
				t.Fatal(err)
			}
			if w.InFile() != tc.inFile {
				t.Fatalf("in file %v, want %v", w.InFile(), tc.inFile)
			}
			f, err := w.Finish()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write([]byte("more")); err == nil {
				t.Fatal("Write after Finish succeeded")
			}
			if _, err := w.Finish(); err == nil {
				t.Fatal("second Finish succeeded")
			}
			w.Abort()
			got, err := f.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, in) {
				t.Fatalf("got %q, want %q", got, in)
			}
			if tc.inFile && entries(t, dir) != 1 {
				t.Fatal("file removed while the File was held")
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if entries(t, dir) != 0 {
				t.Fatal("file left behind")
			}
		})
	}
}

func TestWriterFinishAfterAbort(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		limit int
	}{{"memory", 100}, {"file", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			w := spool.NewWriter(dir, tc.limit)
			if _, err := w.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			w.Abort()
			if entries(t, dir) != 0 {
				t.Fatal("file left behind")
			}
			if _, err := w.Write([]byte("x")); err == nil {
				t.Fatal("Write after Abort succeeded")
			}
			if _, err := w.Finish(); err == nil {
				t.Fatal("Finish after Abort succeeded")
			}
			w.Abort()
		})
	}
}

func TestWriterWriteErrorSticks(t *testing.T) {
	t.Parallel()
	w := spool.NewWriter("", 10)
	if _, err := w.Write([]byte("small")); err != nil {
		t.Fatal(err)
	}
	_, first := w.Write(bytes.Repeat([]byte{1}, 100))
	if first == nil {
		t.Fatal("write past the limit with no directory succeeded")
	}
	if _, err := w.Write([]byte("x")); !errors.Is(err, first) {
		t.Fatalf("second Write returned %v, want %v", err, first)
	}
	if _, err := w.Finish(); !errors.Is(err, first) {
		t.Fatalf("Finish returned %v, want %v", err, first)
	}
	w.Abort()
}

func TestCollectReadErrorRemovesFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	boom := errors.New("boom")
	r := io.MultiReader(strings.NewReader(strings.Repeat("x", 100)), iotest.ErrReader(boom))
	if _, err := spool.Collect(dir, 0, r); !errors.Is(err, boom) {
		t.Fatalf("Collect returned %v, want %v", err, boom)
	}
	if entries(t, dir) != 0 {
		t.Fatal("file left behind")
	}
}
