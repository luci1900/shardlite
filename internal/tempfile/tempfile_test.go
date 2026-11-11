package tempfile

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
)

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.Uint32())
	}
	return b
}

func entries(t *testing.T, dir *Dir) int {
	t.Helper()
	ents, err := os.ReadDir(dir.Path())
	if err != nil {
		t.Fatal(err)
	}
	return len(ents)
}

func appendBytes(t *testing.T, l *Log, b []byte) File {
	t.Helper()
	a, err := l.Append()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write(b); err != nil {
		t.Fatal(err)
	}
	f, err := a.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestAppendsReadBack(t *testing.T) {
	t.Parallel()
	// Sizes on both sides of the buffer, and of the old 4 MB limit.
	for _, n := range []int{0, 1, 4 << 10, bufferSize - 1, bufferSize, bufferSize + 1, 4<<20 - 1, 4 << 20, 6 << 20} {
		dir := NewDir(t.TempDir())
		l, err := dir.Create()
		if err != nil {
			t.Fatal(err)
		}
		want := payload(n)
		f := appendBytes(t, l, want)
		if f.Size() != int64(n) {
			t.Fatalf("%d bytes: size %d", n, f.Size())
		}
		got, err := f.Bytes()
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%d bytes: read back %d bytes, %v", n, len(got), err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTheDirectoryStaysEmpty(t *testing.T) {
	t.Parallel()
	dir := NewDir(t.TempDir() + "/not-yet")
	l, err := dir.Create()
	if err != nil {
		t.Fatal(err)
	}
	appendBytes(t, l, payload(1<<20))
	d, err := l.Dup()
	if err != nil {
		t.Fatal(err)
	}
	if n := entries(t, dir); n != 0 {
		t.Fatalf("%d entries while a log and its dup are open", n)
	}
	l.Close()
	d.Close()
	c, _, err := dir.Build(fillWith(payload(10)))
	if err != nil {
		t.Fatal(err)
	}
	if n := entries(t, dir); n != 0 {
		t.Fatalf("%d entries while a collected log is open", n)
	}
	c.Close()
	if n := entries(t, dir); n != 0 {
		t.Fatalf("%d entries after every close", n)
	}
}

func TestRangesAreStableAcrossLaterAppends(t *testing.T) {
	t.Parallel()
	l, err := NewDir(t.TempDir()).Create()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var files []File
	var want [][]byte
	for _, n := range []int{10, 300 << 10, 1, 5000} {
		b := payload(n)
		files, want = append(files, appendBytes(t, l, b)), append(want, b)
	}
	for i, f := range files {
		if got, err := f.Bytes(); err != nil || !bytes.Equal(got, want[i]) {
			t.Fatalf("file %d: %d bytes, %v", i, len(got), err)
		}
	}
	if l.Size() != 10+300<<10+1+5000 {
		t.Fatalf("the log is %d bytes", l.Size())
	}
}

func TestADupOutlivesTheOriginal(t *testing.T) {
	t.Parallel()
	l, err := NewDir(t.TempDir()).Create()
	if err != nil {
		t.Fatal(err)
	}
	want := payload(1000)
	f := appendBytes(t, l, want)
	d, err := l.Dup()
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := f.In(d).Bytes()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("read through the dup: %d bytes, %v", len(got), err)
	}
	if _, err := d.Append(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("append to a dup: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClosedLogsAreErrorsNotPanics(t *testing.T) {
	t.Parallel()
	l, err := NewDir(t.TempDir()).Create()
	if err != nil {
		t.Fatal(err)
	}
	f := appendBytes(t, l, payload(10))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err == nil {
		t.Fatal("a second close succeeded")
	}
	if _, err := l.Dup(); err == nil {
		t.Fatal("a dup of a closed log succeeded")
	}
	if _, err := f.Bytes(); err == nil {
		t.Fatal("a read of a closed log succeeded")
	}
}

func TestAbortCutsBackAndTheNextAppendReusesTheOffset(t *testing.T) {
	t.Parallel()
	l, err := NewDir(t.TempDir()).Create()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	first := appendBytes(t, l, payload(100))
	a, err := l.Append()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write(payload(bufferSize + 5)); err != nil { // some of it reaches the file
		t.Fatal(err)
	}
	a.Abort()
	a.Abort() // a no-op
	if st, err := l.f.Stat(); err != nil || st.Size() != 100 {
		t.Fatalf("after the abort the file is %v bytes, %v", st.Size(), err)
	}
	want := payload(50)
	second := appendBytes(t, l, want)
	if second.off != first.size {
		t.Fatalf("the next append began at %d, want %d", second.off, first.size)
	}
	if got, _ := second.Bytes(); !bytes.Equal(got, want) {
		t.Fatal("the next append reads back wrong")
	}
	if _, err := a.Finish(); err == nil {
		t.Fatal("an aborted append was finished")
	}
}

func TestOneAppendIsOpenAtATime(t *testing.T) {
	t.Parallel()
	l, err := NewDir(t.TempDir()).Create()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	a, err := l.Append()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(); err == nil {
		t.Fatal("a second append was opened")
	}
	a.Abort()
	if _, err := l.Append(); err != nil {
		t.Fatalf("append after an abort: %v", err)
	}
}

func TestAFailedWriteBreaksTheLog(t *testing.T) {
	t.Parallel()
	dir := NewDir(t.TempDir())
	l, err := dir.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	boom := errors.New("disk full")
	dir.SetHook(func(op Op) error {
		if op == OpAppend {
			return boom
		}
		return nil
	})
	a, err := l.Append()
	if err != nil {
		t.Fatal(err)
	}
	a.Write(payload(10))
	if _, err := a.Finish(); !errors.Is(err, boom) {
		t.Fatalf("finish: %v, want the write's error", err)
	}
	dir.SetHook(nil)
	if l.Broken() == nil {
		t.Fatal("the log is not broken")
	}
	if _, err := l.Append(); !errors.Is(err, ErrBroken) || !errors.Is(err, boom) {
		t.Fatalf("append to a broken log: %v", err)
	}
	// A large write fails at the write that fills the buffer, and goes on failing.
	l2, err := dir.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	dir.SetHook(func(op Op) error {
		if op == OpAppend {
			return boom
		}
		return nil
	})
	a, _ = l2.Append()
	if _, err := a.Write(payload(bufferSize + 1)); !errors.Is(err, boom) {
		t.Fatalf("write: %v", err)
	}
	if _, err := a.Write(payload(1)); !errors.Is(err, boom) {
		t.Fatalf("write after a failure: %v", err)
	}
	a.Abort()
}

func TestTheHookFailsACreateAndADup(t *testing.T) {
	t.Parallel()
	dir := NewDir(t.TempDir())
	l, err := dir.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	boom := errors.New("injected")
	dir.SetHook(func(op Op) error {
		if op == OpCreate || op == OpDup {
			return boom
		}
		return nil
	})
	if _, err := dir.Create(); !errors.Is(err, boom) {
		t.Fatalf("create: %v", err)
	}
	if _, _, err := dir.Build(fillWith(nil)); !errors.Is(err, boom) {
		t.Fatalf("build: %v", err)
	}
	if _, err := l.Dup(); !errors.Is(err, boom) {
		t.Fatalf("dup: %v", err)
	}
	dir.SetHook(nil)
	if d, err := l.Dup(); err != nil {
		t.Fatalf("dup after the hook is gone: %v", err)
	} else {
		d.Close()
	}
}

func TestBuildFillsALogOfItsOwn(t *testing.T) {
	t.Parallel()
	dir := NewDir(t.TempDir())
	want := payload(3<<20 + 7)
	l, f, err := dir.Build(fillWith(want))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got, err := f.Bytes()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("built %d bytes, %v", len(got), err)
	}
	boom := errors.New("fill broke")
	if _, _, err := dir.Build(func(w io.Writer) error {
		if _, err := w.Write(payload(100)); err != nil {
			return err
		}
		return boom
	}); !errors.Is(err, boom) {
		t.Fatalf("build of a failing fill: %v", err)
	}
	if got := dir.Open(); got != 1 {
		t.Fatalf("%d open after a failed build, want only the first log", got)
	}
}

// fillWith is a fill that writes b.
func fillWith(b []byte) func(io.Writer) error {
	return func(w io.Writer) error { _, err := w.Write(b); return err }
}

func TestConcurrentReadersShareADescriptor(t *testing.T) {
	t.Parallel()
	l, err := NewDir(t.TempDir()).Create()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var files []File
	var want [][]byte
	for range 8 {
		b := payload(100 << 10)
		files, want = append(files, appendBytes(t, l, b)), append(want, b)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, f := range files {
				if got, err := f.Bytes(); err != nil || !bytes.Equal(got, want[i]) {
					errs <- errors.New("a reader got other bytes")
					return
				}
			}
		}()
	}
	// The owner keeps appending while they read.
	for range 8 {
		appendBytes(t, l, payload(100<<10))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestOpenCountsTheDescriptorsNotYetClosed(t *testing.T) {
	t.Parallel()
	dir := NewDir(t.TempDir())
	l, err := dir.Create()
	if err != nil {
		t.Fatal(err)
	}
	d, err := l.Dup()
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := dir.Build(fillWith(payload(10)))
	if err != nil {
		t.Fatal(err)
	}
	if got := dir.Open(); got != 3 {
		t.Fatalf("%d open with a log, its dup and a built log", got)
	}
	l.Close()
	l.Close() // an error, and no second decrement
	d.Close()
	c.Close()
	if got := dir.Open(); got != 0 {
		t.Fatalf("%d open after every close", got)
	}
	// A failed build closes its log too.
	boom := errors.New("fill broke")
	if _, _, err := dir.Build(func(io.Writer) error { return boom }); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	if got := dir.Open(); got != 0 {
		t.Fatalf("%d open after a failed build", got)
	}
}
