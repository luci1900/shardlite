package blob

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func newLocal(t *testing.T) *Local {
	t.Helper()
	l, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// newStore returns a Store over a fresh local backend, with short backoffs.
func newStore(t *testing.T) (*Store, *Local) {
	t.Helper()
	l := newLocal(t)
	return NewStore(l, "n1", 1, Options{Backoff: time.Millisecond}), l
}

func readAll(t *testing.T, rc io.ReadCloser) string {
	t.Helper()
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// A Backend maps hook failures onto outcomes: before the operation is "not
// applied", after it is "unknown" (the write happened, the response was lost).
func TestHookOutcomes(t *testing.T) {
	l, ctx := newLocal(t), context.Background()
	boom := errors.New("boom")
	l.SetHook(func(ctx context.Context, op Op, key string, after bool) error {
		if op == OpCreate && key == "before" && !after || op == OpCreate && key == "after" && after {
			return boom
		}
		return nil
	})
	if _, err := l.Create(ctx, "before", strings.NewReader("v"), WriteID{}); !errors.Is(err, ErrNotApplied) {
		t.Fatalf("got %v, want ErrNotApplied", err)
	}
	if _, err := l.Head(ctx, "before"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a not-applied create wrote the object: %v", err)
	}
	if _, err := l.Create(ctx, "after", strings.NewReader("v"), WriteID{}); !errors.Is(err, ErrUnknown) {
		t.Fatalf("got %v, want ErrUnknown", err)
	}
	if _, err := l.Head(ctx, "after"); err != nil {
		t.Fatalf("an unknown-outcome create should have happened: %v", err)
	}
}

func TestWriteIDStoredWithObject(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	s.Create(ctx, "a", strings.NewReader("one"))
	info, err := s.Head(ctx, "a")
	if err != nil || info.WriteID != (WriteID{Node: "n1", Epoch: 1, N: 1}) {
		t.Fatalf("head: %+v %v", info, err)
	}
	if _, err := s.Update(ctx, "a", strings.NewReader("two"), info.ETag); err != nil {
		t.Fatal(err)
	}
	for info, err := range s.List(ctx, "a") {
		if err != nil || info.WriteID.N != 2 {
			t.Fatalf("list: %+v %v", info, err)
		}
	}
}

// TestCreateRace starts two processes that Create the same key. Exactly one wins.
func TestCreateRace(t *testing.T) {
	if dir := os.Getenv("BLOB_RACE_DIR"); dir != "" {
		l, err := NewLocal(dir)
		if err != nil {
			t.Fatal(err)
		}
		s := NewStore(l, "proc"+os.Getenv("BLOB_RACE_ID"), 1, Options{})
		_, err = s.Create(context.Background(), "race", strings.NewReader(os.Getenv("BLOB_RACE_ID")))
		switch {
		case err == nil:
			os.Stdout.WriteString("won\n")
		case errors.Is(err, ErrExists):
			os.Stdout.WriteString("lost\n")
		default:
			t.Fatal(err)
		}
		return
	}
	for round := 0; round < 20; round++ {
		dir := t.TempDir()
		outs := make([]string, 2)
		var wg sync.WaitGroup
		for i := range outs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				cmd := exec.Command(os.Args[0], "-test.run=^TestCreateRace$")
				cmd.Env = append(os.Environ(), "BLOB_RACE_DIR="+dir, "BLOB_RACE_ID="+string(rune('a'+i)))
				b, err := cmd.Output()
				if err != nil {
					t.Error(err)
				}
				outs[i] = string(b)
			}()
		}
		wg.Wait()
		wins := 0
		for _, o := range outs {
			if strings.Contains(o, "won") {
				wins++
			}
		}
		if wins != 1 {
			t.Fatalf("round %d: %d winners (%q)", round, wins, outs)
		}
	}
}

func TestNoSyncStillWritesAndReads(t *testing.T) {
	l, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	l.NoSync = true
	ctx := context.Background()
	etag, err := l.Create(ctx, "a/b", strings.NewReader("hello"), WriteID{Node: "n", Epoch: 1, N: 1})
	if err != nil {
		t.Fatal(err)
	}
	info, err := l.Head(ctx, "a/b")
	if err != nil || info.ETag != etag || info.Size != 5 || info.WriteID.N != 1 {
		t.Fatalf("%+v %v", info, err)
	}
}

func TestWithPrefix(t *testing.T) {
	l, err := NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b := WithPrefix(l, "staging/")
	ctx := context.Background()
	if _, err := b.Create(ctx, "nodes/n1", strings.NewReader("x"), WriteID{}); err != nil {
		t.Fatal(err)
	}
	// It is stored under the prefix, and invisible without it.
	if _, err := l.Head(ctx, "staging/nodes/n1"); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Head(ctx, "nodes/n1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("head without the prefix: %v", err)
	}
	var keys []string
	for info, err := range b.List(ctx, "nodes/") {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, info.Key)
	}
	if len(keys) != 1 || keys[0] != "nodes/n1" {
		t.Fatalf("listing: %v", keys)
	}
}

// The ETag is the MD5 of the contents, as on S3: identical bytes keep it
// (callers treat ETags as opaque, see design.md) and different bytes change it.
func TestETagIsTheContentsMD5(t *testing.T) {
	s, _ := newStore(t)
	ctx := context.Background()
	e1, err := s.Create(ctx, "k", strings.NewReader("same"))
	if err != nil {
		t.Fatal(err)
	}
	sum := md5.Sum([]byte("same"))
	if string(e1) != hex.EncodeToString(sum[:]) {
		t.Fatalf("ETag %q is not the MD5", e1)
	}
	e2, err := s.Update(ctx, "k", strings.NewReader("same"), e1)
	if err != nil || e2 != e1 {
		t.Fatalf("identical bytes: %q %v", e2, err)
	}
	e3, err := s.Update(ctx, "k", strings.NewReader("other"), e1)
	if err != nil || e3 == e1 {
		t.Fatalf("different bytes: %q %v", e3, err)
	}
	if _, err := s.Update(ctx, "k", strings.NewReader("x"), e1); !errors.Is(err, ErrConflict) {
		t.Fatalf("update with the old ETag: got %v, want ErrConflict", err)
	}
}

func TestAttributeRoundTrip(t *testing.T) {
	l, ctx := newLocal(t), context.Background()
	for _, id := range []WriteID{{Node: "n1", Epoch: 3, N: 9}, {Node: "a:b c", Epoch: 1, N: 2}, {}} {
		etag, err := l.Create(ctx, "k", strings.NewReader("v"), id)
		if err != nil {
			t.Fatal(err)
		}
		info, err := l.Head(ctx, "k")
		if err != nil || info.ETag != etag || info.WriteID != id {
			t.Fatalf("id %+v: head %+v %v", id, info, err)
		}
		rc, gotETag, err := l.Get(ctx, "k")
		if err != nil || gotETag != etag {
			t.Fatalf("get: %q %v", gotETag, err)
		}
		rc.Close()
		if err := l.Delete(ctx, "k", ""); err != nil {
			t.Fatal(err)
		}
	}
}

// A directory copied by a tool that drops extended attributes still works:
// ETags are the same as before, write ids are zero, and the attributes are back.
func TestDirectoryCopiedWithoutAttributes(t *testing.T) {
	src, ctx := newLocal(t), context.Background()
	want := map[string]ETag{}
	for k, body := range map[string]string{"journal/n/1": "seg", "nodes/n": "lease", "fence": ""} {
		etag, err := src.Create(ctx, k, strings.NewReader(body), WriteID{Node: "n", Epoch: 1, N: 1})
		if err != nil {
			t.Fatal(err)
		}
		want[k] = etag
	}
	dst := t.TempDir()
	err := filepath.WalkDir(src.dir, func(p string, d fs.DirEntry, err error) error {
		rel, _ := filepath.Rel(src.dir, p)
		if err != nil || d.IsDir() || strings.HasPrefix(rel, ".") {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dst, rel)), 0o755); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewLocal(dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lgetxattr(filepath.Join(dst, "nodes/n")); err == nil {
		t.Fatal("the copy kept its attribute")
	}
	// List repairs; so do Head, Get and Update on other objects.
	n := 0
	for info, err := range l.List(ctx, "journal/") {
		if err != nil || info.ETag != want[info.Key] || !info.WriteID.IsZero() {
			t.Fatalf("list: %+v %v", info, err)
		}
		n++
	}
	if n != 1 {
		t.Fatalf("listed %d", n)
	}
	if info, err := l.Head(ctx, "fence"); err != nil || info.ETag != want["fence"] || !info.WriteID.IsZero() {
		t.Fatalf("head: %+v %v", info, err)
	}
	if rc, etag, err := l.Get(ctx, "nodes/n"); err != nil || etag != want["nodes/n"] || readAll(t, rc) != "lease" {
		t.Fatalf("get: %q %v", etag, err)
	}
	for k := range want {
		if _, err := lgetxattr(filepath.Join(dst, k)); err != nil {
			t.Fatalf("%s: attribute not restored: %v", k, err)
		}
	}
	// Now they are plain objects: no contents are read to answer Head or List.
	before := l.contentReads.Load()
	for _, err := range l.List(ctx, "") {
		if err != nil {
			t.Fatal(err)
		}
	}
	l.Head(ctx, "nodes/n")
	if l.contentReads.Load() != before {
		t.Fatal("Head or List read contents after the repair")
	}
	// Update works on a repaired object, and on one that lost its attribute again.
	if err := unix.Removexattr(filepath.Join(dst, "nodes/n"), writeAttr); err != nil {
		t.Fatal(err)
	}
	if _, err := l.Update(ctx, "nodes/n", strings.NewReader("new"), want["nodes/n"], WriteID{Node: "n", Epoch: 2, N: 1}); err != nil {
		t.Fatal(err)
	}
}

// Head and List answer from the file's attributes: they never open its contents.
func TestHeadAndListDoNotReadContents(t *testing.T) {
	l, ctx := newLocal(t), context.Background()
	big := strings.Repeat("x", 8<<20)
	if _, err := l.Create(ctx, "journal/n/1", strings.NewReader(big), WriteID{Node: "n", Epoch: 1, N: 1}); err != nil {
		t.Fatal(err)
	}
	before := l.contentReads.Load()
	if info, err := l.Head(ctx, "journal/n/1"); err != nil || info.Size != int64(len(big)) {
		t.Fatalf("head: %+v %v", info, err)
	}
	for info, err := range l.List(ctx, "journal/") {
		if err != nil || info.ETag == "" || info.WriteID.N != 1 {
			t.Fatalf("list: %+v %v", info, err)
		}
	}
	if got := l.contentReads.Load(); got != before {
		t.Fatalf("Head and List opened contents %d times", got-before)
	}
	rc, _, err := l.Get(ctx, "journal/n/1")
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	if l.contentReads.Load() != before+1 {
		t.Fatal("the counter doesn't see a Get")
	}
}

// A process that dies after the temporary file is complete and before the
// rename leaves the old object and its old write id.
func TestCrashBeforeRenameKeepsOldObject(t *testing.T) {
	if dir := os.Getenv("BLOB_CRASH_DIR"); dir != "" {
		l, err := NewLocal(dir)
		if err != nil {
			t.Fatal(err)
		}
		l.beforeRename = func() { os.Exit(7) }
		l.Update(context.Background(), "k", strings.NewReader("new"), ETag(os.Getenv("BLOB_CRASH_ETAG")), WriteID{Node: "n1", Epoch: 1, N: 2})
		t.Fatal("survived")
	}
	dir, ctx := t.TempDir(), context.Background()
	l, err := NewLocal(dir)
	if err != nil {
		t.Fatal(err)
	}
	e1, err := l.Create(ctx, "k", strings.NewReader("old"), WriteID{Node: "n1", Epoch: 1, N: 1})
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashBeforeRenameKeepsOldObject$")
	cmd.Env = append(os.Environ(), "BLOB_CRASH_DIR="+dir, "BLOB_CRASH_ETAG="+string(e1))
	if err := cmd.Run(); cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != 7 {
		t.Fatalf("the writer should have died at the hook: %v", err)
	}
	info, err := l.Head(ctx, "k")
	if err != nil || info.ETag != e1 || info.WriteID.N != 1 || info.Size != 3 {
		t.Fatalf("after the crash: %+v %v", info, err)
	}
	rc, _, err := l.Get(ctx, "k")
	if err != nil || readAll(t, rc) != "old" {
		t.Fatalf("contents changed: %v", err)
	}
	// The old ETag still works, and the leftover temporary file is in the way of nothing.
	if _, err := l.Update(ctx, "k", strings.NewReader("next"), e1, WriteID{Node: "n1", Epoch: 1, N: 3}); err != nil {
		t.Fatal(err)
	}
}

// The startup check logic, with an injected failure: a filesystem without user
// extended attributes can't be made in the test environment.
func TestXattrCheck(t *testing.T) {
	if err := checkXattr(t.TempDir(), unix.Fsetxattr); err != nil {
		t.Fatal(err)
	}
	fail := func(int, string, []byte, int) error { return unix.ENOTSUP }
	err := checkXattr(t.TempDir(), fail)
	if err == nil || !strings.Contains(err.Error(), "does not support user extended attributes") {
		t.Fatalf("got %v", err)
	}
	drop := func(int, string, []byte, int) error { return nil } // accepts and forgets
	if err := checkXattr(t.TempDir(), drop); err == nil || !strings.Contains(err.Error(), "does not keep") {
		t.Fatalf("got %v", err)
	}
}
