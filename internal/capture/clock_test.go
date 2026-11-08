package capture_test

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/capture"
)

// tailBytes captures the same commits on a new database with the clock given
// and returns each tail file's bytes and time.
func tailBytes(t *testing.T, now func() time.Time) (bodies [][]byte, times []time.Time) {
	t.Helper()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName("users/1"), Path: path, Generation: "g1", Writer: e.w, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	app.Must(t, "exec", "a", "CREATE TABLE t (id INTEGER PRIMARY KEY, v)")
	app.Must(t, "exec", "a", "INSERT INTO t (v) VALUES (zeroblob(100)), ('x'), ('y')")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	app.Must(t, "exec", "a", "UPDATE t SET v = zeroblob(300) WHERE id = 2")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	for _, f := range tailOf(t, sh).Files {
		b, err := io.ReadAll(f.Body.Reader())
		if err != nil {
			t.Fatal(err)
		}
		bodies, times = append(bodies, b), append(times, f.At)
	}
	if err := sh.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return bodies, times
}

// With the same commits and a fixed clock, capture writes the same bytes, and
// the tail files carry the clock's time.
func TestFixedClockGivesTheSameBytes(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return at }
	a, at1 := tailBytes(t, now)
	b, at2 := tailBytes(t, now)
	if len(a) == 0 || len(a) != len(b) {
		t.Fatalf("tail files: %d and %d", len(a), len(b))
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			t.Errorf("file %d differs between runs (%d and %d bytes)", i, len(a[i]), len(b[i]))
		}
		if !at1[i].Equal(at) || !at2[i].Equal(at) {
			t.Errorf("file %d at %v and %v, want %v", i, at1[i], at2[i], at)
		}
	}
}
