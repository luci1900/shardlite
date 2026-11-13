package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/luci1900/shardlite/internal/model/kind"
	"github.com/luci1900/shardlite/internal/test/fault"
)

// Every new local file gets a path of its own: the id, the generation and a random part, so a path is never used twice.
func TestNewFilesGetUniquePaths(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	n := shardName("users/p1")
	seen := map[string]bool{}
	for range 1000 {
		p := pathFor(data, n, "g1abc")
		if seen[p] {
			t.Fatalf("%s was given twice", p)
		}
		seen[p] = true
		base := filepath.Base(p)
		parts := strings.Split(base, ".")
		if filepath.Dir(p) != filepath.Join(data, "users") || len(parts) != 4 || parts[0] != "p1" || parts[1] != "g1abc" || parts[2] == "" || parts[3] != "db" {
			t.Fatalf("path %s is not id.generation.unique.db in the kind's directory", p)
		}
	}
}

// A start finds the files of a path with a random part: the database, both sidecars and a restore's leftover, and deletes them.
func TestCleanDataDirFindsFilesOfUniquePaths(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	n := shardName("users/p2")
	p := pathFor(data, n, "g1abc")
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	files := []string{p, p + "-wal", p + "-shm", p + ".restoring"}
	for _, f := range files {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := cleanDataDir(data); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if _, err := os.Stat(f); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s survived the start: %v", f, err)
		}
	}
}

// An activation that fails after its file is restored leaves no file behind: the failed activation deletes its files, since the next activation gets a path of its own and nothing else would delete them.
func TestFailedActivationLeavesNoFiles(t *testing.T) {
	t.Parallel()
	const target = "slow/p3"
	var r *mrig
	var armed atomic.Bool
	r = newMRigWith(t, func(o *ShardsetOptions) {
		o.Lifecycle.At = func(p fault.Point) {
			// The file is restored: what is in it can't be opened as a database.
			if p == fault.Restored && armed.CompareAndSwap(true, false) {
				if err := os.WriteFile(r.shard(target).Path, []byte(strings.Repeat("not a database ", 500)), 0o644); err != nil {
					t.Error(err)
				}
			}
		}
	})
	ref := r.acquire(target)
	r.write(ref, "a")
	if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "a")
	ref.Release()
	r.release(target)
	armed.Store(true)
	if _, err := r.m.Acquire(context.Background(), shardName(target)); err == nil {
		t.Fatal("the activation did not fail")
	}
	left, _ := filepath.Glob(filepath.Join(r.data, "slow", "p3.*"))
	if len(left) != 0 {
		t.Fatalf("a failed activation left %v", left)
	}
	ref = r.acquire(target)
	defer ref.Release()
	if got := strings.Count(r.liveDump(ref.Path()), "kv: "); got != 2 {
		t.Fatalf("%d rows after the retry, want the 2 of one write", got)
	}
}
