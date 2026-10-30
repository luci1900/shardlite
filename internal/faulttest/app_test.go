//go:build fault

package faulttest

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

// replayDump replays every captured entry of a shard onto an empty file and dumps it.
func replayDump(t *testing.T, e *harness.Env, name string) (dump string, entries, big int) {
	t.Helper()
	ctx := context.Background()
	target := shard.MustParse(name)
	path := filepath.Join(t.TempDir(), "replay.db")
	ap, err := shardfile.NewApplier(path)
	if err != nil {
		t.Fatal(err)
	}
	err = segment.Scan(ctx, e.Journal, 1, 0, func(seq pos.Seg, en segment.Entry) error {
		if en.Shard != target {
			return nil
		}
		b, err := harness.ReadEntry(ctx, e.Journal, seq, en)
		if err != nil {
			return err
		}
		entries++
		if en.Length > 100*1024 {
			big++
		}
		return ap.Apply(bytes.NewReader(b), shardfile.Range{Min: en.Position, Max: en.Last()})
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if _, err := ap.Finish(); err != nil {
		t.Fatal(err)
	}
	return sqlitetest.Dump(t, path), entries, big
}

// syncUntil syncs the shard every few milliseconds until done is closed.
func syncUntil(t *testing.T, sh *capture.Shard, done <-chan struct{}) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := sh.Sync(context.Background()); err != nil {
				t.Errorf("sync: %v", err)
				return
			}
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	return &wg
}

// An app that checkpoints by itself while it writes (the WAL-reset hazard, Litestream issue #1540's area).
func TestAppCheckpointsItself(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"PASSIVE", "FULL", "RESTART", "TRUNCATE"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			e := harness.New(t)
			// A short busy timeout: the node's read transaction makes the app's
			// FULL, RESTART and TRUNCATE checkpoints wait for it on every call.
			app := apptest.Start(t)
			app.Must(t, "open", "a", e.Path("fault.users/1"), "20")
			app.Must(t, "insert", "a", "50", "1000")
			_, sh := e.NewShard("fault.users/1")
			if err := sh.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			wg := syncUntil(t, sh, done)
			start := time.Now()
			res := app.Must(t, "loop", "a", "30", mode)
			took := time.Since(start)
			close(done)
			wg.Wait()
			if err := sh.Sync(context.Background()); err != nil {
				t.Fatal(err)
			}
			got, entries, big := replayDump(t, e, "fault.users/1")
			if want := apptest.Dump(t, e.Path("fault.users/1")); got != want {
				t.Fatalf("captured history differs from the app's database (%d entries)", entries)
			}
			t.Logf("%s checkpoint in a loop of 30 (rows,busy): %s in %v; %d entries, %d over 100 KB", mode, res, took.Round(time.Millisecond), entries, big)
			sh.Close(context.Background())
		})
	}
}

// An app that closes and reopens its connection all the time.
func TestAppReconnectsConstantly(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	app := e.StartApp("fault.users/1")
	app.Must(t, "insert", "a", "50", "1000")
	_, sh := e.NewShard("fault.users/1")
	if err := sh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	wg := syncUntil(t, sh, done)
	app.Must(t, "churn", "a", "300")
	close(done)
	wg.Wait()
	if err := sh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, entries, big := replayDump(t, e, "fault.users/1")
	if want := apptest.Dump(t, e.Path("fault.users/1")); got != want {
		t.Fatalf("captured history differs from the app's database (%d entries)", entries)
	}
	t.Logf("reconnect churn: %d entries, %d over 100 KB", entries, big)
	sh.Close(context.Background())
}

// Failing temporary files: a tail's log that cannot be created, a write to it
// that fails, a descriptor that cannot be duplicated. Syncs and deltas fail and
// are retried, acknowledged commits are never lost, and the node goes on.
func TestTemporaryFileFaults(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"create", "append", "dup"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			rng := newRand(t)
			r := newRig(t)
			r.node.Must(t, "tempfault", op, "300") // 30% of them
			for range 40 {
				if rng.IntN(5) == 0 {
					r.node.Do("delta", shardName) // may fail too
				}
				r.n++
				r.app.Must(t, "ack", "a", itoa(r.n))
				r.retrySync()
			}
			r.node.Must(t, "tempfault", "none", "0")
			r.verify("with " + op + " faults")
			if err := r.commit(true); err != nil {
				t.Fatalf("node stopped working after faults: %v", err)
			}
			r.verify("after faults")
		})
	}
}

// Failing and slow blob storage: syncs fail and are retried, acknowledged commits are never lost.
func TestStorageFaults(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"fail", "lost"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			rng := newRand(t)
			r := newRig(t)
			r.node.Must(t, "fault", mode, "300", "30") // 30% of segment PUTs, up to 30 ms slow
			for range 40 {
				if rng.IntN(5) == 0 {
					r.node.Do("delta", shardName) // may fail too
				}
				r.n++
				r.app.Must(t, "ack", "a", itoa(r.n))
				r.retrySync()
			}
			r.node.Must(t, "fault", "none", "0", "0")
			r.verify("with " + mode + " faults")
			// The node never fenced itself: a plain commit still goes through.
			if err := r.commit(true); err != nil {
				t.Fatalf("node stopped working after faults: %v", err)
			}
			r.verify("after faults")
		})
	}
}
