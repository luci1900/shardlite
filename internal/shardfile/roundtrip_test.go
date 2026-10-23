package shardfile_test

import (
	"context"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"pgregory.net/rapid"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

// caseTB lets the harness (which wants a testing.TB) run inside one rapid case:
// cleanups run at the end of the case and failures fail the case, so rapid can
// shrink it.
type caseTB struct {
	testing.TB
	rt       *rapid.T
	dir      string
	cleanups []func()
}

func (c *caseTB) Helper()                           {}
func (c *caseTB) Cleanup(f func())                  { c.cleanups = append(c.cleanups, f) }
func (c *caseTB) Fatal(args ...any)                 { c.rt.Fatal(args...) }
func (c *caseTB) Fatalf(format string, args ...any) { c.rt.Fatalf(format, args...) }
func (c *caseTB) Errorf(format string, args ...any) { c.rt.Errorf(format, args...) }
func (c *caseTB) Logf(format string, args ...any)   { c.rt.Logf(format, args...) }
func (c *caseTB) TempDir() string {
	d, err := os.MkdirTemp(c.dir, "d")
	if err != nil {
		c.rt.Fatal(err)
	}
	return d
}

func (c *caseTB) finish() {
	for i := len(c.cleanups) - 1; i >= 0; i-- {
		c.cleanups[i]()
	}
	os.RemoveAll(c.dir)
}

// maxActions bounds the real actions of one case (the app is a separate
// process). Rapid may draw more steps; the rest do nothing.
const maxActions = 30

// shardSim drives one shard through its life: the app writes, the node syncs,
// writes deltas and rebases, lets go of the shard and activates it again from the
// cache or from blob storage, and crashes.
type shardSim struct {
	rt  *rapid.T
	e   *harness.Env
	ctx context.Context

	app    *apptest.App
	sh     *capture.Shard
	h      *shardfile.Handle
	active bool

	synced string // dump as of the last completed sync (or deactivation)
	// The app's tables and indexes as of that sync: a crash forgets later ones.
	syncedTables  map[string]bool
	syncedIndexes map[string]string
	actions       int
	tables        map[string]bool
	indexes       map[string]string // index to its table
	nextID        int
}

// markSynced records that the shard is durable as the app sees it now.
func (s *shardSim) markSynced(dump string) {
	s.synced = dump
	s.syncedTables, s.syncedIndexes = maps.Clone(s.tables), maps.Clone(s.indexes)
}

func (s *shardSim) path() string { return s.e.Path(shard) }

func (s *shardSim) do(fields ...string) string {
	s.rt.Helper()
	v, err := s.app.Do(fields...)
	if err != nil {
		s.rt.Fatalf("app %v: %v", fields[:min(len(fields), 3)], err)
	}
	return v
}

func (s *shardSim) exec(format string, args ...any) { s.do("exec", "a", fmt.Sprintf(format, args...)) }

// liveDump is the dump of the shard as the app has it, read by the app's process.
func (s *shardSim) liveDump() string {
	s.rt.Helper()
	d, err := strconv.Unquote(s.do("dump", "a", s.path()))
	if err != nil {
		s.rt.Fatal(err)
	}
	return d
}

// activate starts capture on the file (restored from blob storage if res is
// set, cached otherwise), takes the first sync before the app opens it, then
// starts the app and checks it sees what the shard held.
func (s *shardSim) activate(res *shardfile.Restored) {
	s.rt.Helper()
	if res != nil {
		s.h, s.sh = res.Handle, s.e.Activate(res.Handle, res.Capture)
	} else {
		s.sh = s.e.Activate(s.h, &capture.Restored{Position: s.h.Manifest.LastPosition()})
	}
	if err := s.sh.Sync(s.ctx); err != nil {
		s.rt.Fatalf("sync of the reactivated shard: %v", err)
	}
	s.startApp()
	s.active = true
	if got := s.liveDump(); got != s.synced {
		s.rt.Fatalf("the reactivated shard differs from what it held:\n--- held\n%s\n--- now\n%s", s.synced, got)
	}
}

// startApp starts the app with a short busy timeout: a TRUNCATE checkpoint
// waits for readers (capture's) until it gives up, which would otherwise be
// the five seconds the other tests use.
func (s *shardSim) startApp() {
	s.rt.Helper()
	s.app = apptest.Start(s.e.T)
	s.do("open", "a", s.path(), "50")
}

// deactivate lets go of the shard as the node does: the app closes it, Release
// makes the final sync, and a delta turns the tail into a file.
func (s *shardSim) deactivate() {
	s.rt.Helper()
	if !s.active {
		return
	}
	want := s.liveDump()
	s.do("close", "a")
	if err := s.app.Exit(); err != nil {
		s.rt.Fatal(err)
	}
	released, _, err := s.sh.Release(s.ctx, nil)
	if err != nil || !released {
		s.rt.Fatalf("release: released=%v err=%v", released, err)
	}
	if err := s.h.WriteDelta(s.ctx, s.sh, shardfile.Hooks{}); err != nil {
		s.rt.Fatalf("delta on deactivation: %v", err)
	}
	s.markSynced(want)
	s.active, s.sh, s.app = false, nil, nil
}

func (s *shardSim) ensureActive() {
	if !s.active {
		s.activate(nil)
	}
}

// durableDump restores the shard from blob storage into a file of its own and dumps it.
func (s *shardSim) durableDump() string {
	s.rt.Helper()
	dst := filepath.Join(s.e.T.TempDir(), "check.db")
	if _, err := s.e.Files.Restore(s.ctx, shardName(shard), dst, shardfile.RestoreOptions{}); err != nil {
		s.rt.Fatalf("restore: %v", err)
	}
	d, err := sqlitetest.TryDump(dst)
	if err != nil {
		s.rt.Fatalf("the restored shard is damaged: %v", err)
	}
	return d
}

func (s *shardSim) checkDurable(what string) {
	s.rt.Helper()
	if got := s.durableDump(); got != s.synced {
		s.rt.Fatalf("%s: what blob storage holds differs from the shard at its last sync:\n--- last sync\n%s\n--- restored\n%s", what, s.synced, got)
	}
}

// step runs fn as a real action, up to maxActions of them.
func (s *shardSim) step(fn func()) {
	if s.actions >= maxActions {
		return
	}
	s.actions++
	fn()
}

func (s *shardSim) pickTable(label string) (string, bool) {
	var names []string
	for n := range s.tables {
		names = append(names, n)
	}
	if len(names) == 0 {
		return "", false
	}
	slices.Sort(names)
	return rapid.SampledFrom(names).Draw(s.rt, label), true
}

// dropLocal deletes a shard's local file, as eviction or a lost disk would.
func dropLocal(t *rapid.T, path string) {
	t.Helper()
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.RemoveAll(p); err != nil {
			t.Fatal(err)
		}
	}
}

func (s *shardSim) actionsMap() map[string]func(*rapid.T) {
	ids := rapid.IntRange(1, 5)
	small, large := rapid.IntRange(1, 100), rapid.IntRange(3000, 20000)
	return map[string]func(*rapid.T){
		"insert small rows": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				s.exec("INSERT INTO kv (payload) VALUES (randomblob(%d))", small.Draw(s.rt, "bytes"))
				s.exec("INSERT INTO kv (payload) VALUES (randomblob(%d))", small.Draw(s.rt, "bytes"))
			})
		},
		"insert large rows": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				s.exec("INSERT INTO kv (payload) VALUES (randomblob(%d))", large.Draw(s.rt, "bytes"))
			})
		},
		"update rows": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				k := ids.Draw(s.rt, "modulus")
				size := rapid.OneOf(small, large).Draw(s.rt, "bytes")
				s.exec("UPDATE kv SET payload = randomblob(%d) WHERE id %% %d = %d", size, k, rapid.IntRange(0, k-1).Draw(s.rt, "remainder"))
			})
		},
		"delete rows": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				k := ids.Draw(s.rt, "modulus")
				s.exec("DELETE FROM kv WHERE id %% %d = %d", k, rapid.IntRange(0, k-1).Draw(s.rt, "remainder"))
			})
		},
		"create table": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				s.nextID++
				name := "t" + strconv.Itoa(s.nextID)
				s.exec("CREATE TABLE %s (id INTEGER PRIMARY KEY, a TEXT, b BLOB)", name)
				s.exec("INSERT INTO %s (a, b) VALUES ('x', randomblob(%d))", name, small.Draw(s.rt, "bytes"))
				s.tables[name] = true
			})
		},
		"write to a table": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				if t, ok := s.pickTable("table"); ok {
					s.exec("INSERT INTO %s (a, b) VALUES (hex(randomblob(%d)), randomblob(%d))", t, small.Draw(s.rt, "text"), rapid.OneOf(small, large).Draw(s.rt, "bytes"))
				}
			})
		},
		"drop table": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				if t, ok := s.pickTable("table"); ok {
					s.exec("DROP TABLE %s", t)
					delete(s.tables, t)
					for ix, tt := range s.indexes {
						if tt == t {
							delete(s.indexes, ix)
						}
					}
				}
			})
		},
		"create index": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				if t, ok := s.pickTable("table"); ok {
					s.nextID++
					ix := "i" + strconv.Itoa(s.nextID)
					s.exec("CREATE INDEX %s ON %s (a)", ix, t)
					s.indexes[ix] = t
				}
			})
		},
		"drop index": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				for ix := range s.indexes {
					s.exec("DROP INDEX %s", ix)
					delete(s.indexes, ix)
					break
				}
			})
		},
		"checkpoint": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				s.do("ckpt", "a", rapid.SampledFrom([]string{"PASSIVE", "TRUNCATE"}).Draw(s.rt, "mode"))
			})
		},
		"vacuum": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				s.exec("VACUUM")
			})
		},
		"sync": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				if err := s.sh.Sync(s.ctx); err != nil {
					s.rt.Fatalf("sync: %v", err)
				}
				s.markSynced(s.liveDump())
			})
		},
		"write a delta": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				if err := s.h.WriteDelta(s.ctx, s.sh, shardfile.Hooks{}); err != nil {
					s.rt.Fatalf("delta: %v", err)
				}
			})
		},
		"rebase": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				if _, err := s.h.Rebase(s.ctx, s.sh, shardfile.Hooks{}); err != nil {
					s.rt.Fatalf("rebase: %v", err)
				}
				// The snapshot syncs first, so the base holds every commit so far.
				s.markSynced(s.liveDump())
			})
		},
		"restore and compare": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				s.checkDurable("restore")
			})
		},
		"deactivate": func(*rapid.T) { s.step(s.deactivate) },
		"reactivate from the cache": func(*rapid.T) {
			s.step(func() {
				s.deactivate()
				s.activate(nil)
			})
		},
		"reactivate from blob storage": func(*rapid.T) {
			s.step(func() {
				s.deactivate()
				dropLocal(s.rt, s.path())
				res, err := s.e.Files.Restore(s.ctx, shardName(shard), s.path(), shardfile.RestoreOptions{})
				if err != nil {
					s.rt.Fatalf("restore: %v", err)
				}
				s.activate(res)
			})
		},
		"crash": func(*rapid.T) {
			s.step(func() {
				s.ensureActive()
				// The process dies: the app, the node's capture (never closed: nothing is
				// synced after this point) and the local files.
				s.app.Kill()
				s.active, s.sh, s.app = false, nil, nil
				dropLocal(s.rt, s.path())
				res, err := s.e.Files.Restore(s.ctx, shardName(shard), s.path(), shardfile.RestoreOptions{})
				if err != nil {
					s.rt.Fatalf("restore after the crash: %v", err)
				}
				// Nothing before the last sync is lost, and nothing after it is needed.
				got, err := sqlitetest.TryDump(s.path())
				if err != nil {
					s.rt.Fatalf("the shard restored after the crash is damaged: %v", err)
				}
				if got != s.synced {
					s.rt.Fatalf("after the crash the shard differs from its last sync:\n--- last sync\n%s\n--- restored\n%s", s.synced, got)
				}
				s.tables, s.indexes = maps.Clone(s.syncedTables), maps.Clone(s.syncedIndexes)
				s.activate(res)
			})
		},
	}
}

// Shard round trips: whatever the app and the node do, a restore or a
// reactivation gives the shard as it was at its last completed sync.
func TestShardRoundTripsProperty(t *testing.T) {
	t.Parallel()
	cheapByDefault(t)
	rapid.Check(t, func(rt *rapid.T) {
		dir, err := os.MkdirTemp("", "roundtrip")
		if err != nil {
			rt.Fatal(err)
		}
		c := &caseTB{TB: t, rt: rt, dir: dir}
		defer c.finish()
		e := harness.New(c)
		s := &shardSim{rt: rt, e: e, ctx: context.Background(), tables: map[string]bool{}, indexes: map[string]string{}}
		s.startApp()
		s.h, s.sh = e.NewShard(shard)
		s.active = true
		if err := s.sh.Sync(s.ctx); err != nil {
			rt.Fatal(err)
		}
		s.markSynced(s.liveDump())

		rt.Repeat(s.actionsMap())

		// Whatever happened, what is durable is the shard at its last sync.
		s.ensureActive()
		s.checkDurable("at the end")
	})
}

// cheapByDefault lowers rapid's default number of checks for this test, since
// every case starts processes. An explicit -rapid.checks still wins.
func cheapByDefault(t *testing.T) {
	f := flag.Lookup("rapid.checks")
	if f != nil && f.Value.String() == f.DefValue && defaultRoundTripChecks != f.DefValue {
		_ = f.Value.Set(defaultRoundTripChecks)
		t.Cleanup(func() { _ = f.Value.Set(f.DefValue) }) // the other tests keep rapid's default
	}
}

const defaultRoundTripChecks = "100"
