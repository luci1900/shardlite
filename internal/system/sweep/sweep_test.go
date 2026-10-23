package sweep

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/migrate"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
	"github.com/luci1900/shardlite/internal/system"
)

// rig is the sweeper over a memory store whose clock the test sets, with its
// shard as a SQLite file of its own.
type rig struct {
	t     *testing.T
	now   atomic.Int64 // Unix seconds, the store's clock and the sweeper's
	mem   *blob.Memory
	store *blob.Store
	db    *sql.DB
	lost  atomic.Bool // the node's lease is gone
	opts  Options
}

func newRig(t *testing.T, opts Options) *rig {
	r := &rig{t: t, opts: opts}
	r.now.Store(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Unix())
	r.mem = blob.NewMemory(blob.WithClock(r.clock))
	r.store = blob.NewStore(r.mem, "n1", 1, blob.Options{})
	db := openDB(t, "sweep.db", migrations)
	r.db = db
	return r
}

func openDB(t *testing.T, name string, migrations fs.FS) *sql.DB {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE _shardlite_reminders (key TEXT PRIMARY KEY, due INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := migrate.Migrate(context.Background(), tx, migrations, migrate.Latest); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return db
}

func txOn(db *sql.DB) func(context.Context, func(*sql.Tx) error) error {
	return func(ctx context.Context, fn func(*sql.Tx) error) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit()
	}
}

func (r *rig) clock() time.Time { return time.Unix(r.now.Load(), 0) }

func (r *rig) advance(d time.Duration) { r.now.Add(int64(d / time.Second)) }

// handler is a new system app over the same shard file, as after a move.
func (r *rig) handler() http.Handler {
	check := func() error {
		if r.lost.Load() {
			return errors.New("lease lost")
		}
		return nil
	}
	k := New(r.store, check, r.clock, slog.New(slog.DiscardHandler), r.opts)
	return k.Handler(&system.System{Name: shard.MustParse("_system.sweep/_cluster"), Tx: txOn(r.db)})
}

// wake sends the wake to a new handler and returns its status.
func (r *rig) wake() int {
	r.t.Helper()
	w := httptest.NewRecorder()
	r.handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/_shardlite/reminders", nil))
	return w.Code
}

// pass wakes until the pass ends (the cursor is back at the start), and returns
// how many wakes it took.
func (r *rig) pass() int {
	r.t.Helper()
	for n := 1; n < 100; n++ {
		if code := r.wake(); code != http.StatusNoContent {
			r.t.Fatalf("wake %d answered %d", n, code)
		}
		if r.cursor() == "" {
			return n
		}
	}
	r.t.Fatal("the pass never ended")
	return 0
}

func (r *rig) cursor() string {
	var c string
	if err := r.db.QueryRow(`SELECT cursor FROM sweep`).Scan(&c); err != nil {
		r.t.Fatal(err)
	}
	return c
}

func (r *rig) due() int64 {
	var d int64
	if err := r.db.QueryRow(`SELECT due FROM _shardlite_reminders WHERE key = 'run'`).Scan(&d); err != nil {
		r.t.Fatal(err)
	}
	return d
}

func (r *rig) put(key string) {
	r.t.Helper()
	if _, err := r.store.Create(context.Background(), key, strings.NewReader("x")); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) manifest(name, gen string, files ...string) {
	r.t.Helper()
	fs := make([]shardfile.File, len(files))
	for i, s := range files {
		f, err := shardfile.ParseFile(s)
		if err != nil {
			r.t.Fatal(err)
		}
		fs[i] = f
	}
	m := &shardfile.Manifest{Generation: shard.Gen(gen), Files: fs}
	if _, err := shardfile.NewManifestStore(r.store).Put(context.Background(), shard.MustParse(name), m, nil); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) keys() []string {
	r.t.Helper()
	var out []string
	for info, err := range r.store.List(context.Background(), "") {
		if err != nil {
			r.t.Fatal(err)
		}
		out = append(out, info.Key)
	}
	return out
}

func file(name, gen, file string) string {
	return shardfile.GenerationPrefix(shard.MustParse(name), shard.Gen(gen)) + file + ".ltx"
}

// The sweeper deletes the files no manifest lists once they are past the age:
// unlisted files of the manifest's generation, other generations whose newest
// file is old, and every generation of a shard with no manifest. It keeps
// listed files, young unlisted ones, a generation with a young file and the
// journal.
func TestSweepDeletesOnlyOldUnlistedFiles(t *testing.T) {
	r := newRig(t, Options{Age: time.Hour, Batch: 3})
	listed := []string{file("test.users/a", "g1", "1-5.1"), file("test.users/a", "g1", "6-9.1")}
	oldUnlisted := file("test.users/a", "g1", "6-8.1")
	oldGen := []string{file("test.users/a", "g0", "1-3.1"), file("test.users/a", "g0", "4-4.1")}
	orphan := file("test.users/b", "g9", "1-2.3")
	for _, k := range append(append(listed, oldUnlisted, orphan, "data/journal/n1/1"), oldGen...) {
		r.put(k)
	}
	r.manifest("test.users/a", "g1", "1-5.1", "6-9.1")
	r.advance(2 * time.Hour)
	youngUnlisted := file("test.users/a", "g1", "10-12.1")
	youngGen := []string{file("test.users/c", "g2", "1-1.1"), file("test.users/c", "g3", "1-1.1")}
	r.put(youngUnlisted)
	r.put(youngGen[0])
	// users/c/g3 has an old file and a young one: the generation is kept whole.
	r.advance(-2 * time.Hour)
	r.put(file("test.users/c", "g3", "2-2.1"))
	r.advance(2 * time.Hour)
	r.put(youngGen[1])

	if n := r.pass(); n < 2 {
		t.Fatalf("the pass took %d wakes with a batch of 3", n)
	}
	got := r.keys()
	for _, k := range append(append(oldGen, oldUnlisted), orphan) {
		if slices.Contains(got, k) {
			t.Errorf("%s was not deleted", k)
		}
	}
	for _, k := range append(append(listed, youngUnlisted, "data/journal/n1/1", file("test.users/c", "g3", "2-2.1")), youngGen...) {
		if !slices.Contains(got, k) {
			t.Errorf("%s was deleted", k)
		}
	}
	if want := r.clock().Add(24 * time.Hour).Unix(); r.due() != want {
		t.Fatalf("after the pass the next is due at %d, want %d", r.due(), want)
	}
}

// Only data/shards/ is swept: what a release before the reminder index left under
// data/nodes/ is left alone, and an unlisted shard file goes.
func TestSweepLeavesOtherPrefixesAlone(t *testing.T) {
	r := newRig(t, Options{Age: time.Hour, Batch: 2})
	other := "data/nodes/n1/g1/2-3.1.ltx"
	unlisted := file("test.users/a", "g", "2-3.1")
	listed := file("test.users/a", "g", "1-1.1")
	for _, k := range []string{other, unlisted, listed} {
		r.put(k)
	}
	r.manifest("test.users/a", "g", "1-1.1")
	r.advance(2 * time.Hour)
	r.pass()
	got := r.keys()
	if slices.Contains(got, unlisted) {
		t.Errorf("%s was not deleted", unlisted)
	}
	for _, k := range []string{other, listed} {
		if !slices.Contains(got, k) {
			t.Errorf("%s was deleted", k)
		}
	}
}

// A pass in progress keeps its cursor in the shard's file, so a new system app
// (after a move or a crash) carries on from it, and its reminder is due a pace
// later. A batch never splits a shard.
func TestSweepResumesFromItsCursor(t *testing.T) {
	r := newRig(t, Options{Age: time.Hour, Batch: 2, Pace: 5 * time.Second})
	for _, s := range []string{"test.users/a", "test.users/b", "test.users/c"} {
		r.put(file(s, "g", "1-1.1"))
		r.put(file(s, "g", "2-2.1"))
		r.put(file(s, "g", "3-3.1"))
	}
	r.advance(2 * time.Hour)
	if code := r.wake(); code != http.StatusNoContent {
		t.Fatalf("answered %d", code)
	}
	if c := r.cursor(); c != file("test.users/a", "g", "3-3.1") {
		t.Fatalf("cursor %q after the first batch, want the end of users/a", c)
	}
	if want := r.clock().Add(5 * time.Second).Unix(); r.due() != want {
		t.Fatalf("next due at %d, want %d", r.due(), want)
	}
	if got := r.keys(); len(got) != 6 {
		t.Fatalf("%d keys left after one batch, want users/b and users/c's 6", len(got))
	}
	r.pass()
	if got := r.keys(); len(got) != 0 {
		t.Fatalf("left %v", got)
	}
}

// A sweeper whose node has lost its lease deletes nothing and fails the wake,
// which the reminders loop tries again.
func TestSweepChecksTheLeaseBeforeDeleting(t *testing.T) {
	r := newRig(t, Options{Age: time.Hour})
	r.put(file("test.users/a", "g", "1-1.1"))
	r.advance(2 * time.Hour)
	r.lost.Store(true)
	if code := r.wake(); code != http.StatusServiceUnavailable {
		t.Fatalf("answered %d with the lease lost", code)
	}
	if len(r.keys()) != 1 {
		t.Fatal("deleted with the lease lost")
	}
	if r.cursor() != "" {
		t.Fatal("the cursor moved")
	}
}

// A file that is already gone when the sweeper deletes it counts as deleted, so
// a batch repeated after a move is harmless.
func TestSweepToleratesAFileAlreadyGone(t *testing.T) {
	r := newRig(t, Options{Age: time.Hour})
	key := file("test.users/a", "g", "1-1.1")
	r.put(key)
	r.advance(2 * time.Hour)
	hooked := blobtest.WithHook(r.mem)
	hooked.SetHook(func(ctx context.Context, op blobtest.Op, k string, after bool) error {
		if op == blobtest.OpDelete && !after {
			r.mem.Delete(ctx, k, "")
		}
		return nil
	})
	r.store = blob.NewStore(hooked, "n1", 1, blob.Options{})
	if code := r.wake(); code != http.StatusNoContent {
		t.Fatalf("answered %d", code)
	}
}

// The sweeper's kind has sync off: a cursor lost with a host only repeats a
// batch.
func TestKindHasTheLocalTier(t *testing.T) {
	k := New(nil, nil, time.Now, slog.New(slog.DiscardHandler), Options{})
	if k.Sync != kind.SyncLocal {
		t.Fatalf("sync %v, want local", k.Sync)
	}
}

func TestMigrations(t *testing.T) {
	t.Parallel()
	sqlitetest.Migrations(t, migrations)
}

// The state table is STRICT: a value of the wrong type is refused, where a lax
// table would store it as whatever the column's affinity makes of it. A number
// into the TEXT cursor still converts, so the refusals tested are the ones
// there are.
func TestSweepTableIsStrict(t *testing.T) {
	db := openDB(t, "strict.db", migrations)
	for _, q := range []string{
		`UPDATE sweep SET deleted = 'none'`,
		`UPDATE sweep SET cursor = x'00'`,
	} {
		if _, err := db.Exec(q); err == nil {
			t.Errorf("%q was accepted", q)
		}
	}
}
