package node

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/remindex"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

// --- helpers ---

// sql has the app (its own process) run a statement on a shard.
func (r *srig) sql(target, stmt string) {
	r.t.Helper()
	if err := r.sqlErr(target, stmt); err != nil {
		r.t.Fatal(err)
	}
}

// sqlErr is sql that wants the statement's error, for a test that wants one.
func (r *srig) sqlErr(target, stmt string) error {
	r.t.Helper()
	ref := r.acquire(target)
	defer ref.Release()
	path := ref.Path()
	r.omu.Lock()
	defer r.omu.Unlock()
	if !r.opened[path] {
		if _, err := r.app.Do("open", path, path); err != nil {
			r.t.Fatal(err)
		}
		r.opened[path] = true
	}
	_, err := r.app.Do("exec", path, strings.ReplaceAll(stmt, "\n", " ")) // the app's protocol is one line per command
	return err
}

// scalar is the answer of a query that returns one value, asked by the app process.
func (r *srig) scalar(target, query string) string {
	r.t.Helper()
	ref := r.acquire(target)
	defer ref.Release()
	out := r.app.Must(r.t, "query", "q", ref.Path(), query)
	s, err := strconv.Unquote(out)
	if err != nil {
		r.t.Fatalf("query %q gave %q", query, out)
	}
	return s
}

// count is the answer of a query that returns one number, asked by the app process.
func (r *srig) count(target, query string) int {
	r.t.Helper()
	s := r.scalar(target, query)
	n, err := strconv.Atoi(s)
	if err != nil {
		r.t.Fatalf("query %q gave %q", query, s)
	}
	return n
}

// reminderRows is how many reminders the shard holds.
func (r *srig) reminderRows(target string) int {
	return r.count(target, `SELECT count(*) FROM _shardlite_reminders`)
}

// durableReminders is how many reminders a restore from blob storage has.
func (r *srig) durableReminders(target string) int {
	r.t.Helper()
	dst := filepath.Join(r.t.TempDir(), "reminders.db")
	if _, err := r.n.Files.Restore(context.Background(), shardName(target), dst, shardfile.RestoreOptions{}); err != nil {
		if errors.Is(err, shardfile.ErrNothingToRestore) || errors.Is(err, blob.ErrNotFound) {
			return 0
		}
		r.t.Fatalf("restore %s: %v", target, err)
	}
	return strings.Count(sqlitetest.Dump(r.t, dst), "_shardlite_reminders: ")
}

// wakeHookFn wraps the wake of an app for a shard's reminders: call makes the
// real one.
type wakeHookFn func(ctx context.Context, call func() (int, error)) (int, error)

// failUntil has every wake fail with 503, without reaching the app, until up is
// set. It returns the number of wakes that reached it.
func (r *srig) failUntil(up *atomic.Bool) *atomic.Int64 {
	var n atomic.Int64
	hook := wakeHookFn(func(_ context.Context, call func() (int, error)) (int, error) {
		n.Add(1)
		if !up.Load() {
			return http.StatusServiceUnavailable, errors.New("the app is down")
		}
		return call()
	})
	r.wakeHook.Store(&hook)
	return &n
}

// armed is true once the shard has its reminder timer set (for the earliest
// reminder that is not due yet), which happens when the commit with it is durable.
func (r *srig) armed(target string) func() bool {
	return func() bool {
		e := r.m.tab.load(shardName(target))
		if e == nil || e.a == nil || !e.is(entryActive) {
			return false
		}
		o := e.a.loop.load()
		return o != nil && o.armed.Load()
	}
}

// loopSettled is true once the shard's reminders loop has finished every read
// it was asked for (with their deliveries started and timer set), or has ended.
func loopSettled(a *actor) bool {
	o := a.loop.load()
	if o == nil {
		return true
	}
	select {
	case <-o.ended:
		return true
	default:
	}
	return o.answered.Load() == o.asked.Load()
}

// lookedAt is true once the idle timer has looked at the shard and found live
// reminders holding it (that sets the shard's next look).
func (r *srig) lookedAt(target string) func() bool {
	return func() bool { return !r.delay(target).IsZero() }
}

// settle returns once the node has read what the shard holds as of now: two
// flush cycles (the first may have begun before the call), then the reminders
// loop's read after them.
func (r *srig) settle(target string) {
	r.t.Helper()
	r.afterCycle()
	r.afterCycle()
	e := r.m.tab.load(shardName(target))
	if e == nil || e.a == nil || !e.is(entryActive) {
		return
	}
	apptest.Eventually(r.t, apptest.Timeout, "the reminders loop of "+target+" to settle", func() bool { return loopSettled(e.a) })
}

// cyclesUntil runs flush cycles until cond holds and returns how many it ran.
func (r *srig) cyclesWhile(what string, cond func() bool) int {
	r.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	n := 0
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for %s after %d cycles", what, n)
		}
		r.cycle()
		n++
		time.Sleep(10 * time.Millisecond) // the flush interval of a rig that cycles by hand: tighter cycles would batch the async deliveries differently
	}
	return n
}

// quick makes retries fast.
func quick(o *CatalogOptions) {
	o.Reminders.Backoff, o.Reminders.MaxBackoff = 20*time.Millisecond, 50*time.Millisecond
}

func both(fs ...func(*CatalogOptions)) func(*CatalogOptions) {
	return func(o *CatalogOptions) {
		for _, f := range fs {
			f(o)
		}
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func logTo(buf *syncBuffer) func(*CatalogOptions) {
	return func(o *CatalogOptions) { o.Logger = slog.New(slog.NewTextHandler(buf, nil)) }
}

// wakeSeen is a wake the fake app got.
type wakeSeen struct {
	target, method, body string
	hdr                  http.Header
	shardRows            int // durable rows of the shard in blob storage when it arrived
}

// recorder is a fake app that records every wake for reminders and answers by a
// function. Unless keep is set it handles the reminders first, the way a handler
// does (deleting those that are due), when the answer is a 2xx or is lost.
type recorder struct {
	mu     sync.Mutex
	got    []wakeSeen
	answer func(n int, q *http.Request) int // n counts the wakes of the same shard, from 1
	keep   bool                             // leave the reminders alone
	hold   chan struct{}                    // if set, wakes wait for it to be closed before they answer
	hits   map[string]int
}

// record has the fake app record the wakes. shardRows, if set, is a shard whose
// durable rows are counted when each wake arrives.
func (r *srig) record(rec *recorder, shardRows string) {
	rec.hits = map[string]int{}
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path != remindersPath {
			return // not a wake
		}
		b := new(bytes.Buffer)
		b.ReadFrom(q.Body)
		d := wakeSeen{target: q.Header.Get("Shardlite-Opened"), method: q.Method, body: b.String(), hdr: q.Header.Clone()}
		if shardRows != "" {
			d.shardRows = r.rows(shardRows)
		}
		rec.mu.Lock()
		rec.got = append(rec.got, d)
		rec.hits[d.target]++
		n := rec.hits[d.target]
		rec.mu.Unlock()
		if rec.hold != nil {
			<-rec.hold
		}
		status := http.StatusOK
		if rec.answer != nil {
			status = rec.answer(n, q)
		}
		if !rec.keep && (status/100 == 2 || status == -1) {
			r.sql(d.target, fmt.Sprintf(`DELETE FROM _shardlite_reminders WHERE due <= %d`, r.m.now().Unix()))
		}
		if status == -1 { // the answer is lost: the connection breaks
			if hj, ok := w.(http.Hijacker); ok {
				if c, _, err := hj.Hijack(); err == nil {
					c.Close()
				}
			}
			return
		}
		w.WriteHeader(status)
	})
}

func (rec *recorder) all() []wakeSeen {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]wakeSeen(nil), rec.got...)
}

func (rec *recorder) count() int { return len(rec.all()) }

// of is the wakes for one shard.
func (rec *recorder) of(target string) []wakeSeen {
	var out []wakeSeen
	for _, d := range rec.all() {
		if d.target == target {
			out = append(out, d)
		}
	}
	return out
}

// downUntil is an app answer function: 503 until up is set.
func downUntil(up *atomic.Bool) func(int, *http.Request) int {
	return func(int, *http.Request) int {
		if up.Load() {
			return 200
		}
		return 503
	}
}

// --- the table ---

func TestRemindersTableExistsAfterActivation(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	ref := r.acquire("test.users/t1")
	defer ref.Release()
	r.sql("test.users/t1", `SELECT 1`)
	out := r.app.Must(t, "query", "q", ref.Path(), `SELECT name FROM sqlite_master WHERE name LIKE '_shardlite%' ORDER BY name`)
	if want := strconv.Quote("_shardlite_reminders\n_shardlite_reminders_due"); out != want {
		t.Fatalf("schema objects %s, want %s", out, want)
	}
	cols := r.app.Must(t, "query", "q", ref.Path(), `SELECT group_concat(name || ' ' || type || ' ' || "notnull" || ' ' || pk, ', ') FROM pragma_table_info('_shardlite_reminders') ORDER BY cid`)
	// key's notnull is STRICT's doing: a primary key column of a STRICT table is
	// implicitly NOT NULL, where a lax table allowed the NULL quirk.
	if want := strconv.Quote("key TEXT 1 1, due INTEGER 1 0"); cols != want {
		t.Fatalf("columns %s, want %s", cols, want)
	}
	// The tables of earlier designs are not created.
	if n := r.count("test.users/t1", `SELECT count(*) FROM sqlite_master WHERE name IN ('_shardlite_outbox', '_shardlite_inbox', '_shardlite_messages')`); n != 0 {
		t.Fatalf("%d old tables", n)
	}
}

// Activating a shard that has the table and its index commits nothing, whether
// it comes from the cache or from blob storage.
func TestActivatingAShardWithTheTableCommitsNothing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		purge bool
	}{{"cached", false}, {"restored", true}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newSRig(t)
			r.sql("test.users/t2", `INSERT INTO kv (payload) VALUES (x'01')`)
			ref := r.acquire("test.users/t2")
			path := ref.Path()
			ref.Release()
			r.closeInApp("test.users/t2")
			apptest.Eventually(t, apptest.Timeout, "deactivation", func() bool { return r.cached("test.users/t2") })
			if tc.purge {
				row, ok, err := r.m.cache.idx.get(shardName("test.users/t2"))
				if err != nil || !ok {
					t.Fatal(err)
				}
				// The entry of the shard's loop may still be in the table for a moment,
				// and eviction leaves a shard with an entry alone.
				apptest.Eventually(t, apptest.Timeout, "the eviction", func() bool {
					evicted, err := r.m.cache.evict(context.Background(), row)
					if err != nil {
						t.Fatal(err)
					}
					return evicted
				})
				if _, err := os.Stat(path); err == nil {
					t.Fatal("still cached")
				}
			}
			man, err := r.manifest("test.users/t2")
			if err != nil {
				t.Fatal(err)
			}
			creates := r.calls("create", journal.Root)
			ref = r.acquire("test.users/t2")
			defer ref.Release()
			sh := r.shard("test.users/t2").cap.load()
			if got := sh.Position(); got != man.LastPosition() {
				t.Fatalf("position %d after activating, the manifest ends at %d: the activation captured a commit", got, man.LastPosition())
			}
			before := header(t, sh)
			if err := ensureTables(context.Background(), sh, nil, 0, 0); err != nil {
				t.Fatal(err)
			}
			if header(t, sh) != before {
				t.Fatal("creating the table again committed")
			}
			r.cycle()
			r.cycle()
			if r.calls("create", journal.Root) != creates {
				t.Fatal("activating the shard wrote to the journal")
			}
		})
	}
}

// A shard whose only objects are the reserved ones is deleted as never written;
// a reminder is the app's data.
func TestNeverWrittenWithTheReservedTable(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	ref := r.acquire("test.users/t3")
	ref.Release()
	apptest.Eventually(t, apptest.Timeout, "idle deactivation", r.inactive("test.users/t3"))
	if _, err := r.rawManifest("test.users/t3"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest of a shard with only an empty reminders table: %v, want none", err)
	}
	if r.m.Stats.Deletes.Load() != 1 {
		t.Fatalf("deletes %d", r.m.Stats.Deletes.Load())
	}

	// The test app creates two tables of its own when it opens a shard, so these
	// drop them to leave only the reserved one.
	r.sql("test.users/t5", `DROP TABLE kv; DROP TABLE acks`)
	r.closeInApp("test.users/t5")
	apptest.Eventually(t, apptest.Timeout, "deactivation", r.inactive("test.users/t5"))
	if _, err := r.rawManifest("test.users/t5"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a shard where the app left only the reserved table was kept: %v", err)
	}
	if r.m.Stats.Deletes.Load() != 2 {
		t.Fatalf("deletes %d", r.m.Stats.Deletes.Load())
	}

	// A stale reminder (which the node ignores) and one far away are kept: they
	// are the app's data.
	r.sql("test.users/t4", `DROP TABLE kv; DROP TABLE acks; INSERT INTO _shardlite_reminders (key, due) VALUES ('old', unixepoch() - 999999)`)
	r.closeInApp("test.users/t4")
	apptest.Eventually(t, apptest.Timeout, "deactivation", func() bool { return r.cached("test.users/t4") })
	if _, err := r.rawManifest("test.users/t4"); err != nil {
		t.Fatalf("a shard holding a stale reminder was deleted: %v", err)
	}
	r.sql("test.users/t6", `DROP TABLE kv; DROP TABLE acks; INSERT INTO _shardlite_reminders (key, due) VALUES ('k', unixepoch() + 999999)`)
	r.closeInApp("test.users/t6")
	apptest.Eventually(t, apptest.Timeout, "deactivation", func() bool { return r.cached("test.users/t6") })
	if _, err := r.rawManifest("test.users/t6"); err != nil {
		t.Fatalf("a shard holding a reminder was deleted: %v", err)
	}
	if r.m.Stats.Deletes.Load() != 2 {
		t.Fatalf("deletes %d", r.m.Stats.Deletes.Load())
	}
}

// The reserved table is STRICT: the app's wrong type in due is refused at its
// INSERT instead of waking nobody later, and a numeric string still coerces.
func TestTheReservedTableIsStrict(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	const target = "test.users/st1"
	if err := r.sqlErr(target, `INSERT INTO _shardlite_reminders (key, due) VALUES ('k', 'soon')`); err == nil {
		t.Fatal("a text due was accepted")
	}
	if r.reminderRows(target) != 0 {
		t.Fatal("the refused insert left a row")
	}
	r.sql(target, `INSERT INTO _shardlite_reminders (key, due) VALUES ('k', '123')`)
	if d := r.count(target, `SELECT due FROM _shardlite_reminders WHERE key = 'k'`); d != 123 {
		t.Fatalf("due is %d, want 123", d)
	}
	if s := r.scalar(target, `SELECT typeof(due) FROM _shardlite_reminders WHERE key = 'k'`); s != "integer" {
		t.Fatalf("the numeric string was stored as %s, want integer", s)
	}
}

// --- the wake ---

// A wake is a POST with no body to /_shardlite/reminders for the shard itself,
// with the shard as target and caller, and the app is not told anything else.
func TestWakeIsAPostWithNoBody(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	r.remind("test.users/ia1", "a", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "the wake", func() bool { return rec.count() == 1 })
	d := rec.all()[0]
	if d.method != "POST" || d.body != "" || d.target != "test.users/ia1" || d.hdr.Get("Shardlite-Caller") != "test.users/ia1" || d.hdr.Get("Shardlite-File") == "" || d.hdr.Get("Shardlite-Target") != "" {
		t.Fatalf("wake %+v", d)
	}
	for _, h := range []string{"Idempotency-Key", "Shardlite-Sync", "Shardlite-If-Active", "Shardlite-Reminder-Id", "Content-Type"} {
		if d.hdr.Get(h) != "" {
			t.Fatalf("%s reached the app", h)
		}
	}
}

// Reminders due together cause one wake, however many there are, and the
// handler handles them all.
func TestSeveralRemindersCauseOneWake(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	var stmt strings.Builder
	stmt.WriteString("BEGIN;")
	for i := range 620 {
		fmt.Fprintf(&stmt, "INSERT INTO _shardlite_reminders (key, due) VALUES ('k%d', unixepoch());", i)
	}
	stmt.WriteString("COMMIT;")
	r.sql("test.users/b1", stmt.String())
	apptest.Eventually(t, apptest.Timeout, "the wake", func() bool { return rec.count() >= 1 })
	apptest.Eventually(t, apptest.Timeout, "the reminders to be handled", func() bool { return r.reminderRows("test.users/b1") == 0 })
	r.settle("test.users/b1")
	if rec.count() != 1 {
		t.Fatalf("%d wakes for 620 reminders", rec.count())
	}
}

// The listeners refuse /_shardlite/reminders: from a client, from the app (with
// a target, as a call between shards), and from a peer. Only the node's own wake
// reaches the app.
func TestRemindersPathIsRefused(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var seen atomic.Int32
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == remindersPath {
			seen.Add(1)
		}
	})
	for _, c := range []struct {
		name, base string
		hdr        map[string]string
	}{
		{"client", r.pub, map[string]string{"Shardlite-Target": "test.slow/rp1"}},
		{"call", r.ctl, map[string]string{"Shardlite-Target": "test.slow/rp1", "Shardlite-Caller": "test.slow/rp1"}},
		{"call between shards", r.ctl, map[string]string{"Shardlite-Target": "test.slow/rp2", "Shardlite-Caller": "test.slow/rp1"}},
	} {
		for _, p := range []string{remindersPath, "/_SHARDLITE/reminders", "//_shardlite/reminders", "/a/../_shardlite/reminders"} {
			if resp, _ := r.req("POST", c.base, p, c.hdr, ""); resp.StatusCode != http.StatusNotFound {
				t.Fatalf("%s %s: %d, want 404", c.name, p, resp.StatusCode)
			}
		}
	}
	if seen.Load() != 0 {
		t.Fatal("a refused request reached the app")
	}
}

// A lost acknowledgement of the wake: the handler's work and its handling of
// the reminder were one transaction, so the failed wake is followed by nothing
// due, and the work was done once.
func TestLostAcknowledgementOfTheWakeDoesNotRepeatTheWork(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{keep: true}
	r.record(rec, "")
	inner := *r.handler.Load()
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == remindersPath {
			r.sql("test.users/la2", fmt.Sprintf(`BEGIN; INSERT INTO kv (payload) VALUES (x'01'); DELETE FROM _shardlite_reminders WHERE due <= %d; COMMIT;`, r.m.now().Unix()))
			if hj, ok := w.(http.Hijacker); ok {
				if c, _, err := hj.Hijack(); err == nil {
					c.Close() // the acknowledgement is lost
				}
			}
			return
		}
		inner(w, q)
	})
	r.remind("test.users/la2", "k", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "the failed wake", func() bool { return r.m.Stats.ReminderWakeFailures.Load() >= 1 })
	r.settle("test.users/la2")
	if n := r.count("test.users/la2", `SELECT count(*) FROM kv`); n != 1 {
		t.Fatalf("the work was done %d times", n)
	}
	if r.m.Stats.ReminderWakes.Load() != 1 {
		t.Fatalf("%d wakes, want 1", r.m.Stats.ReminderWakes.Load())
	}
}

// Shardlite-Inbox: true is an ordinary header now: on the public and control
// listeners the request is the app's ordinary call, and nothing is put in a
// table.
func TestInboxHeaderIsOrdinary(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var seen atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == "/hello" {
			seen.Store(q.Clone(context.Background()))
			w.WriteHeader(http.StatusOK)
		}
	})
	for _, c := range []struct{ name, base string }{{"app", r.ctl}, {"client", r.pub}} {
		seen.Store(nil)
		target := "test.slow/strip-" + c.name
		r.create(target)
		resp, _ := r.req("POST", c.base, "/hello", map[string]string{"Shardlite-Target": target, "Shardlite-Inbox": "true"}, "body")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: answered %d, want the app's 200", c.name, resp.StatusCode)
		}
		if q := seen.Load(); q == nil {
			t.Fatalf("%s: the app was not called", c.name)
		}
		if n := r.reminderRows(target); n != 0 {
			t.Fatalf("%s: %d reminders", c.name, n)
		}
	}
}

// --- liveness ---

// After a crash the shard is reactivated and woken for its reminder.
func TestReminderResumesAfterACrash(t *testing.T) {
	t.Parallel()
	opts := both(quick, withDispatch)
	r := newSRigWith(t, opts)
	rec := &recorder{}
	r.record(rec, "")
	var up atomic.Bool
	attempts := r.failUntil(&up)
	r.remind("test.users/x1", "k", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "a first attempt (the commit is durable by then)", func() bool { return attempts.Load() >= 1 })
	r.closeInApp("test.users/x1")
	r.crash()
	r.stopJournal()

	r.startNode()
	if r.n.Adopted.Deltas == 0 {
		t.Fatalf("adoption reported no shards: %+v", r.n.Adopted)
	}
	// The entry and next_reminder are there before anything has activated the shard.
	if r.nextReminder("test.users/x1") == 0 || r.entryCount("test.users/x1") == 0 {
		t.Fatalf("after adoption next_reminder is %d with %d entries", r.nextReminder("test.users/x1"), r.entryCount("test.users/x1"))
	}
	r.startShards(opts)
	r.startServer()
	up.Store(true)
	apptest.Eventually(t, apptest.Timeout, "the wake after the restart, with nothing asking for the shard", func() bool { return rec.count() == 1 })
	apptest.Eventually(t, apptest.Timeout, "the reminder to be handled", func() bool { return r.reminderRows("test.users/x1") == 0 })
}

// A reminder older than MaxAge is stale: it neither wakes the shard nor keeps
// it active, and it is not deleted.
func TestStaleReminderIsIgnored(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	r.sql("test.users/st1", fmt.Sprintf(`INSERT INTO _shardlite_reminders (key, due) VALUES ('old', %d)`, time.Now().Add(-25*time.Hour).Unix()))
	r.closeInApp("test.users/st1")
	apptest.Eventually(t, apptest.Timeout, "deactivation (a stale reminder does not hold the shard)", func() bool { return r.cached("test.users/st1") })
	if rec.count() != 0 {
		t.Fatal("a stale reminder woke the shard")
	}
	if got := r.nextReminder("test.users/st1"); got != 0 {
		t.Fatalf("next_reminder %d for a stale reminder", got)
	}
	if n := r.reminderRows("test.users/st1"); n != 1 {
		t.Fatalf("%d reminders, the stale one should stay", n)
	}
}

// A shard replayed after a crash with nothing to deliver is woken by its entry, its
// next_reminder goes when it deactivates, and it deactivates when idle.
func TestReplayedShardWithNothingToDeliverIsWokenAndLetGo(t *testing.T) {
	t.Parallel()
	opts := both(quick, withDispatch)
	r := newSRigWith(t, opts)
	r.sql("test.users/y1", `INSERT INTO kv (payload) VALUES (x'01')`)
	apptest.Eventually(t, apptest.Timeout, "the commit to be durable", func() bool { return r.rows("test.users/y1") == 1 })
	r.sql("test.users/y1", `INSERT INTO kv (payload) VALUES (x'02')`) // only in the journal when the node dies
	r.closeInApp("test.users/y1")
	r.crash()
	r.stopJournal()

	r.startNode()
	if r.n.Adopted.Deltas == 0 {
		t.Fatalf("adoption: %+v", r.n.Adopted)
	}
	if r.nextReminder("test.users/y1") == 0 {
		t.Fatal("adoption left no next_reminder")
	}
	r.startShards(opts)
	r.startServer()
	apptest.Eventually(t, apptest.Timeout, "deactivation when idle", func() bool { return r.m.Stats.Activations.Load() > 0 && r.inactive("test.users/y1")() })
	// The deactivation finds no reminder, so it clears the next_reminder, with no delta.
	apptest.Eventually(t, apptest.Timeout, "the next_reminder to go", func() bool { return r.nextReminder("test.users/y1") == 0 })
}

// A restart that dies between a shard's entry and its delta leaves the entry in
// the index; the repeated adoption writes the delta, and the shard is woken once.
func TestCrashBetweenTheEntryAndTheDeltaIsRepaired(t *testing.T) {
	t.Parallel()
	opts := both(quick, withDispatch)
	r := newSRigWith(t, opts)
	rec := &recorder{}
	r.record(rec, "")
	var up atomic.Bool
	attempts := r.failUntil(&up)
	r.remind("test.users/q1", "k", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "a first attempt (the commit is durable by then)", func() bool { return attempts.Load() >= 1 })
	r.closeInApp("test.users/q1")
	r.crash()
	r.stopJournal()

	var failing atomic.Bool
	failing.Store(true)
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && failing.Load() && op == blobtest.OpCreate && strings.HasPrefix(key, shardfile.FilesPrefix) && strings.HasSuffix(key, ".ltx") {
			return errors.New("storage down") // the delta
		}
		return nil
	})
	if _, err := r.tryStartNode(); err == nil {
		t.Fatal("the adoption finished without the delta")
	}
	if r.entryCount("test.users/q1") == 0 {
		t.Fatal("no entry after the failed adoption")
	}
	if m, err := r.manifest("test.users/q1"); err != nil || m.Journal == "" {
		t.Fatalf("manifest %+v (%v): the delta should not be in", m, err)
	}
	failing.Store(false)
	r.startNode() // the adoption is repeated
	if r.nextReminder("test.users/q1") == 0 {
		t.Fatal("no next_reminder after the second adoption")
	}
	r.startShards(opts)
	r.startServer()
	up.Store(true)
	apptest.Eventually(t, apptest.Timeout, "the reminder to be delivered", func() bool { return rec.count() == 1 })
	apptest.Eventually(t, apptest.Timeout, "the reminder to be consumed", func() bool { return r.reminderRows("test.users/q1") == 0 })
	r.closeInApp("test.users/q1") // the handler's delete opened the shard in the app again
	apptest.Eventually(t, apptest.Timeout, "the next_reminder to go", func() bool { return r.cached("test.users/q1") && r.nextReminder("test.users/q1") == 0 })
	r.settle("test.users/q1")
	if rec.count() != 1 {
		t.Fatalf("%d deliveries", rec.count())
	}
}

// --- marked shards ---

// The due reminder of a marked shard survives a crash and the shard is woken
// for it after it, then deleted.
func TestMarkedShardReminderSurvivesACrash(t *testing.T) {
	t.Parallel()
	opts := both(quick, withDispatch)
	r := newSRigWith(t, opts)
	var up atomic.Bool
	rec := &recorder{}
	r.record(rec, "")
	attempts := r.failUntil(&up)
	r.sql("test.lobby/e3", markSQL)
	r.remind("test.lobby/e3", "k", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "a first attempt", func() bool { return attempts.Load() >= 1 })
	r.closeInApp("test.lobby/e3")
	r.crash()
	r.stopJournal()

	r.startNode()
	r.startShards(opts)
	r.startServer()
	up.Store(true)
	apptest.Eventually(t, apptest.Timeout, "the wake after the restart", func() bool { return rec.count() >= 1 })
	r.closeInApp("test.lobby/e3") // the handler's delete opened the shard in the app again
	apptest.Eventually(t, apptest.Timeout, "deletion", func() bool { return r.m.Stats.Deletes.Load() == 1 && !r.m.IsActive(shardName("test.lobby/e3")) })
}

// The reminders loop on its own: a SQLite database with the reserved table in
// place of the shard's capture (with a lock for the capture lock) and a fake
// wake of the app.

// fakeGate is durability for the loop's own tests. Every read is durable at
// once, unless hold is set: then each read waits for the test to close the
// channel it was given (in held), and fail is what check says then. The loop
// never reads again for itself (cycleAfter never fires).
type fakeGate struct {
	mu   sync.Mutex
	hold bool
	held []chan struct{}
	fail error
}

func (g *fakeGate) mark() uint64 { return 0 }

func (g *fakeGate) after(uint64) <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.hold {
		return nil
	}
	c := make(chan struct{})
	g.held = append(g.held, c)
	return c
}

func (g *fakeGate) check() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fail
}

func (g *fakeGate) cycleAfter(uint64) <-chan struct{} { return nil }

func (g *fakeGate) set(hold bool, fail error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.hold, g.fail = hold, fail
}

func (g *fakeGate) waiting() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.held)
}

// release ends the wait of the i-th held read.
func (g *fakeGate) release(i int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	close(g.held[i])
}

type orig struct {
	t    *testing.T
	db   *sql.DB
	gate *fakeGate
	rm   *reminders
	a    *actor
	o    *reminderLoop
	stop func() // ends the loop's Serve and waits for it

	capture sync.Mutex             // the capture lock: one call to the shard's connection at a time
	calls   atomic.Int64           // calls to the shard's connection begun
	hook    atomic.Pointer[func()] // run once, inside the next call
}

// newLoopRig makes a loop over a database of its own. wake is the fake wake of
// the app, and opts the reminders' options (zero values are filled in).
func newLoopRig(t *testing.T, wake wakeFunc, mutate func(*ReminderOptions)) *orig {
	t.Helper()
	dir := t.TempDir()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "shard.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(reservedSchema); err != nil {
		t.Fatal(err)
	}
	opts := ReminderOptions{Backoff: 20 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, MaxAge: 24 * time.Hour, NodeLimit: 256}
	if mutate != nil {
		mutate(&opts)
	}
	rm := newReminders(opts, realClock{}, slog.New(slog.DiscardHandler), &Stats{}, wake)
	r := &orig{t: t, db: db, gate: &fakeGate{}, rm: rm, a: &actor{Name: shardName("test.users/o1")}}
	r.serve()
	return r
}

// use is the shard's connection, under the capture lock.
func (r *orig) use(_ context.Context, fn func(*sql.DB) error) error {
	r.calls.Add(1)
	r.capture.Lock()
	defer r.capture.Unlock()
	if h := r.hook.Swap(nil); h != nil {
		(*h)()
	}
	return fn(r.db)
}

// serve starts a new loop and runs it until stop.
func (r *orig) serve() {
	r.o = newReminderLoop(r.rm, r.a, r.use, r.gate)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	o := r.o
	go func() {
		defer close(done)
		o.Serve(ctx)
	}()
	var once sync.Once
	r.stop = func() { once.Do(func() { cancel(); <-done }) }
	r.t.Cleanup(r.stop)
}

func (r *orig) exec(stmt string) {
	r.t.Helper()
	if _, err := r.db.Exec(stmt); err != nil {
		r.t.Fatal(err)
	}
}

// handle is what a handler does: delete the reminders that are due.
func (r *orig) handle() {
	r.exec(`DELETE FROM _shardlite_reminders WHERE due <= ` + strconv.FormatInt(time.Now().Unix(), 10))
}

// settled is true once the loop has finished every read asked of it.
func (r *orig) settled() bool { return r.o.answered.Load() == r.o.asked.Load() }

// read has the loop read the shard, as a kick from the flush cycle would, and
// waits for the read to finish.
func (r *orig) read() {
	r.t.Helper()
	r.o.kick()
	apptest.Eventually(r.t, 10*time.Second, "the loop's read", r.settled)
}

func (r *orig) remind(key string, due int64) {
	r.exec(fmt.Sprintf(`INSERT OR REPLACE INTO _shardlite_reminders (key, due) VALUES ('%s', %d)`, key, due))
}

// Reminders due together cause one wake, and a wake never overlaps another: the
// loop is one goroutine.
func TestLoopWakesOnceForSeveralReminders(t *testing.T) {
	t.Parallel()
	var wakes, inside, worst atomic.Int32
	var r *orig
	r = newLoopRig(t, func(context.Context, *actor) (int, error) {
		if n := inside.Add(1); n > worst.Load() {
			worst.Store(n)
		}
		wakes.Add(1)
		r.handle()
		inside.Add(-1)
		return http.StatusOK, nil
	}, nil)
	apptest.Eventually(t, 10*time.Second, "the read at activation", r.settled)
	r.exec(`INSERT INTO _shardlite_reminders (key, due) VALUES ('a', unixepoch()), ('b', unixepoch() - 1), ('c', unixepoch() - 2)`)
	r.read()
	apptest.Eventually(t, 10*time.Second, "the wake", func() bool { return wakes.Load() == 1 })
	r.read()
	r.read()
	if wakes.Load() != 1 || worst.Load() != 1 {
		t.Fatalf("%d wakes, %d at once", wakes.Load(), worst.Load())
	}
}

// A wake that fails (an error status, no answer, or a 2xx that leaves a
// reminder due) is repeated after a backoff, and a success resets it.
func TestLoopRetriesFailedWakes(t *testing.T) {
	t.Parallel()
	var wakes atomic.Int32
	var r *orig
	r = newLoopRig(t, func(context.Context, *actor) (int, error) {
		switch n := wakes.Add(1); n {
		case 1:
			return http.StatusInternalServerError, nil
		case 2:
			return 0, errors.New("no answer")
		case 3:
			return http.StatusOK, nil // and the reminder is still due
		}
		r.handle()
		return http.StatusOK, nil
	}, nil)
	r.exec(`INSERT INTO _shardlite_reminders (key, due) VALUES ('a', unixepoch())`)
	r.read()
	apptest.Eventually(t, 10*time.Second, "the fourth wake, which handles it", func() bool {
		var n int
		r.db.QueryRow(`SELECT count(*) FROM _shardlite_reminders`).Scan(&n)
		return wakes.Load() == 4 && n == 0
	})
	r.read() // after the wake's own read
	if f := r.rm.stats.ReminderWakeFailures.Load(); f != 3 {
		t.Fatalf("%d failures, want 3", f)
	}
	if r.o.failures != 0 || !r.o.notBefore.IsZero() {
		t.Fatalf("the backoff was not reset: %d failures, not before %v", r.o.failures, r.o.notBefore)
	}
}

// A reminder older than MaxAge is stale: it doesn't wake the app, and the loop
// sets no timer for it.
func TestLoopIgnoresStaleReminders(t *testing.T) {
	t.Parallel()
	var wakes atomic.Int32
	r := newLoopRig(t, func(context.Context, *actor) (int, error) { wakes.Add(1); return http.StatusOK, nil }, nil)
	r.remind("old", time.Now().Add(-25*time.Hour).Unix())
	r.read()
	if wakes.Load() != 0 || r.o.armed.Load() {
		t.Fatalf("%d wakes, timer set: %v", wakes.Load(), r.o.armed.Load())
	}
	r.remind("fresh", time.Now().Add(-time.Hour).Unix())
	r.read()
	apptest.Eventually(t, 10*time.Second, "the wake for the fresh one", func() bool { return wakes.Load() >= 1 })
}

// The wake of a read whose cycle failed doesn't go out, and the next read finds
// the reminder again.
func TestLoopReadsAgainWhenTheCycleFails(t *testing.T) {
	t.Parallel()
	var wakes atomic.Int32
	var r *orig
	r = newLoopRig(t, func(context.Context, *actor) (int, error) { wakes.Add(1); r.handle(); return http.StatusOK, nil }, nil)
	apptest.Eventually(t, 10*time.Second, "the read at activation", r.settled)
	r.exec(`INSERT INTO _shardlite_reminders (key, due) VALUES ('a', unixepoch())`)
	r.gate.set(true, errors.New("the sync failed"))
	r.o.kick()
	apptest.Eventually(t, 10*time.Second, "the read to wait for its cycle", func() bool { return r.gate.waiting() == 1 })
	r.gate.release(0)
	apptest.Eventually(t, 10*time.Second, "the read to finish", r.settled)
	if wakes.Load() != 0 {
		t.Fatal("woken while check says the sync failed")
	}
	r.gate.set(false, nil)
	r.read()
	apptest.Eventually(t, 10*time.Second, "the reminder read again", func() bool { return wakes.Load() == 1 })
}

// A kick while a read waits for its cycle (one that began before the read, as
// when the read races with the cycle that captures what it saw) reads again at
// once, rather than waiting for a cycle that may be long in coming.
func TestLoopReadsAgainWhenKickedWhileWaiting(t *testing.T) {
	t.Parallel()
	var wakes atomic.Int32
	var r *orig
	r = newLoopRig(t, func(context.Context, *actor) (int, error) { wakes.Add(1); r.handle(); return http.StatusOK, nil }, nil)
	apptest.Eventually(t, 10*time.Second, "the read at activation", r.settled)
	r.exec(`INSERT INTO _shardlite_reminders (key, due) VALUES ('a', unixepoch())`)
	r.gate.set(true, nil)
	r.o.kick()
	apptest.Eventually(t, 10*time.Second, "the read to wait for its cycle", func() bool { return r.gate.waiting() == 1 })
	r.gate.set(false, nil) // the next read is durable at once; the first one's cycle never completes
	r.read()
	apptest.Eventually(t, 10*time.Second, "the wake", func() bool { return wakes.Load() == 1 })
}

// Stopping the loop cancels a wake in progress and returns once it has ended,
// with the reminder still in the table.
func TestLoopStopCancelsAWake(t *testing.T) {
	t.Parallel()
	started, ended := make(chan struct{}), make(chan struct{})
	r := newLoopRig(t, func(ctx context.Context, _ *actor) (int, error) {
		close(started)
		<-ctx.Done() // the app never answers
		close(ended)
		return 0, context.Cause(ctx)
	}, nil)
	r.exec(`INSERT INTO _shardlite_reminders (key, due) VALUES ('a', unixepoch())`)
	r.o.kick()
	<-started
	r.stop()
	select {
	case <-ended:
	default:
		t.Fatal("the wake had not ended by the time the loop stopped")
	}
	var n int
	r.db.QueryRow(`SELECT count(*) FROM _shardlite_reminders`).Scan(&n)
	if n != 1 || r.rm.stats.ReminderWakeFailures.Load() != 0 {
		t.Fatalf("%d reminders, %d failures: a wake cut short is no failure", n, r.rm.stats.ReminderWakeFailures.Load())
	}
}

func TestReminderTimerFiresOnTheClock(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	var tm reminderTimer
	tm.arm(clk, clk.Now(), 0, false, time.Time{})
	if tm.armed() {
		t.Fatal("armed with no reminder")
	}
	tm.arm(clk, clk.Now(), clk.Now().Unix()+60, true, time.Time{})
	clk.Advance(59 * time.Second)
	select {
	case <-tm.c:
		t.Fatal("fired early")
	default:
	}
	clk.Advance(time.Second)
	<-tm.c
	tm.fired()
	if tm.armed() {
		t.Fatal("a timer that fired is still armed")
	}
	// A backoff that ends after the due time moves the timer to its end.
	tm.arm(clk, clk.Now(), clk.Now().Unix()-5, true, clk.Now().Add(30*time.Second))
	clk.Advance(29 * time.Second)
	select {
	case <-tm.c:
		t.Fatal("fired inside the backoff")
	default:
	}
	clk.Advance(time.Second)
	<-tm.c
}

// An entry that can't be created before a release keeps the shard active with its
// reminders loop running, and the next idle look tries again.
func TestEntryFailingBeforeAReleaseKeepsTheLoop(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	r := newSRigWith(t, both(quick, logTo(&logs)))
	var failing atomic.Bool
	failing.Store(true)
	r.local.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if failing.Load() && op == blobtest.OpCreate && strings.HasPrefix(key, remindex.Prefix) {
			return errors.New("storage down")
		}
		return nil
	})
	due := time.Now().Add(time.Hour).Unix()
	r.remind("test.users/hr1", "k", due)
	r.closeInApp("test.users/hr1")
	apptest.Eventually(t, apptest.Timeout, "a release to fail on the entry", func() bool {
		return strings.Contains(logs.String(), "could not deactivate an idle shard") && strings.Contains(logs.String(), "creating a reminder entry")
	})
	if !r.m.IsActive(shardName("test.users/hr1")) {
		t.Fatal("the shard was let go of without its entry")
	}
	o := r.shard("test.users/hr1").loop.load()
	select {
	case <-o.ended:
		t.Fatal("the shard stayed active with no reminders loop")
	default:
	}
	failing.Store(false)
	apptest.Eventually(t, apptest.Timeout, "the next idle look to let go of the shard", func() bool { return r.cached("test.users/hr1") })
	if m, err := r.manifest("test.users/hr1"); err != nil || m.NextReminder != remindex.MinuteOf(due) {
		t.Fatalf("manifest %+v (%v), want next_reminder %d", m, err, remindex.MinuteOf(due))
	}
}

// At shutdown every shard's wakes are joined before any shard is let go
// of: here a wake held in the app, which takes a while to end once it is
// cancelled, has ended before the other shard's release writes anything.
func TestShutdownJoinsEveryWakeBeforeLettingGo(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{keep: true, hold: make(chan struct{})}
	defer close(rec.hold)
	r.record(rec, "")
	var ended, stopping atomic.Bool
	hook := wakeHookFn(func(ctx context.Context, call func() (int, error)) (int, error) {
		status, err := call()
		if ctx.Err() != nil {
			time.Sleep(200 * time.Millisecond)
		}
		ended.Store(true)
		return status, err
	})
	r.wakeHook.Store(&hook)
	var early atomic.Int32 // writes for the other shard while the wake was still running
	var seen atomic.Int32
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && stopping.Load() && strings.Contains(key, "lj2") {
			seen.Add(1)
			if !ended.Load() {
				early.Add(1)
			}
		}
		return nil
	})
	r.sql("test.slow/lj2", `INSERT INTO kv (payload) VALUES (x'01')`)
	r.remind("test.slow/lj1", "k", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "the wake to reach the app", func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.got) > 0
	})
	r.closeInApp("test.slow/lj1")
	r.closeInApp("test.slow/lj2")
	stopping.Store(true)
	if err := r.mrig.stop(); err != nil {
		t.Fatal(err)
	}
	if seen.Load() == 0 {
		t.Fatal("the other shard's release wrote nothing")
	}
	if n := early.Load(); n != 0 {
		t.Fatalf("%d writes of the other shard's release before the wake had ended", n)
	}
}

// The node's limit on wakes holds across shards.
func TestWakesAreLimitedOnTheNode(t *testing.T) {
	t.Parallel()
	const perNode = 3
	var inside, worst atomic.Int32
	rm := newReminders(ReminderOptions{NodeLimit: perNode}, realClock{}, slog.New(slog.DiscardHandler), &Stats{},
		func(context.Context, *actor) (int, error) {
			n := inside.Add(1)
			for {
				m := worst.Load()
				if n <= m || worst.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			inside.Add(-1)
			return http.StatusOK, nil
		})
	var g errgroup.Group
	for i := range 100 {
		g.Go(func() error {
			ok, err := rm.call(context.Background(), &actor{Name: shardName("test.users/" + strconv.Itoa(i))})
			if !ok {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	if worst.Load() > perNode || worst.Load() < 2 {
		t.Fatalf("%d wakes at once, limit %d", worst.Load(), perNode)
	}
	// A wake waiting for a place ends with its context.
	for range perNode {
		rm.node <- struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if ok, err := rm.call(ctx, &actor{}); ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a full node gave a place: %v %v", ok, err)
	}
}
