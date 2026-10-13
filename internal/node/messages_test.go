package node

import (
	"bytes"
	"context"
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

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

// --- helpers ---

// sql has the app (its own process) run a statement on a shard.
func (r *srig) sql(target, stmt string) {
	r.t.Helper()
	ref := r.acquire(target)
	defer ref.Release()
	path := ref.Path()
	r.omu.Lock()
	defer r.omu.Unlock()
	if !r.opened[path] {
		r.app.Must(r.t, "open", path, path)
		r.opened[path] = true
	}
	r.app.Must(r.t, "exec", path, strings.ReplaceAll(stmt, "\n", " ")) // the app's protocol is one line per command
}

// count is the answer of a query that returns one number, asked by the app process.
func (r *srig) count(target, query string) int {
	r.t.Helper()
	ref := r.acquire(target)
	defer ref.Release()
	out := r.app.Must(r.t, "query", "q", ref.Path(), query)
	s, err := strconv.Unquote(out)
	if err != nil {
		r.t.Fatalf("query %q gave %q", query, out)
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		r.t.Fatalf("query %q gave %q", query, s)
	}
	return n
}

// liveRows is how many rows the outbox holds (the node gives up on rows in
// memory only, so a row it gave up on is still counted).
func (r *srig) liveRows(target string) int {
	return r.count(target, `SELECT count(*) FROM _shardlite_outbox`)
}

// journalEntries is how many LTX records, and how many commits in them, the
// node's journal holds for a shard, and in how many segments.
func (r *srig) journalEntries(shard string) (records, commits, segments int) {
	r.t.Helper()
	ctx := context.Background()
	lowest, _, err := segment.Bounds(ctx, r.n.Store, r.n.Name)
	if err != nil {
		r.t.Fatal(err)
	}
	seen := map[uint64]bool{}
	err = segment.Scan(ctx, r.n.Store, r.n.Name, max(lowest, 1), func(seq uint64, e segment.Entry) error {
		if e.Shard == shard {
			records++
			commits += int(e.Count)
			seen[seq] = true
		}
		return nil
	})
	if err != nil {
		r.t.Fatal(err)
	}
	return records, commits, len(seen)
}

func (r *srig) inboxRows(target string) int {
	return r.count(target, `SELECT count(*) FROM _shardlite_inbox`)
}

// durableInbox is how many inbox rows a restore from blob storage has.
func (r *srig) durableInbox(target string) int {
	r.t.Helper()
	dst := filepath.Join(r.t.TempDir(), "inbox.db")
	if _, err := r.n.Files.Restore(context.Background(), target, dst, shardfile.RestoreOptions{}); err != nil {
		if errors.Is(err, shardfile.ErrNothingToRestore) || errors.Is(err, blob.ErrNotFound) {
			return 0
		}
		r.t.Fatalf("restore %s: %v", target, err)
	}
	return strings.Count(sqlitetest.Dump(r.t, dst), "_shardlite_inbox: ")
}

// addMessage inserts an outbox row (in a transaction of its own).
func (r *srig) addMessage(sender, target, path, body string) {
	r.t.Helper()
	r.sql(sender, fmt.Sprintf(`INSERT INTO _shardlite_outbox (target, path, body) VALUES ('%s', '%s', '%s')`, target, path, body))
}

// intakeHookFn wraps the intake of a row: call makes the real one.
type intakeHookFn func(n int, sender *ActiveShard, target string, row *outRow, call func() (int, error)) (int, error)

// hookIntake wraps the node's intake, for this server and the ones started
// after a restart. It returns the number of intakes that reached the hook.
func (r *srig) hookIntake(hook intakeHookFn) *atomic.Int64 {
	r.hook, r.hookN = hook, new(atomic.Int64)
	r.applyHook()
	return r.hookN
}

func (r *srig) applyHook() {
	if r.hook == nil {
		return
	}
	tr := *r.baseTr
	inner, hook, n := tr.intake, r.hook, r.hookN
	tr.intake = func(ctx context.Context, sender *ActiveShard, target string, row *outRow) (int, error) {
		i := int(n.Add(1))
		return hook(i, sender, target, row, func() (int, error) { return inner(ctx, sender, target, row) })
	}
	r.m.msgs.setTransport(&tr)
}

// intakeDownUntil has every intake fail with 503 until up is set, and counts them.
func (r *srig) intakeDownUntil(up *atomic.Bool) *atomic.Int64 {
	return r.hookIntake(func(_ int, _ *ActiveShard, _ string, _ *outRow, call func() (int, error)) (int, error) {
		if !up.Load() {
			return http.StatusServiceUnavailable, errors.New("the receiver's node is down")
		}
		return call()
	})
}

// armed is true once the shard has its inbox timer set (for the earliest row
// that is not due yet), which happens when the commit with the row is durable.
func (r *srig) armed(target string) func() bool {
	return func() bool {
		r.m.mu.Lock()
		a := r.m.active[target]
		r.m.mu.Unlock()
		if a == nil || a.msgs == nil {
			return false
		}
		a.msgs.mu.Lock()
		defer a.msgs.mu.Unlock()
		return a.msgs.timer != nil
	}
}

// lookedAt is true once the janitor has looked at the shard and found live
// messages holding it (that sets the shard's next look).
func (r *srig) lookedAt(target string) func() bool {
	return func() bool { return !r.delay(target).IsZero() }
}

// settle returns once the node has read what the shard holds as of now and
// written the hints that calls for: two flush cycles (the first may have begun
// before the call), then the shard's hint reconciliation.
func (r *srig) settle(target string) {
	r.t.Helper()
	r.afterCycle()
	r.afterCycle()
	r.m.mu.Lock()
	a := r.m.active[target]
	r.m.mu.Unlock()
	if a == nil || a.msgs == nil {
		return
	}
	eventually(r.t, "the hints of "+target+" to be reconciled", func() bool {
		a.msgs.mu.Lock()
		defer a.msgs.mu.Unlock()
		return !a.msgs.recRunning
	})
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
func quick(o *ManagerOptions) {
	o.MessageBackoff, o.MessageMaxBackoff = 20*time.Millisecond, 50*time.Millisecond
}

func both(fs ...func(*ManagerOptions)) func(*ManagerOptions) {
	return func(o *ManagerOptions) {
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

func logTo(buf *syncBuffer) func(*ManagerOptions) {
	return func(o *ManagerOptions) { o.Logger = slog.New(slog.NewTextHandler(buf, nil)) }
}

// delivery is a request the fake app got for an inbox row.
type delivery struct {
	target, path, method, body string
	hdr                        http.Header
	senderRows                 int // durable rows of the shard in blob storage when it arrived
}

// recorder is a fake app that records every call for an inbox row and answers
// by a function. Unless keep is set it consumes the row first, the way a
// handler does, when the answer is a 2xx or is lost.
type recorder struct {
	mu     sync.Mutex
	got    []delivery
	answer func(n int, q *http.Request) int // n counts the calls to the same path, from 1
	keep   bool                             // leave the inbox row alone
	hold   chan struct{}                    // if set, calls wait for it to be closed before they answer
	hits   map[string]int
}

// record has the fake app record the calls for inbox rows. senderRows, if set,
// is a shard whose durable rows are counted when each call arrives.
func (r *srig) record(rec *recorder, senderRows string) {
	rec.hits = map[string]int{}
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		id := q.Header.Get("Shardlite-Inbox-Id")
		if q.Header.Get("Shardlite-Caller") == "" || id == "" {
			return // not a call for an inbox row
		}
		b := new(bytes.Buffer)
		b.ReadFrom(q.Body)
		d := delivery{target: q.Header.Get("Shardlite-Target"), path: q.URL.Path, method: q.Method, body: b.String(), hdr: q.Header.Clone()}
		if senderRows != "" {
			d.senderRows = r.rows(senderRows)
		}
		rec.mu.Lock()
		rec.got = append(rec.got, d)
		rec.hits[q.URL.Path]++
		n := rec.hits[q.URL.Path]
		rec.mu.Unlock()
		if rec.hold != nil {
			<-rec.hold
		}
		status := http.StatusOK
		if rec.answer != nil {
			status = rec.answer(n, q)
		}
		if !rec.keep && (status/100 == 2 || status == -1) {
			r.sql(d.target, `DELETE FROM _shardlite_inbox WHERE id = `+id)
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

func (rec *recorder) all() []delivery {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]delivery(nil), rec.got...)
}

func (rec *recorder) count() int { return len(rec.all()) }

func (rec *recorder) onPath(p string) []delivery {
	var out []delivery
	for _, d := range rec.all() {
		if d.path == p {
			out = append(out, d)
		}
	}
	return out
}

func (d delivery) id() string { return d.hdr.Get("Shardlite-Inbox-Id") }

// downUntil is an app answer function: 503 until up is set.
func downUntil(up *atomic.Bool) func(int, *http.Request) int {
	return func(int, *http.Request) int {
		if up.Load() {
			return 200
		}
		return 503
	}
}

// --- hops ---

// A chain that alternates messages and calls stops at the hop limit: the message
// past it is given up on or the call is refused, and nothing runs further.
func TestChainOfCallsAndMessagesStopsAtTheHopLimit(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	r := newSRigWith(t, both(quick, logTo(&logs)))
	var invocations, maxHops, refused atomic.Int64
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Shardlite-Caller") == "" {
			return
		}
		hops, _ := strconv.Atoi(q.Header.Get("Shardlite-Hops"))
		invocations.Add(1)
		for {
			if old := maxHops.Load(); int64(hops) <= old || maxHops.CompareAndSwap(old, int64(hops)) {
				break
			}
		}
		switch q.URL.Path {
		case "/pong": // a message arrived at h2: consume it, then call h1, passing the hops on
			go r.sql("users/h2", `DELETE FROM _shardlite_inbox WHERE id = `+q.Header.Get("Shardlite-Inbox-Id"))
			c, _ := http.NewRequest("POST", r.ctl+"/relay", nil)
			c.Header.Set("Shardlite-Target", "users/h1")
			c.Header.Set("Shardlite-Caller", "users/h2")
			c.Header.Set("Shardlite-Hops", strconv.Itoa(hops))
			resp, err := r.client.Do(c)
			if err == nil {
				if resp.StatusCode == http.StatusLoopDetected {
					refused.Add(1)
				}
				resp.Body.Close()
			}
		case "/relay": // h1 answers the call by sending a message to h2, passing the hops on
			go r.sql("users/h1", fmt.Sprintf(`INSERT INTO _shardlite_outbox (target, path, hops) VALUES ('users/h2', '/pong', %d)`, hops))
		}
	})
	r.sql("users/h1", `INSERT INTO _shardlite_outbox (target, path, hops) VALUES ('users/h2', '/pong', 0)`)
	eventually(t, "the chain to hit the limit", func() bool { return refused.Load()+r.m.Stats.MessagesGivenUp.Load() >= 1 })
	r.settle("users/h1")
	r.settle("users/h2")
	settled := invocations.Load()
	r.settle("users/h1")
	r.settle("users/h2")
	if invocations.Load() != settled {
		t.Fatalf("the chain kept going: %d then %d invocations", settled, invocations.Load())
	}
	if maxHops.Load() > int64(DefaultMaxHops) {
		t.Fatalf("a handler ran at %d hops, the limit is %d", maxHops.Load(), DefaultMaxHops)
	}
	if maxHops.Load() < int64(DefaultMaxHops)-1 {
		t.Fatalf("the chain stopped early, at %d hops", maxHops.Load())
	}
	if r.m.Stats.MessagesGivenUp.Load() > 0 && !strings.Contains(logs.String(), "giving up on an outbox row") {
		t.Fatal("giving up was not logged")
	}
}

// A message that has used up the hops is given up on, not sent.
func TestMessagePastTheHopLimitIsGivenUpOn(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	r.sql("users/h3", fmt.Sprintf(`INSERT INTO _shardlite_outbox (target, path, hops) VALUES ('users/h4', '/x', %d)`, DefaultMaxHops))
	eventually(t, "giving up", func() bool { return r.m.Stats.MessagesGivenUp.Load() == 1 })
	if rec.count() != 0 || r.m.IsActive("users/h4") || r.liveRows("users/h3") != 1 {
		t.Fatal("a message past the hop limit reached the receiver, or its row was changed")
	}
	// One hop less is delivered.
	r.sql("users/h3", fmt.Sprintf(`INSERT INTO _shardlite_outbox (target, path, hops) VALUES ('users/h4', '/x', %d)`, DefaultMaxHops-1))
	eventually(t, "delivery at the limit", func() bool { return rec.count() == 1 })
	if h := rec.all()[0].hdr.Get("Shardlite-Hops"); h != strconv.Itoa(DefaultMaxHops) {
		t.Fatalf("hops %q", h)
	}
}

// --- the tables ---

func TestOutboxAndInboxExistAfterActivation(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	ref := r.acquire("users/t1")
	defer ref.Release()
	r.sql("users/t1", `SELECT 1`)
	out := r.app.Must(t, "query", "q", ref.Path(), `SELECT name FROM sqlite_master WHERE name LIKE '_shardlite%' ORDER BY name`)
	if want := strconv.Quote("_shardlite_inbox\n_shardlite_inbox_due\n_shardlite_outbox"); out != want {
		t.Fatalf("schema objects %s, want %s", out, want)
	}
	for table, wants := range map[string][]string{
		"_shardlite_outbox": {"id INTEGER 0 1", "target TEXT 1", "path TEXT 1", "content_type TEXT 0", "body BLOB 0", "hops INTEGER 1 0", "created_at INTEGER 1 unixepoch()"},
		"_shardlite_inbox":  {"id INTEGER 0 1", "key TEXT 0", "sender TEXT 0", "path TEXT 1", "content_type TEXT 0", "body BLOB 0", "hops INTEGER 1 0", "due INTEGER 1 unixepoch()"},
	} {
		cols := r.app.Must(t, "query", "q", ref.Path(), `SELECT group_concat(name || ' ' || type || ' ' || "notnull" || ' ' || CASE WHEN pk THEN pk ELSE ifnull(dflt_value, '-') END, ', ') FROM pragma_table_info('`+table+`')`)
		for _, want := range wants {
			if !strings.Contains(cols, want) {
				t.Fatalf("columns of %s: %s lack %q", table, cols, want)
			}
		}
	}
	// The outbox has no dead column, and the old tables are not created.
	if n := r.count("users/t1", `SELECT count(*) FROM pragma_table_info('_shardlite_outbox') WHERE name = 'dead'`); n != 0 {
		t.Fatal("the outbox has a dead column")
	}
	if n := r.count("users/t1", `SELECT count(*) FROM sqlite_master WHERE name IN ('_shardlite_messages', '_shardlite_reminders')`); n != 0 {
		t.Fatalf("%d old tables", n)
	}
}

// Activating a shard that has the tables and indexes commits nothing, whether it
// comes from the cache or from blob storage.
func TestActivatingAShardWithTheTablesCommitsNothing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		purge bool
	}{{"cached", false}, {"restored", true}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newSRig(t)
			r.sql("users/t2", `INSERT INTO kv (payload) VALUES (x'01')`)
			ref := r.acquire("users/t2")
			path := ref.Path()
			ref.Release()
			r.closeInApp("users/t2")
			eventually(t, "deactivation", func() bool { return r.cached("users/t2") })
			if tc.purge {
				row, ok, err := r.m.idx.get("users/t2")
				if err != nil || !ok {
					t.Fatal(err)
				}
				if err := r.m.evict(context.Background(), row); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(path); err == nil {
					t.Fatal("still cached")
				}
			}
			man, err := r.manifest("users/t2")
			if err != nil {
				t.Fatal(err)
			}
			journal := r.calls("create", "journal")
			ref = r.acquire("users/t2")
			defer ref.Release()
			a := r.m.active["users/t2"]
			sh := a.cap.Load()
			if got := sh.Position(); got != man.LastPosition() {
				t.Fatalf("position %d after activating, the manifest ends at %d: the activation captured a commit", got, man.LastPosition())
			}
			before, err := sh.Header()
			if err != nil {
				t.Fatal(err)
			}
			if err := r.m.ensureMessages(context.Background(), sh); err != nil {
				t.Fatal(err)
			}
			if after, _ := sh.Header(); after != before {
				t.Fatal("creating the tables again committed")
			}
			r.cycle()
			r.cycle()
			if r.calls("create", "journal") != journal {
				t.Fatal("activating the shard wrote to the journal")
			}
		})
	}
}

// A shard whose only objects are the reserved ones is deleted as never written;
// a row in either table is the app's data.
func TestNeverWrittenWithReservedTables(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	ref := r.acquire("users/t3")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("users/t3"))
	if _, err := r.rawManifest("users/t3"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest of a shard with only empty reserved tables: %v, want none", err)
	}
	if r.m.Stats.Deletes.Load() != 1 {
		t.Fatalf("deletes %d", r.m.Stats.Deletes.Load())
	}

	// The test app creates two tables of its own when it opens a shard, so these
	// drop them to leave only the reserved ones.
	r.sql("users/t5", `DROP TABLE kv; DROP TABLE acks`)
	r.closeInApp("users/t5")
	eventually(t, "deactivation", r.inactive("users/t5"))
	if _, err := r.rawManifest("users/t5"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a shard where the app left only reserved tables was kept: %v", err)
	}
	if r.m.Stats.Deletes.Load() != 2 {
		t.Fatalf("deletes %d", r.m.Stats.Deletes.Load())
	}

	// An outbox row the node gave up on, and a row in the inbox (far away, so it
	// holds nothing), are kept.
	r.sql("users/t4", `DROP TABLE kv; DROP TABLE acks; INSERT INTO _shardlite_outbox (target, path) VALUES ('users/x', 'no-slash')`)
	r.closeInApp("users/t4")
	eventually(t, "deactivation", func() bool { return r.cached("users/t4") })
	if _, err := r.rawManifest("users/t4"); err != nil {
		t.Fatalf("a shard holding a row the node gave up on was deleted: %v", err)
	}
	r.sql("users/t6", `DROP TABLE kv; DROP TABLE acks; INSERT INTO _shardlite_inbox (path, due) VALUES ('/x', unixepoch() + 999999)`)
	r.closeInApp("users/t6")
	eventually(t, "deactivation", func() bool { return r.cached("users/t6") })
	if _, err := r.rawManifest("users/t6"); err != nil {
		t.Fatalf("a shard holding an inbox row was deleted: %v", err)
	}
	if r.m.Stats.Deletes.Load() != 2 {
		t.Fatalf("deletes %d", r.m.Stats.Deletes.Load())
	}
}

// --- from the outbox into the inbox, and to the handler ---

func TestMessageGoesThroughTheInboxOnlyAfterEachCommitIsDurable(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	// The handler waits until the acknowledgement has been looked at, so that its
	// own delete does not change what is seen.
	rec := &recorder{hold: make(chan struct{})}
	r.record(rec, "")
	inner := *r.handler.Load()
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Shardlite-Inbox-Id") != "" {
			<-rec.hold
			r.commit(q.Header.Get("Shardlite-Path")) // the receiver does its work before answering
		}
		inner(w, q)
	})
	var senderRowsAtIntake, receiverRowsAtAck, liveAtAck atomic.Int64
	var seen atomic.Bool
	intakes := r.hookIntake(func(_ int, sender *ActiveShard, target string, _ *outRow, call func() (int, error)) (int, error) {
		senderRowsAtIntake.Store(int64(r.rows(sender.Name)))
		status, err := call()
		receiverRowsAtAck.Store(int64(r.durableInbox(target)))
		liveAtAck.Store(int64(r.liveRows(sender.Name)))
		seen.Store(true)
		close(rec.hold)
		return status, err
	})
	r.sql("wallet/s1", `BEGIN; INSERT INTO kv (payload) VALUES (x'01'); INSERT INTO _shardlite_outbox (target, path, content_type, body, hops) VALUES ('users/r1', '/in?x=1', 'text/plain', 'hello', 3); COMMIT;`)
	if intakes.Load() != 0 || rec.count() != 0 || r.m.IsActive("users/r1") { // only a flush cycle reads rows, and none runs until cyclesWhile
		t.Fatal("sent before any flush")
	}
	if r.rows("wallet/s1") != 0 {
		t.Fatal("the sender's commit was in blob storage already")
	}
	r.cyclesWhile("the handler to be called", func() bool { return rec.count() >= 1 && seen.Load() })
	if senderRowsAtIntake.Load() != 1 {
		t.Fatalf("the intake was sent with %d of the sender's rows in blob storage, want 1", senderRowsAtIntake.Load())
	}
	if receiverRowsAtAck.Load() != 1 || liveAtAck.Load() != 1 {
		t.Fatalf("at the acknowledgement the receiver's inbox had %d durable rows and the sender %d live ones, want 1 and 1", receiverRowsAtAck.Load(), liveAtAck.Load())
	}
	d := rec.all()[0]
	if d.method != "POST" || d.path != "/in" || d.body != "hello" || d.target != "users/r1" {
		t.Fatalf("request %+v", d)
	}
	if d.hdr.Get("Shardlite-Caller") != "wallet/s1" || d.hdr.Get("Shardlite-Hops") != "4" || d.hdr.Get("Content-Type") != "text/plain" || d.id() == "" {
		t.Fatalf("headers %v", d.hdr)
	}
	for _, h := range []string{"Idempotency-Key", "Shardlite-Sync", "Shardlite-If-Active"} {
		if d.hdr.Get(h) != "" {
			t.Fatalf("%s reached the app", h)
		}
	}
	// The sender's row goes once the intake is acknowledged; the handler's delete
	// and its work are the receiver's one commit.
	r.cyclesWhile("the sender's row to be deleted", func() bool { return r.liveRows("wallet/s1") == 0 })
	r.cyclesWhile("the inbox row to be consumed", func() bool { return r.inboxRows("users/r1") == 0 })
	r.cycle()
	if rec.count() != 1 || r.count("users/r1", `SELECT count(*) FROM kv`) != 1 {
		t.Fatalf("%d calls, %d rows of work", rec.count(), r.count("users/r1", `SELECT count(*) FROM kv`))
	}
	if intakes.Load() != 1 {
		t.Fatalf("%d intakes", intakes.Load())
	}
}

func TestMessageHasContentTypeDefault(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	r.addMessage("users/c1", "users/c2", "/x", "")
	eventually(t, "the delivery", func() bool { return rec.count() == 1 })
	if ct := rec.all()[0].hdr.Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("Content-Type %q", ct)
	}
}

// A message whose sender's commit is lost is never sent.
func TestMessageOfALostCommitIsNeverSent(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	rec := &recorder{}
	r.record(rec, "")
	r.sql("wallet/lost", `BEGIN; INSERT INTO kv (payload) VALUES (x'01'); INSERT INTO _shardlite_outbox (target, path) VALUES ('users/r1', '/in'); COMMIT;`)
	r.closeInApp("wallet/lost")
	r.crash()
	r.n.Close(context.Background(), false)

	r.startNode()
	r.startManager(manualFlush)
	r.startServer()
	r.record(rec, "")
	ref := r.acquire("wallet/lost")
	defer ref.Release()
	for range 10 {
		r.cycle()
	}
	if n := r.liveRows("wallet/lost"); n != 0 { // a row that is not there cannot be sent
		t.Fatalf("%d rows came back from the lost commit", n)
	}
	if rec.count() != 0 || r.m.IsActive("users/r1") {
		t.Fatal("a message whose commit was lost was sent")
	}
}

// A lost acknowledgement of the intake means the batch is sent again, the inbox
// has the row twice and the handler is called for each.
func TestLostAcknowledgementOfTheIntakeDeliversTwice(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	r.hookIntake(func(n int, _ *ActiveShard, _ string, _ *outRow, call func() (int, error)) (int, error) {
		status, err := call()
		if n == 1 {
			return 0, errors.New("the connection broke before the answer") // the intake is durable, the answer is lost
		}
		return status, err
	})
	r.addMessage("users/a1", "users/a2", "/pay", "x")
	eventually(t, "two calls", func() bool { return rec.count() == 2 })
	got := rec.all()
	if got[0].id() == got[1].id() || got[0].body != "x" || got[1].body != "x" {
		t.Fatalf("calls %+v", got)
	}
	eventually(t, "the sender's row to go", func() bool { return r.liveRows("users/a1") == 0 })
	eventually(t, "both inbox rows to be consumed", func() bool { return r.inboxRows("users/a2") == 0 })
	r.settle("users/a2")
	if rec.count() != 2 {
		t.Fatalf("%d calls", rec.count())
	}
}

// A lost acknowledgement of the handler: the row is gone, so there is no second
// call and the work is done once.
func TestLostAcknowledgementOfTheHandlerDoesNotCallTwice(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	inner := *r.handler.Load()
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if id := q.Header.Get("Shardlite-Inbox-Id"); q.URL.Path == "/work" && id != "" {
			r.sql("users/la2", `BEGIN; INSERT INTO kv (payload) VALUES (x'01'); DELETE FROM _shardlite_inbox WHERE id = `+id+`; COMMIT;`)
		}
		inner(w, q)
	})
	rec.answer = func(n int, q *http.Request) int {
		if n == 1 {
			return -1 // the acknowledgement is lost
		}
		return 200
	}
	r.addMessage("users/la1", "users/la2", "/work", "")
	eventually(t, "the first call", func() bool { return len(rec.onPath("/work")) >= 1 })
	// The lost answer is retried. (The retry's look at the row waits for a
	// snapshot that can be missed, see the report, so the test does not wait for it.)
	eventually(t, "a retry", func() bool { return r.m.Stats.MessagesRetried.Load() >= 1 })
	r.settle("users/la2")
	if n := len(rec.onPath("/work")); n != 1 {
		t.Fatalf("%d calls, want 1: the retry should see the row gone", n)
	}
	if n := r.count("users/la2", `SELECT count(*) FROM kv`); n != 1 {
		t.Fatalf("the work was done %d times", n)
	}
}

// An intake is one request for the row, shaped like the call it will become:
// its body unchanged (a large binary one here), its content type, hops and
// sender, answered 202 once the receiver's inbox row is durable.
func TestIntakeIsTheRowAsOneRequest(t *testing.T) {
	t.Parallel()
	var logs syncBuffer // the handler leaves the row alone
	r := newSRigWith(t, both(quick, logTo(&logs)))
	rec := &recorder{keep: true}
	r.record(rec, "")
	body := make([]byte, 3<<20)
	for i := range body {
		body[i] = byte(i * 7 % 256) // every byte value, nulls included
	}
	sender := r.acquire("users/ia1")
	defer sender.Release()
	row := &outRow{id: 1, target: "users/ia2", path: "/in?x=1", contentType: "application/x-thing", body: body, hops: 3, createdAt: time.Now().Unix()}
	status, err := r.srv.sendIntake(context.Background(), sender.a, row.target, row)
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("intake answered %d: %v", status, err)
	}
	// The answer came after the commit: the inbox row is in blob storage.
	if n := r.durableInbox("users/ia2"); n != 1 {
		t.Fatalf("%d durable inbox rows at the acknowledgement", n)
	}
	if n := r.count("users/ia2", fmt.Sprintf(`SELECT count(*) FROM _shardlite_inbox WHERE sender = 'users/ia1' AND key IS NULL AND path = '/in?x=1' AND content_type = 'application/x-thing' AND hops = 4 AND length(body) = %d AND due <= unixepoch()`, len(body))); n != 1 {
		t.Fatalf("the inbox row does not match the outbox row")
	}
	eventually(t, "the handler", func() bool { return rec.count() == 1 })
	d := rec.all()[0]
	if d.body != string(body) || d.path != "/in" || d.hdr.Get("Shardlite-Caller") != "users/ia1" || d.hdr.Get("Shardlite-Hops") != "4" || d.hdr.Get("Content-Type") != "application/x-thing" {
		t.Fatalf("the handler got %q… with headers %v", d.body[:min(len(d.body), 20)], d.hdr)
	}
	// Without a content type the request says application/octet-stream.
	row = &outRow{id: 2, target: "users/ia2", path: "/plain", createdAt: time.Now().Unix()}
	if status, err := r.srv.sendIntake(context.Background(), sender.a, row.target, row); err != nil || status != http.StatusAccepted {
		t.Fatalf("intake answered %d: %v", status, err)
	}
	if n := r.count("users/ia2", `SELECT count(*) FROM _shardlite_inbox WHERE path = '/plain' AND content_type = 'application/octet-stream' AND hops = 1`); n != 1 {
		t.Fatal("the inbox row of the second intake does not match")
	}
	// Errors are answered as for calls: an undeclared kind is a 400.
	row = &outRow{id: 3, target: "nokind/ia3", path: "/x", createdAt: time.Now().Unix()}
	if status, _ := r.srv.sendIntake(context.Background(), sender.a, row.target, row); status != http.StatusBadRequest {
		t.Fatalf("an undeclared kind: %d", status)
	}
}

// Shardlite-Inbox: true from an app or a client is stripped, so the request is
// an ordinary call to the app and nothing is put in an inbox.
func TestInboxHeaderFromAnAppOrAClientIsStripped(t *testing.T) {
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
		target := "slow/strip-" + c.name
		resp, _ := r.req("POST", c.base, "/hello", map[string]string{"Shardlite-Target": target, "Shardlite-Inbox": "true", "Shardlite-Caller": "users/x"}, "body")
		if c.base == r.pub { // clients can't name a caller
			resp, _ = r.req("POST", c.base, "/hello", map[string]string{"Shardlite-Target": target, "Shardlite-Inbox": "true"}, "body")
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: answered %d, want the app's 200: the request was taken for an intake", c.name, resp.StatusCode)
		}
		q := seen.Load()
		if q == nil || q.Header.Get("Shardlite-Inbox") != "" {
			t.Fatalf("%s: the app saw %v", c.name, q)
		}
		if n := r.inboxRows(target); n != 0 {
			t.Fatalf("%s: %d inbox rows", c.name, n)
		}
	}
}

// Fifty messages to one receiver are fifty intakes and fifty inbox commits, but
// they share the receiver's flushes: all are acknowledged after a few.
func TestBurstOfMessagesSharesFlushes(t *testing.T) {
	t.Parallel()
	var logs syncBuffer // the handlers leave their rows alone
	r := newSRigWith(t, both(manualFlush, logTo(&logs)))
	rec := &recorder{keep: true}
	r.record(rec, "")
	intakes := r.hookIntake(func(_ int, _ *ActiveShard, _ string, _ *outRow, call func() (int, error)) (int, error) {
		return call()
	})
	var stmt strings.Builder
	stmt.WriteString("BEGIN;")
	for i := range 50 {
		fmt.Fprintf(&stmt, "INSERT INTO _shardlite_outbox (target, path, body) VALUES ('users/burst', '/n', '%d');", i)
	}
	stmt.WriteString("COMMIT;")
	r.sql("users/b0", stmt.String())
	flushes := r.n.Writer.Flushes()
	// The first cycle makes the sender's commit durable and starts the intakes,
	// at most MessageReceiverLimit at a time. Each wave goes into the receiver's
	// inbox whole before the cycle that makes it durable, so the bound below does
	// not depend on how fast the intakes arrive.
	r.cycle()
	cycles := 1
	for acked := 0; acked < 50; {
		wave := min(50, acked+r.m.opts.MessageReceiverLimit)
		eventually(t, "the next wave of intakes in the receiver's inbox", func() bool { return r.inboxRows("users/burst") >= wave })
		cycles += r.cyclesWhile("the wave to be acknowledged", func() bool { return r.m.Stats.MessagesDelivered.Load() >= int64(wave) })
		acked = wave
	}
	cycles += r.cyclesWhile("the rows to be deleted", func() bool { return r.liveRows("users/b0") == 0 })
	flushes = r.n.Writer.Flushes() - flushes
	cycles += r.cyclesWhile("fifty calls", func() bool { return rec.count() >= 50 })
	bodies := map[string]bool{}
	for _, d := range rec.all() {
		bodies[d.body] = true
	}
	if len(bodies) != 50 || rec.count() != 50 {
		t.Fatalf("%d calls with %d distinct bodies", rec.count(), len(bodies))
	}
	if intakes.Load() != 50 || r.inboxRows("users/burst") != 50 {
		t.Fatalf("%d intakes and %d inbox rows for 50 messages", intakes.Load(), r.inboxRows("users/burst"))
	}
	if flushes > 6 {
		t.Fatalf("50 messages took %d flushes", flushes)
	}
	records, commits, segments := r.journalEntries("users/burst")
	t.Logf("50 messages: %d flush cycles, %d flushes of the journal; the receiver has %d LTX records holding %d commits in %d segments", cycles, flushes, records, commits, segments)
}

// Rows go concurrently under the limits: more than fit at once all arrive.
func TestManyRowsToOneReceiverAllArrive(t *testing.T) {
	t.Parallel()
	var logs syncBuffer // the handlers leave their rows alone
	r := newSRigWith(t, both(quick, logTo(&logs)))
	rec := &recorder{keep: true}
	r.record(rec, "")
	var stmt strings.Builder
	stmt.WriteString("BEGIN;")
	for i := range 620 {
		fmt.Fprintf(&stmt, "INSERT INTO _shardlite_outbox (target, path, body) VALUES ('users/big', '/n', '%d');", i)
	}
	stmt.WriteString("COMMIT;")
	r.sql("users/b1", stmt.String())
	eventually(t, "all rows to be acknowledged", func() bool { return r.liveRows("users/b1") == 0 })
	eventually(t, "all calls", func() bool { return rec.count() >= 620 })
	r.settle("users/big")
	if rec.count() != 620 || r.inboxRows("users/big") != 620 {
		t.Fatalf("%d calls and %d inbox rows for 620 rows", rec.count(), r.inboxRows("users/big"))
	}
}

// A receiver whose kind changed in between answers 409, which is retried.
func TestConflictIsRetried(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	attempts := r.hookIntake(func(n int, _ *ActiveShard, _ string, _ *outRow, call func() (int, error)) (int, error) {
		if n <= 2 {
			return http.StatusConflict, errors.New("the receiver's node answered 409")
		}
		return call()
	})
	r.addMessage("users/cf1", "users/cf2", "/c", "x")
	eventually(t, "the delivery", func() bool { return rec.count() == 1 })
	eventually(t, "the row to go", func() bool { return r.liveRows("users/cf1") == 0 })
	if attempts.Load() != 3 || r.m.Stats.MessagesGivenUp.Load() != 0 {
		t.Fatalf("%d attempts, %d given up", attempts.Load(), r.m.Stats.MessagesGivenUp.Load())
	}
}

// A 400 gives up on that row only; the others arrive.
func TestBadRequestGivesUpOnlyThatRow(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	r := newSRigWith(t, both(quick, logTo(&logs)))
	rec := &recorder{}
	r.record(rec, "")
	r.hookIntake(func(_ int, _ *ActiveShard, _ string, row *outRow, call func() (int, error)) (int, error) {
		if row.path == "/bad" {
			return http.StatusBadRequest, errors.New("the receiver's node answered 400")
		}
		return call()
	})
	r.sql("users/br1", `BEGIN;
INSERT INTO _shardlite_outbox (target, path) VALUES ('users/br2', '/first');
INSERT INTO _shardlite_outbox (target, path) VALUES ('users/br2', '/bad');
INSERT INTO _shardlite_outbox (target, path) VALUES ('users/br2', '/last');
COMMIT;`)
	eventually(t, "the other rows", func() bool { return len(rec.onPath("/first")) == 1 && len(rec.onPath("/last")) == 1 })
	eventually(t, "giving up", func() bool { return r.m.Stats.MessagesGivenUp.Load() == 1 })
	eventually(t, "the rows to go", func() bool { return r.liveRows("users/br1") == 1 })
	r.settle("users/br1")
	if len(rec.onPath("/bad")) != 0 || r.liveRows("users/br1") != 1 {
		t.Fatal("the bad row was delivered or deleted")
	}
	if out := logs.String(); !strings.Contains(out, "giving up on an outbox row") || !strings.Contains(out, "path=/bad") {
		t.Fatalf("the log lacks the error: %s", out)
	}
}

// --- giving up on outbox rows ---

func TestGivingUpOnOutboxRows(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	r := newSRigWith(t, both(quick, logTo(&logs)))
	rec := &recorder{}
	r.record(rec, "")
	var bad atomic.Bool // the receiver's node answers 400 to /400 until this is cleared
	bad.Store(true)
	var attempts400 atomic.Int64
	r.hookIntake(func(_ int, _ *ActiveShard, _ string, row *outRow, call func() (int, error)) (int, error) {
		if row.path == "/400" && bad.Load() {
			attempts400.Add(1)
			return http.StatusBadRequest, &answerError{status: http.StatusBadRequest, body: "unknown reward"}
		}
		return call()
	})
	r.sql("users/d1", fmt.Sprintf(`BEGIN;
INSERT INTO _shardlite_outbox (target, path) VALUES ('users/d2', '/first');
INSERT INTO _shardlite_outbox (target, path) VALUES ('nokind/d2', '/never');
INSERT INTO _shardlite_outbox (target, path) VALUES ('users/not valid', '/never');
INSERT INTO _shardlite_outbox (target, path, created_at) VALUES ('users/d2', '/old', unixepoch() - 90000);
INSERT INTO _shardlite_outbox (target, path, hops) VALUES ('users/d2', '/hops', %d);
INSERT INTO _shardlite_outbox (target, path) VALUES ('users/d2', '/400');
INSERT INTO _shardlite_outbox (target, path) VALUES ('users/d2', '/last');
COMMIT;`, DefaultMaxHops))
	eventually(t, "the rows around the others", func() bool { return len(rec.onPath("/first")) == 1 && len(rec.onPath("/last")) == 1 })
	eventually(t, "six rows to be given up on", func() bool { return r.m.Stats.MessagesGivenUp.Load() == 5 })
	eventually(t, "the delivered rows to go", func() bool { return r.liveRows("users/d1") == 5 })
	out := logs.String()
	for _, want := range []string{"giving up on an outbox row", "sender=users/d1", "target=users/d2", "path=/old", "path=/hops", "path=/400", "target=nokind/d2", "id="} {
		if !strings.Contains(out, want) {
			t.Errorf("the log lacks %q", want)
		}
	}
	// Each row carries its own reason, and no other row is touched.
	for _, c := range []struct{ where, reason string }{
		{`path = '/old'`, `older than 24h0m0s`},
		{`path = '/hops'`, fmt.Sprintf("hops past limit %d", DefaultMaxHops)},
		{`path = '/400'`, `400 from users/d2: unknown reward`},
		{`target = 'nokind/d2'`, `undeclared kind "nokind"`},
		{`target = 'users/not valid'`, `invalid target "users/not valid"`},
	} {
		if got := r.count("users/d1", fmt.Sprintf(`SELECT count(*) FROM _shardlite_outbox WHERE %s AND error = '%s'`, c.where, c.reason)); got != 1 {
			t.Errorf("%s: no row with error %q", c.where, c.reason)
		}
	}
	if n := r.count("users/d1", `SELECT count(*) FROM _shardlite_outbox WHERE error IS NULL`); n != 0 {
		t.Fatalf("%d given-up rows have no error", n)
	}
	// Nothing is sent again while the shard is active, and the node writes nothing more.
	r.settle("users/d1")
	if n := attempts400.Load(); n != 1 {
		t.Fatalf("the 400 row was sent %d times", n)
	}
	for _, p := range []string{"/never", "/old", "/hops", "/400"} {
		if n := len(rec.onPath(p)); n != 0 {
			t.Errorf("%s was delivered %d times", p, n)
		}
	}
	if n := r.count("users/d1", `SELECT count(*) FROM _shardlite_outbox WHERE path IN ('/never', '/old', '/hops', '/400')`); n != 5 {
		t.Fatalf("%d rows left, want the 5 given up on, unchanged", n)
	}
	before, _, _ := r.journalEntries("users/d1")
	// The shard deactivates, with no hint for the rows.
	r.closeInApp("users/d1")
	eventually(t, "deactivation with rows given up on", func() bool { return r.cached("users/d1") })
	if keys := r.wakes(); len(keys) != 0 {
		t.Fatalf("wake hints %v for rows the node gave up on", keys)
	}
	if after, _, _ := r.journalEntries("users/d1"); after != before {
		t.Fatalf("the node committed %d times for rows it gave up on", after-before)
	}
	// After reactivation the rows are tried once more.
	bad.Store(false)
	ref := r.acquire("users/d1")
	defer ref.Release()
	eventually(t, "the 400 row to be tried again", func() bool { return len(rec.onPath("/400")) == 1 })
	eventually(t, "the row that now succeeds to be deleted", func() bool {
		return r.count("users/d1", `SELECT count(*) FROM _shardlite_outbox WHERE path = '/400'`) == 0
	})
	eventually(t, "the others to be given up on again", func() bool { return r.m.Stats.MessagesGivenUp.Load() == 9 })
	if n := len(rec.onPath("/first")); n != 1 {
		t.Fatalf("a delivered row was sent again: %d", n)
	}
}

// The receiver's node failing the intake is retried, and the messages arrive once it is back.
func TestFailedIntakeIsRetried(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	var up atomic.Bool
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/f1", "users/f2", "/a", "1")
	r.addMessage("users/f1", "users/f2", "/b", "2")
	eventually(t, "three failed intakes", func() bool { return attempts.Load() >= 3 })
	if rec.count() != 0 {
		t.Fatal("delivered while the intake failed")
	}
	if r.liveRows("users/f1") != 2 {
		t.Fatal("rows were deleted before the intake was acknowledged")
	}
	up.Store(true)
	eventually(t, "both messages", func() bool { return len(rec.onPath("/a")) == 1 && len(rec.onPath("/b")) == 1 })
	eventually(t, "the rows to go", func() bool { return r.liveRows("users/f1") == 0 })
	r.settle("users/f2")
	if rec.count() != 2 {
		t.Fatalf("%d calls", rec.count())
	}
	if r.m.Stats.MessagesRetried.Load() < 3 {
		t.Fatalf("%d retries", r.m.Stats.MessagesRetried.Load())
	}
}

// --- liveness ---

func TestShardWithLiveOutboxRowsStaysActive(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{}
	r.record(rec, "")
	var up atomic.Bool
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/k1", "users/k2", "/x", "")
	r.closeInApp("users/k1") // idle timeout is 300 ms
	// Past the idle timeout, with the janitor having looked at the shard and the
	// delivery retried (its backoff is on the clock too).
	advanceUntil(t, clk, 100*time.Millisecond, "the janitor to keep the shard", r.lookedAt("users/k1"))
	advanceUntil(t, clk, 50*time.Millisecond, "a second attempt", func() bool { return attempts.Load() >= 2 })
	if !r.m.IsActive("users/k1") {
		t.Fatal("a shard with a live outbox row was deactivated")
	}
	up.Store(true)
	advanceUntil(t, clk, 50*time.Millisecond, "the call (the retry backoff is on the clock)", func() bool { return rec.count() == 1 })
	advanceUntil(t, clk, 100*time.Millisecond, "deactivation after delivery", func() bool { return r.cached("users/k1") })
	if rec.all()[0].hdr.Get("Shardlite-Caller") != "users/k1" {
		t.Fatal("wrong sender")
	}
}

// After a crash the sender is reactivated and its messages go out.
func TestOutboxResumesAfterACrash(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	var up atomic.Bool
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/x1", "users/x2", "/resume", "payload")
	eventually(t, "a first attempt (the commit is durable by then)", func() bool { return attempts.Load() >= 1 })
	r.closeInApp("users/x1")
	r.crash()
	r.n.Close(context.Background(), false)

	r.startNode()
	if r.n.Adopted.Deltas == 0 {
		t.Fatalf("adoption reported no shards: %+v", r.n.Adopted)
	}
	// The hint is in blob storage before anything has activated the shard.
	if keys := r.wakes(); len(keys) != 1 || !strings.HasSuffix(keys[0], "/users/x1") {
		t.Fatalf("hints after adoption: %v", keys)
	}
	r.startManager(quick)
	r.startServer()
	up.Store(true)
	eventually(t, "the message to be delivered after the restart, with nothing asking for the sender", func() bool { return rec.count() == 1 && rec.all()[0].body == "payload" })
	eventually(t, "the row to be deleted", func() bool { return r.liveRows("users/x1") == 0 })
}

// A shard replayed after a crash with nothing to deliver is woken by its hint,
// which is deleted, and it deactivates when idle.
func TestReplayedShardWithNothingToDeliverIsWokenAndLetGo(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	r.sql("users/y1", `INSERT INTO kv (payload) VALUES (x'01')`)
	eventually(t, "the commit to be durable", func() bool { return r.rows("users/y1") == 1 })
	r.sql("users/y1", `INSERT INTO kv (payload) VALUES (x'02')`) // only in the journal when the node dies
	r.closeInApp("users/y1")
	r.crash()
	r.n.Close(context.Background(), false)

	r.startNode()
	if r.n.Adopted.Deltas != 1 {
		t.Fatalf("adoption: %+v", r.n.Adopted)
	}
	if keys := r.wakes(); len(keys) != 1 || !strings.HasSuffix(keys[0], "/users/y1") {
		t.Fatalf("hints after adoption: %v", keys)
	}
	r.startManager(quick)
	r.startServer()
	eventually(t, "the hint to be used", func() bool { return len(r.wakes()) == 0 })
	if r.m.Stats.Activations.Load() == 0 {
		t.Fatal("the shard was not activated")
	}
	eventually(t, "deactivation when idle", r.inactive("users/y1"))
}

// A hint that can't be written during adoption fails the adoption, which is
// repeated; the shard is woken in the end.
func TestFailedHintWriteDuringAdoptionIsRetried(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	var up atomic.Bool
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/z1", "users/z2", "/resume", "payload")
	eventually(t, "a first attempt (the commit is durable by then)", func() bool { return attempts.Load() >= 1 })
	r.closeInApp("users/z1")
	r.crash()
	r.n.Close(context.Background(), false)

	var failing atomic.Bool
	failing.Store(true)
	var failures atomic.Int32
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && failing.Load() && op == blob.OpCreate && strings.HasPrefix(key, "wakes/") {
			failures.Add(1)
			return errors.New("storage down")
		}
		return nil
	})
	if n, err := r.tryStartNode(); err == nil {
		n.Close(context.Background(), false)
		t.Fatal("the adoption finished without the hint")
	}
	if failures.Load() == 0 {
		t.Fatal("the hint write was never tried")
	}
	failing.Store(false)
	r.startNode() // the adoption is repeated
	if keys := r.wakes(); len(keys) != 1 || !strings.HasSuffix(keys[0], "/users/z1") {
		t.Fatalf("hints after the second adoption: %v", keys)
	}
	r.startManager(quick)
	r.startServer()
	up.Store(true)
	eventually(t, "the message to be delivered", func() bool { return rec.count() == 1 && rec.all()[0].body == "payload" })
	eventually(t, "the row to be deleted", func() bool { return r.liveRows("users/z1") == 0 })
	eventually(t, "the hint to go", func() bool { return len(r.wakes()) == 0 })
}

// A node that dies between the hint and the delta leaves the hint; the repeated
// adoption writes the delta, and the shard is woken once.
func TestCrashBetweenTheHintAndTheDeltaIsRepaired(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	var up atomic.Bool
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/q1", "users/q2", "/resume", "payload")
	eventually(t, "a first attempt (the commit is durable by then)", func() bool { return attempts.Load() >= 1 })
	r.closeInApp("users/q1")
	r.crash()
	r.n.Close(context.Background(), false)

	var failing atomic.Bool
	failing.Store(true)
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && failing.Load() && op == blob.OpCreate && strings.HasPrefix(key, "shards/") && strings.HasSuffix(key, ".ltx") {
			return errors.New("storage down") // the delta
		}
		return nil
	})
	if n, err := r.tryStartNode(); err == nil {
		n.Close(context.Background(), false)
		t.Fatal("the adoption finished without the delta")
	}
	if keys := r.wakes(); len(keys) != 1 || !strings.HasSuffix(keys[0], "/users/q1") {
		t.Fatalf("hints after the failed adoption: %v", keys)
	}
	if m, err := r.manifest("users/q1"); err != nil || m.Journal == "" {
		t.Fatalf("manifest %+v (%v): the delta should not be in", m, err)
	}
	failing.Store(false)
	r.startNode() // the adoption is repeated
	if keys := r.wakes(); len(keys) != 1 {
		t.Fatalf("hints after the second adoption: %v", keys)
	}
	r.startManager(quick)
	r.startServer()
	up.Store(true)
	eventually(t, "the message to be delivered", func() bool { return rec.count() == 1 && rec.all()[0].body == "payload" })
	eventually(t, "the row to be deleted", func() bool { return r.liveRows("users/q1") == 0 })
	eventually(t, "the hint to go", func() bool { return len(r.wakes()) == 0 })
	r.settle("users/q1")
	if rec.count() != 1 {
		t.Fatalf("%d deliveries", rec.count())
	}
}

// --- auto-delete shards ---

// An auto-delete sender delivers like any other, and is deleted once its rows are gone.
func TestAutoDeleteSenderDeliversAndIsThenDeleted(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	r.addMessage("lobby/e1", "users/e2", "/hi", "from the lobby")
	eventually(t, "the delivery", func() bool { return rec.count() == 1 })
	if d := rec.all()[0]; d.hdr.Get("Shardlite-Caller") != "lobby/e1" || d.body != "from the lobby" {
		t.Fatalf("delivery %+v", d)
	}
	eventually(t, "the row to go", func() bool { return r.liveRows("lobby/e1") == 0 })
	ref := r.acquire("lobby/e1")
	path := ref.Path()
	ref.Release()
	r.closeInApp("lobby/e1")
	eventually(t, "deletion", func() bool { return r.m.Stats.AutoDeletes.Load() == 1 && !r.m.IsActive("lobby/e1") })
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the file is still there: %v", err)
	}
	if _, err := r.rawManifest("lobby/e1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	if keys := r.wakes(); len(keys) != 0 {
		t.Fatalf("hints %v for a deleted shard", keys)
	}
}

// An auto-delete receiver acknowledges an intake once it is durable, like any other.
func TestAutoDeleteReceiverAcknowledgesIntakesAfterTheCommit(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	rec := &recorder{keep: true}
	r.record(rec, "")
	var acked atomic.Int64
	r.hookIntake(func(_ int, _ *ActiveShard, _ string, _ *outRow, call func() (int, error)) (int, error) {
		status, err := call()
		if status == http.StatusAccepted {
			acked.Add(1)
		}
		return status, err
	})
	r.sql("users/er1", `INSERT INTO _shardlite_outbox (target, path, body) VALUES ('lobby/er2', '/hi', 'x')`)
	r.cycle() // reads the row, captures, flushes and sends it
	r.cyclesWhile("the acknowledgement", func() bool { return acked.Load() == 1 })
	if n := r.durableInbox("lobby/er2"); n != 1 {
		t.Fatalf("%d inbox rows durable when the intake was acknowledged, want 1", n)
	}
}

// The rows of an auto-delete sender and of an auto-delete receiver survive a crash,
// and are delivered after it.
func TestAutoDeleteRowsSurviveACrash(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	var handlerUp, lobbyUp atomic.Bool
	rec := &recorder{answer: downUntil(&handlerUp)}
	r.record(rec, "")
	attempts := r.hookIntake(func(_ int, sender *ActiveShard, _ string, _ *outRow, call func() (int, error)) (int, error) {
		if sender.Name == "lobby/e3" && !lobbyUp.Load() {
			return http.StatusServiceUnavailable, errors.New("the receiver's node is down")
		}
		return call()
	})
	r.addMessage("lobby/e3", "users/e4", "/kept", "")
	eventually(t, "a first attempt by the sender", func() bool { return attempts.Load() >= 1 })
	// A message into an inbox whose handler fails.
	r.addMessage("users/e5", "lobby/e6", "/stuck", "")
	eventually(t, "the handler to fail", func() bool { return len(rec.onPath("/stuck")) >= 1 })
	r.closeInApp("lobby/e3")
	r.closeInApp("lobby/e6")
	r.crash()
	r.n.Close(context.Background(), false)

	r.startNode()
	r.startManager(quick)
	r.startServer()
	handlerUp.Store(true)
	lobbyUp.Store(true)
	eventually(t, "the sender's message after the restart", func() bool { return len(rec.onPath("/kept")) >= 1 })
	eventually(t, "the receiver's message after the restart", func() bool { return len(rec.onPath("/stuck")) >= 2 })
	eventually(t, "both rows to be consumed", func() bool { return r.liveRows("lobby/e3") == 0 && r.inboxRows("lobby/e6") == 0 })
}
