package dispatch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/reconcile"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

type rig struct {
	t         *testing.T
	store     *blob.Store
	hooked    *blobtest.Hooked
	manifests shardfile.ManifestStore
	index     dueindex.Index
	now       time.Time
	p         *Dispatcher
	loop      *reconcile.Loop
	rec       *reconcile.Reconciler

	mu   sync.Mutex
	sent []string // the shard of each call
	reqs []*http.Request
	// reply is what the node answers a call for a shard: the answers a call
	// gets, or an error for a call that failed.
	reply func(name shard.Name) (int, error)
	// mine is the lanes the rig's node ranks first for: every lane unless a test
	// narrows it.
	mine atomic.Pointer[[]shard.Lane]
	// leased is whether the lease check passes.
	leased atomic.Bool
}

// fixedClock is a clock that stands still, for passes the rig runs one at a time.
type fixedClock struct{ r *rig }

func (c fixedClock) Now() time.Time { return c.r.now }

func (fixedClock) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)
	return t.C, t.Stop
}

func allLanes() []shard.Lane {
	var out []shard.Lane
	for i := range shard.Lanes {
		out = append(out, shard.Lane(i))
	}
	return out
}

func newRig(t *testing.T) *rig {
	t.Helper()
	h := blobtest.WithHook(blob.NewMemory())
	store := blob.NewStore(h, "n1", 1, blob.Options{Backoff: time.Millisecond})
	r := &rig{t: t, store: store, hooked: h, manifests: shardfile.NewManifestStore(store), index: shardfile.ReminderIndex(store),
		now: time.Date(2026, 10, 19, 14, 5, 10, 0, time.UTC)}
	r.reply = func(shard.Name) (int, error) { return http.StatusOK, nil }
	all := allLanes()
	r.mine.Store(&all)
	r.leased.Store(true)
	r.p, r.loop, r.rec = r.dispatcher()
	return r
}

// dispatcher is another dispatcher over the rig's store, node and options, with
// the loop that runs it. The loop passes once and waits an hour for the next, so
// that a test's pass is the only one.
func (r *rig) dispatcher() (*Dispatcher, *reconcile.Loop, *reconcile.Reconciler) {
	p := New(Deps{Store: r.store, Clock: fixedClock{r}, Log: slog.Default(), Call: r.call})
	rec := &reconcile.Reconciler{Name: "dispatcher", Index: r.index, Handler: p, Options: reconcile.Options{Rate: 1e6, Pace: time.Hour}}
	return p, reconcile.New(reconcile.Deps{Clock: fixedClock{r}, Log: slog.Default(), Mine: r.lanes, Check: r.check}, rec), rec
}

func (r *rig) lanes() ([]shard.Lane, bool) { return *r.mine.Load(), true }

func (r *rig) check() error {
	if !r.leased.Load() {
		return errors.New("the lease is lost")
	}
	return nil
}

// call is the rig's Server.call: it records the request and answers as r.reply says.
func (r *rig) call(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name := shard.MustParse(req.Header.Get(reconcile.Target))
	r.sent = append(r.sent, name.String())
	r.reqs = append(r.reqs, req)
	status, err := r.reply(name)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

func (r *rig) advance(d time.Duration) { r.now = r.now.Add(d) }

// manifest puts a manifest of shard with next_reminder next (0 for none), owned by
// owner, and returns its version.
func (r *rig) manifest(name, owner string, next dueindex.Minute) blob.ETag {
	r.t.Helper()
	m := &shardfile.Manifest{Generation: "g", Owner: cluster.Node(owner), NodeEpoch: 1, Files: []shardfile.File{}, NextReminder: next}
	v, err := r.manifests.Put(context.Background(), shard.MustParse(name), m, nil)
	if err != nil {
		r.t.Fatal(err)
	}
	return v
}

// entry creates the entry of a write based on version, for minute m.
func (r *rig) entry(name string, version blob.ETag, m time.Time) shardfile.Reminder {
	r.t.Helper()
	e := shardfile.Reminder{Minute: dueindex.MinuteAt(m), Shard: shard.MustParse(name), Base: shardfile.BaseOf(version)}
	if err := r.index.Create(context.Background(), e.Entry()); err != nil {
		r.t.Fatal(err)
	}
	return e
}

// all lists every entry of every lane.
func (r *rig) all() []shardfile.Reminder {
	r.t.Helper()
	var out []shardfile.Reminder
	for _, lane := range allLanes() {
		got, err := r.index.Due(context.Background(), lane, r.now.Add(24*time.Hour), 1000)
		if err != nil {
			r.t.Fatal(err)
		}
		for _, d := range got {
			e, err := shardfile.ParseReminder(d)
			if err != nil {
				r.t.Fatal(err)
			}
			out = append(out, e)
		}
	}
	return out
}

// pass runs one pass of the rig's loop and waits for it to end.
func (r *rig) pass() { r.t.Helper(); runPass(r.t, r.loop, r.rec) }

// runPass serves the loop until its reconciler has made one more pass.
func runPass(t *testing.T, l *reconcile.Loop, rec *reconcile.Reconciler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	want := rec.Passes.Load() + 1
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := l.Serve(ctx); err != nil {
			t.Error(err)
		}
	})
	defer wg.Wait()
	defer cancel()
	if err := rec.Passes.Wait(ctx, want); err != nil {
		t.Error(err)
	}
}

func (r *rig) sentCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent)
}

// A backed entry is dispatched and moved a minute on, never deleted.
func TestBackedEntryIsDispatchedAndMoved(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	v := r.manifest("users/1", "", dueindex.MinuteAt(r.now))
	r.entry("users/1", v, r.now)
	r.pass()
	if r.sentCount() != 1 {
		t.Fatalf("sent %v", r.sent)
	}
	// A dispatch is a call for the shard to the path the node answers itself, and
	// it never creates.
	if q := r.reqs[0]; q.Method != http.MethodPost || q.URL.Path != reconcile.Activate || q.Header.Get(reconcile.IfExists) != "true" {
		t.Fatalf("sent %s %s with %v", q.Method, q.URL.Path, q.Header)
	}
	want := shardfile.Reminder{Minute: dueindex.MinuteAt(r.now) + 1, Shard: shard.MustParse("users/1"), Base: shardfile.BaseOf(v), N: 1}
	if got := r.all(); len(got) != 1 || got[0] != want {
		t.Fatalf("entries %+v, want %+v", got, want)
	}
	r.pass()
	if r.sentCount() != 1 {
		t.Fatalf("sent %v: nothing is due until the move's minute", r.sent)
	}
}

// A shard that was active already has its entry moved to the cap.
func TestActiveShardsEntryGoesToTheCap(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.reply = func(shard.Name) (int, error) { return http.StatusNoContent, nil }
	v := r.manifest("users/1", "n1", dueindex.MinuteAt(r.now))
	r.entry("users/1", v, r.now)
	r.pass()
	got := r.all()
	if len(got) != 1 || got[0].Minute != dueindex.MinuteAt(r.now)+16 {
		t.Fatalf("entries %+v, want one 16 minutes on", got)
	}
}

// A dispatch that keeps failing backs off at 1, 2, 4, 8 and 16 minutes, and an
// entry behind it is dispatched on time.
func TestFailingDispatchBacksOff(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.reply = func(name shard.Name) (int, error) {
		if name.String() == "gone/1" {
			return 0, errors.New("unknown kind")
		}
		return http.StatusOK, nil
	}
	v := r.manifest("gone/1", "", dueindex.MinuteAt(r.now))
	r.entry("gone/1", v, r.now)
	for i, minutes := range []time.Duration{1, 2, 4, 8, 16, 16} {
		// A healthy shard due at the same minute is dispatched on time.
		other := "users/" + string(rune('a'+i))
		r.entry(other, r.manifest(other, "", dueindex.MinuteAt(r.now)), r.now)
		r.pass()
		var failing shardfile.Reminder
		for _, e := range r.all() {
			if e.Shard.String() == "gone/1" {
				failing = e
			}
		}
		if want := dueindex.MinuteAt(r.now) + dueindex.Minute(minutes); failing.Minute != want || failing.N != i+1 {
			t.Fatalf("after attempt %d the entry is %+v, want %d minutes on", i+1, failing, minutes)
		}
		if !slices.Contains(r.sent, other) {
			t.Fatalf("attempt %d: the healthy shard %s wasn't dispatched: %v", i+1, other, r.sent)
		}
		r.advance(minutes * time.Minute)
	}
}

// The pruning rule at work: an entry whose manifest has moved on without backing
// it is deleted, one with no manifest too, and one whose base is still the
// manifest's version is kept.
func TestPruning(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()

	// Moved on without backing it.
	v := r.manifest("a/1", "", 0)
	e := r.entry("a/1", v, r.now)
	if _, err := r.manifests.Put(ctx, shard.MustParse("a/1"), &shardfile.Manifest{Generation: "g", Files: []shardfile.File{{Range: shardfile.Range{Min: 1, Max: 1}, Epoch: 1}}}, &v); err != nil {
		t.Fatal(err)
	}
	// No manifest.
	r.entry("b/2", "gone", r.now)
	// The write may be in flight: its base is current and nothing backs the entry.
	cur := r.manifest("c/3", "", 0)
	r.entry("c/3", cur, r.now)

	r.pass()
	got := r.all()
	if len(got) != 1 || got[0].Shard.String() != "c/3" || got[0].N != 1 {
		t.Fatalf("entries %+v, want only c/3, moved once (a/1 %+v was pruned)", got, e)
	}
	if r.sentCount() != 0 {
		t.Fatalf("an unbacked entry was dispatched: %v", r.sent)
	}
}

// A manifest write that lands after the dispatcher read the manifest still finds
// its entry.
func TestWriteAfterTheReadStillFindsItsEntry(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	v := r.manifest("a/1", "n1", 0)
	r.entry("a/1", v, r.now)
	later := dueindex.MinuteAt(r.now) - 60 // due already: the write sets a reminder for the entry's minute
	var once sync.Once
	r.hooked.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpGet && after && key == shardfile.ManifestKey(shard.MustParse("a/1")) {
			once.Do(func() {
				r.hooked.SetHook(nil)
				m := &shardfile.Manifest{Generation: "g", Owner: "n1", NodeEpoch: 1, Files: []shardfile.File{{Range: shardfile.Range{Min: 1, Max: 1}, Epoch: 1}}, NextReminder: later}
				if _, err := r.manifests.Put(ctx, shard.MustParse("a/1"), m, &v); err != nil {
					t.Error(err)
				}
			})
		}
		return nil
	})
	r.pass()
	got := r.all()
	if len(got) != 1 || got[0].Shard.String() != "a/1" {
		t.Fatalf("entries %+v: the write's entry is gone", got)
	}
	m, version, err := r.manifests.Get(ctx, shard.MustParse("a/1"))
	if err != nil || !shardfile.Backs(m.NextReminder, got[0]) && shardfile.Prunable(true, m.NextReminder, version, got[0]) {
		t.Fatalf("the entry that remains doesn't back the manifest: %+v %v", got, err)
	}
}

// Two dispatchers running a pass at once over the same entries (a lane with two
// runners while the listing is out of step) lose nothing: every backed entry
// stays backed, every shard is dispatched at least once, and a duplicate is only
// a dispatch more.
func TestTwoDispatchersLoseNothing(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	for i := range 20 {
		s := "users/" + string(rune('a'+i))
		v := r.manifest(s, "", dueindex.MinuteAt(r.now))
		r.entry(s, v, r.now)
	}
	_, otherLoop, otherRec := r.dispatcher()
	var wg sync.WaitGroup
	wg.Go(func() { runPass(t, r.loop, r.rec) })
	wg.Go(func() { runPass(t, otherLoop, otherRec) })
	wg.Wait()
	shards := map[shard.Name]bool{}
	for _, e := range r.all() {
		if e.N == 0 {
			t.Errorf("entry %+v was not moved", e)
		}
		m, _, err := r.manifests.Get(context.Background(), e.Shard)
		if err != nil || !shardfile.Backs(m.NextReminder, e) {
			t.Errorf("entry %+v no longer backs its shard's manifest (%v)", e, err)
		}
		shards[e.Shard] = true
	}
	if len(shards) != 20 {
		t.Fatalf("%d shards still have an entry, want 20", len(shards))
	}
	dispatched := map[string]bool{}
	for _, s := range r.sent {
		dispatched[s] = true
	}
	if len(dispatched) != 20 {
		t.Fatalf("%d shards were dispatched, want 20", len(dispatched))
	}
}

// A pass handles the entries of its own lanes and leaves the others' alone.
func TestAPassHandlesOnlyItsOwnLanes(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	byLane := map[shard.Lane][]string{}
	for i := range 60 {
		s := "users/" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		r.entry(s, r.manifest(s, "", dueindex.MinuteAt(r.now)), r.now)
		lane := shard.MustParse(s).Lane()
		byLane[lane] = append(byLane[lane], s)
	}
	mine := []shard.Lane{3, 9}
	if len(byLane[3]) == 0 || len(byLane[9]) == 0 {
		t.Fatalf("the entries should fall in lanes 3 and 9: %v", byLane)
	}
	r.mine.Store(&mine)
	r.pass()
	want := append(slices.Clone(byLane[3]), byLane[9]...)
	got := slices.Clone(r.sent)
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("dispatched %v, want only the shards of lanes 3 and 9: %v", got, want)
	}
	for _, e := range r.all() {
		if lane := e.Shard.Lane(); (lane == 3 || lane == 9) != (e.N == 1) {
			t.Fatalf("entry %+v of lane %v: moved %d times", e, lane, e.N)
		}
	}
}

// A node with no lane handles nothing.
func TestAPassWithNoLaneHandlesNothing(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.entry("users/1", r.manifest("users/1", "", dueindex.MinuteAt(r.now)), r.now)
	none := []shard.Lane{}
	r.mine.Store(&none)
	r.pass()
	if r.sentCount() != 0 {
		t.Fatalf("a pass with no lane sent %v", r.sent)
	}
}

// A lane another node ran is handled here once the ranking gives it to this node:
// its entries were left due, and the next pass takes them.
func TestALaneTakenOverIsHandled(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	var lane shard.Lane
	var names []string
	for i := range 30 {
		s := fmt.Sprintf("users/u%d", i)
		r.entry(s, r.manifest(s, "", dueindex.MinuteAt(r.now)), r.now)
		if l := shard.MustParse(s).Lane(); len(names) == 0 {
			lane, names = l, append(names, s)
		} else if l == lane {
			names = append(names, s)
		}
	}
	rest := slices.DeleteFunc(allLanes(), func(l shard.Lane) bool { return l == lane })
	r.mine.Store(&rest)
	r.pass()
	for _, s := range r.sent {
		if slices.Contains(names, s) {
			t.Fatalf("%s of lane %v was dispatched before the lane was taken over", s, lane)
		}
	}
	all := allLanes()
	r.mine.Store(&all)
	r.pass()
	for _, s := range names {
		if !slices.Contains(r.sent, s) {
			t.Fatalf("%s of lane %v wasn't dispatched after the lane was taken over: %v", s, lane, r.sent)
		}
	}
}

// While the node's lease fails nothing is sent, deleted or moved.
func TestNothingHappensWithoutALease(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.entry("users/1", r.manifest("users/1", "", dueindex.MinuteAt(r.now)), r.now)
	r.entry("gone/2", "gone", r.now) // prunable
	r.leased.Store(false)
	r.pass()
	if r.sentCount() != 0 {
		t.Fatalf("sent %v without a lease", r.sent)
	}
	for _, e := range r.all() {
		if e.N != 0 {
			t.Fatalf("entry %+v was moved without a lease", e)
		}
	}
	if got := r.all(); len(got) != 2 {
		t.Fatalf("entries %+v, want both left alone", got)
	}
}

// What a call to the node answers other than an activation is a failure to back
// off from: a shard that isn't there (the prune rule reads the manifest, not this
// answer) and a node that answers 503 for an owner that doesn't reply or hasn't
// been adopted. The entry is moved a minute on and tried again.
func TestAnswersThatAreNotActivationsBackOff(t *testing.T) {
	t.Parallel()
	for name, status := range map[string]int{"not found": http.StatusNotFound, "unavailable": http.StatusServiceUnavailable} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := newRig(t)
			r.reply = func(shard.Name) (int, error) { return status, nil }
			v := r.manifest("users/1", "n2", dueindex.MinuteAt(r.now))
			r.entry("users/1", v, r.now)
			r.pass()
			if got := r.all(); len(got) != 1 || got[0].N != 1 || got[0].Minute != dueindex.MinuteAt(r.now)+1 {
				t.Fatalf("entries %+v, want one moved a minute on", got)
			}
			if r.p.Failed.Load() != 1 {
				t.Fatalf("%d dispatches counted failed, want 1", r.p.Failed.Load())
			}
		})
	}
}
