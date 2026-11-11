package dispatch

import (
	"context"
	"errors"
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
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

type rig struct {
	t         *testing.T
	store     *blob.Store
	hooked    *blobtest.Hooked
	manifests shardfile.ManifestStore
	index     shardfile.ReminderIndex
	now       time.Time
	p         *Dispatcher

	mu   sync.Mutex
	sent []string // the shard of each call
	reqs []*http.Request
	// reply is what the node answers a call for a shard: the answers a call
	// gets, or an error for a call that failed. It runs outside the rig's lock, so
	// a test can make a call take time.
	reply func(ctx context.Context, name shard.Name) (int, error)
	// mine is the lanes the rig's node ranks first for: every lane unless a test
	// narrows it. listed is whether it has a listing.
	mine   atomic.Pointer[[]shard.Lane]
	listed atomic.Bool
	// leased is whether the lease check passes.
	leased atomic.Bool
	warns  warnings
}

// fixedClock is a clock that stands still, for passes the rig runs one at a time.
type fixedClock struct{ r *rig }

func (c fixedClock) Now() time.Time { return c.r.now }

func (fixedClock) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)
	return t.C, t.Stop
}

// realClock is the time package's, which runs on the fake time of a synctest
// bubble.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)
	return t.C, t.Stop
}

// warnings counts the warnings the dispatcher logs.
type warnings struct {
	slog.Handler
	n atomic.Int64
}

func (w *warnings) Enabled(context.Context, slog.Level) bool { return true }

func (w *warnings) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		w.n.Add(1)
	}
	return nil
}

func (w *warnings) WithAttrs([]slog.Attr) slog.Handler { return w }
func (w *warnings) WithGroup(string) slog.Handler      { return w }

func allLanes() []shard.Lane {
	var out []shard.Lane
	for i := range shard.Lanes {
		out = append(out, shard.Lane(i))
	}
	return out
}

// newRig makes a rig whose dispatcher passes at a fixed time, every lane its own, and
// waits an hour between passes if a test serves it.
func newRig(t *testing.T) *rig {
	t.Helper()
	return newRigOn(t, nil, Options{Pace: time.Hour})
}

// newRigOn is a rig on clock (the rig's fixed one if nil) with the options. Its time
// is the clock's now.
func newRigOn(t *testing.T, clock Clock, opts Options) *rig {
	t.Helper()
	h := blobtest.WithHook(blob.NewMemory())
	store := blob.NewStore(h, "n1", 1, blob.Options{Backoff: time.Millisecond})
	r := &rig{t: t, store: store, hooked: h, manifests: shardfile.NewManifestStore(store), index: shardfile.NewReminderIndex(store),
		now: time.Date(2026, 10, 19, 14, 5, 10, 0, time.UTC)}
	r.reply = func(context.Context, shard.Name) (int, error) { return http.StatusOK, nil }
	all := allLanes()
	r.mine.Store(&all)
	r.listed.Store(true)
	r.leased.Store(true)
	if clock == nil {
		clock = fixedClock{r}
	}
	r.now = clock.Now()
	r.p = r.dispatcher(clock, opts)
	return r
}

// dispatcher is another dispatcher over the rig's store, node and options.
func (r *rig) dispatcher(clock Clock, opts Options) *Dispatcher {
	return New(Deps{
		Store: r.store, Clock: clock, Log: slog.New(&r.warns), Call: r.call,
		Mine: func() ([]shard.Lane, bool) { return *r.mine.Load(), r.listed.Load() },
		Check: func() error {
			if !r.leased.Load() {
				return errors.New("the lease is lost")
			}
			return nil
		},
	}, opts)
}

// call is the rig's Server.call: it records the request and answers as r.reply says.
func (r *rig) call(req *http.Request) (*http.Response, error) {
	name := shard.MustParse(req.Header.Get(Target))
	r.mu.Lock()
	r.sent = append(r.sent, name.String())
	r.reqs = append(r.reqs, req)
	r.mu.Unlock()
	status, err := r.reply(req.Context(), name)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
}

func (r *rig) advance(d time.Duration) { r.now = r.now.Add(d) }

// manifest puts a manifest of shard with next_reminder next (0 for none), owned by
// owner, and returns its version.
func (r *rig) manifest(name, owner string, next shardfile.Minute) blob.ETag {
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
	e := shardfile.Reminder{Minute: shardfile.MinuteAt(m), Shard: shard.MustParse(name), Base: shardfile.BaseOf(version)}
	if err := r.index.Create(context.Background(), e); err != nil {
		r.t.Fatal(err)
	}
	return e
}

// backed makes a shard owned by nobody whose manifest backs an entry due now, and
// creates the entry.
func (r *rig) backed(name string) shardfile.Reminder {
	r.t.Helper()
	return r.entry(name, r.manifest(name, "", shardfile.MinuteAt(r.now)), r.now)
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
		out = append(out, got...)
	}
	return out
}

// pass makes one pass of the rig's dispatcher over the lanes it has now.
func (r *rig) pass() (busy bool, err error) {
	r.t.Helper()
	return r.p.pass(context.Background(), *r.mine.Load())
}

// passOK makes a pass that must not fail.
func (r *rig) passOK() {
	r.t.Helper()
	if _, err := r.pass(); err != nil {
		r.t.Fatal(err)
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
	v := r.manifest("users/1", "", shardfile.MinuteAt(r.now))
	r.entry("users/1", v, r.now)
	r.passOK()
	if r.sentCount() != 1 {
		t.Fatalf("sent %v", r.sent)
	}
	// A dispatch is a call for the shard to the path the node answers itself, and
	// it never creates.
	if q := r.reqs[0]; q.Method != http.MethodPost || q.URL.Path != Activate || q.Header.Get(IfExists) != "true" {
		t.Fatalf("sent %s %s with %v", q.Method, q.URL.Path, q.Header)
	}
	want := shardfile.Reminder{Minute: shardfile.MinuteAt(r.now) + 1, Shard: shard.MustParse("users/1"), Base: shardfile.BaseOf(v), N: 1}
	if got := r.all(); len(got) != 1 || got[0] != want {
		t.Fatalf("entries %+v, want %+v", got, want)
	}
	r.passOK()
	if r.sentCount() != 1 {
		t.Fatalf("sent %v: nothing is due until the move's minute", r.sent)
	}
}

// A shard that was active already has its entry moved to the cap.
func TestActiveShardsEntryGoesToTheCap(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.reply = func(context.Context, shard.Name) (int, error) { return http.StatusNoContent, nil }
	v := r.manifest("users/1", "n1", shardfile.MinuteAt(r.now))
	r.entry("users/1", v, r.now)
	r.passOK()
	got := r.all()
	if len(got) != 1 || got[0].Minute != shardfile.MinuteAt(r.now)+16 {
		t.Fatalf("entries %+v, want one 16 minutes on", got)
	}
}

// A dispatch that keeps failing backs off at 1, 2, 4, 8 and 16 minutes, and an
// entry behind it is dispatched on time.
func TestFailingDispatchBacksOff(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.reply = func(_ context.Context, name shard.Name) (int, error) {
		if name.String() == "gone/1" {
			return 0, errors.New("unknown kind")
		}
		return http.StatusOK, nil
	}
	v := r.manifest("gone/1", "", shardfile.MinuteAt(r.now))
	r.entry("gone/1", v, r.now)
	for i, minutes := range []time.Duration{1, 2, 4, 8, 16, 16} {
		// A healthy shard due at the same minute is dispatched on time.
		other := "users/" + string(rune('a'+i))
		r.entry(other, r.manifest(other, "", shardfile.MinuteAt(r.now)), r.now)
		r.passOK()
		var failing shardfile.Reminder
		for _, e := range r.all() {
			if e.Shard.String() == "gone/1" {
				failing = e
			}
		}
		if want := shardfile.MinuteAt(r.now) + shardfile.Minute(minutes); failing.Minute != want || failing.N != i+1 {
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
	if _, err := r.manifests.Put(ctx, shard.MustParse("a/1"), &shardfile.Manifest{Generation: "g", Files: []shardfile.File{{Range: shardfile.Range{Min: 1, Max: 1}, By: lease.Incarnation{Node: "n1", Epoch: 1}}}}, &v); err != nil {
		t.Fatal(err)
	}
	// No manifest.
	r.entry("b/2", "gone", r.now)
	// The write may be in flight: its base is current and nothing backs the entry.
	cur := r.manifest("c/3", "", 0)
	r.entry("c/3", cur, r.now)

	r.passOK()
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
	later := shardfile.MinuteAt(r.now) - 60 // due already: the write sets a reminder for the entry's minute
	var once sync.Once
	r.hooked.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpGet && after && key == shardfile.ManifestKey(shard.MustParse("a/1")) {
			once.Do(func() {
				r.hooked.SetHook(nil)
				m := &shardfile.Manifest{Generation: "g", Owner: "n1", NodeEpoch: 1, Files: []shardfile.File{{Range: shardfile.Range{Min: 1, Max: 1}, By: lease.Incarnation{Node: "n1", Epoch: 1}}}, NextReminder: later}
				if _, err := r.manifests.Put(ctx, shard.MustParse("a/1"), m, &v); err != nil {
					t.Error(err)
				}
			})
		}
		return nil
	})
	r.passOK()
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
		v := r.manifest(s, "", shardfile.MinuteAt(r.now))
		r.entry(s, v, r.now)
	}
	other := r.dispatcher(fixedClock{r}, Options{Pace: time.Hour})
	var wg sync.WaitGroup
	for _, p := range []*Dispatcher{r.p, other} {
		wg.Go(func() {
			if _, err := p.pass(context.Background(), allLanes()); err != nil {
				t.Error(err)
			}
		})
	}
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
			r.reply = func(context.Context, shard.Name) (int, error) { return status, nil }
			v := r.manifest("users/1", "n2", shardfile.MinuteAt(r.now))
			r.entry("users/1", v, r.now)
			r.passOK()
			if got := r.all(); len(got) != 1 || got[0].N != 1 || got[0].Minute != shardfile.MinuteAt(r.now)+1 {
				t.Fatalf("entries %+v, want one moved a minute on", got)
			}
			if r.p.Failed.Load() != 1 {
				t.Fatalf("%d dispatches counted failed, want 1", r.p.Failed.Load())
			}
		})
	}
}

// A manifest that can't be read leaves the entry as it is, with the failure logged,
// and sends nothing.
func TestFailedManifestReadLeavesTheEntry(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	e := r.backed("users/1")
	r.hooked.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpGet && !after && key == shardfile.ManifestKey(shard.MustParse("users/1")) {
			return errors.New("the read failed")
		}
		return nil
	})
	r.passOK()
	if got := r.all(); len(got) != 1 || got[0] != e {
		t.Fatalf("entries %+v, want %+v left as it was", got, e)
	}
	if r.sentCount() != 0 || r.warns.n.Load() != 1 {
		t.Fatalf("sent %v and %d warnings logged, want nothing sent and 1 warning", r.sent, r.warns.n.Load())
	}
}
