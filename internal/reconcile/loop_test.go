package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/shard"
)

// handlerFunc is a handler that records the entries it is given.
type handlerFunc struct {
	fn   func(ctx context.Context, e dueindex.Entry, check func() error) Answer
	mu   sync.Mutex
	seen []dueindex.Entry
}

func (h *handlerFunc) Handle(ctx context.Context, e dueindex.Entry, check func() error) Answer {
	h.mu.Lock()
	h.seen = append(h.seen, e)
	h.mu.Unlock()
	return h.fn(ctx, e, check)
}

func (h *handlerFunc) handled() []dueindex.Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.seen)
}

// fixedClock stands still, for passes called directly.
type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

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

// warnings counts the warnings the loop logs.
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

var testPrefix = "index/test/"

type rig struct {
	t      *testing.T
	store  *blob.Store
	index  dueindex.Index
	now    time.Time
	mine   atomic.Pointer[[]shard.Lane]
	listed atomic.Bool
	leased atomic.Bool
	warns  warnings
	h      *handlerFunc
	rec    *Reconciler
	loop   *Loop
}

func allLanes() []shard.Lane {
	var out []shard.Lane
	for i := range shard.Lanes {
		out = append(out, shard.Lane(i))
	}
	return out
}

// newRig makes a loop over one reconciler with the options and the handler, over
// the lanes of all, a listing and a lease.
func newRig(t *testing.T, clock Clock, opts Options, fn func(ctx context.Context, e dueindex.Entry, check func() error) Answer) *rig {
	t.Helper()
	r := &rig{t: t, now: time.Date(2026, 10, 19, 14, 5, 10, 0, time.UTC)}
	r.store = blob.NewStore(blob.NewMemory(), "n1", 1, blob.Options{Backoff: time.Millisecond})
	r.index = dueindex.New(r.store, testPrefix, "test", func(dueindex.Entry) bool { return true })
	all := allLanes()
	r.mine.Store(&all)
	r.listed.Store(true)
	r.leased.Store(true)
	r.h = &handlerFunc{fn: fn}
	if clock == nil {
		clock = fixedClock{r.now}
	}
	r.now = clock.Now()
	r.add(clock, opts)
	return r
}

func (r *rig) add(clock Clock, opts Options) {
	r.rec = &Reconciler{Name: "test", Index: r.index, Handler: r.h, Options: opts}
	r.loop = r.another(clock, r.rec)
}

// another is a loop of the rig's node over reconcilers.
func (r *rig) another(clock Clock, recs ...*Reconciler) *Loop {
	return New(Deps{
		Clock: clock, Log: slog.New(&r.warns),
		Mine: func() ([]shard.Lane, bool) { return *r.mine.Load(), r.listed.Load() },
		Check: func() error {
			if !r.leased.Load() {
				return fmt.Errorf("the lease is lost")
			}
			return nil
		},
	}, recs...)
}

// create makes an entry due at the rig's time in the lane.
func (r *rig) create(lane shard.Lane, subject string) dueindex.Entry {
	r.t.Helper()
	e := dueindex.Entry{Lane: lane, Minute: dueindex.MinuteAt(r.now), Subject: subject}
	if err := r.index.Create(context.Background(), e); err != nil {
		r.t.Fatal(err)
	}
	return e
}

// all lists every entry of every lane, far into the future.
func (r *rig) all() []dueindex.Entry {
	r.t.Helper()
	var out []dueindex.Entry
	for _, lane := range allLanes() {
		got, err := r.index.Due(context.Background(), lane, r.now.AddDate(10, 0, 0), 100000)
		if err != nil {
			r.t.Fatal(err)
		}
		out = append(out, got...)
	}
	return out
}

func (r *rig) pass() (busy bool, err error) {
	r.t.Helper()
	return r.loop.pass(context.Background(), r.loop.runs[0], r.mineNow())
}

func (r *rig) mineNow() []shard.Lane { return *r.mine.Load() }

func answer(a Answer) func(context.Context, dueindex.Entry, func() error) Answer {
	return func(context.Context, dueindex.Entry, func() error) Answer { return a }
}

// A pass handles the entries of the node's own lanes and leaves the others' alone.
func TestAPassHandlesOnlyItsOwnLanes(t *testing.T) {
	t.Parallel()
	r := newRig(t, nil, Options{Rate: 1e6}, answer(&Done{}))
	for _, lane := range allLanes() {
		r.create(lane, "s")
	}
	mine := []shard.Lane{3, 9}
	r.mine.Store(&mine)
	busy, err := r.pass()
	if err != nil || !busy {
		t.Fatalf("pass: busy %v, %v", busy, err)
	}
	var got []shard.Lane
	for _, e := range r.h.handled() {
		got = append(got, e.Lane)
	}
	slices.Sort(got)
	if !slices.Equal(got, mine) {
		t.Fatalf("handled the lanes %v, want %v", got, mine)
	}
	if left := r.all(); len(left) != shard.Lanes-2 {
		t.Fatalf("%d entries left, want the %d of the other lanes", len(left), shard.Lanes-2)
	}
}

// A node with no lane handles nothing, and the pass isn't busy.
func TestAPassWithNoLaneHandlesNothing(t *testing.T) {
	t.Parallel()
	r := newRig(t, nil, Options{}, answer(&Done{}))
	r.create(1, "s")
	none := []shard.Lane{}
	r.mine.Store(&none)
	if busy, err := r.pass(); busy || err != nil || len(r.h.handled()) != 0 {
		t.Fatalf("a pass with no lane: busy %v, %v, handled %v", busy, err, r.h.handled())
	}
}

// A lane another node ran is handled once the ranking gives it to this node: its
// entries were left due, and the next pass takes them.
func TestALaneTakenOverIsHandled(t *testing.T) {
	t.Parallel()
	r := newRig(t, nil, Options{}, answer(&Done{}))
	r.create(4, "s")
	rest := slices.DeleteFunc(allLanes(), func(l shard.Lane) bool { return l == 4 })
	r.mine.Store(&rest)
	r.pass()
	if len(r.h.handled()) != 0 {
		t.Fatalf("handled %v before the lane was taken over", r.h.handled())
	}
	all := allLanes()
	r.mine.Store(&all)
	r.pass()
	if len(r.h.handled()) != 1 || len(r.all()) != 0 {
		t.Fatalf("handled %v, entries %v after the lane was taken over", r.h.handled(), r.all())
	}
}

// Each answer does what it says: done deletes the entry, move puts it at a later
// minute under the new subject, and leave keeps it due.
func TestEachAnswerIsApplied(t *testing.T) {
	t.Parallel()
	r := newRig(t, nil, Options{}, func(_ context.Context, e dueindex.Entry, _ func() error) Answer {
		switch {
		case strings.HasPrefix(e.Subject, "done"):
			return &Done{}
		case strings.HasPrefix(e.Subject, "move"):
			return &Move{Minute: e.Minute + 5, Subject: e.Subject + ".1"}
		default:
			return &Leave{}
		}
	})
	for _, s := range []string{"done", "move", "leave"} {
		r.create(2, s)
	}
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	m := dueindex.MinuteAt(r.now)
	want := []dueindex.Entry{{Lane: 2, Minute: m, Subject: "leave"}, {Lane: 2, Minute: m + 5, Subject: "move.1"}}
	if got := r.all(); !slices.Equal(got, want) {
		t.Fatalf("entries %+v, want %+v", got, want)
	}
	if r.warns.n.Load() != 0 {
		t.Fatalf("%d warnings logged", r.warns.n.Load())
	}
}

// A move onto the entry's own key is refused and the entry stays, with the failure
// logged.
func TestAMoveOntoItselfIsLogged(t *testing.T) {
	t.Parallel()
	r := newRig(t, nil, Options{}, func(_ context.Context, e dueindex.Entry, _ func() error) Answer {
		return &Move{Minute: e.Minute, Subject: e.Subject}
	})
	e := r.create(2, "s")
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if got := r.all(); !slices.Equal(got, []dueindex.Entry{e}) {
		t.Fatalf("entries %+v, want %+v", got, e)
	}
	if r.warns.n.Load() != 1 {
		t.Fatalf("%d warnings logged, want 1", r.warns.n.Load())
	}
}

// While the node's lease fails the loop deletes and moves nothing and the pass
// says so, and the handler is given the same check.
func TestNothingIsWrittenWithoutALease(t *testing.T) {
	t.Parallel()
	var checked atomic.Int64
	r := newRig(t, nil, Options{}, func(_ context.Context, e dueindex.Entry, check func() error) Answer {
		if check() != nil {
			checked.Add(1)
		}
		if strings.HasPrefix(e.Subject, "done") {
			return &Done{}
		}
		return &Move{Minute: e.Minute + 5, Subject: e.Subject + ".1"}
	})
	r.create(2, "done")
	r.create(2, "move")
	before := r.all()
	r.leased.Store(false)
	busy, err := r.pass()
	if err == nil || !busy {
		t.Fatalf("a pass without a lease: busy %v, %v", busy, err)
	}
	if checked.Load() != 2 {
		t.Fatalf("the handler's check failed %d times, want 2", checked.Load())
	}
	if got := r.all(); !slices.Equal(got, before) {
		t.Fatalf("entries %+v, want %+v left alone", got, before)
	}
	// With the lease back the same entries are handled.
	r.leased.Store(true)
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if got := r.all(); len(got) != 1 || got[0].Subject != "move.1" {
		t.Fatalf("entries %+v, want just the moved one", got)
	}
}

// A pass lists one page over all the lanes together, not one page for each.
func TestPageIsSharedOverLanes(t *testing.T) {
	t.Parallel()
	r := newRig(t, nil, Options{Page: 5}, answer(&Leave{}))
	for i := range 40 {
		r.create(shard.Lane(i%shard.Lanes), fmt.Sprintf("s%d", i))
	}
	if _, err := r.pass(); err != nil {
		t.Fatal(err)
	}
	if got := len(r.h.handled()); got != 5 {
		t.Fatalf("a pass with a page of 5 handled %d", got)
	}
}

// A pass hands at most InFlight entries to the handler at once.
func TestPassKeepsToInFlight(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var cur, most atomic.Int64
		r := newRig(t, realClock{}, Options{InFlight: 3, Rate: 1e6}, func(context.Context, dueindex.Entry, func() error) Answer {
			n := cur.Add(1)
			most.Store(max(most.Load(), n))
			time.Sleep(time.Second)
			cur.Add(-1)
			return &Done{}
		})
		for i := range 10 {
			r.create(shard.Lane(i), "s")
		}
		start := time.Now()
		if _, err := r.pass(); err != nil {
			t.Fatal(err)
		}
		if most.Load() != 3 {
			t.Fatalf("%d handled at once, want 3", most.Load())
		}
		if took := time.Since(start); took < 4*time.Second || took > 4*time.Second+time.Millisecond {
			t.Fatalf("10 entries, 3 at a time, took %v, want 4s", took)
		}
		if len(r.all()) != 0 {
			t.Fatalf("entries left: %v", r.all())
		}
	})
}

// A pass stops handing out entries when its Budget ends, and the handler sees its
// context end. What it had not finished stays due.
func TestPassKeepsToItsBudget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, realClock{}, Options{InFlight: 1, Rate: 1e6, Budget: 2500 * time.Millisecond}, func(ctx context.Context, _ dueindex.Entry, _ func() error) Answer {
			select {
			case <-time.After(time.Second):
				return &Done{}
			case <-ctx.Done():
				return &Leave{}
			}
		})
		for i := range 10 {
			r.create(shard.Lane(i), "s")
		}
		if _, err := r.pass(); err != nil {
			t.Fatal(err)
		}
		if got := len(r.h.handled()); got != 3 {
			t.Fatalf("%d entries handled, want 3 (two finished and one cut off)", got)
		}
		if got := len(r.all()); got != 8 {
			t.Fatalf("%d entries left, want 8", got)
		}
	})
}

// A pass hands entries to the handler at Rate a second.
func TestPassKeepsToRate(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var starts []time.Time
		r := newRig(t, realClock{}, Options{Rate: 2}, func(context.Context, dueindex.Entry, func() error) Answer {
			mu.Lock()
			starts = append(starts, time.Now())
			mu.Unlock()
			return &Done{}
		})
		for i := range 5 {
			r.create(shard.Lane(i), "s")
		}
		if _, err := r.pass(); err != nil {
			t.Fatal(err)
		}
		if len(starts) != 5 {
			t.Fatalf("%d entries handled", len(starts))
		}
		if span := starts[4].Sub(starts[0]); span != 2*time.Second {
			t.Fatalf("5 entries at 2 a second took %v from first to last, want 2s", span)
		}
	})
}

// The loop makes a pass at once and then at each minute, and again after Pace when
// a pass found entries or the node has no listing yet. A node with a listing and no
// lane waits for the next minute, as an idle pass does.
func TestLoopTiming(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, realClock{}, Options{Rate: 1e6}, answer(&Done{}))
		none := []shard.Lane{}
		r.mine.Store(&none)
		r.listed.Store(false)
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Go(func() { r.loop.Serve(ctx) })
		passes := func(want uint64, what string) {
			t.Helper()
			synctest.Wait()
			if got := r.rec.Passes.Load(); got != want {
				t.Fatalf("%s: %d passes, want %d", what, got, want)
			}
		}
		passes(1, "the first pass is at once")
		// With no listing the node looks again after a second.
		time.Sleep(time.Second)
		passes(2, "no listing")
		// With a listing and no lane it waits for the next minute.
		r.listed.Store(true)
		time.Sleep(time.Second)
		passes(3, "a listing with no lane")
		time.Sleep(30 * time.Second)
		passes(3, "no lane waits for the next minute")
		// With the lanes it has an idle pass at the minute, which waits for the next.
		all := allLanes()
		r.mine.Store(&all)
		time.Sleep(30 * time.Second)
		passes(4, "the next minute")
		time.Sleep(30 * time.Second)
		passes(4, "an idle pass waits")
		// A pass that handles entries is followed by another after Pace.
		r.now = time.Now()
		r.create(3, "s")
		time.Sleep(time.Until(time.Now().Truncate(time.Minute).Add(time.Minute)))
		passes(5, "the minute of the entry")
		if len(r.h.handled()) != 1 {
			t.Fatalf("%d entries handled, want 1", len(r.h.handled()))
		}
		time.Sleep(time.Second)
		passes(6, "a pass after a busy one")
		cancel()
		wg.Wait()
	})
}

// A pass that fails (the lease check does) is tried again after Pace and then after
// longer, up to a minute.
func TestFailedPassesBackOff(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, realClock{}, Options{Rate: 1e6}, answer(&Done{}))
		r.leased.Store(false)
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Go(func() { r.loop.Serve(ctx) })
		passes := func(want uint64, what string) {
			t.Helper()
			synctest.Wait()
			if got := r.rec.Passes.Load(); got != want {
				t.Fatalf("%s: %d passes, want %d", what, got, want)
			}
		}
		passes(1, "the first pass")
		for i, wait := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
			time.Sleep(wait - time.Millisecond)
			passes(uint64(i+1), fmt.Sprintf("just before %v", wait))
			time.Sleep(time.Millisecond)
			passes(uint64(i+2), fmt.Sprintf("after %v", wait))
		}
		if r.warns.n.Load() != 4 {
			t.Fatalf("%d warnings, want one for each failed pass", r.warns.n.Load())
		}
		cancel()
		wg.Wait()
	})
}

// Two loops over the same entries (a lane with two runners while the listing is out
// of step) leave what one would: every entry is dealt with, and a duplicate is only
// a handling more.
func TestTwoLoopsLeaveTheResultOfOne(t *testing.T) {
	t.Parallel()
	fn := func(_ context.Context, e dueindex.Entry, _ func() error) Answer {
		if strings.HasPrefix(e.Subject, "done") {
			return &Done{}
		}
		return &Move{Minute: e.Minute + 3, Subject: e.Subject + ".1"}
	}
	fill := func(r *rig) {
		for i := range 30 {
			r.create(shard.Lane(i%shard.Lanes), fmt.Sprintf("done%d", i))
			r.create(shard.Lane(i%shard.Lanes), fmt.Sprintf("move%d", i))
		}
	}
	one := newRig(t, nil, Options{Rate: 1e6}, fn)
	fill(one)
	if _, err := one.pass(); err != nil {
		t.Fatal(err)
	}

	two := newRig(t, nil, Options{Rate: 1e6}, fn)
	fill(two)
	other := two.another(fixedClock{two.now}, &Reconciler{Name: "other", Index: two.index, Handler: two.h, Options: Options{Rate: 1e6}})
	var wg sync.WaitGroup
	for _, l := range []*Loop{two.loop, other} {
		wg.Go(func() {
			if _, err := l.pass(context.Background(), l.runs[0], allLanes()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if got, want := two.all(), one.all(); !slices.Equal(got, want) {
		t.Fatalf("two loops left %+v, one left %+v", got, want)
	}
	if len(one.all()) != 30 {
		t.Fatalf("%d entries left, want the 30 moved ones", len(one.all()))
	}
}

// Reconcilers pass on their own, each at its own pace.
func TestEachReconcilerPassesOnItsOwn(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, realClock{}, Options{Rate: 1e6, Every: time.Minute}, answer(&Done{}))
		slow := &Reconciler{Name: "slow", Index: r.index, Handler: r.h, Options: Options{Rate: 1e6, Every: time.Hour}}
		loop := r.another(realClock{}, r.rec, slow)
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Go(func() { loop.Serve(ctx) })
		synctest.Wait()
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if a, b := r.rec.Passes.Load(), slow.Passes.Load(); a < 3 || b != 1 {
			t.Fatalf("%d passes of the one that passes every minute and %d of the one every hour", a, b)
		}
		cancel()
		wg.Wait()
	})
}
