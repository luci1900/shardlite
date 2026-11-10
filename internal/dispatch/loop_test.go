package dispatch

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// A pass handles the entries of its own lanes and leaves the others' alone.
func TestAPassHandlesOnlyItsOwnLanes(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	byLane := map[shard.Lane][]string{}
	for i := range 60 {
		s := "users/" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		r.backed(s)
		lane := shard.MustParse(s).Lane()
		byLane[lane] = append(byLane[lane], s)
	}
	mine := []shard.Lane{3, 9}
	if len(byLane[3]) == 0 || len(byLane[9]) == 0 {
		t.Fatalf("the entries should fall in lanes 3 and 9: %v", byLane)
	}
	r.mine.Store(&mine)
	r.passOK()
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
	r.entry("users/1", r.manifest("users/1", "", shardfile.MinuteAt(r.now)), r.now)
	none := []shard.Lane{}
	r.mine.Store(&none)
	r.passOK()
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
		r.backed(s)
		if l := shard.MustParse(s).Lane(); len(names) == 0 {
			lane, names = l, append(names, s)
		} else if l == lane {
			names = append(names, s)
		}
	}
	rest := slices.DeleteFunc(allLanes(), func(l shard.Lane) bool { return l == lane })
	r.mine.Store(&rest)
	r.passOK()
	for _, s := range r.sent {
		if slices.Contains(names, s) {
			t.Fatalf("%s of lane %v was dispatched before the lane was taken over", s, lane)
		}
	}
	all := allLanes()
	r.mine.Store(&all)
	r.passOK()
	for _, s := range names {
		if !slices.Contains(r.sent, s) {
			t.Fatalf("%s of lane %v wasn't dispatched after the lane was taken over: %v", s, lane, r.sent)
		}
	}
}

// A pass lists one page over all the lanes together, not one page for each.
func TestPageIsSharedOverLanes(t *testing.T) {
	t.Parallel()
	r := newRigOn(t, nil, Options{Rate: 1e6, Page: 5})
	for i := range 40 {
		r.backed(fmt.Sprintf("users/u%d", i))
	}
	r.passOK()
	if got := r.sentCount(); got != 5 {
		t.Fatalf("a pass with a page of 5 handled %d", got)
	}
}

// A pass that fails because the node's lease does reports it and says it was busy,
// and sends, deletes and moves nothing, so with the lease back the same entries are
// handled.
func TestAPassWithoutALeaseFails(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.backed("users/1")
	r.entry("gone/2", "gone", r.now) // prunable
	before := r.all()
	r.leased.Store(false)
	busy, err := r.pass()
	if err == nil || !busy {
		t.Fatalf("a pass without a lease: busy %v, %v", busy, err)
	}
	if got := r.all(); !slices.Equal(got, before) || r.sentCount() != 0 {
		t.Fatalf("entries %+v and sent %v, want %+v left alone and nothing sent", got, r.sent, before)
	}
	r.leased.Store(true)
	r.passOK()
	if got := r.all(); len(got) != 1 || got[0].N != 1 || r.sentCount() != 1 {
		t.Fatalf("entries %+v and sent %v, want the backed one moved and sent", got, r.sent)
	}
}

// A pass handles at most InFlight entries at once.
func TestPassKeepsToInFlight(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRigOn(t, realClock{}, Options{InFlight: 3, Rate: 1e6})
		var cur, most atomic.Int64
		r.reply = func(context.Context, shard.Name) (int, error) {
			n := cur.Add(1)
			most.Store(max(most.Load(), n))
			time.Sleep(time.Second)
			cur.Add(-1)
			return http.StatusOK, nil
		}
		for i := range 10 {
			r.backed(fmt.Sprintf("users/u%d", i))
		}
		start := time.Now()
		r.passOK()
		if most.Load() != 3 {
			t.Fatalf("%d handled at once, want 3", most.Load())
		}
		if took := time.Since(start); took < 4*time.Second || took > 4*time.Second+time.Millisecond {
			t.Fatalf("10 entries, 3 at a time, took %v, want 4s", took)
		}
		for _, e := range r.all() {
			if e.N != 1 {
				t.Fatalf("entry %+v was not moved", e)
			}
		}
	})
}

// A pass stops handing out entries when its Budget ends, and the call it was making
// sees its context end. What it had not finished stays due.
func TestPassKeepsToItsBudget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRigOn(t, realClock{}, Options{InFlight: 1, Rate: 1e6, Budget: 2500 * time.Millisecond})
		r.reply = func(ctx context.Context, _ shard.Name) (int, error) {
			select {
			case <-time.After(time.Second):
				return http.StatusOK, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		for i := range 10 {
			r.backed(fmt.Sprintf("users/u%d", i))
		}
		r.passOK()
		if got := r.sentCount(); got != 3 {
			t.Fatalf("%d entries sent, want 3 (two finished and one cut off)", got)
		}
		moved := 0
		for _, e := range r.all() {
			moved += e.N
		}
		if got := len(r.all()); got != 10 || moved != 2 {
			t.Fatalf("%d entries left and %d moves, want 10 entries and 2 moves", got, moved)
		}
		if r.p.Failed.Load() != 0 {
			t.Fatalf("%d dispatches counted failed: the one cut off is not a failure", r.p.Failed.Load())
		}
	})
}

// A pass sends at Rate a second.
func TestPassKeepsToRate(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRigOn(t, realClock{}, Options{Rate: 2})
		var mu sync.Mutex
		var starts []time.Time
		r.reply = func(context.Context, shard.Name) (int, error) {
			mu.Lock()
			starts = append(starts, time.Now())
			mu.Unlock()
			return http.StatusOK, nil
		}
		for i := range 5 {
			r.backed(fmt.Sprintf("users/u%d", i))
		}
		r.passOK()
		if len(starts) != 5 {
			t.Fatalf("%d entries handled", len(starts))
		}
		if span := starts[4].Sub(starts[0]); span != 2*time.Second {
			t.Fatalf("5 entries at 2 a second took %v from first to last, want 2s", span)
		}
	})
}

// The dispatcher makes a pass at once and then at each minute, and again after Pace
// when a pass found entries or the node has no listing yet. A node with a listing
// and no lane waits for the next minute, as an idle pass does.
func TestServeTiming(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r := newRigOn(t, realClock{}, Options{Rate: 1e6})
		none := []shard.Lane{}
		r.mine.Store(&none)
		r.listed.Store(false)
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Go(func() { r.p.Serve(ctx) })
		passes := func(want uint64, what string) {
			t.Helper()
			synctest.Wait()
			if got := r.p.Passes.Load(); got != want {
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
		r.backed("users/1")
		time.Sleep(time.Until(time.Now().Truncate(time.Minute).Add(time.Minute)))
		passes(5, "the minute of the entry")
		if r.sentCount() != 1 {
			t.Fatalf("%d entries handled, want 1", r.sentCount())
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
		r := newRigOn(t, realClock{}, Options{Rate: 1e6})
		r.leased.Store(false)
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Go(func() { r.p.Serve(ctx) })
		passes := func(want uint64, what string) {
			t.Helper()
			synctest.Wait()
			if got := r.p.Passes.Load(); got != want {
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
