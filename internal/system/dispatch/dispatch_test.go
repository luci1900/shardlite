package dispatch

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/remindex"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

type rig struct {
	t         *testing.T
	hooked    *blobtest.Hooked
	manifests shardfile.ManifestStore
	index     remindex.Index
	now       time.Time
	p         *dispatcher

	mu    sync.Mutex
	sent  []string // "node shard"
	reply func(node string, name shard.Name) (Answer, error)
}

func newRig(t *testing.T, live ...string) *rig {
	t.Helper()
	h := blobtest.WithHook(blob.NewMemory())
	store := blob.NewStore(h, "n1", 1, blob.Options{Backoff: time.Millisecond})
	r := &rig{t: t, hooked: h, manifests: shardfile.NewManifestStore(store), index: remindex.New(store),
		now: time.Date(2026, 10, 19, 14, 5, 10, 0, time.UTC)}
	if len(live) == 0 {
		live = []string{"n1"}
	}
	r.reply = func(string, shard.Name) (Answer, error) { return Activated, nil }
	r.p = newDispatcher(Deps{Store: store, Live: func() []cluster.Node { return liveNodes(live) }, Now: func() time.Time { return r.now }, Log: slog.Default(),
		Dispatch: func(_ context.Context, node cluster.Node, name shard.Name) (Answer, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.sent = append(r.sent, string(node)+" "+name.String())
			return r.reply(string(node), name)
		}}, Options{Rate: 1e6})
	return r
}

// liveNodes converts names to the type Deps.Live speaks.
func liveNodes(names []string) []cluster.Node {
	out := make([]cluster.Node, len(names))
	for i, n := range names {
		out[i] = cluster.Node(n)
	}
	return out
}

func (r *rig) advance(d time.Duration) { r.now = r.now.Add(d) }

// manifest puts a manifest of shard with next_reminder next (0 for none), owned by
// owner, and returns its version.
func (r *rig) manifest(name, owner string, next remindex.Minute) blob.ETag {
	r.t.Helper()
	m := &shardfile.Manifest{Generation: "g", Owner: cluster.Node(owner), NodeEpoch: 1, Files: []shardfile.File{}, NextReminder: next}
	v, err := r.manifests.Put(context.Background(), shard.MustParse(name), m, nil)
	if err != nil {
		r.t.Fatal(err)
	}
	return v
}

// entry creates the entry of a write based on version, for minute m.
func (r *rig) entry(name string, version blob.ETag, m time.Time) remindex.Entry {
	r.t.Helper()
	e := remindex.Entry{Minute: remindex.MinuteAt(m), Shard: shard.MustParse(name), Base: remindex.BaseOf(version)}
	if err := r.index.Create(context.Background(), e); err != nil {
		r.t.Fatal(err)
	}
	return e
}

func (r *rig) all() []remindex.Entry {
	r.t.Helper()
	got, err := r.index.Due(context.Background(), r.now.Add(24*time.Hour), 1000)
	if err != nil {
		r.t.Fatal(err)
	}
	return got
}

func (r *rig) pass() bool {
	r.t.Helper()
	busy, err := r.p.pass(context.Background())
	if err != nil {
		r.t.Fatal(err)
	}
	return busy
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
	v := r.manifest("test.users/1", "", remindex.MinuteAt(r.now))
	r.entry("test.users/1", v, r.now)
	if !r.pass() {
		t.Fatal("a pass with a due entry should ask for the next one soon")
	}
	if r.sentCount() != 1 {
		t.Fatalf("sent %v", r.sent)
	}
	want := remindex.Entry{Minute: remindex.MinuteAt(r.now) + 1, Shard: shard.MustParse("test.users/1"), Base: remindex.BaseOf(v), N: 1}
	if got := r.all(); len(got) != 1 || got[0] != want {
		t.Fatalf("entries %+v, want %+v", got, want)
	}
	if r.pass() {
		t.Fatal("nothing is due until the move's minute")
	}
}

// A shard that was active already has its entry moved to the cap.
func TestActiveShardsEntryGoesToTheCap(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.reply = func(string, shard.Name) (Answer, error) { return Active, nil }
	v := r.manifest("test.users/1", "n1", remindex.MinuteAt(r.now))
	r.entry("test.users/1", v, r.now)
	r.pass()
	got := r.all()
	if len(got) != 1 || got[0].Minute != remindex.MinuteAt(r.now)+16 {
		t.Fatalf("entries %+v, want one 16 minutes on", got)
	}
}

// A dispatch that keeps failing backs off at 1, 2, 4, 8 and 16 minutes, and an
// entry behind it is dispatched on time.
func TestFailingDispatchBacksOff(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.reply = func(_ string, name shard.Name) (Answer, error) {
		if name.String() == "test.gone/1" {
			return 0, errors.New("unknown kind")
		}
		return Activated, nil
	}
	v := r.manifest("test.gone/1", "", remindex.MinuteAt(r.now))
	r.entry("test.gone/1", v, r.now)
	for i, minutes := range []time.Duration{1, 2, 4, 8, 16, 16} {
		// A healthy shard due at the same minute is dispatched on time.
		other := "test.users/" + string(rune('a'+i))
		r.entry(other, r.manifest(other, "", remindex.MinuteAt(r.now)), r.now)
		r.pass()
		var failing remindex.Entry
		for _, e := range r.all() {
			if e.Shard.String() == "test.gone/1" {
				failing = e
			}
		}
		if want := remindex.MinuteAt(r.now) + remindex.Minute(minutes); failing.Minute != want || failing.N != i+1 {
			t.Fatalf("after attempt %d the entry is %+v, want %d minutes on", i+1, failing, minutes)
		}
		if !slices.Contains(r.sent, "n1 "+other) {
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
	v := r.manifest("test.a/1", "", 0)
	e := r.entry("test.a/1", v, r.now)
	if _, err := r.manifests.Put(ctx, shard.MustParse("test.a/1"), &shardfile.Manifest{Generation: "g", Files: []shardfile.File{{Range: shardfile.Range{Min: 1, Max: 1}, Epoch: 1}}}, &v); err != nil {
		t.Fatal(err)
	}
	// No manifest.
	r.entry("test.b/2", "gone", r.now)
	// The write may be in flight: its base is current and nothing backs the entry.
	cur := r.manifest("test.c/3", "", 0)
	r.entry("test.c/3", cur, r.now)

	r.pass()
	got := r.all()
	if len(got) != 1 || got[0].Shard.String() != "test.c/3" || got[0].N != 1 {
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
	v := r.manifest("test.a/1", "n1", 0)
	e := r.entry("test.a/1", v, r.now)
	later := remindex.MinuteAt(r.now) - 60 // due already: the write sets a reminder for the entry's minute
	var once sync.Once
	r.hooked.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpGet && after && key == shardfile.ManifestKey(shard.MustParse("test.a/1")) {
			once.Do(func() {
				r.hooked.SetHook(nil)
				m := &shardfile.Manifest{Generation: "g", Owner: "n1", NodeEpoch: 1, Files: []shardfile.File{{Range: shardfile.Range{Min: 1, Max: 1}, Epoch: 1}}, NextReminder: later}
				if _, err := r.manifests.Put(ctx, shard.MustParse("test.a/1"), m, &v); err != nil {
					t.Error(err)
				}
			})
		}
		return nil
	})
	r.p.handle(ctx, e, r.now)
	got := r.all()
	if len(got) != 1 || got[0].Shard.String() != "test.a/1" {
		t.Fatalf("entries %+v: the write's entry is gone", got)
	}
	m, version, err := r.manifests.Get(ctx, shard.MustParse("test.a/1"))
	if err != nil || !remindex.Backs(m.NextReminder, got[0]) && remindex.Prunable(true, m.NextReminder, version, got[0]) {
		t.Fatalf("the entry that remains doesn't back the manifest: %+v %v", got, err)
	}
}

// Two dispatchers running a pass at once lose nothing, and wake nothing twice
// beyond extra dispatches.
func TestTwoDispatchersLoseNothing(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	for i := range 20 {
		s := "test.users/" + string(rune('a'+i))
		v := r.manifest(s, "", remindex.MinuteAt(r.now))
		r.entry(s, v, r.now)
	}
	other := newDispatcher(r.p.Deps, r.p.opts)
	var wg sync.WaitGroup
	for _, p := range []*dispatcher{r.p, other} {
		wg.Go(func() {
			if _, err := p.pass(context.Background()); err != nil {
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
		shards[e.Shard] = true
	}
	if len(shards) != 20 {
		t.Fatalf("%d shards still have an entry, want 20", len(shards))
	}
}

// With no owner, dispatches spread over the live nodes, the dispatcher's own node
// getting no more than the others.
func TestPlacementSpreadsLoad(t *testing.T) {
	t.Parallel()
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}
	r := newRig(t, nodes...)
	counts := map[string]int{}
	for range 1000 {
		node, ok := r.p.place("", "")
		if !ok {
			t.Fatal("no node")
		}
		counts[string(node)]++
	}
	for _, n := range nodes {
		if counts[n] > 400 || counts[n] == 0 {
			t.Errorf("%s took %d of 1000 (an even share is 200): %v", n, counts[n], counts)
		}
	}
}

// A shard whose owner is alive is dispatched to it, and to a live node otherwise.
func TestPlacementPrefersALiveOwner(t *testing.T) {
	t.Parallel()
	r := newRig(t, "n1", "n2", "n3")
	for range 50 {
		if node, _ := r.p.place("n3", ""); node != "n3" {
			t.Fatalf("went to %s, want the owner n3", node)
		}
	}
	for range 50 {
		if node, _ := r.p.place("dead", ""); !strings.HasPrefix(string(node), "n") {
			t.Fatalf("went to %q", node)
		}
	}
}

// An owner that doesn't take the dispatch (a node that stopped, still listed as
// alive) has it tried once on another node.
func TestDispatchFallsBackFromAnOwnerThatDoesntAnswer(t *testing.T) {
	t.Parallel()
	r := newRig(t, "n1", "n2")
	r.reply = func(node string, _ shard.Name) (Answer, error) {
		if node == "n1" {
			return 0, errors.New("connection refused")
		}
		return Activated, nil
	}
	v := r.manifest("test.users/1", "n1", remindex.MinuteAt(r.now))
	r.entry("test.users/1", v, r.now)
	r.pass()
	if want := []string{"n1 test.users/1", "n2 test.users/1"}; !slices.Equal(r.sent, want) {
		t.Fatalf("sent %v, want %v", r.sent, want)
	}
	if got := r.all(); len(got) != 1 || got[0].N != 1 || got[0].Minute != remindex.MinuteAt(r.now)+1 {
		t.Fatalf("entries %+v, want one moved a minute on", got)
	}
}
