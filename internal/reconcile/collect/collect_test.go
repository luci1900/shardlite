package collect

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/reconcile"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

const age = time.Hour

// rig is a collector over a memory store whose clock the test sets.
type rig struct {
	t      *testing.T
	now    atomic.Int64 // Unix seconds, the store's clock and the collector's
	mem    *blob.Memory
	hooked *blobtest.Hooked
	store  *blob.Store
	index  dueindex.Index
	c      *Collector
	lost   atomic.Bool // the node's lease is gone
	checks atomic.Int64
	logs   bytes.Buffer
	mu     sync.Mutex // guards logs
	mine   atomic.Pointer[[]shard.Lane]
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{t: t}
	r.now.Store(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Unix())
	r.mem = blob.NewMemory(blob.WithClock(r.clock))
	r.hooked = blobtest.WithHook(r.mem)
	r.store = blob.NewStore(r.hooked, "n1", 1, blob.Options{Backoff: time.Millisecond})
	r.index = shardfile.OrphanIndex(r.store)
	r.c = New(Deps{Store: r.store, Clock: r, Log: slog.New(slog.NewTextHandler(logSink{r}, nil)), Age: age})
	all := allLanes()
	r.mine.Store(&all)
	return r
}

func allLanes() []shard.Lane {
	var out []shard.Lane
	for i := range shard.Lanes {
		out = append(out, shard.Lane(i))
	}
	return out
}

func (r *rig) clock() time.Time { return time.Unix(r.now.Load(), 0) }

func (r *rig) advance(d time.Duration) { r.now.Add(int64(d / time.Second)) }

// Now makes the rig the collector's clock. Timers are the real ones: the tests that
// run the loop set an hour of Pace, so a test's pass is the only one.
func (r *rig) Now() time.Time { return r.clock() }

func (*rig) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)
	return t.C, t.Stop
}

type logSink struct{ r *rig }

func (l logSink) Write(p []byte) (int, error) {
	l.r.mu.Lock()
	defer l.r.mu.Unlock()
	return l.r.logs.Write(p)
}

func (r *rig) logged() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.logs.String()
}

// check is the rig's lease check, which counts its calls.
func (r *rig) check() error {
	r.checks.Add(1)
	if r.lost.Load() {
		return errors.New("lease lost")
	}
	return nil
}

func (r *rig) put(key string) {
	r.t.Helper()
	if _, err := r.store.Create(context.Background(), key, strings.NewReader("x")); err != nil {
		r.t.Fatal(err)
	}
}

// manifest puts a manifest of the shard for the generation listing files.
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

// keys is every key in the store.
func (r *rig) keys() []string {
	r.t.Helper()
	var out []string
	for info, err := range r.store.List(context.Background(), shardfile.FilesPrefix) {
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

// orphan is the orphan index's entry for the shard's generation, due now.
func (r *rig) orphan(name, gen string) dueindex.Entry {
	return shardfile.Orphan{Minute: dueindex.MinuteAt(r.clock()), Shard: shard.MustParse(name), Generation: shard.Gen(gen)}.Entry()
}

func (r *rig) handle(e dueindex.Entry) reconcile.Answer {
	r.t.Helper()
	return r.c.Handle(context.Background(), e, r.check)
}

func (r *rig) wantKept(want ...string) {
	r.t.Helper()
	slices.Sort(want)
	if got := r.keys(); !slices.Equal(got, want) {
		r.t.Fatalf("the store holds\n%v\nwant\n%v", got, want)
	}
}

func wantDone(t *testing.T, a reconcile.Answer) {
	t.Helper()
	if _, ok := a.(*reconcile.Done); !ok {
		t.Fatalf("answered %#v, want done", a)
	}
}

func wantLeave(t *testing.T, a reconcile.Answer) {
	t.Helper()
	if _, ok := a.(*reconcile.Leave); !ok {
		t.Fatalf("answered %#v, want leave", a)
	}
}

func wantMove(t *testing.T, a reconcile.Answer, e dueindex.Entry, minute dueindex.Minute) {
	t.Helper()
	m, ok := a.(*reconcile.Move)
	if !ok {
		t.Fatalf("answered %#v, want a move", a)
	}
	if m.Minute != minute || m.Subject != e.Subject {
		t.Fatalf("moved to minute %d with subject %q, want %d and %q", m.Minute, m.Subject, minute, e.Subject)
	}
}

// An entry for the generation the manifest still uses deletes only the files it
// doesn't list that are past the age: listed files stay, so do young unlisted ones
// (the entry then moves to when the youngest turns the age), another generation's
// files and what isn't an LTX file.
func TestEntryForTheManifestsGenerationDeletesOnlyOldUnlistedFiles(t *testing.T) {
	r := newRig(t)
	listed := []string{file("users/a", "g1", "1-5.1"), file("users/a", "g1", "6-9.1")}
	oldUnlisted := file("users/a", "g1", "6-8.1")
	elsewhere := []string{file("users/a", "g0", "1-3.1"), file("users/b", "g1", "1-2.1")}
	notLTX := shardfile.GenerationPrefix(shard.MustParse("users/a"), "g1") + "notes"
	for _, k := range append(append(listed, oldUnlisted, notLTX), elsewhere...) {
		r.put(k)
	}
	r.manifest("users/a", "g1", "1-5.1", "6-9.1")
	r.advance(2 * time.Hour)
	young := file("users/a", "g1", "10-12.1")
	r.put(young)
	turns := r.clock().Add(age)

	e := r.orphan("users/a", "g1")
	a := r.handle(e)
	r.wantKept(append(append(listed, young, notLTX), elsewhere...)...)
	wantMove(t, a, e, dueindex.MinuteAt(turns)+1)

	// Once the young file turns the age it goes too, and the entry is done.
	r.advance(age + time.Minute)
	wantDone(t, r.handle(r.orphan("users/a", "g1")))
	r.wantKept(append(append(listed, notLTX), elsewhere...)...)
}

// An entry for a generation the manifest moved off deletes every file of it that is
// past the age, and leaves the manifest's generation alone.
func TestEntryForAGenerationTheManifestMovedOffDeletesEveryOldFile(t *testing.T) {
	r := newRig(t)
	old := []string{file("users/a", "g1", "1-5.1"), file("users/a", "g1", "6-9.1"), file("users/a", "g1", "10-10.2")}
	current := file("users/a", "g2", "1-1.1")
	for _, k := range append(old, current) {
		r.put(k)
	}
	r.manifest("users/a", "g2", "1-1.1")
	r.advance(2 * time.Hour)

	wantDone(t, r.handle(r.orphan("users/a", "g1")))
	r.wantKept(current)
}

// An entry for a shard with no manifest (it was deleted) deletes every file past the
// age, and a young one is kept whatever the entry.
func TestEntryForAShardWithNoManifestDeletesEveryOldFile(t *testing.T) {
	r := newRig(t)
	old := []string{file("users/a", "g1", "1-5.1"), file("users/a", "g1", "6-9.1")}
	for _, k := range old {
		r.put(k)
	}
	r.advance(2 * time.Hour)
	wantDone(t, r.handle(r.orphan("users/a", "g1")))
	r.wantKept()

	// A file a write is still sending is younger than the age however old the entry is.
	young := file("users/a", "g1", "10-12.1")
	r.put(young)
	r.advance(age / 2)
	e := r.orphan("users/a", "g1")
	wantMove(t, r.handle(e), e, dueindex.MinuteAt(r.clock().Add(age/2))+1)
	r.wantKept(young)
}

// The entry moves to when the youngest of the files it kept turns the age, not the
// first.
func TestMoveIsToWhenTheYoungestFileTurnsTheAge(t *testing.T) {
	r := newRig(t)
	r.put(file("users/a", "g", "1-1.1"))
	r.advance(10 * time.Minute)
	r.put(file("users/a", "g", "2-2.1"))
	r.advance(25 * time.Minute)
	last := r.clock()
	r.put(file("users/a", "g", "3-3.1"))
	r.advance(5 * time.Minute)

	e := r.orphan("users/a", "g")
	wantMove(t, r.handle(e), e, dueindex.MinuteAt(last.Add(age))+1)
	if got := len(r.keys()); got != 3 {
		t.Fatalf("%d files left, want all 3 (none is past the age)", got)
	}
}

// An entry whose files are all listed has nothing to delete and is done.
func TestEntryWithNothingUnlistedIsDone(t *testing.T) {
	r := newRig(t)
	r.put(file("users/a", "g", "1-1.1"))
	r.manifest("users/a", "g", "1-1.1")
	r.advance(2 * time.Hour)
	wantDone(t, r.handle(r.orphan("users/a", "g")))
	wantDone(t, r.handle(r.orphan("users/b", "g"))) // no files at all
	if len(r.keys()) != 1 {
		t.Fatal("deleted a listed file")
	}
}

// The lease is checked before each delete, and nothing is deleted while it fails.
func TestTheLeaseIsCheckedBeforeEachDelete(t *testing.T) {
	r := newRig(t)
	keys := []string{file("users/a", "g", "1-1.1"), file("users/a", "g", "2-2.1"), file("users/a", "g", "3-3.1")}
	for _, k := range keys {
		r.put(k)
	}
	r.advance(2 * time.Hour)

	// The log of what happened: each delete must follow a check.
	var mu sync.Mutex
	var log []string
	r.hooked.SetHook(func(_ context.Context, op blobtest.Op, k string, after bool) error {
		if op == blobtest.OpDelete && !after {
			mu.Lock()
			log = append(log, "delete")
			mu.Unlock()
		}
		return nil
	})
	check := func() error {
		mu.Lock()
		defer mu.Unlock()
		log = append(log, "check")
		return nil
	}
	e := r.orphan("users/a", "g")
	wantDone(t, r.c.Handle(context.Background(), e, check))
	if want := []string{"check", "delete", "check", "delete", "check", "delete"}; !slices.Equal(log, want) {
		t.Fatalf("the order was %v, want %v", log, want)
	}
	r.wantKept()
}

func TestNothingIsDeletedWhileTheLeaseFails(t *testing.T) {
	r := newRig(t)
	key := file("users/a", "g", "1-1.1")
	r.put(key)
	r.advance(2 * time.Hour)
	r.lost.Store(true)
	e := r.orphan("users/a", "g")
	wantLeave(t, r.handle(e))
	r.wantKept(key)

	r.lost.Store(false)
	wantDone(t, r.handle(e))
	r.wantKept()
}

// A lease lost part way stops the deletes there, and the entry is left for the
// next pass, which finishes.
func TestALeaseLostPartWayStopsTheDeletes(t *testing.T) {
	r := newRig(t)
	for _, f := range []string{"1-1.1", "2-2.1", "3-3.1"} {
		r.put(file("users/a", "g", f))
	}
	r.advance(2 * time.Hour)
	calls := 0
	check := func() error {
		calls++
		if calls > 1 {
			return errors.New("lease lost")
		}
		return nil
	}
	e := r.orphan("users/a", "g")
	wantLeave(t, r.c.Handle(context.Background(), e, check))
	if got := len(r.keys()); got != 2 {
		t.Fatalf("%d files left, want the 2 after the first delete", got)
	}
	wantDone(t, r.handle(e))
	r.wantKept()
}

// A failure answers a move by the retry, with the entry's subject, and deletes what
// it deleted before: a failed delete, a failed listing and a failed manifest read.
func TestAFailureAnswersMove(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   blobtest.Op
		on   func(key string) bool
	}{
		{"delete", blobtest.OpDelete, func(k string) bool { return strings.HasSuffix(k, "2-2.1.ltx") }},
		{"list", blobtest.OpList, func(k string) bool { return strings.HasPrefix(k, shardfile.FilesPrefix) }},
		{"manifest", blobtest.OpGet, func(k string) bool { return k == shardfile.ManifestKey(shard.MustParse("users/a")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			for _, f := range []string{"1-1.1", "2-2.1", "3-3.1"} {
				r.put(file("users/a", "g", f))
			}
			r.manifest("users/a", "g", "9-9.1")
			r.advance(2 * time.Hour)
			r.hooked.SetHook(func(_ context.Context, op blobtest.Op, k string, after bool) error {
				if op == tc.op && !after && tc.on(k) {
					return errors.New("storage failed")
				}
				return nil
			})
			e := r.orphan("users/a", "g")
			wantMove(t, r.handle(e), e, dueindex.MinuteAt(r.clock())+retry)
			if !strings.Contains(r.logged(), "backing off") {
				t.Errorf("the failure was not logged: %s", r.logged())
			}

			r.hooked.SetHook(nil)
			wantDone(t, r.handle(e))
			r.wantKept()
		})
	}
}

// A budget that ended leaves the entry, as a lease that failed does.
func TestAnEndedBudgetLeavesTheEntry(t *testing.T) {
	r := newRig(t)
	key := file("users/a", "g", "1-1.1")
	r.put(key)
	r.advance(2 * time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	wantLeave(t, r.c.Handle(ctx, r.orphan("users/a", "g"), r.check))
	r.wantKept(key)
}

// A file that is already gone when the collector deletes it counts as deleted.
func TestAFileAlreadyGoneIsFine(t *testing.T) {
	r := newRig(t)
	r.put(file("users/a", "g", "1-1.1"))
	r.advance(2 * time.Hour)
	r.hooked.SetHook(func(ctx context.Context, op blobtest.Op, k string, after bool) error {
		if op == blobtest.OpDelete && !after {
			r.mem.Delete(ctx, k, "")
		}
		return nil
	})
	wantDone(t, r.handle(r.orphan("users/a", "g")))
}

// Only the entry's own generation is looked at, so an entry of one shard leaves a
// shard whose name is a prefix of it alone.
func TestOnlyTheEntrysGenerationIsListed(t *testing.T) {
	r := newRig(t)
	other := []string{file("users/1", "g", "1-1.1"), file("users/10", "g", "1-1.1"), file("users/1", "gg", "1-1.1")}
	for _, k := range other {
		r.put(k)
	}
	r.put(file("users/1", "g", "2-2.1"))
	r.advance(2 * time.Hour)
	wantDone(t, r.handle(r.orphan("users/1", "g")))
	r.wantKept(other[1:]...)
}

func TestAnEntryThatIsNotAnOrphanIsLeft(t *testing.T) {
	r := newRig(t)
	wantLeave(t, r.handle(dueindex.Entry{Lane: 0, Minute: dueindex.MinuteAt(r.clock()), Subject: "nonsense"}))
	if !strings.Contains(r.logged(), "not an orphan") {
		t.Errorf("not logged: %s", r.logged())
	}
}

// Handling an entry twice, or two collectors at once, leaves what one does.
func TestTwoCollectorsOnOneEntryDeleteOnlyGarbage(t *testing.T) {
	r := newRig(t)
	listed := file("users/a", "g", "1-1.1")
	for _, f := range []string{"1-1.1", "2-2.1", "3-3.1", "4-4.1"} {
		r.put(file("users/a", "g", f))
	}
	r.manifest("users/a", "g", "1-1.1")
	r.advance(2 * time.Hour)
	e := r.orphan("users/a", "g")
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if a, ok := r.handle(e).(*reconcile.Done); !ok {
				t.Errorf("answered %#v, want done", a)
			}
		})
	}
	wg.Wait()
	r.wantKept(listed)
}

// The collector runs on the reconcile loop: a pass deletes the files of the entries
// of the node's lanes and applies the answers, and writes nothing while the lease
// fails.
func TestThroughTheLoop(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	doneKey := file("users/a", "g", "1-1.1")
	r.put(doneKey)
	r.put(file("users/b", "g", "1-1.1"))
	r.advance(2 * time.Hour)
	youngKey := file("users/b", "g", "2-2.1")
	r.put(youngKey)
	turns := r.clock().Add(age)
	entries := []dueindex.Entry{r.orphan("users/a", "g"), r.orphan("users/b", "g")}
	for _, e := range entries {
		if err := r.index.Create(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	rec := &reconcile.Reconciler{Name: "collector", Index: r.index, Handler: r.c, Options: reconcile.Options{Rate: 1e6, Pace: time.Hour}}
	loop := reconcile.New(reconcile.Deps{
		Clock: r, Log: slog.Default(), Check: r.check,
		Mine: func() ([]shard.Lane, bool) { return *r.mine.Load(), true },
	}, rec)

	// With the lease lost the loop deletes no file and no entry, and moves none.
	r.lost.Store(true)
	runPass(t, loop, rec)
	if got := r.keys(); len(got) != 3 {
		t.Fatalf("%d files after a pass without a lease, want 3", len(got))
	}
	if got := r.entries(); !slices.Equal(got, entries) {
		t.Fatalf("entries %v after a pass without a lease, want %v", got, entries)
	}

	r.lost.Store(false)
	runPass(t, loop, rec)
	if got := r.keys(); !slices.Equal(got, []string{youngKey}) {
		t.Fatalf("files %v after the pass, want only the young one", got)
	}
	moved := shardfile.Orphan{Minute: dueindex.MinuteAt(turns) + 1, Shard: shard.MustParse("users/b"), Generation: "g"}.Entry()
	if got := r.entries(); !slices.Equal(got, []dueindex.Entry{moved}) {
		t.Fatalf("entries %v after the pass, want only the second, moved to %v", got, moved)
	}
}

// entries is every entry of the orphan index, whatever its minute.
func (r *rig) entries() []dueindex.Entry {
	r.t.Helper()
	var out []dueindex.Entry
	for _, lane := range allLanes() {
		got, err := r.index.Due(context.Background(), lane, r.clock().Add(10*24*time.Hour), 1000)
		if err != nil {
			r.t.Fatal(err)
		}
		out = append(out, got...)
	}
	slices.SortFunc(out, func(a, b dueindex.Entry) int { return strings.Compare(a.Subject, b.Subject) })
	return out
}

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
