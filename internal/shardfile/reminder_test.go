package shardfile_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/pos"
	shardpkg "github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// opLog records the creates under the reminder index and the manifest's updates,
// in order.
type opLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *opLog) hook(_ context.Context, op blobtest.Op, key string, after bool) error {
	if after {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case op == blobtest.OpCreate && strings.HasPrefix(key, shardfile.ReminderPrefix):
		l.ops = append(l.ops, "entry")
	case op == blobtest.OpUpdate && key == shardfile.ManifestKey(shardName(shard)):
		l.ops = append(l.ops, "manifest")
	}
	return nil
}

func (l *opLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.ops
	l.ops = nil
	return out
}

// reminderAt is a hook that records the reminder at the minute of unix seconds.
func reminderAt(unix int64) shardfile.Hooks {
	return shardfile.Hooks{BeforeUpdate: func(context.Context, pos.Position) (shardfile.Minute, error) {
		if unix == 0 {
			return 0, nil
		}
		return shardfile.MinuteOf(unix), nil
	}}
}

func dueEntries(t *testing.T, e *harness.Env, now time.Time) []shardfile.Reminder {
	t.Helper()
	var out []shardfile.Reminder
	for i := range shardpkg.Lanes {
		got, err := shardfile.NewReminderIndex(e.Store).Due(context.Background(), shardpkg.Lane(i), now, 1000)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, got...)
	}
	return out
}

// A manifest write that sets next_reminder is preceded by the entry for its
// minute, keyed by the version the write CASes against. A write whose reminder
// falls in the minute the manifest already has creates none, and one that has no
// reminder omits it.
func TestNextReminderHasItsEntryFirst(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	var log opLog
	e.Blob.SetHook(log.hook)

	due := time.Date(2026, 10, 19, 14, 5, 30, 0, time.UTC).Unix()
	base := shardfile.BaseOf(h.ETag)
	ack(t, app, sh, 1)
	if err := h.WriteDelta(ctx, sh, reminderAt(due)); err != nil {
		t.Fatal(err)
	}
	if got := log.take(); len(got) != 2 || got[0] != "entry" || got[1] != "manifest" {
		t.Fatalf("operations %v, want the entry and then the manifest", got)
	}
	m, _, err := e.Files.GetManifest(ctx, shardName(shard))
	if err != nil || m.NextReminder != shardfile.MinuteOf(due) {
		t.Fatalf("manifest %+v (%v), want next_reminder %d", m, err, shardfile.MinuteOf(due))
	}
	want := shardfile.Reminder{Minute: shardfile.MinuteOf(due), Shard: shardName(shard), Base: base}
	if got := dueEntries(t, e, time.Unix(due, 0)); len(got) != 1 || got[0] != want {
		t.Fatalf("entries %+v, want %+v", got, want)
	}

	// The same minute: the entry that backs the old reminder backs this one.
	ack(t, app, sh, 2)
	if err := h.WriteDelta(ctx, sh, reminderAt(due+10)); err != nil {
		t.Fatal(err)
	}
	if got := log.take(); len(got) != 1 || got[0] != "manifest" {
		t.Fatalf("operations %v, want the manifest alone", got)
	}

	// Another minute: an entry of its own, under the new base.
	ack(t, app, sh, 3)
	if err := h.WriteDelta(ctx, sh, reminderAt(due+3600)); err != nil {
		t.Fatal(err)
	}
	if got := log.take(); len(got) != 2 {
		t.Fatalf("operations %v, want the entry and the manifest", got)
	}
	if got := dueEntries(t, e, time.Unix(due+3600, 0)); len(got) != 2 {
		t.Fatalf("entries %+v, want two", got)
	}

	// None: omitted, with no entry.
	ack(t, app, sh, 4)
	if err := h.WriteDelta(ctx, sh, reminderAt(0)); err != nil {
		t.Fatal(err)
	}
	if got := log.take(); len(got) != 1 || got[0] != "manifest" {
		t.Fatalf("operations %v, want the manifest alone", got)
	}
	if m, _, err := e.Files.GetManifest(ctx, shardName(shard)); err != nil || m.NextReminder != 0 {
		t.Fatalf("manifest %+v (%v), want no next_reminder", m, err)
	}
}

// A failed entry fails the write and leaves the manifest, and a retry (which uses
// the same key) goes through.
func TestFailedEntryFailsTheWrite(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)

	boom := errors.New("storage is down")
	e.Blob.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && strings.HasPrefix(key, shardfile.ReminderPrefix) {
			return boom
		}
		return nil
	})
	etag := h.ETag
	if err := h.WriteDelta(ctx, sh, reminderAt(time.Now().Unix()+60)); err == nil {
		t.Fatal("the delta went through without its entry")
	}
	if h.ETag != etag || len(storedFiles(t, e)) != 0 {
		t.Fatal("the manifest changed")
	}
	e.Blob.SetHook(nil)
	if err := h.WriteDelta(ctx, sh, reminderAt(time.Now().Unix()+60)); err != nil {
		t.Fatal(err)
	}
}

// Adoption records a reminder due now for the tail it writes a delta for, with the
// entry created before the delta.
func TestAdoptionRecordsAnEntryDueNow(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	write(t, e, app, func() error { return sh.Sync(ctx) }, 0)
	now := time.Date(2026, 10, 19, 14, 5, 30, 0, time.UTC)
	e.Files.Now = func() time.Time { return now }
	base := shardfile.BaseOf(h.ETag)
	var log opLog
	e.Blob.SetHook(log.hook)

	rep, err := e.Files.AdoptJournal(ctx, e.Node, 1, e.W.NextSeq(), nil)
	if err != nil || rep.Deltas != 1 {
		t.Fatalf("adoption: %+v %v", rep, err)
	}
	if got := log.take(); len(got) != 2 || got[0] != "entry" || got[1] != "manifest" {
		t.Fatalf("operations %v, want the entry and then the manifest", got)
	}
	m, _, err := e.Files.GetManifest(ctx, shardName(shard))
	if err != nil || m.NextReminder != shardfile.MinuteAt(now) {
		t.Fatalf("manifest %+v (%v), want next_reminder %d", m, err, shardfile.MinuteAt(now))
	}
	want := shardfile.Reminder{Minute: shardfile.MinuteOf(now.Unix()), Shard: shardName(shard), Base: base}
	if got := dueEntries(t, e, now); len(got) != 1 || got[0] != want {
		t.Fatalf("entries %+v, want %+v", got, want)
	}
}

// A failed entry leaves adoption's delta unwritten and the tail in the journal for
// the next adoption.
func TestAdoptionWithAFailedEntryWritesNoDelta(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	_, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	write(t, e, app, func() error { return sh.Sync(ctx) }, 0)

	e.Blob.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && strings.HasPrefix(key, shardfile.ReminderPrefix) {
			return errors.New("storage is down")
		}
		return nil
	})
	rep, err := e.Files.AdoptJournal(ctx, e.Node, 1, e.W.NextSeq(), nil)
	if err == nil || rep.Deltas != 0 {
		t.Fatalf("adoption with a failing entry: %+v %v", rep, err)
	}
	if files := storedFiles(t, e); len(files) != 0 {
		t.Fatalf("a delta was written: %v", files)
	}
	e.Blob.SetHook(nil)
	if rep, err := e.Files.AdoptJournal(ctx, e.Node, 1, e.W.NextSeq(), nil); err != nil || rep.Deltas != 1 {
		t.Fatalf("adoption: %+v %v", rep, err)
	}
}

// A manifest that returns to an earlier state (released, then claimed again by the
// same node) has a different version, so the pruning rule's reading of "the
// manifest moved off the entry's base" can't be fooled by identical bytes.
func TestReleaseThenClaimGivesANewVersion(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	first := h.ETag
	var seen []string
	for range 3 {
		if err := h.SetReminder(ctx, h.Manifest.NextReminder, true); err != nil {
			t.Fatal(err)
		}
		if err := h.Claim(ctx); err != nil {
			t.Fatal(err)
		}
		seen = append(seen, string(h.ETag))
	}
	seen = append(seen, string(first))
	for i, a := range seen {
		for _, b := range seen[:i] {
			if a == b {
				t.Fatalf("two versions of the manifest share the ETag %q", a)
			}
		}
	}
}

// The reminder tests below are about the reminder's subject and its keys, which
// are what is in storage.

func at(h, m int) time.Time { return time.Date(2026, 10, 19, h, m, 0, 0, time.UTC) }

// minuteAt is the minute of 14:05, say, on the test day.
func minuteAt(h, m int) shardfile.Minute { return shardfile.MinuteAt(at(h, m)) }

func TestMinutes(t *testing.T) {
	t.Parallel()
	if got := shardfile.MinuteOf(119); got != 1 {
		t.Errorf("MinuteOf(119) = %d, want 1", got)
	}
	if got, want := shardfile.MinuteAt(at(14, 5).Add(59*time.Second)), shardfile.MinuteOf(at(14, 5).Unix()); got != want {
		t.Errorf("MinuteAt = %d, want %d", got, want)
	}
}

func TestReminderKeyRoundTrip(t *testing.T) {
	t.Parallel()
	for _, r := range []shardfile.Reminder{
		{Minute: minuteAt(14, 5), Shard: shardpkg.MustParse("users/42"), Base: shardfile.BaseOf("etag"), N: 3},
		{Minute: minuteAt(0, 0), Shard: shardpkg.MustParse("a/b"), Base: "x", N: 0},
		{Minute: minuteAt(23, 59), Shard: shardpkg.MustParse("users/7"), Base: "y", N: 12},
	} {
		got, err := shardfile.ParseKey(r.Key())
		if err != nil || got != r {
			t.Errorf("%q parses to %+v (%v), want %+v", r.Key(), got, err, r)
		}
	}
	r := shardfile.Reminder{Minute: minuteAt(14, 5), Shard: shardpkg.MustParse("users/42"), Base: "b", N: 3}
	lane := r.Shard.Lane().String()
	p := shardfile.ReminderPrefix
	for _, bad := range []string{
		"", p, shardfile.ManifestKey(shardpkg.MustParse("users/42")),
		"index/other/" + lane + "/2026-10-19T14:05Z/users/42/b.3",
		p + lane + "/2026-10-19T14:05Z",                                // no shard
		p + lane + "/2026-10-19T14:05Z/users/42",                       // no base
		p + lane + "/2026-10-19T14:05Z/users/42/b",                     // no move count
		p + lane + "/2026-10-19T14:05Z/users/42/b.x",                   // move count not a number
		p + lane + "/2026-10-19T14:05Z/users/42/b.-1",                  // negative move count
		p + lane + "/2026-10-19T14:05Z/Users/42/b.3",                   // not a shard name
		p + lane + "/2026-10-19T14:05Z/users/42/b.3/c",                 // more after the base
		p + "x/2026-10-19T14:05Z/users/42/b.3",                         // lane not a digit
		"index/reminders/0" + lane + "/2026-10-19T14:05Z/users/42/b.3", // lane not as written
		p + lane + "/2026-10-19T14:5Z/users/42/b.3",                    // minute not as written
		p + lane + "/2026-10-19T14:05:30Z/users/42/b.3",                // minute with seconds
		// The old layout, with the minute first.
		p + "2026-10-19T14:05Z/" + lane + "/users/42/b.3",
		// Not the shard's lane.
		p + ((r.Shard.Lane() + 1) % shardpkg.Lanes).String() + "/2026-10-19T14:05Z/users/42/b.3",
	} {
		if _, err := shardfile.ParseKey(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

// The reminder index's keys are fixed: they are what is in storage. The lane comes
// first (one hex digit, the shard's) and then the UTC minute, the shard's name and
// the base with the move count.
func TestKeysAreFixed(t *testing.T) {
	t.Parallel()
	name := shardpkg.MustParse("users/42")
	if name.Lane().String() != "6" {
		t.Fatalf("lane of %s is %s, the expected keys below are for 6", name, name.Lane())
	}
	for _, c := range []struct {
		e    shardfile.Reminder
		want string
	}{
		{shardfile.Reminder{Minute: minuteAt(14, 5), Shard: name, Base: "b", N: 2}, "index/reminders/6/2026-10-19T14:05Z/users/42/b.2"},
		{shardfile.Reminder{Minute: minuteAt(15, 0), Shard: name, Base: shardfile.BaseOf("etag"), N: 0}, "index/reminders/6/2026-10-19T15:00Z/users/42/fcaec3a55087.0"},
	} {
		if got := c.e.Key(); got != c.want {
			t.Errorf("key %q, want %q", got, c.want)
		}
	}
	if got := shardfile.BaseOf("etag"); got != "fcaec3a55087" {
		t.Errorf("BaseOf(etag) = %q, want fcaec3a55087", got)
	}
}

// shardsIn returns n shard names that fall in lane b.
func shardsIn(t *testing.T, b shardpkg.Lane, n int) []shardpkg.Name {
	t.Helper()
	var out []shardpkg.Name
	for i := 0; len(out) < n; i++ {
		if i > 100000 {
			t.Fatalf("no %d shards found in lane %s", n, b)
		}
		name := shardpkg.MustParse("users/s" + strconv.Itoa(i))
		if name.Lane() == b {
			out = append(out, name)
		}
	}
	return out
}

func newIndex(t *testing.T) (shardfile.ReminderIndex, *blob.Store) {
	t.Helper()
	store := blob.NewStore(blob.NewMemory(), "n1", 1, blob.Options{Backoff: time.Millisecond})
	return shardfile.NewReminderIndex(store), store
}

// remindersIn makes reminders of the lane: the shard is the i'th of its lane.
func reminderIn(t *testing.T, lane shardpkg.Lane, i int, m shardfile.Minute) shardfile.Reminder {
	t.Helper()
	return shardfile.Reminder{Minute: m, Shard: shardsIn(t, lane, i+1)[i], Base: "b"}
}

// dueAll lists the due reminders of every lane.
func dueAll(t *testing.T, x shardfile.ReminderIndex, now time.Time) []shardfile.Reminder {
	t.Helper()
	var out []shardfile.Reminder
	for i := range shardpkg.Lanes {
		got, err := x.Due(context.Background(), shardpkg.Lane(i), now, 1000)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, got...)
	}
	return out
}

// The reminder index lists a lane's reminders earliest first, up to now and a page,
// leaves what isn't a reminder alone without it counting against the page, and
// keeps lanes apart.
func TestDueListsALaneInTimeOrderUpToNow(t *testing.T) {
	t.Parallel()
	x, store := newIndex(t)
	ctx := context.Background()
	a, b, c, d := reminderIn(t, 3, 0, minuteAt(14, 7)), reminderIn(t, 3, 1, minuteAt(14, 5)), reminderIn(t, 3, 2, minuteAt(14, 6)), reminderIn(t, 3, 3, minuteAt(14, 9))
	e := reminderIn(t, 4, 0, minuteAt(14, 5))
	for _, r := range []shardfile.Reminder{a, b, c, d, e} {
		for range 2 { // the second is a retry of the same write
			if err := x.Create(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Objects under the prefix that aren't reminders are skipped, early or late.
	for _, junk := range []string{"3/not-a-minute/c", "3/2026-10-19T14:00Z/junk", "3/2026-10-19T14:06Z/users/zz/b"} {
		if _, err := store.Create(ctx, shardfile.ReminderPrefix+junk, bytes.NewReader(nil)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := x.Due(ctx, 3, at(14, 7).Add(30*time.Second), 10)
	if want := []shardfile.Reminder{b, c, a}; err != nil || !slices.Equal(got, want) {
		t.Fatalf("due %+v (%v), want the lane's %+v in time order", got, err, want)
	}
	if got, _ := x.Due(ctx, 3, at(14, 9), 2); !slices.Equal(got, []shardfile.Reminder{b, c}) {
		t.Fatalf("a page of 2 held %+v, want the two earliest", got)
	}
	if got, _ := x.Due(ctx, 4, at(14, 9), 10); !slices.Equal(got, []shardfile.Reminder{e}) {
		t.Fatalf("the other lane lists %+v, want just its own", got)
	}
	if got, _ := x.Due(ctx, 5, at(14, 9), 10); len(got) != 0 {
		t.Fatalf("an empty lane lists %+v", got)
	}
}

// Every lane lists on its own.
func TestLanesAreSeparate(t *testing.T) {
	t.Parallel()
	x, _ := newIndex(t)
	ctx := context.Background()
	var all []shardfile.Reminder
	for i := range shardpkg.Lanes {
		r := reminderIn(t, shardpkg.Lane(i), 0, minuteAt(14, 5))
		all = append(all, r)
		if err := x.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range all {
		got, err := x.Due(ctx, shardpkg.Lane(i), at(15, 0), 10)
		if err != nil || !slices.Equal(got, []shardfile.Reminder{want}) {
			t.Fatalf("lane %d lists %+v (%v), want %+v", i, got, err, want)
		}
	}
}

// A move creates the next entry before it deletes the old one, so a crash between
// them leaves a duplicate and never a gap.
func TestMoveCreatesTheNextThenDeletesTheOld(t *testing.T) {
	t.Parallel()
	hooked := blobtest.WithHook(blob.NewMemory())
	store := blob.NewStore(hooked, "n1", 1, blob.Options{Backoff: time.Millisecond})
	x := shardfile.NewReminderIndex(store)
	ctx := context.Background()
	r := reminderIn(t, 5, 0, minuteAt(14, 5))
	next := r
	next.Minute, next.N = minuteAt(14, 6), 1
	if err := x.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var ops []string
	hooked.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if after {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if op == blobtest.OpCreate {
			ops = append(ops, "create "+key)
		}
		if op == blobtest.OpDelete {
			ops = append(ops, "delete "+key)
		}
		return nil
	})
	if err := x.Move(ctx, r, next); err != nil {
		t.Fatal(err)
	}
	if want := []string{"create " + next.Key(), "delete " + r.Key()}; !slices.Equal(ops, want) {
		t.Fatalf("operations %v, want %v", ops, want)
	}
	hooked.SetHook(nil)
	if got := dueAll(t, x, at(15, 0)); !slices.Equal(got, []shardfile.Reminder{next}) {
		t.Fatalf("entries %+v, want just %+v", got, next)
	}
	if err := x.Delete(ctx, next); err != nil {
		t.Fatal(err)
	}
	if err := x.Delete(ctx, next); err != nil { // already gone
		t.Fatal(err)
	}
	if got := dueAll(t, x, at(15, 0)); len(got) != 0 {
		t.Fatalf("entries %+v after the delete", got)
	}
}

// A move to the entry's own key would delete the entry it made, so it is refused
// and leaves the entry where it was.
func TestMoveOntoItselfIsRefused(t *testing.T) {
	t.Parallel()
	x, _ := newIndex(t)
	ctx := context.Background()
	r := reminderIn(t, 5, 0, minuteAt(14, 5))
	if err := x.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := x.Move(ctx, r, r); err == nil {
		t.Fatal("a move onto the same key succeeded")
	}
	if got := dueAll(t, x, at(15, 0)); !slices.Equal(got, []shardfile.Reminder{r}) {
		t.Fatalf("entries %+v, want just %+v", got, r)
	}
	// A new move count in the same minute is a move.
	next := r
	next.N++
	if err := x.Move(ctx, r, next); err != nil {
		t.Fatal(err)
	}
	if got := dueAll(t, x, at(15, 0)); !slices.Equal(got, []shardfile.Reminder{next}) {
		t.Fatalf("entries %+v, want just %+v", got, next)
	}
}

func TestBacks(t *testing.T) {
	t.Parallel()
	r := shardfile.Reminder{Minute: minuteAt(14, 5)}
	for _, c := range []struct {
		next shardfile.Minute
		want bool
	}{
		{0, false},
		{minuteAt(14, 5), true},
		{minuteAt(14, 4), true},
		{minuteAt(14, 6), false},
	} {
		if got := shardfile.Backs(c.next, r); got != c.want {
			t.Errorf("Backs(%d) = %v, want %v", c.next, got, c.want)
		}
	}
}

// The pruning rule: a reminder is kept while its base is the manifest's version, and
// a backed one is never deleted.
func TestPrunable(t *testing.T) {
	t.Parallel()
	r := shardfile.Reminder{Minute: minuteAt(14, 5), Base: shardfile.BaseOf("v1")}
	later, earlier := minuteAt(15, 0), minuteAt(14, 5)
	for _, c := range []struct {
		name    string
		exists  bool
		next    shardfile.Minute
		version blob.ETag
		want    bool
	}{
		{"no manifest", false, 0, "", true},
		{"backed, base current", true, earlier, "v1", false},
		{"backed, moved on", true, earlier, "v2", false},
		{"not backed, base current: the write may be in flight", true, 0, "v1", false},
		{"later reminder, base current", true, later, "v1", false},
		{"not backed, moved on", true, 0, "v2", true},
		{"later reminder, moved on", true, later, "v2", true},
	} {
		if got := shardfile.Prunable(c.exists, c.next, c.version, r); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}
