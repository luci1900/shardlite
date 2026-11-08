package shardfile_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/dueindex"
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
	return shardfile.Hooks{BeforeUpdate: func(context.Context, pos.Position) (dueindex.Minute, error) {
		if unix == 0 {
			return 0, nil
		}
		return dueindex.MinuteOf(unix), nil
	}}
}

func dueEntries(t *testing.T, e *harness.Env, now time.Time) []shardfile.Reminder {
	t.Helper()
	var out []shardfile.Reminder
	for i := range shardpkg.Lanes {
		got, err := shardfile.ReminderIndex(e.Store).Due(context.Background(), shardpkg.Lane(i), now, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range got {
			r, err := shardfile.ParseReminder(d)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, r)
		}
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
	if err != nil || m.NextReminder != dueindex.MinuteOf(due) {
		t.Fatalf("manifest %+v (%v), want next_reminder %d", m, err, dueindex.MinuteOf(due))
	}
	want := shardfile.Reminder{Minute: dueindex.MinuteOf(due), Shard: shardName(shard), Base: base}
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
	if err != nil || m.NextReminder != dueindex.MinuteAt(now) {
		t.Fatalf("manifest %+v (%v), want next_reminder %d", m, err, dueindex.MinuteAt(now))
	}
	want := shardfile.Reminder{Minute: dueindex.MinuteOf(now.Unix()), Shard: shardName(shard), Base: base}
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
func minuteAt(h, m int) dueindex.Minute { return dueindex.MinuteAt(at(h, m)) }

// reminderKey is the object key of a reminder.
func reminderKey(r shardfile.Reminder) string {
	return dueindex.Key(shardfile.ReminderPrefix, r.Entry())
}

// parseReminderKey is reminderKey's inverse.
func parseReminderKey(key string) (shardfile.Reminder, error) {
	d, err := dueindex.Parse(shardfile.ReminderPrefix, key)
	if err != nil {
		return shardfile.Reminder{}, err
	}
	return shardfile.ParseReminder(d)
}

func TestReminderKeyRoundTrip(t *testing.T) {
	t.Parallel()
	r := shardfile.Reminder{Minute: minuteAt(14, 5), Shard: shardpkg.MustParse("users/42"), Base: shardfile.BaseOf("etag"), N: 3}
	got, err := parseReminderKey(reminderKey(r))
	if err != nil || got != r {
		t.Fatalf("%q parses to %+v (%v), want %+v", reminderKey(r), got, err, r)
	}
	p := shardfile.ReminderPrefix
	for _, bad := range []string{
		"", p, shardfile.ManifestKey(shardpkg.MustParse("users/42")),
		p + "x/2026-10-19T14:05Z/users/42/a.0",
		// The old layout, with the minute first.
		p + "2026-10-19T14:05Z/" + r.Shard.Lane().String() + "/users/42/a.0",
		// Not the shard's lane.
		p + ((r.Shard.Lane() + 1) % shardpkg.Lanes).String() + "/2026-10-19T14:05Z/users/42/a.0",
	} {
		if _, err := parseReminderKey(bad); err == nil {
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
		if got := reminderKey(c.e); got != c.want {
			t.Errorf("key %q, want %q", got, c.want)
		}
	}
	if got := shardfile.BaseOf("etag"); got != "fcaec3a55087" {
		t.Errorf("BaseOf(etag) = %q, want fcaec3a55087", got)
	}
}

// The subject is kind/id/base.n, and the lane is the shard's.
func TestReminderSubjectRoundTrip(t *testing.T) {
	t.Parallel()
	r := shardfile.Reminder{Minute: minuteAt(14, 5), Shard: shardpkg.MustParse("users/42"), Base: shardfile.BaseOf("etag"), N: 3}
	d := r.Entry()
	if want := "users/42/" + shardfile.BaseOf("etag") + ".3"; d.Subject != want || d.Lane != r.Shard.Lane() || d.Minute != r.Minute {
		t.Fatalf("due entry %+v, want subject %q in the shard's lane", d, want)
	}
	got, err := shardfile.ParseReminder(d)
	if err != nil || got != r {
		t.Fatalf("%+v parses to %+v (%v), want %+v", d, got, err, r)
	}
	for _, subject := range []string{"", "a", "users/42", "users/42/b", "users/42/b.x", "users/42/b.-1", "Users/42/b.0", "users/42/b.0/c"} {
		bad := d
		bad.Subject = subject
		if _, err := shardfile.ParseReminder(bad); err == nil {
			t.Errorf("subject %q parsed", subject)
		}
	}
	other := d
	other.Lane = (d.Lane + 1) % shardpkg.Lanes
	if _, err := shardfile.ParseReminder(other); err == nil {
		t.Error("an entry under another shard's lane parsed")
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

// The reminder index lists its lane's reminders earliest first and leaves what isn't
// a reminder alone, without it counting against the page.
func TestReminderIndexListsOnlyReminders(t *testing.T) {
	t.Parallel()
	store := blob.NewStore(blob.NewMemory(), "n1", 1, blob.Options{Backoff: time.Millisecond})
	x := shardfile.ReminderIndex(store)
	ctx := context.Background()
	names := shardsIn(t, 3, 2)
	late := shardfile.Reminder{Minute: minuteAt(14, 7), Shard: names[0], Base: "b"}
	early := shardfile.Reminder{Minute: minuteAt(14, 5), Shard: names[1], Base: "b"}
	for _, r := range []shardfile.Reminder{late, early} {
		if err := x.Create(ctx, r.Entry()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Create(ctx, shardfile.ReminderPrefix+"3/2026-10-19T14:00Z/junk", bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	got, err := x.Due(ctx, 3, at(15, 0), 2)
	if err != nil || len(got) != 2 || got[0] != early.Entry() || got[1] != late.Entry() {
		t.Fatalf("due %+v (%v), want %+v then %+v", got, err, early.Entry(), late.Entry())
	}
}

func TestBacks(t *testing.T) {
	t.Parallel()
	r := shardfile.Reminder{Minute: minuteAt(14, 5)}
	for _, c := range []struct {
		next dueindex.Minute
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
		next    dueindex.Minute
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
