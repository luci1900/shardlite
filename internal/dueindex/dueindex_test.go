package dueindex

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/shard"
)

const testPrefix = "index/test/"

func newIndex(t *testing.T) Index {
	t.Helper()
	s := blob.NewStore(blob.NewMemory(), "n1", 1, blob.Options{Backoff: time.Millisecond})
	return New(s, testPrefix, "test", func(e Entry) bool { return e.Subject != "junk" })
}

func at(h, m int) time.Time { return time.Date(2026, 10, 19, h, m, 0, 0, time.UTC) }

// minuteAt is the minute of 14:05, say, on the test day.
func minuteAt(h, m int) Minute { return MinuteAt(at(h, m)) }

func TestMinutes(t *testing.T) {
	t.Parallel()
	if got := MinuteOf(119); got != 1 {
		t.Errorf("MinuteOf(119) = %d, want 1", got)
	}
	if got, want := MinuteAt(at(14, 5).Add(59*time.Second)), MinuteOf(at(14, 5).Unix()); got != want {
		t.Errorf("MinuteAt = %d, want %d", got, want)
	}
}

func TestKeyRoundTrip(t *testing.T) {
	t.Parallel()
	for _, e := range []Entry{
		{Lane: 0, Minute: minuteAt(14, 5), Subject: "a"},
		{Lane: 15, Minute: minuteAt(0, 0), Subject: "users/42/b.3"},
		{Lane: 7, Minute: minuteAt(23, 59), Subject: "x/y/z/w"},
	} {
		got, err := Parse(testPrefix, Key(testPrefix, e))
		if err != nil || got != e {
			t.Errorf("%q parses to %+v (%v), want %+v", Key(testPrefix, e), got, err, e)
		}
	}
	for _, bad := range []string{
		"", testPrefix, "other/3/2026-10-19T14:05Z/a",
		testPrefix + "3/2026-10-19T14:05Z",      // no subject
		testPrefix + "3/2026-10-19T14:05Z/",     // empty subject
		testPrefix + "x/2026-10-19T14:05Z/a",    // lane not a digit
		testPrefix + "10/2026-10-19T14:05Z/a",   // lane out of range
		testPrefix + "03/2026-10-19T14:05Z/a",   // lane not as written
		testPrefix + "3/2026-10-19T14:5Z/a",     // minute not as written
		testPrefix + "3/2026-10-19T14:05:30Z/a", // minute with seconds
		// The minute before the lane.
		testPrefix + "2026-10-19T14:05Z/3/a",
	} {
		if _, err := Parse(testPrefix, bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

// A key starts with its lane, which is one hex digit.
func TestKeyPutsTheLaneFirst(t *testing.T) {
	t.Parallel()
	got := Key("index/x/", Entry{Lane: 11, Minute: minuteAt(14, 5), Subject: "a/b"})
	if want := "index/x/b/2026-10-19T14:05Z/a/b"; got != want {
		t.Fatalf("key %q, want %q", got, want)
	}
}

// subjects returns the subjects of entries.
func subjects(es []Entry) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.Subject)
	}
	return out
}

func TestDueListsALaneInTimeOrderUpToNow(t *testing.T) {
	t.Parallel()
	x := newIndex(t)
	ctx := context.Background()
	mk := func(lane shard.Lane, subject string, m time.Time) Entry {
		return Entry{Lane: lane, Minute: MinuteAt(m), Subject: subject}
	}
	all := []Entry{mk(3, "a", at(14, 7)), mk(3, "b", at(14, 5)), mk(3, "c", at(14, 6)), mk(3, "d", at(14, 9)), mk(4, "e", at(14, 5))}
	for _, e := range all {
		if err := x.Create(ctx, e); err != nil {
			t.Fatal(err)
		}
		if err := x.Create(ctx, e); err != nil { // a retry of the same write
			t.Fatal(err)
		}
	}
	got, err := x.Due(ctx, 3, at(14, 7).Add(30*time.Second), 10)
	if err != nil || len(got) != 3 || got[0].Subject != "b" || got[1].Subject != "c" || got[2].Subject != "a" {
		t.Fatalf("due %v (%v), want the lane's b, c and a in time order", subjects(got), err)
	}
	if got, _ := x.Due(ctx, 3, at(14, 9), 2); len(got) != 2 || got[0].Subject != "b" || got[1].Subject != "c" {
		t.Fatalf("a page of 2 held %v, want the two earliest", subjects(got))
	}
	if got, _ := x.Due(ctx, 4, at(14, 9), 10); len(got) != 1 || got[0].Subject != "e" {
		t.Fatalf("the other lane lists %v, want just its own", subjects(got))
	}
	if got, _ := x.Due(ctx, 5, at(14, 9), 10); len(got) != 0 {
		t.Fatalf("an empty lane lists %v", subjects(got))
	}
}

// Objects under the prefix that aren't entries, or that the index's user doesn't
// recognise, are left alone and don't take up room in a page.
func TestDueSkipsWhatIsNotAnEntry(t *testing.T) {
	t.Parallel()
	s := blob.NewStore(blob.NewMemory(), "n1", 1, blob.Options{Backoff: time.Millisecond})
	x := New(s, testPrefix, "test", func(e Entry) bool { return e.Subject != "junk" })
	ctx := context.Background()
	for _, e := range []Entry{{3, minuteAt(14, 1), "junk"}, {3, minuteAt(14, 2), "a"}, {3, minuteAt(14, 3), "b"}} {
		if err := x.Create(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Create(ctx, testPrefix+"3/not-a-minute/c", bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	got, err := x.Due(ctx, 3, at(15, 0), 2)
	if err != nil || len(got) != 2 || got[0].Subject != "a" || got[1].Subject != "b" {
		t.Fatalf("due %v (%v), want a and b", subjects(got), err)
	}
}

// dueAll lists the due entries of every lane.
func dueAll(t *testing.T, x Index, now time.Time) []Entry {
	t.Helper()
	var out []Entry
	for i := range shard.Lanes {
		got, err := x.Due(context.Background(), shard.Lane(i), now, 1000)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, got...)
	}
	return out
}

func TestMoveCreatesTheNextThenDeletesTheOld(t *testing.T) {
	t.Parallel()
	x := newIndex(t)
	ctx := context.Background()
	e := Entry{Lane: 5, Minute: minuteAt(14, 5), Subject: "a.0"}
	if err := x.Create(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := x.Move(ctx, e, minuteAt(14, 6), "a.1"); err != nil {
		t.Fatal(err)
	}
	got := dueAll(t, x, at(15, 0))
	want := Entry{Lane: 5, Minute: minuteAt(14, 6), Subject: "a.1"}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("entries %+v, want just %+v", got, want)
	}
	if err := x.Delete(ctx, want); err != nil {
		t.Fatal(err)
	}
	if err := x.Delete(ctx, want); err != nil { // already gone
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
	x := newIndex(t)
	ctx := context.Background()
	e := Entry{Lane: 5, Minute: minuteAt(14, 5), Subject: "a"}
	if err := x.Create(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := x.Move(ctx, e, e.Minute, e.Subject); err == nil {
		t.Fatal("a move onto the same key succeeded")
	}
	if got := dueAll(t, x, at(15, 0)); len(got) != 1 || got[0] != e {
		t.Fatalf("entries %+v, want just %+v", got, e)
	}
	// A new subject in the same minute is a move.
	if err := x.Move(ctx, e, e.Minute, "a.1"); err != nil {
		t.Fatal(err)
	}
	if got := dueAll(t, x, at(15, 0)); len(got) != 1 || got[0].Subject != "a.1" {
		t.Fatalf("entries %+v, want just a.1", got)
	}
}

// Every lane lists on its own, whatever the subjects.
func TestLanesAreSeparate(t *testing.T) {
	t.Parallel()
	x := newIndex(t)
	ctx := context.Background()
	for i := range shard.Lanes {
		if err := x.Create(ctx, Entry{Lane: shard.Lane(i), Minute: minuteAt(14, 5), Subject: "s" + strconv.Itoa(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range shard.Lanes {
		got, err := x.Due(ctx, shard.Lane(i), at(15, 0), 10)
		if err != nil || len(got) != 1 || got[0].Subject != "s"+strconv.Itoa(i) {
			t.Fatalf("lane %d lists %v (%v)", i, subjects(got), err)
		}
	}
}
