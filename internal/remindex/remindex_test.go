package remindex

import (
	"context"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/shard"
)

func newIndex(t *testing.T) (Index, *blob.Store) {
	t.Helper()
	s := blob.NewStore(blob.NewMemory(), "n1", 1, blob.Options{Backoff: time.Millisecond})
	return New(s), s
}

func at(h, m int) time.Time { return time.Date(2026, 10, 19, h, m, 0, 0, time.UTC) }

// minuteAt is the minute of 14:05, say, on the test day.
func minuteAt(h, m int) Minute { return MinuteAt(at(h, m)) }

func TestKeyRoundTrip(t *testing.T) {
	t.Parallel()
	e := Entry{Minute: minuteAt(14, 5), Shard: shard.MustParse("app.users/42"), Base: BaseOf("etag"), N: 3}
	got, err := ParseKey(e.Key())
	if err != nil || got != e {
		t.Fatalf("%q parses to %+v (%v), want %+v", e.Key(), got, err, e)
	}
	for _, bad := range []string{"", Prefix, Prefix + "x/0/users/42/a.0", "state/shards/users/42"} {
		if _, err := ParseKey(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

// A minute's keys spread over the 16 prefixes by shard.
func TestKeysSpreadOverPrefixes(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for i := range 200 {
		e := Entry{Minute: minuteAt(14, 5), Shard: shard.MustParse("app.users/" + string(rune('a'+i%26)) + string(rune('a'+i/26))), Base: "b"}
		seen[e.Key()[len(Prefix)+len("2026-10-19T14:05Z/"):][:1]] = true
	}
	if len(seen) != 16 {
		t.Fatalf("%d prefixes used, want 16", len(seen))
	}
}

func TestBacks(t *testing.T) {
	t.Parallel()
	e := Entry{Minute: minuteAt(14, 5)}
	for _, c := range []struct {
		next Minute
		want bool
	}{
		{0, false},
		{minuteAt(14, 5), true},
		{minuteAt(14, 4), true},
		{minuteAt(14, 6), false},
	} {
		if got := Backs(c.next, e); got != c.want {
			t.Errorf("Backs(%d) = %v, want %v", c.next, got, c.want)
		}
	}
}

// The pruning rule: an entry is kept while its base is the manifest's version, and
// a backed one is never deleted.
func TestPrunable(t *testing.T) {
	t.Parallel()
	e := Entry{Minute: minuteAt(14, 5), Base: BaseOf("v1")}
	later, earlier := minuteAt(15, 0), minuteAt(14, 5)
	for _, c := range []struct {
		name    string
		exists  bool
		next    Minute
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
		if got := Prunable(c.exists, c.next, c.version, e); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestDueListsInTimeOrderUpToNow(t *testing.T) {
	t.Parallel()
	x, _ := newIndex(t)
	ctx := context.Background()
	mk := func(name string, m time.Time) Entry {
		return Entry{Minute: MinuteAt(m), Shard: shard.MustParse(name), Base: "b"}
	}
	all := []Entry{mk("app.a/1", at(14, 7)), mk("app.b/2", at(14, 5)), mk("app.c/3", at(14, 6)), mk("app.d/4", at(14, 9))}
	for _, e := range all {
		if err := x.Create(ctx, e); err != nil {
			t.Fatal(err)
		}
		if err := x.Create(ctx, e); err != nil { // a retry of the same write
			t.Fatal(err)
		}
	}
	got, err := x.Due(ctx, at(14, 7).Add(30*time.Second), 10)
	if err != nil || len(got) != 3 || got[0].Shard.String() != "app.b/2" || got[1].Shard.String() != "app.c/3" || got[2].Shard.String() != "app.a/1" {
		t.Fatalf("due %+v (%v), want b/2 c/3 a/1", got, err)
	}
	if got, _ := x.Due(ctx, at(14, 9), 2); len(got) != 2 {
		t.Fatalf("a page of 2 held %d", len(got))
	}
}

func TestMoveCreatesTheNextThenDeletesTheOld(t *testing.T) {
	t.Parallel()
	x, _ := newIndex(t)
	ctx := context.Background()
	e := Entry{Minute: minuteAt(14, 5), Shard: shard.MustParse("app.a/1"), Base: "b"}
	if err := x.Create(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := x.Move(ctx, e, minuteAt(14, 6)); err != nil {
		t.Fatal(err)
	}
	got, err := x.Due(ctx, at(15, 0), 10)
	want := Entry{Minute: minuteAt(14, 6), Shard: shard.MustParse("app.a/1"), Base: "b", N: 1}
	if err != nil || len(got) != 1 || got[0] != want {
		t.Fatalf("entries %+v (%v), want just %+v", got, err, want)
	}
	if err := x.Delete(ctx, want); err != nil {
		t.Fatal(err)
	}
	if err := x.Delete(ctx, want); err != nil { // already gone
		t.Fatal(err)
	}
}
