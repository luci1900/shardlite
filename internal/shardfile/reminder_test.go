package shardfile_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/remindex"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// opLog records the creates under the reminder index and the manifest's updates,
// in order.
type opLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *opLog) hook(_ context.Context, op blob.Op, key string, after bool) error {
	if after {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case op == blob.OpCreate && strings.HasPrefix(key, remindex.Prefix):
		l.ops = append(l.ops, "entry")
	case op == blob.OpUpdate && key == shardfile.ManifestKey(shard):
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
	return shardfile.Hooks{BeforeUpdate: func(context.Context, uint64) (int64, error) {
		if unix == 0 {
			return 0, nil
		}
		return remindex.MinuteOf(unix), nil
	}}
}

func dueEntries(t *testing.T, e *harness.Env, now time.Time) []remindex.Entry {
	t.Helper()
	out, err := remindex.New(e.Store).Due(context.Background(), now, 1000)
	if err != nil {
		t.Fatal(err)
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
	base := remindex.BaseOf(h.ETag)
	ack(t, app, sh, 1)
	if err := h.WriteDelta(ctx, sh, reminderAt(due)); err != nil {
		t.Fatal(err)
	}
	if got := log.take(); len(got) != 2 || got[0] != "entry" || got[1] != "manifest" {
		t.Fatalf("operations %v, want the entry and then the manifest", got)
	}
	m, _, err := e.Files.GetManifest(ctx, shard)
	if err != nil || m.NextReminder != remindex.MinuteOf(due) {
		t.Fatalf("manifest %+v (%v), want next_reminder %d", m, err, remindex.MinuteOf(due))
	}
	want := remindex.Entry{Minute: remindex.MinuteOf(due), Shard: shard, Base: base}
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
	if m, _, err := e.Files.GetManifest(ctx, shard); err != nil || m.NextReminder != 0 {
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
	e.Blob.SetHook(func(_ context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && strings.HasPrefix(key, remindex.Prefix) {
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
	base := remindex.BaseOf(h.ETag)
	var log opLog
	e.Blob.SetHook(log.hook)

	rep, err := e.Files.AdoptJournal(ctx, e.Node, 1, e.W.NextSeq(), nil)
	if err != nil || rep.Deltas != 1 {
		t.Fatalf("adoption: %+v %v", rep, err)
	}
	if got := log.take(); len(got) != 2 || got[0] != "entry" || got[1] != "manifest" {
		t.Fatalf("operations %v, want the entry and then the manifest", got)
	}
	m, _, err := e.Files.GetManifest(ctx, shard)
	if err != nil || m.NextReminder != remindex.MinuteAt(now) {
		t.Fatalf("manifest %+v (%v), want next_reminder %d", m, err, remindex.MinuteAt(now))
	}
	want := remindex.Entry{Minute: remindex.MinuteOf(now.Unix()), Shard: shard, Base: base}
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

	e.Blob.SetHook(func(_ context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && strings.HasPrefix(key, remindex.Prefix) {
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
		if err := h.ReleaseOwnership(ctx); err != nil {
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
