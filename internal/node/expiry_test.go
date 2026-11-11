package node

import (
	"context"
	"errors"
	"github.com/luci1900/shardlite/internal/apptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// segments counts the non-empty and empty (fence) objects under the journal prefix.
func (r *mrig) segments() (data, fences int) {
	r.t.Helper()
	for info, err := range r.local.List(context.Background(), journal.Root, "") {
		if err != nil {
			r.t.Fatal(err)
		}
		if info.Size == 0 {
			fences++
		} else {
			data++
		}
	}
	return data, fences
}

// A shard whose tail is not turned into a delta (its manifest updates fail
// here) raises the alert once the tail is 3 times the delta age old, while its
// segments still exist, and they expire after.
func TestTailOlderThanTheAlertAgeRaisesIt(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	r.crash()
	const deltaAge, expiry = 170 * time.Millisecond, 2 * time.Second // the alert is at 510 ms
	r.startShardset(func(o *ShardsetOptions) { o.Lifecycle.DeltaAge = deltaAge })
	r.startExpiry(expiry)

	ctx := context.Background()
	ref := r.acquire("slow/tail")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if (op == blobtest.OpUpdate || op == blobtest.OpCreate) && strings.HasPrefix(key, shardfile.ManifestPrefix) {
			return errors.New("storage refuses manifests")
		}
		return nil
	})
	r.app.Must(t, "insert", "a", "2", "100") // a tail whose delta can never be recorded
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, apptest.Timeout, "the tail alert", func() bool { return r.m.Stats.TailAlerts.Load() > 0 })
	if data, _ := r.segments(); data == 0 {
		t.Fatal("the segments were gone before the alert")
	}
	apptest.Eventually(t, apptest.Timeout, "the segments to expire", func() bool { d, _ := r.segments(); return d == 0 })
}

// The alert doesn't depend on the delta succeeding: with every manifest update
// failing, the tail only grows older, and the alert comes all the same.
func TestTailAlertWhenDeltasFail(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	r.crash()
	r.startShardset(func(o *ShardsetOptions) { o.Lifecycle.DeltaAge = 100 * time.Millisecond }) // the alert is at 300 ms
	ctx := context.Background()
	ref := r.acquire("slow/failing")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if (op == blobtest.OpUpdate || op == blobtest.OpCreate) && strings.HasPrefix(key, shardfile.ManifestPrefix) {
			return errors.New("storage refuses manifests")
		}
		return nil
	})
	r.app.Must(t, "insert", "a", "2", "100") // a tail whose delta can never be recorded
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, apptest.Timeout, "the tail alert", func() bool { return r.m.Stats.TailAlerts.Load() > 0 })
}

// A node restarted after its old segments expired adopts its own journal from the
// lease's range, keeps its fences, and every shard restores.
func TestRestartAfterSegmentsExpired(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	r.crash()
	r.startShardset(func(o *ShardsetOptions) { o.Lifecycle.DeltaAge = 30 * time.Millisecond })
	ctx := context.Background()

	want := map[string]string{}
	for _, name := range []string{"wallet/1", "wallet/2", "users/3"} {
		ref := r.acquire(name)
		r.write(ref, name)
		for range 3 {
			r.app.Must(t, "insert", name, "2", "100")
		}
		if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
		want[name] = r.liveDump(ref.Path())
	}
	// Every tail becomes a delta, so the journal holds nothing that is not in a file.
	apptest.Eventually(t, apptest.Timeout, "deltas for every shard", func() bool {
		for name := range want {
			m, err := r.manifest(name)
			if err != nil || len(m.Files) == 0 || r.dumpBlob(name) != want[name] {
				return false
			}
		}
		return true
	})
	r.afterSnapshotLooks(2, "wallet/1", "wallet/2", "users/3") // a few more looks of the delta timers
	dataBefore, _ := r.segments()
	if dataBefore == 0 {
		t.Fatal("no segments")
	}
	for name := range want {
		r.app.Must(t, "close", name)
	}
	r.crash()

	// Everything written so far is older than the expiry age.
	stop := r.startExpiry(300 * time.Millisecond)
	apptest.Eventually(t, apptest.Timeout, "the old segments to expire", func() bool { d, _ := r.segments(); return d == 0 })
	stop()
	if _, fences := r.segments(); fences != 0 {
		t.Fatalf("%d fences before the restart", fences)
	}

	r.startNode()
	if r.n.Adopted.Fence == 0 || len(r.n.Adopted.Lost) != 0 {
		t.Fatalf("adoption: %+v", r.n.Adopted)
	}
	r.startShardset(nil)
	for name, dump := range want {
		ref := r.acquire(name)
		if got := r.liveDump(ref.Path()); got != dump {
			t.Fatalf("%s restored differently", name)
		}
		ref.Release()
	}
	if _, fences := r.segments(); fences == 0 {
		t.Fatal("the adoption's fence is gone")
	}
}

func envWith(extra map[string]string) (Env, error) {
	m := map[string]string{"SHARDLITE_NODE": "n", "SHARDLITE_BLOB": "s3://b", "SHARDLITE_DATA": "d"}
	for k, v := range extra {
		m[k] = v
	}
	return ParseEnv(func(k string) string { return m[k] })
}

func TestFlushAndDeltaEnv(t *testing.T) {
	t.Parallel()
	e, err := envWith(nil)
	if err != nil || e.DeltaAge != 15*time.Minute || e.DeltaSize != 256<<10 || e.FlushMin != DefaultFlushMin {
		t.Fatalf("defaults: %+v %v", e, err)
	}
	e, err = envWith(map[string]string{"SHARDLITE_DELTA_AGE": "1m", "SHARDLITE_DELTA_SIZE": "64K", "SHARDLITE_FLUSH_MIN": "10ms"})
	if err != nil || e.DeltaAge != time.Minute || e.DeltaSize != 64<<10 || e.FlushMin != 10*time.Millisecond {
		t.Fatalf("set: %+v %v", e, err)
	}
	for name, vars := range map[string]map[string]string{
		"bad delta age":   {"SHARDLITE_DELTA_AGE": "soon"},
		"bad delta size":  {"SHARDLITE_DELTA_SIZE": "lots"},
		"zero delta size": {"SHARDLITE_DELTA_SIZE": "0"},
		"bad flush min":   {"SHARDLITE_FLUSH_MIN": "-1s"},
		"node with a /":   {"SHARDLITE_NODE": "pods/a"},
		"node too long":   {"SHARDLITE_NODE": strings.Repeat("a", 129)},
	} {
		if _, err := envWith(vars); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The tail alert is 3 times the delta age, and segments expire after the
// larger of 24 hours and 4 times the alert.
func TestTailAlertAndJournalExpiryFollowTheDeltaAge(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ age, alert, expiry time.Duration }{
		{15 * time.Minute, 45 * time.Minute, 24 * time.Hour},
		{time.Minute, 3 * time.Minute, 24 * time.Hour},
		{2 * time.Hour, 6 * time.Hour, 24 * time.Hour},
		{3 * time.Hour, 9 * time.Hour, 36 * time.Hour},
	} {
		if got := tailAlertAge(tc.age); got != tc.alert {
			t.Errorf("delta age %v: alert %v, want %v", tc.age, got, tc.alert)
		}
		if got := JournalExpiry(tc.age); got != tc.expiry {
			t.Errorf("delta age %v: expiry %v, want %v", tc.age, got, tc.expiry)
		}
	}
}

// Journal expiry starts after adoption: a node that was down for longer than the
// expiry age still adopts the old segments, then they are deleted.
func TestExpiryStartsAfterAdoption(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	r.skew.Store(int64(24 * time.Hour)) // everything written from here on is a day old
	ctx := context.Background()
	ref := r.acquire("wallet/1")
	r.write(ref, "a")
	r.app.Must(t, "insert", "a", "2", "100")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	want := r.liveDump(ref.Path())
	r.app.Must(t, "close", "a")
	r.crash() // the shard's commits are only in journal segments
	if data, _ := r.segments(); data == 0 {
		t.Fatal("no segments")
	}
	// The node was down for a day: every segment is older than the expiry age.
	r.skew.Store(0)
	r.startNode() // the next process adopts the old segments
	if r.n.Adopted.Deltas != 1 {
		t.Fatalf("the segments were not adopted: %+v", r.n.Adopted)
	}
	r.startExpiry(200 * time.Millisecond)
	apptest.Eventually(t, apptest.Timeout, "the old segments to expire", func() bool { d, _ := r.segments(); return d == 0 })
	if got := r.dumpBlob("wallet/1"); got != want {
		t.Fatal("the shard restores differently")
	}
}

// putSegment writes a stand-in for a journal segment (a node's journal is only
// its writer's business, so a test can name any number).
func (r *mrig) putSegment(node cluster.Node, seq pos.Seg, body string) string {
	r.t.Helper()
	key := journal.Key(node, seq)
	if _, err := r.n.Store.Create(context.Background(), key, strings.NewReader(body)); err != nil {
		r.t.Fatal(err)
	}
	return key
}

func (r *mrig) exists(key string) bool {
	r.t.Helper()
	_, err := r.n.Store.Head(context.Background(), key)
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		r.t.Fatal(err)
	}
	return err == nil
}

// Journal expiry deletes the node's non-empty segments older than the expiry age and
// nothing else: fences, recent segments and other nodes' journals stay.
func TestExpiryDeletesOldSegmentsKeepsFencesAndRecent(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	old := r.putSegment("n1", 1001, "old")
	fence := r.putSegment("n1", 1002, "")
	r.putSegment("n1", 1003, "recent")
	foreign := r.putSegment("other", 1, "someone else's")
	key := lease.Key("n1")

	// With a fake now: the first two are older than the age, the third is not.
	// (Their real modification times are all about now.)
	realNow := time.Now()
	age := time.Hour
	deleted, err := r.expiry(age).expire(ctx, realNow.Add(30*time.Minute))
	if err != nil || deleted != 0 {
		t.Fatalf("nothing is old yet: deleted %d, %v", deleted, err)
	}
	deleted, err = r.expiry(age).expire(ctx, realNow.Add(2*time.Hour))
	if err != nil || deleted != 3-1 {
		t.Fatalf("deleted %d, %v, want the two non-empty segments", deleted, err)
	}
	if r.exists(old) || r.exists(journal.Key("n1", 1003)) {
		t.Fatal("old segments are still there")
	}
	if !r.exists(fence) {
		t.Fatal("a fence was deleted")
	}
	if !r.exists(foreign) {
		t.Fatal("another node's journal was swept")
	}
	if !r.exists(key) {
		t.Fatal("the lease was swept")
	}
	// Nothing left to delete.
	if deleted, err := r.expiry(age).expire(ctx, realNow.Add(3*time.Hour)); err != nil || deleted != 0 {
		t.Fatalf("second pass: %d %v", deleted, err)
	}
}

// Journal expiry works through List and Delete alone: no Get, Head, Create or Update.
func TestExpiryUsesOnlyListAndDelete(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	r.putSegment("n1", 2001, "a")
	r.putSegment("n1", 2002, "b")
	r.putSegment("n1", 2003, "")

	var mu sync.Mutex
	// Keyed by name because it counts what expiry made, rather than dispatching
	// on it: an enum-keyed map literal must hold every member.
	ops := map[string]int{}
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && strings.HasPrefix(key, journal.Root) {
			mu.Lock()
			ops[op.String()]++
			mu.Unlock()
		}
		return nil
	})
	deleted, err := r.expiry(time.Minute).expire(ctx, time.Now().Add(time.Hour))
	if err != nil || deleted != 2 {
		t.Fatalf("deleted %d, %v", deleted, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if ops[blobtest.OpList.String()] == 0 || ops[blobtest.OpDelete.String()] != 2 || len(ops) != 2 {
		t.Fatalf("journal expiry made these calls: %v, want only list (once or more) and two deletes", ops)
	}
}

// The timer-driven journal expiry deletes, and once stopped it is joined: nothing can
// sweep after the stop returns.
func TestJournalExpiryRunsAndStopJoins(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	key := r.putSegment("n1", 3001, "seg")
	stop := r.startExpiry(300 * time.Millisecond)
	apptest.Eventually(t, apptest.Timeout, "the segment to expire", func() bool { return !r.exists(key) })
	stop()
	// Joined: a segment written now is not swept, whatever its age (the ticker
	// is every 30 ms).
	r.local.Create(context.Background(), key, strings.NewReader("seg"), blob.WriteID{})
	time.Sleep(100 * time.Millisecond)
	if !r.exists(key) {
		t.Fatal("journal expiry ran after it was stopped")
	}
}

// expiry builds the node's journal expiry from the node's parts, as Run does.
func (r *mrig) expiry(age time.Duration) *expiry {
	return &expiry{journal: r.n.Journal, age: age, log: r.n.Logger}
}

// startExpiry runs the node's journal expiry, as Run does, until the returned
// stop (or the end of the test), which joins it.
func (r *mrig) startExpiry(age time.Duration) (stop func()) {
	r.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.expiry(age).Serve(ctx) }()
	stop = func() { cancel(); <-done }
	r.t.Cleanup(stop)
	return stop
}
