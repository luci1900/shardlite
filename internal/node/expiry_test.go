package node

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/segment"
)

// segments counts the non-empty and empty (fence) objects under journal/.
func (r *mrig) segments() (data, fences int) {
	r.t.Helper()
	for info, err := range r.local.List(context.Background(), "journal/") {
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

// A shard whose tail is not turned into a delta (deltas disabled here) raises
// the alert once the tail is older than the alert age, while its segments
// still exist, and they expire after.
func TestTailOlderThanTheAlertAgeRaisesIt(t *testing.T) {
	r := newMRig(t)
	r.m.cancel()
	r.m.g.Wait()
	r.m.idx.close()
	const alert, expiry = 500 * time.Millisecond, 2 * time.Second
	r.startManager(func(o *ManagerOptions) { o.DeltaAge = time.Hour; o.TailAlert = alert })
	if err := r.n.StartJournalSweeper(expiry); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	ref := r.acquire("slow/tail")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	if r.m.Stats.TailAlerts.Load() != 0 {
		t.Fatal("alert for a fresh tail")
	}
	eventually(t, "the tail alert", func() bool { return r.m.Stats.TailAlerts.Load() > 0 })
	if data, _ := r.segments(); data == 0 {
		t.Fatal("the segments were gone before the alert")
	}
	eventually(t, "the segments to expire", func() bool { d, _ := r.segments(); return d == 0 })
}

// The alert doesn't depend on the delta succeeding: with every manifest update
// failing, the tail only grows older, and the alert comes all the same.
func TestTailAlertWhenDeltasFail(t *testing.T) {
	r := newMRig(t)
	r.m.cancel()
	r.m.g.Wait()
	r.m.idx.close()
	r.startManager(func(o *ManagerOptions) { o.DeltaAge = 30 * time.Millisecond; o.TailAlert = 300 * time.Millisecond })
	ctx := context.Background()
	ref := r.acquire("slow/failing")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if (op == blob.OpUpdate || op == blob.OpCreate) && strings.HasPrefix(key, "manifests/") {
			return errors.New("storage refuses manifests")
		}
		return nil
	})
	r.app.Must(t, "insert", "a", "2", "100") // a tail whose delta can never be recorded
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the tail alert", func() bool { return r.m.Stats.TailAlerts.Load() > 0 })
}

// A node restarted after its old segments expired adopts its own journal from the
// lease's range, keeps its fences, and every shard restores.
func TestRestartAfterSegmentsExpired(t *testing.T) {
	r := newMRig(t)
	r.m.cancel()
	r.m.g.Wait()
	r.m.idx.close()
	r.startManager(func(o *ManagerOptions) { o.DeltaAge = 30 * time.Millisecond })
	ctx := context.Background()

	want := map[string]string{}
	for _, name := range []string{"wallet/1", "wallet/2", "users/3"} {
		ref := r.acquire(name)
		r.write(ref, name)
		for range 3 {
			r.app.Must(t, "insert", name, "2", "100")
		}
		if err := r.m.Gate(ctx, ref, true); err != nil {
			t.Fatal(err)
		}
		want[name] = r.liveDump(ref.Path())
	}
	// Every tail becomes a delta, so the journal holds nothing that is not in a file.
	eventually(t, "deltas for every shard", func() bool {
		for name := range want {
			m, err := r.manifest(name)
			if err != nil || len(m.Files) == 0 || r.dumpBlob(name) != want[name] {
				return false
			}
		}
		return true
	})
	time.Sleep(200 * time.Millisecond) // a few more background passes
	dataBefore, _ := r.segments()
	if dataBefore == 0 {
		t.Fatal("no segments")
	}
	for name := range want {
		r.app.Must(t, "close", name)
	}
	r.crash()

	// Everything written so far is older than the expiry age.
	if err := r.n.StartJournalSweeper(300 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the old segments to expire", func() bool { d, _ := r.segments(); return d == 0 })
	r.n.stopSweeper()
	if _, fences := r.segments(); fences != 0 {
		t.Fatalf("%d fences before the restart", fences)
	}

	r.startNode()
	if r.n.Adopted.Fence == 0 || len(r.n.Adopted.Lost) != 0 {
		t.Fatalf("adoption: %+v", r.n.Adopted)
	}
	r.startManager(nil)
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
	m := map[string]string{"SHARDLITE_NODE": "n", "SHARDLITE_BLOB": "b", "SHARDLITE_KINDS": "k", "SHARDLITE_DATA": "d", "SHARDLITE_META": "m"}
	for k, v := range extra {
		m[k] = v
	}
	return ParseEnv(func(k string) string { return m[k] })
}

func TestTailAlertAndExpiryEnv(t *testing.T) {
	e, err := envWith(nil)
	if err != nil || e.TailAlert != 15*time.Minute || e.JournalExpiry != 24*time.Hour {
		t.Fatalf("defaults: %v %v %v", e.TailAlert, e.JournalExpiry, err)
	}
	e, err = envWith(map[string]string{"SHARDLITE_TAIL_ALERT": "1m", "SHARDLITE_JOURNAL_EXPIRY": "4m"})
	if err != nil || e.TailAlert != time.Minute || e.JournalExpiry != 4*time.Minute {
		t.Fatalf("set: %v %v %v", e.TailAlert, e.JournalExpiry, err)
	}
	for name, vars := range map[string]map[string]string{
		"expiry under 4x the alert":  {"SHARDLITE_TAIL_ALERT": "1m", "SHARDLITE_JOURNAL_EXPIRY": "3m59s"},
		"default expiry, long alert": {"SHARDLITE_TAIL_ALERT": "7h"},
		"bad duration":               {"SHARDLITE_JOURNAL_EXPIRY": "soon"},
	} {
		if _, err := envWith(vars); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The sweeper starts after adoption: a node that was down for longer than the
// expiry age still adopts the old segments, then they are deleted.
func TestSweeperStartsAfterAdoption(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("wallet/1")
	r.write(ref, "a")
	r.app.Must(t, "insert", "a", "2", "100")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	want := r.liveDump(ref.Path())
	r.app.Must(t, "close", "a")
	r.crash() // the shard's commits are only in journal segments
	if data, _ := r.segments(); data == 0 {
		t.Fatal("no segments")
	}
	// The node was down for a day: every segment is older than the expiry age.
	old := time.Now().Add(-24 * time.Hour)
	err := filepath.WalkDir(filepath.Join(r.blobDir, "journal"), func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		return os.Chtimes(p, old, old)
	})
	if err != nil {
		t.Fatal(err)
	}

	health := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer health.Close()
	dir := t.TempDir()
	vars := map[string]string{
		"SHARDLITE_NODE": "n1", "SHARDLITE_BLOB": r.blobDir, "SHARDLITE_KINDS": r.kindsPath,
		"SHARDLITE_DATA": filepath.Join(dir, "data"), "SHARDLITE_META": filepath.Join(dir, "meta"),
		"SHARDLITE_APP": health.URL, "SHARDLITE_LISTEN": "127.0.0.1:0", "SHARDLITE_CONTROL": "127.0.0.1:0",
		"SHARDLITE_FLUSH": "50ms", "SHARDLITE_TAIL_ALERT": "50ms", "SHARDLITE_JOURNAL_EXPIRY": "200ms",
	}
	env, err := ParseEnv(func(k string) string { return vars[k] })
	if err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	runCtx, cancel := context.WithCancel(ctx)
	ran := make(chan error, 1)
	up := make(chan struct{})
	go func() {
		ran <- Run(runCtx, env, slog.New(slog.NewTextHandler(&logs, nil)), func(string, string) { close(up) })
	}()
	select {
	case <-up:
	case err := <-ran:
		t.Fatalf("Run ended early: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the node did not come up")
	}
	if !strings.Contains(logs.String(), "adopted the previous process's journal") || !strings.Contains(logs.String(), "deltas=1") {
		t.Fatalf("the segments were not adopted:\n%s", logs.String())
	}
	eventually(t, "the old segments to expire", func() bool { d, _ := r.segments(); return d == 0 })
	cancel()
	if err := <-ran; err != nil {
		t.Fatal(err)
	}
	if got := r.dumpBlob("wallet/1"); got != want {
		t.Fatal("the shard restores differently")
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// putSegment writes a stand-in for a journal segment (a node's journal is only
// its writer's business, so a test can name any number).
func (r *mrig) putSegment(node string, seq uint64, body string) string {
	r.t.Helper()
	key := segment.Key(node, seq)
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

// The sweeper deletes the node's non-empty segments older than the expiry age and
// nothing else: fences, recent segments and other nodes' journals stay.
func TestSweepDeletesOldSegmentsKeepsFencesAndRecent(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	old := r.putSegment("n1", 1001, "old")
	fence := r.putSegment("n1", 1002, "")
	r.putSegment("n1", 1003, "recent")
	foreign := r.putSegment("other", 1, "someone else's")
	lease := "nodes/n1"

	// With a fake now: the first two are older than the age, the third is not.
	// (Their real modification times are all about now.)
	realNow := time.Now()
	age := time.Hour
	deleted, err := r.n.sweepJournal(ctx, age, realNow.Add(30*time.Minute))
	if err != nil || deleted != 0 {
		t.Fatalf("nothing is old yet: deleted %d, %v", deleted, err)
	}
	deleted, err = r.n.sweepJournal(ctx, age, realNow.Add(2*time.Hour))
	if err != nil || deleted != 3-1 {
		t.Fatalf("deleted %d, %v, want the two non-empty segments", deleted, err)
	}
	if r.exists(old) || r.exists(segment.Key("n1", 1003)) {
		t.Fatal("old segments are still there")
	}
	if !r.exists(fence) {
		t.Fatal("a fence was deleted")
	}
	if !r.exists(foreign) {
		t.Fatal("another node's journal was swept")
	}
	if !r.exists(lease) {
		t.Fatal("the lease was swept")
	}
	// Nothing left to delete.
	if deleted, err := r.n.sweepJournal(ctx, age, realNow.Add(3*time.Hour)); err != nil || deleted != 0 {
		t.Fatalf("second sweep: %d %v", deleted, err)
	}
}

// The sweeper works through List and Delete alone: no Get, Head, Create or Update.
func TestSweeperUsesOnlyListAndDelete(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	r.putSegment("n1", 2001, "a")
	r.putSegment("n1", 2002, "b")
	r.putSegment("n1", 2003, "")

	var mu sync.Mutex
	ops := map[blob.Op]int{}
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && strings.HasPrefix(key, "journal/") {
			mu.Lock()
			ops[op]++
			mu.Unlock()
		}
		return nil
	})
	deleted, err := r.n.sweepJournal(ctx, time.Minute, time.Now().Add(time.Hour))
	if err != nil || deleted != 2 {
		t.Fatalf("deleted %d, %v", deleted, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if ops[blob.OpList] == 0 || ops[blob.OpDelete] != 2 || len(ops) != 2 {
		t.Fatalf("the sweeper made these calls: %v, want only list (once or more) and two deletes", ops)
	}
}

// The timer-driven sweeper deletes, can't be started twice, and Close joins it.
func TestJournalSweeperRunsAndCloseJoins(t *testing.T) {
	r := newMRig(t)
	key := r.putSegment("n1", 3001, "seg")
	if err := r.n.StartJournalSweeper(300 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := r.n.StartJournalSweeper(time.Second); err == nil {
		t.Fatal("started twice")
	}
	eventually(t, "the segment to expire", func() bool { return !r.exists(key) })
	if err := r.n.Close(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	// Joined: a segment written now is not swept, whatever its age.
	r.local.Create(context.Background(), key, strings.NewReader("seg"), blob.WriteID{})
	time.Sleep(700 * time.Millisecond)
	if !r.exists(key) {
		t.Fatal("the sweeper ran after Close")
	}
	if err := r.n.StartJournalSweeper(0); err == nil {
		t.Fatal("a zero expiry was accepted")
	}
}
