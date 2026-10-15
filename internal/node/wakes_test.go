package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
)

// --- ids ---

// A message sent after the newest one was consumed gets a new inbox id, so a
// handler that tracks ids sees both.
func TestInboxIdsAreNeverReused(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	r.addMessage("users/i1", "users/i2", "/x", "first")
	eventually(t, "the first delivery", func() bool { return rec.count() == 1 })
	eventually(t, "the row to go", func() bool { return r.liveRows("users/i1") == 0 })
	r.addMessage("users/i1", "users/i2", "/x", "second")
	eventually(t, "the second delivery", func() bool { return rec.count() == 2 })
	eventually(t, "the first inbox row to go", func() bool { return r.inboxRows("users/i2") == 0 })
	got := rec.all()
	if got[0].id() != "1" || got[1].id() != "2" {
		t.Fatalf("inbox ids %q and %q", got[0].id(), got[1].id())
	}
}

func TestNeverWrittenWithAnAutoincrementSequence(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	// A message that came and went leaves a row in sqlite_sequence.
	r.sql("users/s1", `DROP TABLE kv; DROP TABLE acks; BEGIN; INSERT INTO _shardlite_outbox (target, path) VALUES ('users/x', '/x'); DELETE FROM _shardlite_outbox; COMMIT`)
	if n := r.count("users/s1", `SELECT count(*) FROM sqlite_sequence`); n != 1 {
		t.Fatalf("sqlite_sequence has %d rows", n)
	}
	r.closeInApp("users/s1")
	eventually(t, "deactivation", r.inactive("users/s1"))
	if _, err := r.rawManifest("users/s1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a shard with only reserved tables and a sequence was kept: %v", err)
	}
	// The app's own AUTOINCREMENT table is the app's data.
	r.sql("users/s2", `DROP TABLE kv; DROP TABLE acks; CREATE TABLE mine (id INTEGER PRIMARY KEY AUTOINCREMENT)`)
	r.closeInApp("users/s2")
	eventually(t, "deactivation", func() bool { return r.cached("users/s2") })
}

// --- wake hints and the drain ---

func (r *srig) wakes() []string {
	r.t.Helper()
	var keys []string
	for info, err := range r.n.Store.List(context.Background(), "wakes/") {
		if err != nil {
			r.t.Fatal(err)
		}
		keys = append(keys, info.Key)
	}
	return keys
}

// shutdown is what Run does: drain, then let go of every shard and give up the lease.
func (r *srig) shutdown(appHandles ...string) {
	r.t.Helper()
	ctx := context.Background()
	r.m.Drain(ctx)
	for _, s := range appHandles {
		r.closeInApp(s)
	}
	if err := r.mrig.stop(); err != nil {
		r.t.Fatal(err)
	}
	r.stopJournal()
	if err := r.n.Keeper.Release(ctx); err != nil {
		r.t.Fatal(err)
	}
}

func (r *srig) restart(mutate func(*ShardsOptions)) {
	r.t.Helper()
	r.startNode()
	r.startShards(mutate)
	r.startServer()
}

func drainFor(d time.Duration) func(*ShardsOptions) {
	return func(o *ShardsOptions) { o.Drain, o.Wakes.Every = d, 50*time.Millisecond }
}

func TestDrainDeliversBeforeLettingGo(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, drainFor(5*time.Second)))
	rec := &recorder{}
	r.record(rec, "")
	inner := *r.handler.Load()
	// The first receiver sends a message of its own, to a third shard on this node.
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == "/hop" && q.Header.Get("Shardlite-Target") == "users/d2" {
			r.sql("users/d2", `INSERT INTO _shardlite_outbox (target, path) VALUES ('users/d3', '/end')`)
		}
		inner(w, q)
	})
	r.addMessage("users/d1", "users/d2", "/hop", "")
	r.shutdown("users/d1", "users/d2", "users/d3")
	got := rec.all()
	if len(got) != 2 || got[0].path != "/hop" || got[1].path != "/end" {
		t.Fatalf("deliveries during the drain: %+v", got)
	}
	if keys := r.wakes(); len(keys) != 0 {
		t.Fatalf("hints after a drain that delivered everything: %v", keys)
	}
}

func TestShutdownWithAFailingReceiverLeavesAHintThatWakesTheSender(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		deleteData bool
	}{{"same machine", false}, {"new machine", true}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newSRigWith(t, both(quick, drainFor(300*time.Millisecond)))
			var up atomic.Bool
			rec := &recorder{}
			r.record(rec, "")
			attempts := r.intakeDownUntil(&up)
			r.addMessage("users/w1", "users/w2", "/late", "payload")
			eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
			r.shutdown("users/w1")
			keys := r.wakes()
			if len(keys) != 1 || !strings.HasPrefix(keys[0], "wakes/") || !strings.HasSuffix(keys[0], "Z/users/w1") {
				t.Fatalf("hints %v, want one for users/w1", keys)
			}
			if tc.deleteData {
				if err := os.RemoveAll(r.data); err != nil {
					t.Fatal(err)
				}
			}
			up.Store(true)
			r.restart(both(quick, drainFor(300*time.Millisecond)))
			eventually(t, "the message to be delivered after the restart, with no request for the sender", func() bool {
				return rec.count() == 1 && rec.all()[0].body == "payload"
			})
			eventually(t, "the hint to go", func() bool { return len(r.wakes()) == 0 })
			if r.liveRows("users/w1") != 0 {
				t.Fatal("the row is still live")
			}
		})
	}
}

func TestHintsThatMeanNothingAreDeleted(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	ctx := context.Background()
	now := time.Now().UTC().Format(wakeLayout)
	future := "wakes/2999-01-01T00:00Z/users/future"
	for _, k := range []string{"wakes/" + now + "/users/ghost", "wakes/" + now + "/nokind/x", "wakes/" + now + "/lobby/e1", "wakes/" + now + "/users/bad id", "wakes/0000-garbage", future} {
		if _, err := r.n.Store.Create(ctx, k, strings.NewReader("")); err != nil {
			t.Fatal(err)
		}
	}
	r.m.wakes.scan(ctx)
	if keys := r.wakes(); len(keys) != 1 || keys[0] != future {
		t.Fatalf("hints left: %v, want only the one in the future", keys)
	}
	for _, s := range []string{"users/ghost", "lobby/e1"} {
		if _, err := r.rawManifest(s); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("a hint created %s: %v", s, err)
		}
	}
	if len(r.m.Active()) != 0 {
		t.Fatalf("shards were activated: %v", r.m.Active())
	}
}

func TestCrashAfterTheWakeKeepsTheHint(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, drainFor(200*time.Millisecond)))
	var up atomic.Bool
	rec := &recorder{}
	r.record(rec, "")
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/c1", "users/c2", "/c", "x")
	eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
	r.shutdown("users/c1")
	if len(r.wakes()) != 1 {
		t.Fatalf("hints %v", r.wakes())
	}

	// Woken, but the receiver still fails, and the node crashes.
	before := attempts.Load()
	r.restart(both(quick, drainFor(200*time.Millisecond)))
	eventually(t, "the wake to start deliveries", func() bool { return attempts.Load() > before })
	r.crash()
	r.stopJournal()
	if len(r.wakes()) != 1 {
		t.Fatalf("the hint is gone after a crash: %v", r.wakes())
	}

	// Woken again.
	up.Store(true)
	r.restart(both(quick, drainFor(200*time.Millisecond)))
	eventually(t, "delivery after the second wake", func() bool { return rec.count() == 1 })
	eventually(t, "the hint to go", func() bool { return len(r.wakes()) == 0 })
}

func TestDuplicateHintsDeliverOnce(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, drainFor(200*time.Millisecond)))
	var up atomic.Bool
	rec := &recorder{}
	r.record(rec, "")
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/p1", "users/p2", "/once", "x")
	eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
	r.shutdown("users/p1")
	earlier := "wakes/" + time.Now().UTC().Add(-3*time.Minute).Format(wakeLayout) + "/users/p1"
	if _, err := r.n.Store.Create(context.Background(), earlier, strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
	if len(r.wakes()) != 2 {
		t.Fatalf("hints %v", r.wakes())
	}
	up.Store(true)
	r.restart(both(quick, drainFor(200*time.Millisecond)))
	eventually(t, "the hints to go", func() bool { return len(r.wakes()) == 0 })
	eventually(t, "the inbox row to be consumed", func() bool { return r.inboxRows("users/p2") == 0 })
	r.settle("users/p2")
	if n := rec.count(); n != 1 {
		t.Fatalf("%d deliveries after two hints, want 1 (%s)", n, fmt.Sprint(rec.all()))
	}
}

// A look that can't tell whether a shard has live rows counts as live: here the
// shard is busy and its owner removed, as during a release that won't go
// through (the app has the file open), and its row is still there. The drain
// used to count that as nothing live and end with the row undelivered.
func TestDrainCountsALookThatCannotTellAsLive(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	var up atomic.Bool
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/dl1", "users/dl2", "/x", "")
	eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
	a := r.shard("users/dl1")
	inside, proceed := make(chan struct{}), make(chan struct{})
	held := make(chan bool, 1)
	go func() {
		ok, _ := do(context.Background(), a.ops, a.ended, func() bool {
			if !a.e.close() {
				return false
			}
			defer a.e.reopen()
			defer a.addOwner()
			if err := a.removeOwner(); err != nil {
				return false
			}
			close(inside)
			<-proceed
			return true
		})
		held <- ok
	}()
	select {
	case <-inside:
	case ok := <-held:
		t.Fatalf("could not hold the shard busy (%v)", ok)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	live := r.m.anyLiveMessages(ctx)
	cancel()
	close(proceed)
	if !<-held {
		t.Fatal("the shard was not held busy")
	}
	if !live {
		t.Fatal("the drain counted a busy shard with a live row as having nothing live")
	}
	up.Store(true)
	eventually(t, "nothing live once the row is delivered", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return !r.m.anyLiveMessages(ctx)
	})
}
