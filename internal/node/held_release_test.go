package node

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/kind"
)

// A release that did not go through (the app still has the shard open) leaves
// the shard usable: the node can still query it through capture's connection.
func TestShardIsUsableAfterAReleaseThatDidNotGoThrough(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("slow/g1")
	r.write(ref, "a") // the app keeps its connection open
	ref.Release()
	a := r.shard("slow/g1")
	for i := range 3 {
		if ok, err := a.deactivateNow(context.Background()); ok || err != nil {
			t.Fatalf("deactivate %d: %v %v", i, ok, err)
		}
		if err := a.useDB(context.Background(), func(db *sql.DB) error { return db.PingContext(context.Background()) }); err != nil {
			t.Fatalf("after %d held releases: %v", i, err)
		}
	}
}

// heldRig is a rig whose clock the test moves and whose log it can read, with a shard the app has written to and keeps open. The rig's hold off is 100 ms and the users kind's idle timeout 300 ms, so the looks at a shard the app holds come 100 ms, 200 ms and then 300 ms apart (the wait doubles up to the idle timeout), and a hold has outlasted an idle timeout at its third look.
type heldRig struct {
	*mrig
	clk  *fakeClock
	logs *syncBuffer
}

const heldWarning = "kept the shard's file open past its idle timeout"

func newHeldRig(t *testing.T) *heldRig {
	t.Helper()
	clk, logs := newFakeClock(), &syncBuffer{}
	return &heldRig{newMRigWith(t, func(o *ShardsetOptions) { withClock(clk)(o); logTo(logs)(o) }), clk, logs}
}

// held makes a shard the app has written to and keeps open, unused by any request, and returns the path of its file.
func (r *heldRig) held(target string) (path string) {
	r.t.Helper()
	ref := r.acquire(target)
	r.write(ref, "a") // the app keeps its connection open
	path = ref.Path()
	ref.Release()
	return path
}

// found moves the clock until the idle timer has looked at the shard after its idle timeout and found the app holding the file. The hold is counted from here.
func (r *heldRig) found(target string) {
	r.t.Helper()
	r.pass(target, r.shard(target).idleTimeout()+time.Millisecond)
}

// pass moves the clock by d and waits for the idle timer's look that it causes, so that nothing the test does next races it.
func (r *heldRig) pass(target string, d time.Duration) {
	r.t.Helper()
	a := r.shard(target)
	looks := a.looks.idle.Load()
	r.clk.Advance(d)
	waitCounter(r.t, "a look of the idle timer", &a.looks.idle, looks+1)
}

// next is how long after the clock the idle timer looks at the shard again.
func (r *heldRig) next(target string) time.Duration { return r.delay(target).Sub(r.clk.Now()) }

// stays checks that the shard is still active, owned by this node and with its file where it was.
func (r *heldRig) stays(target, path string) {
	r.t.Helper()
	if !r.m.IsActive(shardName(target)) {
		r.t.Fatal("the shard was let go of")
	}
	if man, err := r.manifest(target); err != nil || man.Owner != "n1" {
		r.t.Fatalf("the manifest of a shard that stays: %+v %v", man, err)
	}
	if _, err := os.Stat(path); err != nil {
		r.t.Fatalf("the file the app has open: %v", err)
	}
}

// unowned waits until the shard is off the node and its manifest names nobody.
func (r *heldRig) unowned(target string) {
	r.t.Helper()
	advanceUntil(r.t, r.clk, 100*time.Millisecond, "the shard to be let go of", func() bool { return !r.m.IsActive(shardName(target)) })
	if man, err := r.manifest(target); err != nil || man.Owner != "" || man.Journal != "" {
		r.t.Fatalf("the manifest of a shard let go of: %+v %v", man, err)
	}
}

// warnings is how many times the held shard's warning has been logged.
func (r *heldRig) warnings() int { return strings.Count(r.logs.String(), heldWarning) }

// A shard only the app's open handle keeps (no request holds it) stays active past its idle timeout, however long the app keeps the file, and nothing deletes the file. The app's commits are still captured and durable.
func TestHeldShardStaysActiveAndItsCommitsAreCaptured(t *testing.T) {
	t.Parallel()
	r := newHeldRig(t)
	const target = "users/h1"
	path := r.held(target)

	r.found(target)
	for _, d := range []time.Duration{100, 200, 300, 300, 300} {
		r.pass(target, d*time.Millisecond)
		r.stays(target, path)
	}

	ref := r.acquire(target)
	r.app.Must(t, "insert", "a", "1", "100")
	if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob(target) != r.liveDump(ref.Path()) {
		t.Fatal("not captured after the failed releases")
	}
	ref.Release()
	if n := r.m.Stats.LetGoHeld.Load(); n != 0 {
		t.Fatalf("%d shards let go of over an open file, want none", n)
	}
}

// The looks at a held shard back off: the first waits the hold off, each next one twice as long, up to the kind's idle timeout.
func TestLooksAtAHeldShardBackOffToTheIdleTimeout(t *testing.T) {
	t.Parallel()
	r := newHeldRig(t)
	const target = "users/h2"
	path := r.held(target)

	r.found(target)
	for _, want := range []time.Duration{100, 200, 300, 300, 300} {
		want *= time.Millisecond
		if got := r.next(target); got != want {
			t.Fatalf("the next look is %v away, want %v", got, want)
		}
		r.pass(target, want)
		r.stays(target, path)
	}
}

// A hold that lasts an idle timeout from the first look that found it is warned of and counted once, however long it goes on. Before that it is neither.
func TestHeldShardIsWarnedAndCountedOnceAfterAnotherIdleTimeout(t *testing.T) {
	t.Parallel()
	r := newHeldRig(t)
	const target = "users/h3"
	r.held(target)

	r.found(target)
	r.pass(target, 100*time.Millisecond) // 100 ms in
	if n := r.m.Stats.HeldPastIdle.Load(); n != 0 || r.warnings() != 0 {
		t.Fatalf("warned within the idle timeout: %d counted\n%s", n, r.logs.String())
	}
	r.pass(target, 200*time.Millisecond) // 300 ms in, which is one idle timeout
	if n := r.m.Stats.HeldPastIdle.Load(); n != 1 || r.warnings() != 1 {
		t.Fatalf("after the idle timeout: %d counted, %d warnings\n%s", n, r.warnings(), r.logs.String())
	}
	if logs := r.logs.String(); !strings.Contains(logs, target) || !strings.Contains(logs, "stays active until the app closes it") {
		t.Fatalf("the warning does not name the shard and say what happens:\n%s", logs)
	}
	for range 4 {
		r.pass(target, 300*time.Millisecond)
	}
	if n := r.m.Stats.HeldPastIdle.Load(); n != 1 || r.warnings() != 1 {
		t.Fatalf("the same hold again: %d counted, %d warnings", n, r.warnings())
	}
	if n := r.m.Stats.LetGoHeld.Load(); n != 0 {
		t.Fatalf("%d shards let go of over an open file, want none", n)
	}
}

// An app that closes the file within the idle timeout of the first look (a pool that closes late) is neither warned nor counted, and the next look lets the shard go.
func TestFileClosedWithinAnIdleTimeoutIsNeitherWarnedNorCounted(t *testing.T) {
	t.Parallel()
	r := newHeldRig(t)
	const target = "users/h4"
	path := r.held(target)

	r.found(target)
	r.pass(target, 100*time.Millisecond)
	r.stays(target, path)
	r.app.Must(t, "close", "a")
	r.unowned(target)
	if n := r.m.Stats.HeldPastIdle.Load(); n != 0 || r.warnings() != 0 {
		t.Fatalf("%d counted, %d warnings\n%s", n, r.warnings(), r.logs.String())
	}
}

// Once the app closes the file of a shard that has been held for long, the next look lets the shard go: released, its files deleted, and nothing lost.
func TestShardIsLetGoOfAtTheNextLookOnceTheAppClosesTheFile(t *testing.T) {
	t.Parallel()
	r := newHeldRig(t)
	const target = "users/h5"
	path := r.held(target)

	r.found(target)
	for range 5 {
		r.pass(target, r.next(target))
	}
	r.stays(target, path)
	if n := r.m.Stats.HeldPastIdle.Load(); n != 1 {
		t.Fatalf("%d holds counted, want 1", n)
	}
	r.app.Must(t, "close", "a")
	r.pass(target, r.next(target)) // the very next look
	if r.m.IsActive(shardName(target)) {
		t.Fatal("the shard stayed active after the app closed the file")
	}
	r.releasedFrom(target, path)
	if n := r.m.Stats.LetGoHeld.Load(); n != 0 {
		t.Fatalf("%d shards let go of over an open file, want none", n)
	}

	ref := r.acquire(target)
	defer ref.Release()
	if got := strings.Count(r.liveDump(ref.Path()), "kv: "); got != 2 {
		t.Fatalf("%d rows after the claim, want the 2 of one write", got)
	}
}

// A request that uses the shard starts the hold over: the looks back off from the hold off again, and a hold that outlasts an idle timeout once more is a new one, warned of and counted again.
func TestRequestUsingAHeldShardStartsItsHoldOver(t *testing.T) {
	t.Parallel()
	r := newHeldRig(t)
	const target = "users/h6"
	path := r.held(target)

	r.found(target)
	r.pass(target, 100*time.Millisecond)
	r.pass(target, 200*time.Millisecond)
	if n := r.m.Stats.HeldPastIdle.Load(); n != 1 {
		t.Fatalf("%d holds counted, want 1", n)
	}
	r.acquire(target).Release() // used again
	r.pass(target, 300*time.Millisecond)
	if got := r.next(target); got != 100*time.Millisecond {
		t.Fatalf("the next look after a request is %v away, want the hold off", got)
	}
	r.pass(target, 100*time.Millisecond)
	if n := r.m.Stats.HeldPastIdle.Load(); n != 1 {
		t.Fatalf("%d holds counted within the new hold, want 1", n)
	}
	r.pass(target, 200*time.Millisecond)
	if n := r.m.Stats.HeldPastIdle.Load(); n != 2 || r.warnings() != 2 {
		t.Fatalf("%d holds counted, %d warnings, want 2 of each", n, r.warnings())
	}
	r.stays(target, path)
}

// A shard a request holds is not looked at as held by the app, whatever the idle timeout: it is not looked at at all until the request ends.
func TestShardHeldByARequestIsNeverLetGoOfEarly(t *testing.T) {
	t.Parallel()
	r := newHeldRig(t)
	const target = "users/h7"
	ref := r.acquire(target)
	r.write(ref, "a")
	a := r.shard(target)
	for range 3 {
		looks := a.looks.idle.Load()
		r.clk.Advance(2 * a.idleTimeout())
		waitCounter(t, "a look of the idle timer", &a.looks.idle, looks+1)
	}
	if !r.m.IsActive(shardName(target)) || !r.delay(target).IsZero() || r.m.Stats.HeldPastIdle.Load() != 0 {
		t.Fatal("a shard a request holds was looked at as held by the app")
	}

	ref.Release()
	r.found(target)
	if got := r.next(target); got != 100*time.Millisecond {
		t.Fatalf("the first look after the request is %v away: the time the request held the shard counted", got)
	}
	r.app.Must(t, "close", "a")
	r.unowned(target)
}

// A stop goes ahead over a file the app keeps open, within its deadline: the shard is released, the file deleted and the stop counts it. It is the only path that does, so the held shards' counter stays at zero.
func TestStopGoesAheadOverAFileTheAppHolds(t *testing.T) {
	t.Parallel()
	logs := &syncBuffer{}
	r := newMRigWith(t, logTo(logs))
	const target = "slow/h8"
	ref := r.acquire(target)
	r.write(ref, "a") // the app keeps its connection open
	path := ref.Path()
	ref.Release()

	if err := r.stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if n := r.m.Stats.LetGoHeld.Load(); n != 1 {
		t.Fatalf("%d shards let go of over an open file, want 1", n)
	}
	if n := r.m.Stats.HeldPastIdle.Load(); n != 0 {
		t.Fatalf("%d held shards counted at a stop, want none", n)
	}
	if !strings.Contains(logs.String(), "when the node stopped") {
		t.Fatalf("no warning for the shard the stop let go of:\n%s", logs.String())
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the file the app has open is still there: %v", err)
	}
	man, err := r.manifest(target)
	if err != nil || man.Owner != "" || man.Journal != "" {
		t.Fatalf("the manifest after the stop: %+v %v", man, err)
	}
}

// The next claim of a shard that was let go of gets a file of its own (every new file does). The old file was deleted at the let-go, so an app that opens the old path without create (as an app must) fails loudly instead of making a new empty file there.
func TestClaimAfterALetGoUsesANewFile(t *testing.T) {
	t.Parallel()
	r := newHeldRig(t)
	const target = "users/h9"
	old := r.held(target)
	r.found(target)
	r.app.Must(t, "close", "a")
	r.unowned(target)

	if _, err := r.app.Do("open", "b", old); err == nil {
		t.Fatal("the app opened the path of a shard let go of without creating it")
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the failed open made the old path again: %v", err)
	}
	ref := r.acquire(target)
	defer ref.Release()
	if ref.Path() == old {
		t.Fatal("the claim reused the path of the file the app had open")
	}
	if got := strings.Count(r.liveDump(ref.Path()), "kv: "); got != 2 {
		t.Fatalf("%d rows in the new file, want the 2 of one write", got)
	}
	if _, err := r.app.Do("open", "c", ref.Path()); err != nil {
		t.Fatalf("the app could not open the new file: %v", err)
	}
}

// A deactivation deletes the shard's files at once, and nothing is counted as let go of over an open file when the app had closed it.
func TestDeactivationDeletesTheFilesAtOnce(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("slow/h10")
	r.write(ref, "a")
	path := ref.Path()
	r.app.Must(t, "close", "a")
	ref.Release()
	r.release("slow/h10")
	r.releasedFrom("slow/h10", path)
	if n := r.m.Stats.LetGoHeld.Load(); n != 0 {
		t.Fatalf("%d lets go over an open file, want none", n)
	}
}
