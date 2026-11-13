package node

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/tempfile"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/blobtest"
	"github.com/luci1900/shardlite/internal/test/harness"
	"github.com/luci1900/shardlite/internal/test/sqlitetest"
)

const testKinds = `{"kinds": {
	"users":  {"public": true, "sync": "none", "idle_timeout": "300ms"},
	"wallet": {"public": true, "idle_timeout": "300ms"},
	"slow":   {"public": true, "sync": "none", "idle_timeout": "1h"},
	"audit":  {},
	"lobby":  {"public": true, "idle_timeout": "300ms"},
	"arena":  {"public": true, "sync": "none", "idle_timeout": "300ms"}
}}`

type mrig struct {
	t           *testing.T
	stopJournal func()           // stops the writer and the keeper of the current node
	local       *blobtest.Hooked // the bucket, behind its fault hook
	skew        atomic.Int64     // nanoseconds the bucket's clock runs behind, to age objects
	leaseAhead  atomic.Int64     // nanoseconds the keeper's clock runs ahead, to expire the lease
	data        string
	kinds       *Kinds
	app         *apptest.App
	logger      *slog.Logger  // the node's logger, the default if nil
	keeperEvery time.Duration // the keeper's renewal interval, 100 ms if zero
	n           *Node
	m           *Shardset
	stopM       func() error // stops the shards' Serve and returns what it returned

	// newRouting makes the shards' wake of the app and calls (the server's,
	// for the rigs that have one). The default reaches nobody.
	newRouting func(*Shardset) routing

	cmu    sync.Mutex
	counts map[string]int
}

func newMRig(t *testing.T) *mrig { return newMRigWith(t, nil) }

func newMRigWith(t *testing.T, mutate func(*ShardsetOptions)) *mrig { return newMRigOn(t, mutate, nil) }

// newMRigOn is newMRigWith over the given backend (in memory if nil).
func newMRigOn(t *testing.T, mutate func(*ShardsetOptions), backend blob.Backend) *mrig {
	t.Helper()
	r := newUnstartedMRig(t, backend)
	r.startNode()
	r.startShardset(mutate)
	return r
}

// newUnstartedMRig is the rig with no node and no shards yet.
func newUnstartedMRig(t *testing.T, backend blob.Backend) *mrig {
	t.Helper()
	r := &mrig{t: t, data: filepath.Join(t.TempDir(), "data"), app: apptest.Start(t), counts: map[string]int{}}
	if backend == nil {
		backend = blob.NewMemory(blob.WithClock(func() time.Time { return time.Now().Add(-time.Duration(r.skew.Load())) }))
	}
	r.local = blobtest.WithHook(backend)
	a, err := kind.ParseApp([]byte(testKinds), 0)
	if err != nil {
		t.Fatal(err)
	}
	r.kinds = NewKinds(0)
	r.kinds.Update(a)
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after {
			r.cmu.Lock()
			r.counts[string(op)+" "+key]++
			r.cmu.Unlock()
		}
		return nil
	})
	return r
}

func (r *mrig) startNode() {
	r.t.Helper()
	n, err := r.tryStartNode()
	if err != nil {
		r.t.Fatal(err)
	}
	r.n = n
	r.stopJournal = serveJournal(r.t, n)
}

// tryStartNode is startNode that reports a failed start (an adoption that failed).
func (r *mrig) tryStartNode() (*Node, error) {
	return Start(context.Background(), Config{
		Name: "n1", Backend: r.local, Data: r.data, TempDir: filepath.Join(r.data, tempDirName),
		Blob:  blob.Options{Backoff: time.Millisecond},
		Lease: lease.RegistryOptions{TTL: time.Hour, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: lease.KeeperOptions{Interval: cmp.Or(r.keeperEvery, 100*time.Millisecond), Margin: time.Minute,
			Now: func() time.Time { return time.Now().Add(time.Duration(r.leaseAhead.Load())) }},
		Logger: r.logger,
	})
}

func (r *mrig) startShardset(mutate func(*ShardsetOptions)) {
	r.t.Helper()
	o := ShardsetOptions{Kinds: r.kinds, Data: r.data,
		Flush:     FlushOptions{Interval: 20 * time.Millisecond},
		Lifecycle: LifecycleOptions{SnapshotEvery: 20 * time.Millisecond, HoldOff: 100 * time.Millisecond, IdleRecheck: 20 * time.Millisecond}}
	if mutate != nil {
		mutate(&o)
	}
	newRouting := r.newRouting
	if newRouting == nil {
		newRouting = func(*Shardset) routing {
			unserved := errors.New("the node is not serving")
			return routing{
				wake: func(context.Context, *actor) (int, error) { return 0, unserved },
				call: func(*http.Request) (*http.Response, error) { return nil, unserved },
			}
		}
	}
	m, err := NewShardset(r.n, o, newRouting)
	if err != nil {
		r.t.Fatal(err)
	}
	r.m = m
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	var g errgroup.Group
	g.Go(func() error {
		served <- m.Serve(ctx)
		return nil
	})
	var once sync.Once
	var err2 error
	r.stopM = func() error {
		once.Do(func() {
			cancel()
			g.Wait()
			err2 = <-served
		})
		return err2
	}
	// At the end of the test the shards stop as a crash would: letting go of
	// every shard would only slow the test down.
	stop := r.stopM
	r.t.Cleanup(func() { m.abandon.Store(true); stop() })
}

// stop stops the shards in order, letting go of every one, and returns what
// Serve returned.
func (r *mrig) stop() error { return r.stopM() }

// crash stops the shards without letting go of anything, as a SIGKILL would
// (the old writer still exists, as a zombie, and so do the captures).
func (r *mrig) crash() {
	r.m.abandon.Store(true)
	r.stopM()
}

func (r *mrig) calls(op, prefix string) int {
	r.cmu.Lock()
	defer r.cmu.Unlock()
	n := 0
	for k, c := range r.counts {
		if strings.HasPrefix(k, op+" "+prefix) {
			n += c
		}
	}
	return n
}

// create creates shards as an app's call would: clients can only reach shards
// that exist.
func (r *mrig) create(targets ...string) {
	r.t.Helper()
	for _, target := range targets {
		r.acquire(target).Release()
	}
}

func (r *mrig) acquire(target string) *Hold {
	r.t.Helper()
	ref, err := r.m.Acquire(context.Background(), shardName(target))
	if err != nil {
		r.t.Fatalf("acquire %s: %v", target, err)
	}
	return ref
}

// write has the app open the shard and commit to it.
func (r *mrig) write(ref *Hold, id string) {
	r.t.Helper()
	if _, err := r.app.Do("open", id, ref.Path()); err != nil {
		if !strings.Contains(err.Error(), "already") {
			r.t.Fatal(err)
		}
	}
	r.app.Must(r.t, "insert", id, "2", "100")
}

func (r *mrig) dumpBlob(target string) string {
	r.t.Helper()
	dst := filepath.Join(r.t.TempDir(), "restored.db")
	if _, err := r.n.Files.Restore(context.Background(), shardName(target), dst, shardfile.RestoreOptions{}); err != nil {
		r.t.Fatalf("restore %s: %v", target, err)
	}
	return sqlitetest.Dump(r.t, dst)
}

// liveDump reads a captured file from the app's process. The test's own
// process hosts capture, and closing a descriptor on the file here would
// drop the locks capture holds on it.
func (r *mrig) liveDump(path string) string {
	r.t.Helper()
	q := r.app.Must(r.t, "dump", "-", path)
	s, err := strconv.Unquote(q)
	if err != nil {
		r.t.Fatalf("dump: %v (%q)", err, q)
	}
	return s
}

// header reads a capture's wal-index header, asking again while the capture's
// loop is busy (the messages owner or the flush cycle may be using it).
func header(t *testing.T, sh *capture.Shard) capture.Header {
	t.Helper()
	h, err := sh.Header()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// afterLooks returns once the idle timer of each shard named has looked at it n
// times since the call (the first may have begun before it).
func (r *mrig) afterLooks(n uint64, targets ...string) {
	r.t.Helper()
	for _, target := range targets {
		a := r.shard(target)
		waitCounter(r.t, "looks of the idle timer at "+target, &a.looks.idle, a.looks.idle.Load()+n+1)
	}
}

// afterSnapshotLooks returns once the delta timer of each shard named has looked
// at it n times since the call (the first may have begun before it).
func (r *mrig) afterSnapshotLooks(n uint64, targets ...string) {
	r.t.Helper()
	for _, target := range targets {
		a := r.shard(target)
		waitCounter(r.t, "looks of the delta timer at "+target, &a.looks.snap, a.looks.snap.Load()+n+1)
	}
}

// refs is how many holds a shard has (-1 if it is not active).
func (r *mrig) refs(target string) int {
	e := r.m.tab.load(shardName(target))
	if e == nil || e.a == nil || !e.is(entryActive, entryBusy) {
		return -1
	}
	n, _ := e.use()
	return n
}

// shard is the actor of an active shard.
func (r *mrig) shard(target string) *actor {
	r.t.Helper()
	e := r.m.tab.load(shardName(target))
	if e == nil || e.a == nil {
		r.t.Fatalf("%s is not active", target)
	}
	return e.a
}

func waitCounter(t *testing.T, what string, c *notify.Counter, n uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Wait(ctx, n); err != nil {
		t.Fatalf("waiting for %s: %v", what, err)
	}
}

// afterCycle returns once a flush cycle that began after the call has completed
// (the node has read, captured and delivered what was committed before it).
func (r *mrig) afterCycle() {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.m.Flusher().WaitCycle(ctx); err != nil {
		r.t.Fatalf("waiting for a flush cycle: %v", err)
	}
}

// advanceUntil moves the clock by step each time it looks, until cond holds:
// the way to wait for something that only time brings (an idle shard going).
func advanceUntil(t *testing.T, clk *harness.FakeClock, step time.Duration, what string, cond func() bool) {
	t.Helper()
	apptest.Eventually(t, apptest.Timeout, what, func() bool {
		clk.Advance(step)
		return cond()
	})
}

// delay is when the idle timer next looks at the shard (zero until it has been
// held off, by messages or by the app keeping the file open).
func (r *mrig) delay(target string) time.Time {
	e := r.m.tab.load(shardName(target))
	if e == nil || e.a == nil {
		return time.Time{}
	}
	if ns := e.a.idle.nextLook.Load(); ns != 0 {
		return time.Unix(0, ns)
	}
	return time.Time{}
}

func (r *mrig) manifest(target string) (*shardfile.Manifest, error) {
	m, _, err := r.n.Files.GetManifest(context.Background(), shardName(target))
	return m, err
}

func (r *mrig) inactive(target string) func() bool {
	return func() bool { return !r.m.IsActive(shardName(target)) }
}

// released reports whether the shard has been let go of: it is off the node and its manifest names nobody.
func (r *mrig) released(target string) bool {
	if r.m.IsActive(shardName(target)) {
		return false
	}
	m, err := r.manifest(target)
	return err == nil && m.Owner == ""
}

// release lets go of the shard as an idle deactivation does, without waiting for its idle timeout, and returns once the shard is off the node. The app must not have the shard's file open.
func (r *mrig) release(target string) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), apptest.Timeout)
	defer cancel()
	if ok, err := r.shard(target).deactivateNow(ctx); !ok || err != nil {
		r.t.Fatalf("release %s: %v %v", target, ok, err)
	}
}

func TestRefusesUndeclaredAndBadTargets(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	if _, err := r.m.Acquire(ctx, shardName("typo/1")); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("a kind no app declares: %v", err)
	}
	for _, bad := range []string{"Users/1", "users", "a..b/1", "_/1"} {
		if _, err := ParseTarget(bad); !errors.Is(err, ErrBadTarget) {
			t.Fatalf("%q: %v", bad, err)
		}
	}
}

// A shard with no manifest is created when it is activated: its manifest claims
// it for this node, and its file carries the generation.
func TestCreationOnActivation(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("users/1")
	defer ref.Release()
	m, err := r.manifest("users/1")
	if err != nil {
		t.Fatalf("no manifest after activation: %v", err)
	}
	if m.Owner != "n1" || m.NodeEpoch != r.n.Keeper.Lease().Epoch || !strings.Contains(ref.Path(), "."+string(m.Generation)+".") {
		t.Fatalf("manifest %+v for path %s", m, ref.Path())
	}
	if r.m.Stats.Creates.Load() != 1 {
		t.Fatalf("%d creates", r.m.Stats.Creates.Load())
	}
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	if got, want := r.dumpBlob("users/1"), r.liveDump(ref.Path()); got != want {
		t.Fatal("what the gate released is not what blob storage restores")
	}
}

// If another node creates the shard between this node finding no manifest and
// creating one, this node claims the other's shard like any other.
func TestCreateRaceClaimsTheOther(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	raced := false
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && key == shardfile.ManifestKey(shardName("users/2")) && !after && !raced {
			raced = true
			body, _ := json.Marshal(shardfile.Manifest{Generation: "other", Files: []shardfile.File{}})
			if _, err := r.local.Create(ctx, key, bytes.NewReader(body), blob.WriteID{Node: "n2", Epoch: 1, N: 1}); err != nil {
				return err
			}
		}
		return nil
	})
	ref := r.acquire("users/2")
	defer ref.Release()
	// The winner's manifest has nothing to restore, so the shard starts over in a
	// generation of its own.
	m, err := r.manifest("users/2")
	if !raced || err != nil || m.Owner != "n1" || m.Generation == "other" || !strings.Contains(ref.Path(), "."+string(m.Generation)+".") {
		t.Fatalf("raced %v, path %s, manifest %+v: %v", raced, ref.Path(), m, err)
	}
	if r.m.Stats.Creates.Load() != 0 {
		t.Fatal("counted as created")
	}
}

func TestIdleShardThatWasOnlyReadLeavesNothing(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("users/3")
	r.app.Must(t, "openro", "a", ref.Path())
	r.app.Must(t, "close", "a")
	ref.Release()
	apptest.Eventually(t, apptest.Timeout, "idle deactivation", r.inactive("users/3"))
	if _, err := r.manifest("users/3"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v", err)
	}
	if left := leftBehind(r.data); len(left) != 0 {
		t.Fatalf("left behind: %v", left)
	}
}

// leftBehind is the files in the data directory.
func leftBehind(data string) []string {
	var files []string
	filepath.WalkDir(data, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		files = append(files, p)
		return nil
	})
	return files
}

func TestGates(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("wallet/1")
	defer ref.Release()
	// The kind names no sync, so it parses to storage; the gate below
	// names storage explicitly.
	if ref.Sync() != kind.SyncStorage {
		t.Fatal("wallet should default to storage")
	}
	r.write(ref, "a")

	// Sync on: when the gate returns, blob storage restores to the live file.
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob("wallet/1") != r.liveDump(ref.Path()) {
		t.Fatal("gate released output before the write was in blob storage")
	}

	// A shard with no new frames passes straight through.
	apptest.Eventually(t, apptest.Timeout, "the shard to be captured", func() bool { return !ref.a.cap.hasNewFrames() })
	// No flush can complete while the journal's writes are stalled, so a gate that
	// returns now did not wait for one.
	stall := make(chan struct{})
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpCreate && strings.HasPrefix(key, journal.Root) {
			select {
			case <-stall:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	quick, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := r.m.Gate(quick, ref, kind.SyncStorage); err != nil {
		t.Fatalf("an unchanged shard waited for a flush: %v", err)
	}

	// Sync off never waits, even with new frames.
	r.app.Must(t, "insert", "a", "2", "100")
	if err := r.m.Gate(quick, ref, kind.SyncNone); err != nil {
		t.Fatalf("sync off waited for a flush: %v", err)
	}
	close(stall)
	// ...but the commit is captured by the flush loop all the same.
	apptest.Eventually(t, apptest.Timeout, "the flush loop to capture the commit", func() bool { return !ref.a.cap.hasNewFrames() })
	if r.dumpBlob("wallet/1") != r.liveDump(ref.Path()) {
		t.Fatal("commit not in blob storage after the flush loop captured it")
	}

	// The barrier behind POST /_shardlite/sync.
	r.app.Must(t, "insert", "a", "2", "100")
	if err := r.m.Barrier(ctx); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob("wallet/1") != r.liveDump(ref.Path()) {
		t.Fatal("Barrier returned before the commit was in blob storage")
	}
}

func TestIdleNodeWritesNoSegments(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("slow/1")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, apptest.Timeout, "the shard to be captured", func() bool { return !ref.a.cap.hasNewFrames() })
	r.afterCycle()
	next := r.n.Writer.NextSeq()
	cycles := r.m.Flusher().Completed()
	apptest.Eventually(t, apptest.Timeout, "five more flush cycles (the flush loop is not running)", func() bool { return r.m.Flusher().Completed() >= cycles+5 })
	if r.n.Writer.NextSeq() != next {
		t.Fatalf("segments written while nothing changed: %d to %d", next, r.n.Writer.NextSeq())
	}
}

func TestOneActivationForConcurrentRequests(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	var wg errgroup.Group
	paths := make([]string, 40)
	for i := range paths {
		wg.Go(func() error {
			ref := r.acquire("slow/7")
			paths[i] = ref.Path()
			defer ref.Release()
			return nil
		})
	}
	wg.Wait()
	if n := r.m.Stats.Activations.Load(); n != 1 {
		t.Fatalf("%d activations", n)
	}
	for _, p := range paths {
		if p != paths[0] {
			t.Fatalf("paths differ: %s %s", p, paths[0])
		}
	}
}

// An idle deactivation gives the shard up in the one manifest update that
// records its last delta, and deletes its files: the manifest names no owner and
// no journal, and nothing of the shard is left on the node.
func TestDeactivationReleasesInOneUpdateAndDeletesTheFiles(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newMRigWith(t, withClock(clk))
	ctx := context.Background()
	const target = "users/4"
	ref := r.acquire(target)
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "a")
	path := ref.Path()
	ref.Release()
	if m, err := r.manifest(target); err != nil || m.Owner != "n1" {
		t.Fatalf("manifest while active: %+v %v", m, err)
	}

	manifestKey := shardfile.ManifestKey(shardName(target))
	updates := r.calls("update", manifestKey)
	advanceUntil(t, clk, 100*time.Millisecond, "idle deactivation", r.inactive(target))

	m, err := r.manifest(target)
	if err != nil || m.Owner != "" || m.Journal != "" || len(m.Files) == 0 {
		t.Fatalf("manifest after deactivation: %+v %v", m, err)
	}
	if got := r.calls("update", manifestKey) - updates; got != 1 {
		t.Fatalf("%d manifest updates for the deactivation, want the one that records the delta and releases", got)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the shard's file is still there: %v", err)
	}
	if left := leftBehind(r.data); len(left) != 0 {
		t.Fatalf("left behind: %v", left)
	}
	if r.m.Stats.Deletes.Load() != 0 {
		t.Fatal("deleted")
	}
}

// A shard that was let go of is activated again like any shard another node
// left: its manifest is read, claimed and restored into a new file.
func TestReactivationAttachesClaimsAndRestores(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	const target = "slow/5"
	ref := r.acquire(target)
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	want := r.liveDump(ref.Path())
	path := ref.Path()
	r.app.Must(t, "close", "a")
	ref.Release()
	r.release(target)

	manifestKey := shardfile.ManifestKey(shardName(target))
	gets, updates := r.calls("get", manifestKey), r.calls("update", manifestKey)
	files := r.calls("get", shardfile.FilesPrefix) + r.calls("get-range", shardfile.FilesPrefix)
	ref = r.acquire(target)
	defer ref.Release()
	if r.calls("get", manifestKey) <= gets {
		t.Fatal("the manifest was not read")
	}
	if got := r.calls("update", manifestKey) - updates; got != 1 {
		t.Fatalf("%d manifest updates, want the claim", got)
	}
	if r.calls("get", shardfile.FilesPrefix)+r.calls("get-range", shardfile.FilesPrefix) <= files {
		t.Fatal("no file was read: the shard was not restored")
	}
	if r.m.Stats.Restores.Load() != 1 || r.m.Stats.Activations.Load() != 2 {
		t.Fatalf("stats: %d restores, %d activations, want 1 and 2", r.m.Stats.Restores.Load(), r.m.Stats.Activations.Load())
	}
	if m, err := r.manifest(target); err != nil || m.Owner != "n1" {
		t.Fatalf("manifest after the claim: %+v %v", m, err)
	}
	if ref.Path() == path {
		t.Fatal("the shard was restored into the path it had before")
	}
	if got := r.liveDump(ref.Path()); got != want {
		t.Fatal("the restored shard differs")
	}
	// And it is captured again.
	r.app.Must(t, "open", "b", ref.Path())
	r.app.Must(t, "insert", "b", "2", "100")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob(target) != r.liveDump(ref.Path()) {
		t.Fatal("reactivated shard is not captured")
	}
}

// A restart starts with nothing on disk, as a new node would: the files of
// shards a crash left and the temporary files are gone, and every shard comes
// from blob storage.
func TestRestartStartsWithNothingOnDisk(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	want := map[string]string{}
	for _, id := range []string{"slow/6", "slow/7"} {
		ref := r.acquire(id)
		r.write(ref, "a")
		if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
		r.app.Must(t, "close", "a")
		ref.Release()
		r.release(id)
		want[id] = r.dumpBlob(id)
	}
	ref := r.acquire("slow/8") // active when the node crashes
	r.write(ref, "w")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "w")
	ref.Release()
	r.crash()
	r.stopJournal()
	// A file that is not a shard's, and a temporary one, go too.
	if err := os.WriteFile(filepath.Join(r.data, tempDirName, "leftover"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	r.startNode()
	r.startShardset(nil)
	if left := leftBehind(r.data); len(left) != 0 {
		t.Fatalf("files left in the data directory: %v", left)
	}
	journalReads := func() int {
		return r.calls("get", journal.Root) + r.calls("head", journal.Root) + r.calls("get-range", journal.Root)
	}
	before := journalReads()
	for _, id := range []string{"slow/6", "slow/7"} {
		ref := r.acquire(id)
		if got := r.liveDump(ref.Path()); got != want[id] {
			t.Fatalf("%s differs from blob storage", id)
		}
		ref.Release()
	}
	// A shard that was let go of names no journal, so the restores read only its files.
	if got := journalReads() - before; got != 0 {
		t.Fatalf("%d reads of the journal while restoring", got)
	}
	if r.m.Stats.Restores.Load() != 2 {
		t.Fatalf("stats: %+v", &r.m.Stats)
	}
	if m, err := r.manifest("slow/6"); err != nil || m.NodeEpoch != 2 || m.Owner != "n1" {
		t.Fatalf("manifest not claimed: %+v %v", m, err)
	}
}

// A shard file that an app still has open across a restart is deleted at the
// start like any other (the app keeps its handle on the inode, and a commit
// through it is lost, which is the app's bug), and the shard does not wait for
// anything: it activates at once at a new path, with its data.
func TestRestartDeletesAFileTheAppHasOpen(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("users/9")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	path := ref.Path()
	r.crash() // the app still has the file open
	r.stopJournal()

	r.startNode()
	r.startShardset(nil)
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the file the app had open before the restart is still there: %v", err)
	}
	ref, err := r.m.Acquire(ctx, shardName("users/9"))
	if err != nil {
		t.Fatalf("acquire after the restart: %v", err)
	}
	defer ref.Release()
	if ref.Path() == path {
		t.Fatal("the shard was activated at the path of the file the app had open")
	}
	if got := strings.Count(r.liveDump(ref.Path()), "kv: "); got != 2 {
		t.Fatalf("%d rows in the new file, want the 2 of one write", got)
	}
	if _, err := r.app.Do("open", "b", path); err == nil {
		t.Fatal("the app opened the old path again without creating it")
	}
}

// A crash leaves files for shards that were active. They may be ahead of what
// reached the journal, so they are discarded, and the shard comes back from blob storage.
func TestCrashLeavesActiveFilesToBeDiscarded(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("wallet/2")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	want := r.liveDump(ref.Path())
	r.app.Must(t, "insert", "a", "2", "100") // not gated: may be lost
	path := ref.Path()
	r.app.Must(t, "close", "a")
	r.crash()

	r.startNode()
	r.startShardset(nil)
	if r.n.Adopted.Deltas != 1 {
		t.Fatalf("adoption: %+v", r.n.Adopted)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("the active file of the crashed process was kept")
	}
	ref = r.acquire("wallet/2")
	defer ref.Release()
	if r.m.Stats.Restores.Load() != 1 {
		t.Fatalf("stats: %+v", &r.m.Stats)
	}
	got := r.liveDump(ref.Path())
	// The gated write is there. The one after it may be.
	if !strings.HasPrefix(got, want[:len(want)/2]) {
		t.Fatalf("restored shard lost the acknowledged write")
	}
}

func TestIdleTimeoutAndOpenConnections(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newMRigWith(t, withClock(clk))
	ref := r.acquire("users/8")
	r.write(ref, "a")
	r.app.Must(t, "close", "a")
	// A hold (an open connection) keeps the shard active past its 300 ms timeout:
	// the clock moves three timeouts, and the idle timer looks at each.
	a := r.shard("users/8")
	for range 3 {
		looks := a.looks.idle.Load()
		clk.Advance(300 * time.Millisecond)
		waitCounter(t, "a look of the idle timer", &a.looks.idle, looks+1)
	}
	if !r.m.IsActive(shardName("users/8")) {
		t.Fatal("deactivated while held")
	}
	ref.Release()
	advanceUntil(t, clk, 100*time.Millisecond, "deactivation after the last hold", func() bool { return r.released("users/8") })
}

func TestBackgroundDeltasAndRebases(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	r.crash()
	r.startShardset(func(o *ShardsetOptions) { o.Lifecycle.DeltaAge = 30 * time.Millisecond; o.Lifecycle.MaxFiles = 3 })
	ctx := context.Background()
	ref := r.acquire("slow/bg")
	defer ref.Release()
	r.write(ref, "a")
	maxFiles := 0
	var before []shardfile.File
	if m, err := r.manifest("slow/bg"); err == nil {
		before = m.Files
	}
	for range 12 {
		r.app.Must(t, "insert", "a", "2", "100")
		if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
		// Each commit gets a snapshot once its tail is old enough: a delta, or a rebase.
		apptest.Eventually(t, apptest.Timeout, "a snapshot", func() bool {
			m, err := r.manifest("slow/bg")
			if err != nil || slices.Equal(m.Files, before) {
				return false
			}
			maxFiles = max(maxFiles, len(m.Files))
			before = m.Files
			return true
		})
	}
	m, err := r.manifest("slow/bg")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) == 0 || len(m.Files) > 4 || maxFiles < 3 {
		t.Fatalf("files %v (max seen %d): deltas and rebases should keep a handful", m.Files, maxFiles)
	}
	// A snapshot seen above may be an earlier commit's late rebase, so the last
	// commit's own snapshot can still be on its way, and a restore can find a file
	// that a rebase has replaced since it read the manifest.
	apptest.Eventually(t, apptest.Timeout, "the blobs to restore to what is live", func() bool {
		dst := filepath.Join(t.TempDir(), "restored.db")
		if _, err := r.n.Files.Restore(ctx, shardName("slow/bg"), dst, shardfile.RestoreOptions{}); errors.Is(err, blob.ErrNotFound) {
			return false
		} else if err != nil {
			t.Fatal(err)
		}
		return sqlitetest.Dump(t, dst) == r.liveDump(ref.Path())
	})
}

// Once the lease can't be renewed the node acknowledges nothing: the barrier
// behind /_shardlite/sync fails, and so do gates.
func TestLostLeaseFailsTheBarrier(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	r.crash()
	r.stopJournal()
	n, err := Start(context.Background(), Config{
		Name: "n1", Backend: r.local, Data: r.data, TempDir: filepath.Join(r.data, tempDirName),
		Blob:   blob.Options{Backoff: time.Millisecond},
		Lease:  lease.RegistryOptions{TTL: 400 * time.Millisecond, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: lease.KeeperOptions{Interval: 50 * time.Millisecond, Margin: 100 * time.Millisecond, Logger: nil},
	})
	if err != nil {
		t.Fatal(err)
	}
	r.n = n
	serveJournal(t, n)
	r.startShardset(nil)
	ctx := context.Background()
	if err := r.m.Barrier(ctx); err != nil {
		t.Fatal(err)
	}
	var down atomic.Bool
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if down.Load() {
			return errors.New("storage down")
		}
		return nil
	})
	down.Store(true)
	apptest.Eventually(t, apptest.Timeout, "the lease to expire", func() bool { return r.n.Keeper.Check() != nil })
	if err := r.m.Barrier(ctx); !errors.Is(err, lease.ErrExpired) {
		t.Fatalf("Barrier = %v", err)
	}
	if _, err := r.m.Acquire(ctx, shardName("users/x")); !errors.Is(err, lease.ErrExpired) {
		t.Fatalf("Acquire = %v", err)
	}
}

// A commit written to the journal after the lease was lost is not acknowledged:
// the cycle that wrote it doesn't count it as captured, so the gate waits for a
// cycle and fails rather than letting the output through.
func TestWrittenAfterTheLeaseWasLostIsNotAcknowledged(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("wallet/1")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	// Another life of the node takes the lease. Nothing fences the journal, so
	// this node's writer still writes.
	other := lease.NewRegistry(r.local, "n1", lease.RegistryOptions{TTL: time.Hour})
	if _, err := other.Acquire(ctx, lease.NodeInfo{Name: "n1"}); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, apptest.Timeout, "the lease to be lost", func() bool { return r.n.Keeper.Check() != nil })
	next := r.n.Writer.NextSeq()
	r.app.Must(t, "insert", "a", "3", "100")
	apptest.Eventually(t, apptest.Timeout, "a segment holding the commit", func() bool { return r.n.Writer.NextSeq() > next })
	cycles := r.m.Flusher().Completed()
	apptest.Eventually(t, apptest.Timeout, "the cycle that wrote it to complete", func() bool { return r.m.Flusher().Completed() >= cycles+1 })
	if !ref.a.cap.hasNewFrames() {
		t.Fatal("a commit written after the lease was lost counts as captured")
	}
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err == nil {
		t.Fatal("the gate let output through after the lease was lost")
	}
}

// A shard the app wrote to without adding a table (only user_version) is kept
// when it deactivates, like any written shard.
func TestHeaderOnlyWriteKeepsAShard(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("users/uv")
	r.app.Must(t, "openro", "a", ref.Path()) // opens without creating tables
	r.app.Must(t, "exec", "a", "PRAGMA user_version = 7")
	r.app.Must(t, "close", "a")
	ref.Release()
	apptest.Eventually(t, apptest.Timeout, "idle deactivation", r.inactive("users/uv"))
	if _, err := r.manifest("users/uv"); err != nil {
		t.Fatalf("a user_version write was lost: %v", err)
	}
	if !r.released("users/uv") {
		t.Fatal("not released")
	}
	if r.m.Stats.Deletes.Load() != 0 {
		t.Fatal("deleted")
	}
}

// Shards deactivated together share the flush cycle's segments: their final
// syncs queue files and each release waits for the next cycle, it doesn't flush
// the writer itself.
func TestReleasesShareSegments(t *testing.T) {
	t.Parallel()
	const n = 100
	r := newMRigWith(t, slowFlush)
	refs := make([]*Hold, n)
	for i := range refs {
		refs[i] = r.acquire(fmt.Sprintf("slow/%d", i))
		r.write(refs[i], fmt.Sprint(i))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.m.Barrier(ctx); err != nil {
		t.Fatal(err)
	}
	// Uploading a segment takes a while, so flushes that don't share would show.
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after && strings.HasPrefix(key, journal.Root) {
			time.Sleep(40 * time.Millisecond)
		}
		return nil
	})
	before, cyclesBefore := r.n.Writer.Flushes(), r.m.Flusher().Completed()
	var g errgroup.Group
	for i, ref := range refs {
		time.Sleep(15 * time.Millisecond) // spread over several cycles
		// A commit no cycle has captured yet: the release's final sync queues a file.
		r.app.Must(t, "insert", fmt.Sprint(i), "1", "100")
		r.app.Must(t, "close", fmt.Sprint(i)) // the app lets go of the file, or the node keeps the shard
		ref.Release()
		g.Go(func() error {
			ok, err := ref.a.deactivateNow(ctx)
			if err == nil && !ok {
				err = errors.New("not deactivated")
			}
			return err
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	segments, cycles := r.n.Writer.Flushes()-before, r.m.Flusher().Completed()-cyclesBefore
	t.Logf("%d releases: %d segments in %d cycles", n, segments, cycles)
	if segments < 1 || segments > int64(cycles)+1 {
		t.Fatalf("%d releases wrote %d segments in %d cycles: they should share the cycles' flushes", n, segments, cycles)
	}
	for _, i := range []int{0, n / 2, n - 1} {
		if got := r.dumpBlob(fmt.Sprintf("slow/%d", i)); strings.Count(got, "kv: ") != 3 {
			t.Fatalf("shard %d: blob storage has %q after its release", i, got)
		}
	}
}

// A base's age doesn't matter: with the file limit far off, a day later the
// shard still collects a delta per snapshot, and its manifest has no base time.
func TestBackgroundSnapshotsAreNotRebasedByAge(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	r.crash()
	clock := harness.NewFakeClock()
	r.startShardset(func(o *ShardsetOptions) {
		o.Lifecycle.DeltaAge = 20 * time.Millisecond
		o.Lifecycle.MaxFiles = 100
		o.Clock = clock
	})
	ctx := context.Background()
	ref := r.acquire("slow/age")
	defer ref.Release()
	r.write(ref, "a")

	snapshot := func() *shardfile.Manifest {
		t.Helper()
		before, _ := r.manifest("slow/age")
		r.app.Must(t, "insert", "a", "2", "100")
		if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
		var m *shardfile.Manifest
		// The delta timer is on the clock.
		advanceUntil(t, clock, 20*time.Millisecond, "a snapshot to be taken", func() bool {
			var err error
			m, err = r.manifest("slow/age")
			return err == nil && !slices.Equal(m.Files, before.Files)
		})
		return m
	}
	var m *shardfile.Manifest
	for range 3 {
		m = snapshot()
	}
	n := len(m.Files)
	if n < 2 {
		t.Fatalf("files %v: want a delta per snapshot", m.Files)
	}
	clock.Advance(25 * time.Hour)
	m = snapshot()
	if len(m.Files) != n+1 {
		t.Fatalf("files %v after a day (it had %d): want one more delta, not a rebase", m.Files, n)
	}
	if r.dumpBlob("slow/age") != r.liveDump(ref.Path()) {
		t.Fatal("restore differs")
	}
}

// The shardset stops in order: the flusher still runs once the shards are being let
// go of (a gate during a release goes through, and so does each release's own final
// sync, which no longer has a fallback).
func TestShardsStopInOrder(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	// An active shard whose release is held in its delta.
	ref := r.acquire("wallet/so2")
	r.write(ref, "so2")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	ref.Release()
	r.app.Must(t, "close", "so2")
	entered, _, unblock := r.blockCreates(shardfile.ShardPrefix(shardName("wallet/so2")))
	defer unblock()
	stopped := make(chan error, 1)
	go func() { stopped <- r.stop() }()
	<-entered // letting go of wallet/so2, past its final sync

	quick, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.m.Barrier(quick); err != nil {
		t.Fatalf("a gate during the release: %v", err)
	}
	unblock()
	if err := <-stopped; err != nil {
		t.Fatalf("stop: %v", err)
	}
	if r.m.IsActive(shardName("wallet/so2")) {
		t.Fatal("wallet/so2 was not let go of")
	}
}

// An activation that fails after its first sync leaves its commits in the
// journal, and the next one starts the shard over from position 1. It does so
// in a new generation, so adopting the journal never mixes the two
// activations' commits.
func TestActivationAfterAFailedOneStartsANewGeneration(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	// An active shard keeps the lease's journal range from moving past the failed
	// activation's segment.
	other := r.acquire("slow/other")
	r.write(other, "o")

	// What the failed activation leaves: a manifest with no files, and commits in
	// the journal. The extra table stands for anything it committed that the next
	// activation doesn't.
	h, created, err := r.n.Files.Open(ctx, shardName("slow/f"))
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	failed := h.Manifest.Generation
	path := pathFor(r.data, shardName("slow/f"), failed)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sh, err := capture.New(capture.Config{Name: shardName("slow/f"), Path: path, Generation: failed, Writer: r.n.Writer, TempDir: tempfile.NewDir(t.TempDir())})
	if err != nil {
		t.Fatal(err)
	}
	if err := sh.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sh.Use(ctx, func(db *sql.DB) error { _, err := db.ExecContext(ctx, `CREATE TABLE stray (x)`); return err }); err != nil {
		t.Fatal(err)
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	sh.Close(ctx)
	sh.Discard()

	ref := r.acquire("slow/f")
	if r.shard("slow/f").h.Manifest.Generation == failed {
		t.Fatal("the shard started over in the failed activation's generation")
	}
	r.write(ref, "f")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	want := r.liveDump(ref.Path())
	r.app.Must(t, "close", "f")
	r.app.Must(t, "close", "o")
	r.crash() // the shard's commits are only in journal segments
	r.startNode()
	if r.n.Adopted.Deltas == 0 {
		t.Fatalf("nothing was adopted: %+v", r.n.Adopted)
	}
	if got := r.dumpBlob("slow/f"); got != want {
		t.Fatalf("restored after adoption:\n%s\nwant:\n%s", got, want)
	}
}

// Acquire activates the shard if it is not active and holds it. However many
// requests ask at once, the shard is activated once: the first adds its entry
// and the others wait for it. The activation runs in the shard's own loop, so
// one caller giving up doesn't fail the others. A shard another node owns is not
// activated at all: the manifest is read before the entry is added, and the
// owner is remembered (*shardfile.OwnedElsewhereError).
func (m *Shardset) Acquire(ctx context.Context, n shard.Name) (*Hold, error) {
	return m.take(ctx, n, acquireOpts{})
}
