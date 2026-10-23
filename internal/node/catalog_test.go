package node

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
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

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

const testKinds = `{"app": "test", "kinds": {
	"users":  {"public": true, "sync": "local", "idle_timeout": "300ms"},
	"wallet": {"public": true, "idle_timeout": "300ms"},
	"slow":   {"public": true, "sync": "local", "idle_timeout": "1h"},
	"audit":  {},
	"lobby":  {"public": true, "idle_timeout": "300ms"},
	"arena":  {"public": true, "sync": "local", "idle_timeout": "300ms"}
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
	n           *Node
	m           *Catalog
	stopM       func() error // stops the shards' Serve and returns what it returned

	// newRouting makes the shards' wake of the app and calls (the server's,
	// for the rigs that have one). The default reaches nobody.
	newRouting func(*Catalog) routing

	cmu    sync.Mutex
	counts map[string]int
}

func newMRig(t *testing.T) *mrig { return newMRigWith(t, nil) }

func newMRigWith(t *testing.T, mutate func(*CatalogOptions)) *mrig { return newMRigOn(t, mutate, nil) }

// newMRigOn is newMRigWith over the given backend (in memory if nil).
func newMRigOn(t *testing.T, mutate func(*CatalogOptions), backend blob.Backend) *mrig {
	t.Helper()
	r := newUnstartedMRig(t, backend)
	r.startNode()
	r.startShards(mutate)
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
		Lease: coord.RegistryOptions{TTL: time.Hour, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: coord.KeeperOptions{Interval: 100 * time.Millisecond, Margin: time.Minute,
			Now: func() time.Time { return time.Now().Add(time.Duration(r.leaseAhead.Load())) }},
	})
}

func (r *mrig) startShards(mutate func(*CatalogOptions)) {
	r.t.Helper()
	o := CatalogOptions{Kinds: r.kinds, Data: r.data,
		Flush:     FlushOptions{Interval: 20 * time.Millisecond},
		Cache:     CacheOptions{Every: 20 * time.Millisecond},
		Lifecycle: LifecycleOptions{SnapshotEvery: 20 * time.Millisecond, HoldOff: 100 * time.Millisecond, IdleRecheck: 20 * time.Millisecond}}
	if mutate != nil {
		mutate(&o)
	}
	newRouting := r.newRouting
	if newRouting == nil {
		newRouting = func(*Catalog) routing {
			unserved := errors.New("the node is not serving")
			return routing{
				wake: func(context.Context, *actor) (int, error) { return 0, unserved },
				call: func(*http.Request) (*http.Response, error) { return nil, unserved },
			}
		}
	}
	m, err := NewCatalog(r.n, o, newRouting)
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

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	apptest.Eventually(t, 30*time.Second, what, cond) // a deadline for failures only: a slow machine under -race takes a while
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

// afterCachePasses returns once n passes of the cache loop have completed,
// which began after the call.
func (r *mrig) afterCachePasses(n uint64) {
	r.t.Helper()
	waitCounter(r.t, "passes of the cache loop", &r.m.Stats.CachePasses, r.m.Stats.CachePasses.Load()+n+1)
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
func advanceUntil(t *testing.T, clk *fakeClock, step time.Duration, what string, cond func() bool) {
	t.Helper()
	eventually(t, what, func() bool {
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

func (r *mrig) cached(target string) bool {
	names, err := r.m.Cached()
	if err != nil {
		r.t.Fatal(err)
	}
	for _, n := range names {
		if n.String() == target {
			return true
		}
	}
	return false
}

func TestRefusesUndeclaredAndBadTargets(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	if _, err := r.m.Acquire(ctx, shardName("test.typo/1")); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("a kind no app declares: %v", err)
	}
	for _, bad := range []string{"users/1", "test.users"} {
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
	ref := r.acquire("test.users/1")
	defer ref.Release()
	m, err := r.manifest("test.users/1")
	if err != nil {
		t.Fatalf("no manifest after activation: %v", err)
	}
	if m.Owner != "n1" || m.NodeEpoch != r.n.Keeper.Lease().Epoch || !strings.Contains(ref.Path(), "."+string(m.Generation)+".db") {
		t.Fatalf("manifest %+v for path %s", m, ref.Path())
	}
	if r.m.Stats.Creates.Load() != 1 {
		t.Fatalf("%d creates", r.m.Stats.Creates.Load())
	}
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	if got, want := r.dumpBlob("test.users/1"), r.liveDump(ref.Path()); got != want {
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
		if op == blobtest.OpCreate && key == shardfile.ManifestKey(shardName("test.users/2")) && !after && !raced {
			raced = true
			body, _ := json.Marshal(shardfile.Manifest{Generation: "other", Files: []shardfile.File{}})
			if _, err := r.local.Create(ctx, key, bytes.NewReader(body), blob.WriteID{Node: "n2", Epoch: 1, N: 1}); err != nil {
				return err
			}
		}
		return nil
	})
	ref := r.acquire("test.users/2")
	defer ref.Release()
	// The winner's manifest has nothing to restore, so the shard starts over in a
	// generation of its own.
	m, err := r.manifest("test.users/2")
	if !raced || err != nil || m.Owner != "n1" || m.Generation == "other" || !strings.Contains(ref.Path(), "."+string(m.Generation)+".db") {
		t.Fatalf("raced %v, path %s, manifest %+v: %v", raced, ref.Path(), m, err)
	}
	if r.m.Stats.Creates.Load() != 0 {
		t.Fatal("counted as created")
	}
}

func TestIdleShardThatWasOnlyReadLeavesNothing(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("test.users/3")
	r.app.Must(t, "openro", "a", ref.Path())
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("test.users/3"))
	if _, err := r.manifest("test.users/3"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v", err)
	}
	if left := leftBehind(r.data); len(left) != 0 {
		t.Fatalf("left behind: %v", left)
	}
	if names, _ := r.m.Cached(); len(names) != 0 {
		t.Fatalf("cached: %v", names)
	}
}

// leftBehind is the files in the data directory that are neither the node's
// own database (the journal's rows are kept, and the sidecars beside it are
// its) nor the old index files the first start deletes.
func leftBehind(data string) []string {
	var mine []string
	filepath.WalkDir(data, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		base := p
		for _, suffix := range []string{"-wal", "-shm", "-journal"} {
			base = strings.TrimSuffix(base, suffix)
		}
		if filepath.Base(base) == localdb.Name || strings.HasPrefix(filepath.Base(p), indexName) {
			return nil
		}
		mine = append(mine, p)
		return nil
	})
	return mine
}

func TestGates(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("test.wallet/1")
	defer ref.Release()
	if ref.Sync() != kind.SyncStorage {
		t.Fatal("wallet should have sync on")
	}
	r.write(ref, "a")

	// Sync on: when the gate returns, blob storage restores to the live file.
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob("test.wallet/1") != r.liveDump(ref.Path()) {
		t.Fatal("gate released output before the write was in blob storage")
	}

	// A shard with no new frames passes straight through.
	eventually(t, "the shard to be captured", func() bool { return !ref.a.cap.hasNewFrames() })
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
	if err := r.m.Gate(quick, ref, kind.SyncLocal); err != nil {
		t.Fatalf("sync off waited for a flush: %v", err)
	}
	close(stall)
	// ...but the commit is captured by the flush loop all the same.
	eventually(t, "the flush loop to capture the commit", func() bool { return !ref.a.cap.hasNewFrames() })
	if r.dumpBlob("test.wallet/1") != r.liveDump(ref.Path()) {
		t.Fatal("commit not in blob storage after the flush loop captured it")
	}

	// The barrier behind POST /_shardlite/sync.
	r.app.Must(t, "insert", "a", "2", "100")
	if err := r.m.Barrier(ctx); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob("test.wallet/1") != r.liveDump(ref.Path()) {
		t.Fatal("Barrier returned before the commit was in blob storage")
	}
}

func TestIdleNodeWritesNoSegments(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("test.slow/1")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the shard to be captured", func() bool { return !ref.a.cap.hasNewFrames() })
	r.afterCycle()
	next := r.n.Writer.NextSeq()
	cycles := r.m.Flusher().Completed()
	eventually(t, "five more flush cycles (the flush loop is not running)", func() bool { return r.m.Flusher().Completed() >= cycles+5 })
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
			ref := r.acquire("test.slow/7")
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

// A cached shard is reopened without a call to blob storage.
func TestCachedShardNeedsNoBlobCalls(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("test.users/4")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	want := r.liveDump(ref.Path())
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "deactivation", func() bool { return r.cached("test.users/4") })

	// The deactivation wrote a delta, so the tail is in a file.
	m, err := r.manifest("test.users/4")
	if err != nil || len(m.Files) == 0 || m.Owner != "n1" {
		t.Fatalf("manifest after deactivation: %+v %v", m, err)
	}

	before := map[string]int{}
	for _, p := range []string{shardfile.ManifestPrefix, shardfile.FilesPrefix, journal.Root} {
		for _, op := range []string{"get", "head", "get-range", "create", "update", "list"} {
			before[op+" "+p] = r.calls(op, p)
		}
	}
	ref = r.acquire("test.users/4")
	defer ref.Release()
	for k, v := range before {
		parts := strings.SplitN(k, " ", 2)
		if got := r.calls(parts[0], parts[1]); got != v {
			t.Fatalf("%s: %d calls while reactivating a cached shard", k, got-v)
		}
	}
	if r.m.Stats.CachedHits.Load() != 1 || r.m.Stats.Restores.Load() != 0 {
		t.Fatalf("stats: %+v", &r.m.Stats)
	}
	if got := r.liveDump(ref.Path()); got != want {
		t.Fatal("cached file differs")
	}
	// And it is captured again.
	r.app.Must(t, "open", "b", ref.Path())
	r.app.Must(t, "insert", "b", "2", "100")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob("test.users/4") != r.liveDump(ref.Path()) {
		t.Fatal("reactivated shard is not captured")
	}
}

// A file the app changed while the node had it cached can't be trusted.
func TestCachedFileTouchedBehindTheNodesBack(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("test.users/5")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	path := ref.Path()
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "deactivation", func() bool { return r.cached("test.users/5") })
	want := r.dumpBlob("test.users/5")

	r.app.Must(t, "open", "x", path)
	r.app.Must(t, "insert", "x", "5", "100")
	r.app.Must(t, "close", "x")

	ref = r.acquire("test.users/5")
	defer ref.Release()
	if r.m.Stats.Discards.Load() != 1 || r.m.Stats.Restores.Load() != 1 || r.m.Stats.CachedHits.Load() != 0 {
		t.Fatalf("stats: discards=%d restores=%d hits=%d", r.m.Stats.Discards.Load(), r.m.Stats.Restores.Load(), r.m.Stats.CachedHits.Load())
	}
	if got := r.liveDump(ref.Path()); got != want {
		t.Fatal("the shard should be what blob storage holds, without the unobserved write")
	}
}

// A restart starts with an empty cache, as a new node would: the files of
// deactivated shards are gone, the index is new, and every shard comes from blob storage.
func TestRestartStartsWithAnEmptyCache(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	want := map[string]string{}
	for _, id := range []string{"test.users/6", "test.users/7"} {
		ref := r.acquire(id)
		r.write(ref, "a")
		if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
		r.app.Must(t, "close", "a")
		ref.Release()
		eventually(t, "deactivation", func() bool { return r.cached(id) })
		want[id] = r.dumpBlob(id)
	}
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	r.stopJournal()
	// A file that is not a shard's, and a temporary one, go too.
	if err := os.WriteFile(filepath.Join(r.data, tempDirName, "leftover"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	r.startNode()
	r.startShards(nil)
	if left := leftBehind(r.data); len(left) != 0 {
		t.Fatalf("files left in the data directory: %v", left)
	}
	if names, _ := r.m.Cached(); len(names) != 0 {
		t.Fatalf("the cache has %v", names)
	}
	journalReads := func() int {
		return r.calls("get", journal.Root) + r.calls("head", journal.Root) + r.calls("get-range", journal.Root)
	}
	before := journalReads()
	for _, id := range []string{"test.users/6", "test.users/7"} {
		ref := r.acquire(id)
		if got := r.liveDump(ref.Path()); got != want[id] {
			t.Fatalf("%s differs from blob storage", id)
		}
		ref.Release()
	}
	// Their manifests still name this node's journal, but a claimed shard has no
	// tail anywhere, so the restores read only its files.
	if got := journalReads() - before; got != 0 {
		t.Fatalf("%d reads of the journal while restoring", got)
	}
	if r.m.Stats.CachedHits.Load() != 0 || r.m.Stats.Restores.Load() != 2 || r.m.Stats.Discards.Load() != 0 {
		t.Fatalf("stats: %+v", &r.m.Stats)
	}
	if m, err := r.manifest("test.users/6"); err != nil || m.NodeEpoch != 2 || m.Owner != "n1" {
		t.Fatalf("manifest not reclaimed: %+v %v", m, err)
	}
}

// An index that is corrupt (or ahead of the files, as after an OS crash) is
// never read: it is deleted at the start.
func TestRestartWithACorruptIndex(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("test.users/8")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "deactivation", func() bool { return r.cached("test.users/8") })
	want := r.dumpBlob("test.users/8")
	r.crash()
	r.stopJournal()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		os.WriteFile(filepath.Join(r.data, indexName+suffix), []byte("this is not a database, and the files are not the ones it names"), 0o644)
	}

	r.startNode()
	r.startShards(nil)
	ref = r.acquire("test.users/8")
	defer ref.Release()
	if got := r.liveDump(ref.Path()); got != want {
		t.Fatal("the shard differs from blob storage")
	}
	if r.m.Stats.CachedHits.Load() != 0 || r.m.Stats.Restores.Load() != 1 {
		t.Fatalf("stats: %+v", &r.m.Stats)
	}
}

// A shard file that an app still has open across a restart is never deleted
// (the app could go on committing to it without an error), and activating the
// shard is refused. The cache loop deletes it once the app has closed it.
func TestRestartLeavesAFileTheAppHasOpenUntilItIsClosed(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	r := newMRigWith(t, logTo(&logs))
	ctx := context.Background()
	ref := r.acquire("test.users/9")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	path := ref.Path()
	r.crash() // the app still has the file open
	r.stopJournal()

	r.startNode()
	r.startShards(logTo(&logs))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the file the app has open was deleted: %v", err)
	}
	if !strings.Contains(logs.String(), "still open in another process") {
		t.Fatalf("no log line about the open file:\n%s", logs.String())
	}
	r.afterCachePasses(2)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the cache loop deleted a file the app has open: %v", err)
	}
	if _, err := r.m.Acquire(ctx, shardName("test.users/9")); !errors.Is(err, ErrAppHolds) {
		t.Fatalf("acquire: %v, want ErrAppHolds", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("a refused activation deleted the file: %v", err)
	}
	r.app.Must(t, "close", "a")
	eventually(t, "the cache loop to delete the closed file", func() bool {
		_, err := os.Stat(path)
		return errors.Is(err, os.ErrNotExist)
	})
	ref = r.acquire("test.users/9")
	defer ref.Release()
	if got := strings.Count(r.liveDump(ref.Path()), "kv: "); got != 2 {
		t.Fatalf("%d rows after the file was deleted, want the 2 of one write", got)
	}
}

// A crash leaves files for shards that were active. They may be ahead of what
// reached the journal, so they are discarded, and the shard comes back from blob storage.
func TestCrashLeavesActiveFilesToBeDiscarded(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("test.wallet/2")
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
	r.startShards(nil)
	if r.n.Adopted.Deltas != 1 {
		t.Fatalf("adoption: %+v", r.n.Adopted)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("the active file of the crashed process was kept")
	}
	ref = r.acquire("test.wallet/2")
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
	clk := newFakeClock()
	r := newMRigWith(t, withClock(clk))
	ref := r.acquire("test.users/8")
	r.write(ref, "a")
	r.app.Must(t, "close", "a")
	// A hold (an open connection) keeps the shard active past its 300 ms timeout:
	// the clock moves three timeouts, and the idle timer looks at each.
	a := r.shard("test.users/8")
	for range 3 {
		looks := a.looks.idle.Load()
		clk.Advance(300 * time.Millisecond)
		waitCounter(t, "a look of the idle timer", &a.looks.idle, looks+1)
	}
	if !r.m.IsActive(shardName("test.users/8")) {
		t.Fatal("deactivated while held")
	}
	ref.Release()
	advanceUntil(t, clk, 100*time.Millisecond, "deactivation after the last hold", func() bool { return r.cached("test.users/8") })
}

func TestAppHoldingTheShardKeepsItActive(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newMRigWith(t, withClock(clk))
	ref := r.acquire("test.users/9")
	r.write(ref, "a") // the app keeps its connection open
	ref.Release()
	// Several attempts, each with a warning and each setting the shard's next look.
	var seen []time.Time
	for len(seen) < 3 {
		advanceUntil(t, clk, 100*time.Millisecond, "an attempt to deactivate", func() bool {
			d := r.delay("test.users/9")
			return !d.IsZero() && (len(seen) == 0 || d.After(seen[len(seen)-1]))
		})
		seen = append(seen, r.delay("test.users/9"))
	}
	if !r.m.IsActive(shardName("test.users/9")) {
		t.Fatal("deactivated while the app had the file open")
	}
	// It is still captured and usable.
	ref = r.acquire("test.users/9")
	r.app.Must(t, "insert", "a", "1", "100")
	if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob("test.users/9") != r.liveDump(ref.Path()) {
		t.Fatal("not captured after the failed release")
	}
	ref.Release()
	r.app.Must(t, "close", "a")
	advanceUntil(t, clk, 200*time.Millisecond, "deactivation once the app let go", func() bool { return r.cached("test.users/9") })
}

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	make1 := func(id string) {
		ref := r.acquire(id)
		r.write(ref, "a")
		if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
		r.app.Must(t, "close", "a")
		ref.Release()
		eventually(t, "deactivation", func() bool { return r.cached(id) })
	}
	make1("test.users/a")
	make1("test.users/b")
	used, err := r.m.cache.idx.cachedBytes()
	if err != nil {
		t.Fatal(err)
	}
	want := r.dumpBlob("test.users/a")
	// Room for two and a half: the third pushes the oldest out.
	r.m.SetCacheLimit(used + used/4)
	make1("test.users/c")
	eventually(t, "eviction", func() bool { return !r.cached("test.users/a") })
	if !r.cached("test.users/b") || !r.cached("test.users/c") {
		t.Fatal("evicted the wrong shard")
	}
	m, err := r.manifest("test.users/a")
	if err != nil || m.Owner != "" || m.Journal != "" || len(m.Files) == 0 {
		t.Fatalf("manifest of the evicted shard: %+v %v", m, err)
	}
	var left []string
	filepath.WalkDir(r.data, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.Contains(p, "test.users/a") || strings.Contains(filepath.Base(p), "a.") && strings.HasSuffix(p, ".db") && strings.Contains(p, string(filepath.Separator)+"users"+string(filepath.Separator)) && strings.HasPrefix(filepath.Base(p), "a.") {
			left = append(left, p)
		}
		return nil
	})
	if len(left) != 0 {
		t.Fatalf("files of the evicted shard remain: %v", left)
	}
	// The next request claims it like any unowned shard and restores it.
	r.m.SetCacheLimit(0)
	ref := r.acquire("test.users/a")
	defer ref.Release()
	if r.m.Stats.Restores.Load() != 4 && r.m.Stats.Restores.Load() < 1 {
		t.Fatalf("stats: %+v", &r.m.Stats)
	}
	if got := r.liveDump(ref.Path()); got != want {
		t.Fatal("restored shard differs")
	}
	if m, _ := r.manifest("test.users/a"); m.Owner != "n1" {
		t.Fatalf("not claimed: %+v", m)
	}
}

func TestBackgroundDeltasAndRebases(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	r.crash()
	r.startShards(func(o *CatalogOptions) { o.Lifecycle.DeltaAge = 30 * time.Millisecond; o.Lifecycle.MaxFiles = 3 })
	ctx := context.Background()
	ref := r.acquire("test.slow/bg")
	defer ref.Release()
	r.write(ref, "a")
	maxFiles := 0
	var before []shardfile.File
	if m, err := r.manifest("test.slow/bg"); err == nil {
		before = m.Files
	}
	for range 12 {
		r.app.Must(t, "insert", "a", "2", "100")
		if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
		// Each commit gets a snapshot once its tail is old enough: a delta, or a rebase.
		eventually(t, "a snapshot", func() bool {
			m, err := r.manifest("test.slow/bg")
			if err != nil || slices.Equal(m.Files, before) {
				return false
			}
			maxFiles = max(maxFiles, len(m.Files))
			before = m.Files
			return true
		})
	}
	m, err := r.manifest("test.slow/bg")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) == 0 || len(m.Files) > 4 || maxFiles < 3 {
		t.Fatalf("files %v (max seen %d): deltas and rebases should keep a handful", m.Files, maxFiles)
	}
	// A snapshot seen above may be an earlier commit's late rebase, so the last
	// commit's own snapshot can still be on its way, and a restore can find a file
	// that a rebase has replaced since it read the manifest.
	eventually(t, "the blobs to restore to what is live", func() bool {
		dst := filepath.Join(t.TempDir(), "restored.db")
		if _, err := r.n.Files.Restore(ctx, shardName("test.slow/bg"), dst, shardfile.RestoreOptions{}); errors.Is(err, blob.ErrNotFound) {
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
		Lease:  coord.RegistryOptions{TTL: 400 * time.Millisecond, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: coord.KeeperOptions{Interval: 50 * time.Millisecond, Margin: 100 * time.Millisecond, Logger: nil},
	})
	if err != nil {
		t.Fatal(err)
	}
	r.n = n
	serveJournal(t, n)
	r.startShards(nil)
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
	eventually(t, "the lease to expire", func() bool { return r.n.Keeper.Check() != nil })
	if err := r.m.Barrier(ctx); !errors.Is(err, coord.ErrLeaseExpired) {
		t.Fatalf("Barrier = %v", err)
	}
	if _, err := r.m.Acquire(ctx, shardName("test.users/x")); !errors.Is(err, coord.ErrLeaseExpired) {
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
	ref := r.acquire("test.wallet/1")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	// Another life of the node takes the lease. Nothing fences the journal, so
	// this node's writer still writes.
	other := coord.NewRegistry(r.local, "n1", coord.RegistryOptions{TTL: time.Hour})
	if _, err := other.Acquire(ctx, coord.NodeInfo{Name: "n1"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the lease to be lost", func() bool { return r.n.Keeper.Check() != nil })
	next := r.n.Writer.NextSeq()
	r.app.Must(t, "insert", "a", "3", "100")
	eventually(t, "a segment holding the commit", func() bool { return r.n.Writer.NextSeq() > next })
	cycles := r.m.Flusher().Completed()
	eventually(t, "the cycle that wrote it to complete", func() bool { return r.m.Flusher().Completed() >= cycles+1 })
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
	ref := r.acquire("test.users/uv")
	r.app.Must(t, "openro", "a", ref.Path()) // opens without creating tables
	r.app.Must(t, "exec", "a", "PRAGMA user_version = 7")
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("test.users/uv"))
	if _, err := r.manifest("test.users/uv"); err != nil {
		t.Fatalf("a user_version write was lost: %v", err)
	}
	if !r.cached("test.users/uv") {
		t.Fatal("not cached")
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
		refs[i] = r.acquire(fmt.Sprintf("test.slow/%d", i))
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
		if got := r.dumpBlob(fmt.Sprintf("test.slow/%d", i)); strings.Count(got, "kv: ") != 3 {
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
	clock := newFakeClock()
	r.startShards(func(o *CatalogOptions) {
		o.Lifecycle.DeltaAge = 20 * time.Millisecond
		o.Lifecycle.MaxFiles = 100
		o.Clock = clock
	})
	ctx := context.Background()
	ref := r.acquire("test.slow/age")
	defer ref.Release()
	r.write(ref, "a")

	snapshot := func() *shardfile.Manifest {
		t.Helper()
		before, _ := r.manifest("test.slow/age")
		r.app.Must(t, "insert", "a", "2", "100")
		if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
		var m *shardfile.Manifest
		// The delta timer is on the clock.
		advanceUntil(t, clock, 20*time.Millisecond, "a snapshot to be taken", func() bool {
			var err error
			m, err = r.manifest("test.slow/age")
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
	if r.dumpBlob("test.slow/age") != r.liveDump(ref.Path()) {
		t.Fatal("restore differs")
	}
}

// The catalog stops in order: the flusher still runs once the shards are being let
// go of (a gate during a release goes through, and so does each release's own final
// sync, which no longer has a fallback).
func TestShardsStopInOrder(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	// An active shard whose release is held in its delta.
	ref := r.acquire("test.wallet/so2")
	r.write(ref, "so2")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	ref.Release()
	r.app.Must(t, "close", "so2")
	entered, _, unblock := r.blockCreates(shardfile.ShardPrefix(shardName("test.wallet/so2")))
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
	if r.m.IsActive(shardName("test.wallet/so2")) {
		t.Fatal("test.wallet/so2 was not let go of")
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
	other := r.acquire("test.slow/other")
	r.write(other, "o")

	// What the failed activation leaves: a manifest with no files, and commits in
	// the journal. The extra table stands for anything it committed that the next
	// activation doesn't.
	h, created, err := r.n.Files.Open(ctx, shardName("test.slow/f"))
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	failed := h.Manifest.Generation
	path := r.m.cache.pathFor(shardName("test.slow/f"), failed)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sh, err := capture.New(capture.Config{Name: shardName("test.slow/f"), Path: path, Generation: failed, Writer: r.n.Writer, TempDir: t.TempDir()})
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

	ref := r.acquire("test.slow/f")
	if r.shard("test.slow/f").h.Manifest.Generation == failed {
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
	if got := r.dumpBlob("test.slow/f"); got != want {
		t.Fatalf("restored after adoption:\n%s\nwant:\n%s", got, want)
	}
}
