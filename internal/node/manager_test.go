package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

const testKinds = `{"kinds": {
	"users":  {"public": true, "sync": "off", "idle_timeout": "300ms"},
	"wallet": {"public": true, "idle_timeout": "300ms"},
	"slow":   {"public": true, "sync": "off", "idle_timeout": "1h"},
	"audit":  {},
	"lobby":  {"public": true, "ephemeral": true, "idle_timeout": "300ms"}
}}`

type mrig struct {
	t         *testing.T
	local     *blob.Local
	blobDir   string
	data      string
	meta      string
	kinds     *Kinds
	kindsPath string
	app       *apptest.App
	n         *Node
	m         *Manager

	cmu    sync.Mutex
	counts map[string]int
}

func newMRig(t *testing.T) *mrig { return newMRigWith(t, nil) }

func newMRigWith(t *testing.T, mutate func(*ManagerOptions)) *mrig {
	t.Helper()
	blobDir := filepath.Join(t.TempDir(), "blobs")
	l, err := blob.NewLocal(blobDir)
	if err != nil {
		t.Fatal(err)
	}
	l.NoSync = true
	r := &mrig{t: t, local: l, blobDir: blobDir, data: filepath.Join(t.TempDir(), "data"), meta: filepath.Join(t.TempDir(), "meta"), app: apptest.Start(t), counts: map[string]int{}}
	r.kindsPath = filepath.Join(t.TempDir(), "kinds.json")
	if err := os.WriteFile(r.kindsPath, []byte(testKinds), 0o644); err != nil {
		t.Fatal(err)
	}
	r.kinds, err = LoadKinds(r.kindsPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	l.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after {
			r.cmu.Lock()
			r.counts[string(op)+" "+strings.SplitN(key, "/", 2)[0]]++
			r.cmu.Unlock()
		}
		return nil
	})
	r.startNode()
	r.startManager(mutate)
	return r
}

func (r *mrig) startNode() {
	r.t.Helper()
	n, err := Start(context.Background(), Config{
		Name: "n1", Backend: r.local, Flush: 20 * time.Millisecond, ManualFlush: true,
		Blob:   blob.Options{Backoff: time.Millisecond},
		Lease:  coord.RegistryOptions{TTL: time.Hour, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: coord.KeeperOptions{Interval: 100 * time.Millisecond, Margin: time.Minute},
	})
	if err != nil {
		r.t.Fatal(err)
	}
	r.n = n
	r.t.Cleanup(func() { n.Close(context.Background(), false) })
}

func (r *mrig) startManager(mutate func(*ManagerOptions)) {
	r.t.Helper()
	o := ManagerOptions{Kinds: r.kinds, Data: r.data, Meta: r.meta, Flush: 20 * time.Millisecond, Janitor: 20 * time.Millisecond,
		Background: 20 * time.Millisecond, HoldOff: 100 * time.Millisecond}
	if mutate != nil {
		mutate(&o)
	}
	m, err := NewManager(r.n, o)
	if err != nil {
		r.t.Fatal(err)
	}
	r.m = m
	r.t.Cleanup(func() { m.cancel(); m.g.Wait() })
}

// crash stops the manager and node loops without letting go of anything, as a
// SIGKILL would (the old writer still exists, as a zombie).
func (r *mrig) crash() {
	r.m.cancel()
	r.m.g.Wait()
	r.m.idx.close()
}

func (r *mrig) calls(op, prefix string) int {
	r.cmu.Lock()
	defer r.cmu.Unlock()
	return r.counts[op+" "+prefix]
}

func (r *mrig) acquire(target string) *Ref {
	r.t.Helper()
	ref, err := r.m.Acquire(context.Background(), target)
	if err != nil {
		r.t.Fatalf("acquire %s: %v", target, err)
	}
	return ref
}

// write has the app open the shard and commit to it.
func (r *mrig) write(ref *Ref, id string) {
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
	if _, err := r.n.Files.Restore(context.Background(), target, dst, shardfile.RestoreOptions{}); err != nil {
		r.t.Fatalf("restore %s: %v", target, err)
	}
	return sqlitetest.Dump(r.t, dst)
}

// liveDump reads a captured file from the app's process. The test's own process
// hosts Litestream, and closing a descriptor on the file here would drop the
// locks Litestream holds on it.
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
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (r *mrig) manifest(target string) (*shardfile.Manifest, error) {
	m, _, err := r.n.Files.GetManifest(context.Background(), target)
	return m, err
}

func (r *mrig) inactive(target string) func() bool {
	return func() bool { return !r.m.IsActive(target) }
}

func (r *mrig) cached(target string) bool {
	names, err := r.m.Cached()
	if err != nil {
		r.t.Fatal(err)
	}
	for _, n := range names {
		if n == target {
			return true
		}
	}
	return false
}

func TestRefusesUndeclaredAndBadTargets(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	if _, err := r.m.Acquire(ctx, "typo/1"); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("undeclared: %v", err)
	}
	if _, err := r.m.Acquire(ctx, "users"); !errors.Is(err, ErrBadTarget) {
		t.Fatalf("no id: %v", err)
	}
}

// A shard with no manifest is created when it is activated: its manifest claims
// it for this node, and its file carries the generation.
func TestCreationOnActivation(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("users/1")
	defer ref.Release()
	m, err := r.manifest("users/1")
	if err != nil {
		t.Fatalf("no manifest after activation: %v", err)
	}
	if m.Owner != "n1" || m.NodeEpoch != r.n.Keeper.Lease().Epoch || !strings.Contains(ref.Path(), "."+m.Generation+".db") {
		t.Fatalf("manifest %+v for path %s", m, ref.Path())
	}
	if r.m.Stats.Creates.Load() != 1 {
		t.Fatalf("%d creates", r.m.Stats.Creates.Load())
	}
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	if got, want := r.dumpBlob("users/1"), r.liveDump(ref.Path()); got != want {
		t.Fatal("what the gate released is not what blob storage restores")
	}
}

// If another node creates the shard between this node finding no manifest and
// creating one, this node claims the other's shard like any other.
func TestCreateRaceClaimsTheOther(t *testing.T) {
	r := newMRig(t)
	raced := false
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && key == "manifests/users/2" && !after && !raced {
			raced = true
			body, _ := json.Marshal(shardfile.Manifest{Generation: "other", Files: []string{}})
			if _, err := r.local.Create(ctx, key, bytes.NewReader(body), blob.WriteID{Node: "n2", Epoch: 1, N: 1}); err != nil {
				return err
			}
		}
		return nil
	})
	ref := r.acquire("users/2")
	defer ref.Release()
	if !raced || !strings.Contains(ref.Path(), ".other.db") {
		t.Fatalf("raced %v, path %s", raced, ref.Path())
	}
	if m, err := r.manifest("users/2"); err != nil || m.Owner != "n1" || m.Generation != "other" {
		t.Fatalf("manifest %+v: %v", m, err)
	}
	if r.m.Stats.Creates.Load() != 0 {
		t.Fatal("counted as created")
	}
}

func TestIdleShardThatWasOnlyReadLeavesNothing(t *testing.T) {
	r := newMRig(t)
	ref := r.acquire("users/3")
	r.app.Must(t, "openro", "a", ref.Path())
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("users/3"))
	if _, err := r.manifest("users/3"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v", err)
	}
	var left []string
	filepath.WalkDir(r.data, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && !strings.HasPrefix(filepath.Base(p), ".index.db") {
			left = append(left, p)
		}
		return nil
	})
	if len(left) != 0 {
		t.Fatalf("left behind: %v", left)
	}
	if names, _ := r.m.Cached(); len(names) != 0 {
		t.Fatalf("cached: %v", names)
	}
}

func TestGates(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("wallet/1")
	defer ref.Release()
	if !ref.Sync() {
		t.Fatal("wallet should have sync on")
	}
	r.write(ref, "a")

	// Sync on: when the gate returns, blob storage restores to the live file.
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob("wallet/1") != r.liveDump(ref.Path()) {
		t.Fatal("gate released output before the write was in blob storage")
	}

	// A shard with no new frames passes straight through.
	time.Sleep(200 * time.Millisecond)
	before := r.m.Flusher().Mark()
	start := time.Now()
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 5*time.Millisecond || r.m.Flusher().Mark() != before && d > 5*time.Millisecond {
		t.Fatalf("an unchanged shard waited %v", d)
	}

	// Sync off never waits, even with new frames.
	r.app.Must(t, "insert", "a", "2", "100")
	start = time.Now()
	if err := r.m.Gate(ctx, ref, false); err != nil || time.Since(start) > 5*time.Millisecond {
		t.Fatalf("sync off: %v after %v", err, time.Since(start))
	}
	// ...but the commit is captured by the flush loop all the same.
	eventually(t, "the flush loop to capture the commit", func() bool { return !ref.a.hasNewFrames() })
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
	r := newMRig(t)
	ref := r.acquire("slow/1")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(context.Background(), ref, true); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	next := r.n.Writer.NextSeq()
	cycles := r.m.Flusher().Completed()
	time.Sleep(400 * time.Millisecond)
	if r.m.Flusher().Completed() < cycles+5 {
		t.Fatal("the flush loop is not running")
	}
	if r.n.Writer.NextSeq() != next {
		t.Fatalf("segments written while nothing changed: %d to %d", next, r.n.Writer.NextSeq())
	}
}

func TestOneActivationForConcurrentRequests(t *testing.T) {
	r := newMRig(t)
	var wg sync.WaitGroup
	paths := make([]string, 40)
	for i := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ref := r.acquire("slow/7")
			paths[i] = ref.Path()
			defer ref.Release()
		}()
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
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("users/4")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	want := r.liveDump(ref.Path())
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "deactivation", func() bool { return r.cached("users/4") })

	// The deactivation wrote a delta, so the tail is in a file.
	m, err := r.manifest("users/4")
	if err != nil || len(m.Files) == 0 || m.Owner != "n1" {
		t.Fatalf("manifest after deactivation: %+v %v", m, err)
	}

	before := map[string]int{}
	for _, p := range []string{"manifests", "shards", "journal"} {
		for _, op := range []string{"get", "head", "get-range", "create", "update", "list"} {
			before[op+" "+p] = r.calls(op, p)
		}
	}
	ref = r.acquire("users/4")
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
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob("users/4") != r.liveDump(ref.Path()) {
		t.Fatal("reactivated shard is not captured")
	}
}

// A file the app changed while the node had it cached can't be trusted.
func TestCachedFileTouchedBehindTheNodesBack(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("users/5")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	path := ref.Path()
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "deactivation", func() bool { return r.cached("users/5") })
	want := r.dumpBlob("users/5")

	r.app.Must(t, "open", "x", path)
	r.app.Must(t, "insert", "x", "5", "100")
	r.app.Must(t, "close", "x")

	ref = r.acquire("users/5")
	defer ref.Release()
	if r.m.Stats.Discards.Load() != 1 || r.m.Stats.Restores.Load() != 1 || r.m.Stats.CachedHits.Load() != 0 {
		t.Fatalf("stats: discards=%d restores=%d hits=%d", r.m.Stats.Discards.Load(), r.m.Stats.Restores.Load(), r.m.Stats.CachedHits.Load())
	}
	if got := r.liveDump(ref.Path()); got != want {
		t.Fatal("the shard should be what blob storage holds, without the unobserved write")
	}
}

// After a restart the cached files that still match their manifests are
// reclaimed by one CAS each and reused. The others are discarded.
func TestRestartReusesMatchingCachedFiles(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	for _, id := range []string{"users/6", "users/7"} {
		ref := r.acquire(id)
		r.write(ref, "a")
		if err := r.m.Gate(ctx, ref, true); err != nil {
			t.Fatal(err)
		}
		r.app.Must(t, "close", "a")
		ref.Release()
		eventually(t, "deactivation", func() bool { return r.cached(id) })
	}
	want6 := r.dumpBlob("users/6")

	// users/7 moves on elsewhere: a delta is added to its manifest by someone else.
	h, err := r.n.Files.Attach(ctx, "users/7")
	if err != nil {
		t.Fatal(err)
	}
	_ = h

	if err := r.m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	r.n.Close(ctx, false)
	r.startNode()
	if r.n.Keeper.Lease().Epoch != 2 {
		t.Fatalf("epoch %d", r.n.Keeper.Lease().Epoch)
	}
	// Make users/7's file stale: its manifest gets a position the file doesn't have.
	h, err = r.n.Files.Attach(ctx, "users/7")
	if err != nil {
		t.Fatal(err)
	}
	next := *h.Manifest
	next.Files = append(append([]string(nil), next.Files...), fmt.Sprintf("%d-%d.1", h.Manifest.LastPosition()+1, h.Manifest.LastPosition()+1))
	if _, err := shardfile.NewManifestStore(r.n.Store).Put(ctx, "users/7", &next, &h.ETag); err != nil {
		t.Fatal(err)
	}

	r.startManager(nil)
	gets := r.calls("get", "shards")
	ref := r.acquire("users/6")
	defer ref.Release()
	if r.m.Stats.CachedHits.Load() != 1 || r.m.Stats.Restores.Load() != 0 {
		t.Fatalf("stats: %+v", &r.m.Stats)
	}
	if r.calls("get", "shards") != gets {
		t.Fatal("LTX files were downloaded for a file that could be reused")
	}
	if got := r.liveDump(ref.Path()); got != want6 {
		t.Fatal("reused file differs")
	}
	m, err := r.manifest("users/6")
	if err != nil || m.NodeEpoch != 2 || m.Owner != "n1" {
		t.Fatalf("manifest not reclaimed: %+v %v", m, err)
	}

	// users/7's manifest is ahead of the file: discarded, and restoring it fails
	// here only because the test made up a file (so check the discard).
	if _, err := r.m.Acquire(ctx, "users/7"); err == nil {
		t.Fatal("expected the made-up position to fail the restore")
	}
	if r.m.Stats.Discards.Load() != 1 {
		t.Fatalf("discards %d", r.m.Stats.Discards.Load())
	}
}

// A crash leaves files for shards that were active. They may be ahead of what
// reached the journal, so they are discarded, and the shard comes back from blob storage.
func TestCrashLeavesActiveFilesToBeDiscarded(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("wallet/2")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	want := r.liveDump(ref.Path())
	r.app.Must(t, "insert", "a", "2", "100") // not gated: may be lost
	path := ref.Path()
	r.app.Must(t, "close", "a")
	r.crash()

	r.startNode()
	r.startManager(nil)
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
	r := newMRig(t)
	ref := r.acquire("users/8")
	r.write(ref, "a")
	r.app.Must(t, "close", "a")
	// A hold (an open connection) keeps the shard active past its 300 ms timeout.
	time.Sleep(900 * time.Millisecond)
	if !r.m.IsActive("users/8") {
		t.Fatal("deactivated while held")
	}
	ref.Release()
	eventually(t, "deactivation after the last hold", func() bool { return r.cached("users/8") })
}

func TestAppHoldingTheShardKeepsItActive(t *testing.T) {
	r := newMRig(t)
	ref := r.acquire("users/9")
	r.write(ref, "a") // the app keeps its connection open
	ref.Release()
	time.Sleep(900 * time.Millisecond) // several attempts, each with a warning
	if !r.m.IsActive("users/9") {
		t.Fatal("deactivated while the app had the file open")
	}
	// It is still captured and usable.
	ref = r.acquire("users/9")
	r.app.Must(t, "insert", "a", "1", "100")
	if err := r.m.Gate(context.Background(), ref, true); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob("users/9") != r.liveDump(ref.Path()) {
		t.Fatal("not captured after the failed release")
	}
	ref.Release()
	r.app.Must(t, "close", "a")
	eventually(t, "deactivation once the app let go", func() bool { return r.cached("users/9") })
}

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	make1 := func(id string) {
		ref := r.acquire(id)
		r.write(ref, "a")
		if err := r.m.Gate(ctx, ref, true); err != nil {
			t.Fatal(err)
		}
		r.app.Must(t, "close", "a")
		ref.Release()
		eventually(t, "deactivation", func() bool { return r.cached(id) })
		time.Sleep(10 * time.Millisecond)
	}
	make1("users/a")
	make1("users/b")
	used, err := r.m.idx.cachedBytes()
	if err != nil {
		t.Fatal(err)
	}
	want := r.dumpBlob("users/a")
	// Room for two and a half: the third pushes the oldest out.
	r.m.SetCacheLimit(used + used/4)
	make1("users/c")
	eventually(t, "eviction", func() bool { return !r.cached("users/a") })
	if !r.cached("users/b") || !r.cached("users/c") {
		t.Fatal("evicted the wrong shard")
	}
	m, err := r.manifest("users/a")
	if err != nil || m.Owner != "" || m.Journal != "" || len(m.Files) == 0 {
		t.Fatalf("manifest of the evicted shard: %+v %v", m, err)
	}
	var left []string
	filepath.WalkDir(r.data, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.Contains(p, "users/a") || strings.Contains(filepath.Base(p), "a.") && strings.HasSuffix(p, ".db") && strings.Contains(p, string(filepath.Separator)+"users"+string(filepath.Separator)) && strings.HasPrefix(filepath.Base(p), "a.") {
			left = append(left, p)
		}
		return nil
	})
	if len(left) != 0 {
		t.Fatalf("files of the evicted shard remain: %v", left)
	}
	// The next request claims it like any unowned shard and restores it.
	r.m.SetCacheLimit(0)
	ref := r.acquire("users/a")
	defer ref.Release()
	if r.m.Stats.Restores.Load() != 4 && r.m.Stats.Restores.Load() < 1 {
		t.Fatalf("stats: %+v", &r.m.Stats)
	}
	if got := r.liveDump(ref.Path()); got != want {
		t.Fatal("restored shard differs")
	}
	if m, _ := r.manifest("users/a"); m.Owner != "n1" {
		t.Fatalf("not claimed: %+v", m)
	}
}

func TestDelete(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("slow/del")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	m1, _ := r.manifest("slow/del")
	path := ref.Path()

	// In use: refused.
	if err := r.m.Delete(ctx, "slow/del"); !errors.Is(err, ErrBusy) {
		t.Fatalf("delete of a shard in use: %v", err)
	}
	ref.Release()
	// The app still has the file open: refused.
	if err := r.m.Delete(ctx, "slow/del"); !errors.Is(err, ErrBusy) {
		t.Fatalf("delete of a shard the app has open: %v", err)
	}
	if !r.m.IsActive("slow/del") {
		t.Fatal("a refused delete deactivated the shard")
	}
	r.app.Must(t, "close", "a")
	if err := r.m.Delete(ctx, "slow/del"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.manifest("slow/del"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v", err)
	}
	for info, err := range r.n.Store.List(ctx, "shards/slow/del/") {
		t.Fatalf("LTX file left: %v %v", info.Key, err)
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("local file left")
	}
	if r.m.IsActive("slow/del") {
		t.Fatal("still active")
	}
	// Deleting what isn't there is done. A later write makes a new, empty shard.
	if err := r.m.Delete(ctx, "slow/del"); err != nil {
		t.Fatal(err)
	}
	ref = r.acquire("slow/del")
	defer ref.Release()
	r.write(ref, "b")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	m2, err := r.manifest("slow/del")
	if err != nil || m2.Generation == m1.Generation || ref.Path() == path {
		t.Fatalf("recreated shard reuses the old generation: %+v %v", m2, err)
	}
}

func TestDeleteOfACachedShard(t *testing.T) {
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("users/10")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, true); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "deactivation", func() bool { return r.cached("users/10") })
	if err := r.m.Delete(ctx, "users/10"); err != nil {
		t.Fatal(err)
	}
	if r.cached("users/10") {
		t.Fatal("still cached")
	}
	if _, err := r.manifest("users/10"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v", err)
	}
}

func TestBackgroundDeltasAndMerges(t *testing.T) {
	r := newMRig(t)
	r.m.cancel()
	r.m.g.Wait()
	r.m.idx.close()
	r.startManager(func(o *ManagerOptions) { o.DeltaAge = 30 * time.Millisecond; o.MaxFiles = 3 })
	ctx := context.Background()
	ref := r.acquire("slow/bg")
	defer ref.Release()
	r.write(ref, "a")
	maxFiles := 0
	for range 12 {
		r.app.Must(t, "insert", "a", "2", "100")
		if err := r.m.Gate(ctx, ref, true); err != nil {
			t.Fatal(err)
		}
		time.Sleep(90 * time.Millisecond)
		if m, err := r.manifest("slow/bg"); err == nil {
			maxFiles = max(maxFiles, len(m.Files))
		}
	}
	m, err := r.manifest("slow/bg")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) == 0 || len(m.Files) > 4 || maxFiles < 3 {
		t.Fatalf("files %v (max seen %d): deltas and merges should keep a handful", m.Files, maxFiles)
	}
	if r.dumpBlob("slow/bg") != r.liveDump(ref.Path()) {
		t.Fatal("restore differs")
	}
}

// Once the lease can't be renewed the node acknowledges nothing: the barrier
// behind /_shardlite/sync fails, and so do gates.
func TestLostLeaseFailsTheBarrier(t *testing.T) {
	r := newMRig(t)
	r.m.cancel()
	r.m.g.Wait()
	r.m.idx.close()
	r.n.Close(context.Background(), false)
	n, err := Start(context.Background(), Config{
		Name: "n1", Backend: r.local, Flush: 20 * time.Millisecond, ManualFlush: true,
		Blob:   blob.Options{Backoff: time.Millisecond},
		Lease:  coord.RegistryOptions{TTL: 400 * time.Millisecond, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: coord.KeeperOptions{Interval: 50 * time.Millisecond, Margin: 100 * time.Millisecond, Logger: nil},
	})
	if err != nil {
		t.Fatal(err)
	}
	r.n = n
	t.Cleanup(func() { n.Close(context.Background(), false) })
	r.startManager(nil)
	ctx := context.Background()
	if err := r.m.Barrier(ctx); err != nil {
		t.Fatal(err)
	}
	var down atomic.Bool
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
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
	if _, err := r.m.Acquire(ctx, "users/x"); !errors.Is(err, coord.ErrLeaseExpired) {
		t.Fatalf("Acquire = %v", err)
	}
}

// A shard the app wrote to without adding a table (only user_version) is kept
// when it deactivates, like any written shard.
func TestHeaderOnlyWriteKeepsAShard(t *testing.T) {
	r := newMRig(t)
	ref := r.acquire("users/uv")
	r.app.Must(t, "openro", "a", ref.Path()) // opens without creating tables
	r.app.Must(t, "exec", "a", "PRAGMA user_version = 7")
	r.app.Must(t, "close", "a")
	ref.Release()
	eventually(t, "idle deactivation", r.inactive("users/uv"))
	if _, err := r.manifest("users/uv"); err != nil {
		t.Fatalf("a user_version write was lost: %v", err)
	}
	if !r.cached("users/uv") {
		t.Fatal("not cached")
	}
	if r.m.Stats.Deletes.Load() != 0 {
		t.Fatal("deleted")
	}
}

func writeFile(path, content string) error { return os.WriteFile(path, []byte(content), 0o644) }

// A sync that uploads two files (Litestream uploads every LTX file since the
// replica's position) queues the second one after the flush cycle's flush. The
// cycle must still finish: it used to wait for a flush only it could do, and
// every sync-on write behind it hung.
func TestCycleFinishesWhenASyncUploadsTwoFiles(t *testing.T) {
	r := newMRig(t)
	ref := r.acquire("wallet/1")
	defer ref.Release()
	r.write(ref, "wallet/1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.m.Barrier(ctx); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		r.app.Must(t, "insert", "wallet/1", "1", "100")
		// A local LTX file nobody has uploaded yet, so the next round uploads two.
		if err := ref.a.cap.Load().DB().Sync(ctx); err != nil {
			t.Fatal(err)
		}
		r.app.Must(t, "insert", "wallet/1", "1", "100")
		// The gate returns when a cycle that began after the commit is complete: the
		// sync's files were queued, the cycle flushed once, and they are in blob storage.
		if err := r.m.Gate(ctx, ref, true); err != nil {
			t.Fatalf("the flush cycle did not finish: %v", err)
		}
		if r.dumpBlob("wallet/1") != r.liveDump(ref.Path()) {
			t.Fatal("commits are not in blob storage after the barrier")
		}
	}
}

// Shards deactivated together share the flush cycle's segments: their final
// syncs queue files and each release waits for the next cycle, it doesn't flush
// the writer itself.
func TestReleasesShareSegments(t *testing.T) {
	const n = 100
	r := newMRigWith(t, slowFlush)
	refs := make([]*Ref, n)
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
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after && strings.HasPrefix(key, "journal/") {
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
			ok, err := r.m.deactivate(ctx, ref.a)
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
