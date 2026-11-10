package shardfile_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// The tests here drive the writers and watch what they leave in storage: what
// a write whose update loses its compare-and-swap deletes, what a rebase's
// sweep reclaims, and what each failure reports (docs/internals.md, Garbage).

// garbageRig is an Env whose blob operations are recorded and can be made to
// fail, lose their response or race another writer, with the Files' leaks
// counted and their log lines kept.
type garbageRig struct {
	*harness.Env
	log   bytes.Buffer
	leaks notify.Counter
	ops   blobLog
}

// blobLog records the LTX files that land and the deletes that are attempted,
// and holds the fault switches: each "while set" one fails a kind of operation
// until the test clears it, and each one-shot one fires once.
type blobLog struct {
	mu  sync.Mutex
	out []string

	failUpdates bool // every update of the manifest fails while set
	// supersede makes another handle change the manifest between the writer's
	// read and its update (before the update applies), once.
	supersede atomic.Bool
	// loseUpdate loses the response of the manifest's update once it has
	// applied, once. supersedeHead changes the manifest from another handle at
	// the store's first read-back of that lost update, once, and failHeads
	// keeps the store from settling its own write while set: the write's
	// outcome stays unknown to the store, so the shardfile layer settles it.
	loseUpdate    atomic.Bool
	supersedeHead atomic.Bool
	failHeads     atomic.Bool
	failDrops     bool // deletes of the shard's LTX files fail while set
	failList      bool // listings of the shard's files fail while set
	// failManifestGets and failLeaseGets fail the reads of the shard's
	// manifest and of the node's lease while set.
	failManifestGets atomic.Bool
	failLeaseGets    atomic.Bool
}

func (r *garbageRig) add(s string) {
	r.ops.mu.Lock()
	defer r.ops.mu.Unlock()
	r.ops.out = append(r.ops.out, s)
}

// count counts the recorded operations that start with prefix.
func (r *garbageRig) count(prefix string) int {
	r.ops.mu.Lock()
	defer r.ops.mu.Unlock()
	n := 0
	for _, s := range r.ops.out {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}

// changeManifest changes the shard's manifest from another handle, as another
// writer would between this one's read and its update.
func (r *garbageRig) changeManifest(ctx context.Context) error {
	other, err := r.Files.Attach(ctx, shardName(shard))
	if err != nil {
		return err
	}
	return other.SetReminder(ctx, dueindex.MinuteAt(time.Now().Add(time.Hour)), false)
}

func (r *garbageRig) hook() blobtest.Hook {
	manifest := shardfile.ManifestKey(shardName(shard))
	files := shardfile.ShardPrefix(shardName(shard))
	leaseKey := lease.Key(r.Node)
	return func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if after {
			if op == blobtest.OpCreate && strings.HasPrefix(key, shardfile.FilesPrefix) {
				r.add("file " + key)
			}
			if op == blobtest.OpUpdate && key == manifest && r.ops.loseUpdate.CompareAndSwap(true, false) {
				return errors.New("response lost")
			}
			return nil
		}
		switch {
		case op == blobtest.OpDelete && strings.HasPrefix(key, files):
			r.add("drop " + key)
			if r.ops.failDrops {
				return errors.New("storage is down")
			}
		case op == blobtest.OpGet && key == manifest && r.ops.failManifestGets.Load(),
			op == blobtest.OpGet && key == leaseKey && r.ops.failLeaseGets.Load():
			return errors.New("storage is down")
		case op == blobtest.OpList && strings.HasPrefix(key, files):
			if r.ops.failList {
				return errors.New("storage is down")
			}
		case op == blobtest.OpHead && key == manifest:
			if r.ops.supersedeHead.CompareAndSwap(true, false) {
				if err := r.changeManifest(ctx); err != nil {
					return err
				}
				return errors.New("storage is down")
			}
			if r.ops.failHeads.Load() {
				return errors.New("storage is down")
			}
		case op == blobtest.OpUpdate && key == manifest:
			if r.ops.supersede.CompareAndSwap(true, false) {
				if err := r.changeManifest(ctx); err != nil {
					return err
				}
			}
			if r.ops.failUpdates {
				return errors.New("storage is down")
			}
		}
		return nil
	}
}

func newGarbageRig(t *testing.T) *garbageRig {
	t.Helper()
	r := &garbageRig{Env: harness.New(t)}
	r.Files.Leaked = &r.leaks
	r.Files.Log = slog.New(slog.NewTextHandler(&r.log, nil))
	r.Blob.SetHook(r.hook())
	return r
}

// keys is every object under a prefix, in key order.
func (r *garbageRig) keys(t *testing.T, prefix string) []string {
	t.Helper()
	var out []string
	for info, err := range r.Store.List(context.Background(), prefix) {
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, info.Key)
	}
	slices.Sort(out)
	return out
}

// A write whose manifest update is rejected because the manifest changed
// deletes the file it uploaded: the manifest changed under the handle, so no
// update of its can ever list it.
func TestADeltaWhoseUpdateLosesToAnotherWriterLeavesNoFile(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	r.ops.supersede.Store(true)

	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("got %v, want the update to lose to the other writer", err)
	}
	if keys := r.keys(t, shardfile.ShardPrefix(shardName(shard))); len(keys) != 0 {
		t.Fatalf("the lost delta left %v behind", keys)
	}
	if n := r.count("drop"); n != 1 {
		t.Fatalf("%d deletes, want the one best-effort delete", n)
	}
	if r.leaks.Load() != 0 {
		t.Fatalf("%d leaks, want none: the delete worked", r.leaks.Load())
	}
}

// The same write as a rebase's base: the base goes when the swap loses, and
// the files the manifest lists stay.
func TestARebaseWhoseUpdateLosesToAnotherWriterLeavesNoBase(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	for i := 1; i <= 2; i++ {
		ack(t, app, sh, i)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	r.ops.supersede.Store(true)

	if did, err := h.Rebase(ctx, sh, shardfile.Hooks{}); did || !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("rebase %v %v, want the swap to lose to the other writer", did, err)
	}
	gen := h.Manifest.Generation
	want := make([]string, 0, len(h.Manifest.Files))
	for _, f := range h.Manifest.Files {
		want = append(want, shardfile.FileKey(shardName(shard), gen, f))
	}
	if keys := r.keys(t, shardfile.ShardPrefix(shardName(shard))); !slices.Equal(keys, want) {
		t.Fatalf("objects %v, want the listed deltas %v and no base", keys, want)
	}
}

// The same write as an adoption's delta. The adoption looks again after the
// conflict and writes its delta again, so the generation ends with the one
// file the manifest lists.
func TestAnAdoptionDeltaWhoseUpdateLosesToAnotherWriterLeavesNoFile(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	write(t, r.Env, app, func() error { return sh.Sync(ctx) }, 0)
	r.ops.supersede.Store(true)

	rep, err := r.Files.AdoptJournal(ctx, r.Node, 1, r.W.NextSeq(), nil)
	if err != nil || rep.Deltas != 1 {
		t.Fatalf("adoption %+v %v, want one delta on the retry", rep, err)
	}
	m, _, err := r.Files.GetManifest(ctx, shardName(shard))
	if err != nil || len(m.Files) != 1 {
		t.Fatalf("manifest %+v (%v), want the one delta listed", m, err)
	}
	if keys := r.keys(t, shardfile.ShardPrefix(shardName(shard))); len(keys) != 1 || keys[0] != shardfile.FileKey(shardName(shard), h.Manifest.Generation, m.Files[0]) {
		t.Fatalf("objects %v, want the one listed delta", keys)
	}
}

// A manifest update that fails any other way than a conflict keeps the file:
// a transient failure is retried with the same name and write id, so the retry
// recognises its own object and uploads nothing.
func TestATransientFailureKeepsTheFileAndTheRetryReusesIt(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	r.ops.failUpdates = true

	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err == nil {
		t.Fatal("the write went through although the update failed")
	}
	keys := r.keys(t, shardfile.ShardPrefix(shardName(shard)))
	if len(keys) != 1 {
		t.Fatalf("objects %v, want the uploaded file kept", keys)
	}
	before, err := r.Store.Head(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	r.ops.failUpdates = false
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if n := r.count("file "); n != 1 {
		t.Fatalf("%d uploads for the write and its retry, want one", n)
	}
	after, err := r.Store.Head(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if after.ETag != before.ETag || after.WriteID != before.WriteID {
		t.Fatal("the retry uploaded the file again")
	}
	if n := r.count("drop"); n != 0 {
		t.Fatalf("%d deletes, want none", n)
	}
	if len(h.Manifest.Files) != 1 {
		t.Fatalf("files %v, want the retry's delta listed", h.Manifest.Files)
	}
}

// An update that lands but loses its response is recognised as this writer's
// own, so nothing is deleted and the file stays listed.
func TestAnUpdateThatLosesItsResponseDeletesNothing(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	r.ops.loseUpdate.Store(true)

	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatalf("the write failed: %v", err)
	}
	if h.Unsettled() {
		t.Fatal("the outcome stayed unknown")
	}
	if n := r.count("drop"); n != 0 {
		t.Fatalf("%d deletes, want none", n)
	}
	if len(h.Manifest.Files) != 1 {
		t.Fatalf("files %v, want the delta listed", h.Manifest.Files)
	}
	if keys := r.keys(t, shardfile.ShardPrefix(shardName(shard))); len(keys) != 1 {
		t.Fatalf("objects %v, want the one listed delta", keys)
	}
}

// An update whose outcome stays unknown and turns out to have landed and been
// superseded (ErrMovedOn) may be listed by the manifest that replaced it: the
// file is data, and nothing is deleted.
func TestAMovedOnUpdateDeletesNothing(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	r.ops.loseUpdate.Store(true)
	r.ops.supersedeHead.Store(true)
	r.ops.failHeads.Store(true)

	err := h.WriteDelta(ctx, sh, shardfile.Hooks{})
	if !errors.Is(err, shardfile.ErrMovedOn) {
		t.Fatalf("got %v, want ErrMovedOn", err)
	}
	r.ops.failHeads.Store(false)
	if n := r.count("drop"); n != 0 {
		t.Fatalf("%d deletes, want none: the delta may be listed", n)
	}
	m, _, err := r.Files.GetManifest(ctx, shardName(shard))
	if err != nil || len(m.Files) != 1 {
		t.Fatalf("manifest %+v (%v), want the other writer kept the delta listed", m, err)
	}
	if keys := r.keys(t, shardfile.ShardPrefix(shardName(shard))); len(keys) != 1 {
		t.Fatalf("objects %v, want the one listed delta", keys)
	}
}

// A conflict can hide this writer's own update: the first attempt lands and
// loses its response, another writer then changes the manifest (keeping the
// delta it lists), and the store's retry sees a manifest that is not its own.
// The write fails with the conflict, but the stored manifest lists the delta,
// so it is data and stays.
func TestAConflictThatHidesALandedUpdateDeletesNothing(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	r.ops.loseUpdate.Store(true)
	r.ops.supersedeHead.Store(true)

	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("got %v, want a conflict that hides the landed update", err)
	}
	if n := r.count("drop"); n != 0 {
		t.Fatalf("%d deletes, want none: the stored manifest lists the delta", n)
	}
	m, _, err := r.Files.GetManifest(ctx, shardName(shard))
	if err != nil || len(m.Files) != 1 {
		t.Fatalf("manifest %+v (%v), want the other writer kept the delta listed", m, err)
	}
	if keys := r.keys(t, shardfile.ShardPrefix(shardName(shard))); len(keys) != 1 || keys[0] != shardfile.FileKey(shardName(shard), m.Generation, m.Files[0]) {
		t.Fatalf("objects %v, want the one listed delta", keys)
	}
}

// A conflict whose stored manifest can't then be read deletes nothing, since
// nothing says whether the file is listed, and reports the file as a leak.
func TestAFailedReadAfterAConflictReportsALeak(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	// Another writer changes the manifest once the file is up, and from then
	// on the manifest can't be read.
	supersede := func() error {
		if err := r.changeManifest(ctx); err != nil {
			return err
		}
		r.ops.failManifestGets.Store(true)
		return nil
	}

	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{AfterFileWrite: supersede}); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("got %v, want the update's conflict", err)
	}
	r.ops.failManifestGets.Store(false)
	if n := r.count("drop"); n != 0 {
		t.Fatalf("%d deletes, want none on an unread manifest", n)
	}
	keys := r.keys(t, shardfile.ShardPrefix(shardName(shard)))
	if len(keys) != 1 {
		t.Fatalf("objects %v, want the file kept", keys)
	}
	if r.leaks.Load() != 1 {
		t.Fatalf("%d leaks, want one", r.leaks.Load())
	}
	if msg := r.log.String(); !strings.Contains(msg, keys[0]) {
		t.Fatalf("the leak was not logged with its key:\n%s", msg)
	}
}

// A best-effort delete that fails is reported as a leak and the write still
// fails with its own error, the update's.
func TestAFailedBestEffortDeleteReportsALeakAndKeepsTheUpdateError(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	r.ops.supersede.Store(true)
	r.ops.failDrops = true

	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("got %v, want the update's conflict, not the delete's failure", err)
	}
	keys := r.keys(t, shardfile.ShardPrefix(shardName(shard)))
	if len(keys) != 1 {
		t.Fatalf("objects %v, want the undeleted file", keys)
	}
	if r.leaks.Load() != 1 {
		t.Fatalf("%d leaks, want one", r.leaks.Load())
	}
	if msg := r.log.String(); !strings.Contains(msg, keys[0]) {
		t.Fatalf("the leak was not logged with its key:\n%s", msg)
	}
}

// A rebase's sweep deletes not only the files the rebase supersedes but the
// delta an earlier crash left unlisted in the generation, keeps every file the
// new manifest lists, skips a file its own handle holds a write id for (an
// upload waiting on its update) and leaves other generations alone.
func TestARebaseSweepsItsGeneration(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	for i := 1; i <= 2; i++ {
		ack(t, app, sh, i)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	// A crash between the upload and its update: the file is unlisted, and
	// this handle keeps its write id as one waiting on a retry would.
	crash := errors.New("crash")
	crashedDelta := func(h *shardfile.Handle, n int) shardfile.File {
		t.Helper()
		prev := h.Manifest.LastPosition()
		ack(t, app, sh, n)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{AfterFileWrite: func() error { return crash }}); !errors.Is(err, crash) {
			t.Fatalf("crashed delta: %v", err)
		}
		return shardfile.File{Range: shardfile.Range{Min: prev + 1, Max: sh.Position()}, Epoch: r.Store.Epoch()}
	}
	left := crashedDelta(h, 3) // this handle's leftover: no write id of h2's
	if _, err := r.Store.Head(ctx, shardfile.FileKey(shardName(shard), h.Manifest.Generation, left)); err != nil {
		t.Fatalf("the crashed delta left no file behind: %v", err)
	}
	h2, err := r.Files.Attach(ctx, shardName(shard))
	if err != nil {
		t.Fatal(err)
	}
	held := crashedDelta(h2, 4) // this handle's own write id is for the whole tail

	other := shardfile.GenerationPrefix(shardName(shard), "other") + "1-1.9.ltx"
	if _, err := r.Store.Create(ctx, other, strings.NewReader("junk")); err != nil {
		t.Fatal(err)
	}

	if did, err := h2.Rebase(ctx, sh, shardfile.Hooks{}); !did || err != nil {
		t.Fatalf("rebase %v %v", did, err)
	}
	gen := h2.Manifest.Generation
	base := shardfile.File{Range: shardfile.Range{Min: 1, Max: sh.Position()}, Epoch: r.Store.Epoch()}
	want := []string{other, shardfile.FileKey(shardName(shard), gen, base), shardfile.FileKey(shardName(shard), gen, held)}
	slices.Sort(want)
	if keys := r.keys(t, shardfile.ShardPrefix(shardName(shard))); !slices.Equal(keys, want) {
		t.Fatalf("objects %v, want %v: the superseded files and the crashed delta gone, the base, the held file and the other generation kept", keys, want)
	}
	if len(h2.Manifest.Files) != 1 || h2.Manifest.Files[0] != base {
		t.Fatalf("files %v, want the base alone", h2.Manifest.Files)
	}
	if n := r.count("drop"); n != 3 { // the two listed deltas and the first handle's leftover
		t.Fatalf("%d deletes, want the three unlisted files", n)
	}
	if r.leaks.Load() != 0 {
		t.Fatalf("%d leaks, want none", r.leaks.Load())
	}
}

// A sweep deletes nothing once another incarnation has marked the node's lease,
// as an adopter does before it uploads into the node's shards: the adopter's
// file is unlisted until its update, and the sweep of a zombie must not take
// it. Nothing is reported, since the shard's new owner sweeps the generation at
// its next rebase.
func TestASweepDeletesNothingOnceTheLeaseIsTaken(t *testing.T) {
	t.Parallel()
	r := newGarbageRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	for i := 1; i <= 2; i++ {
		ack(t, app, sh, i)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	var adopters string
	adopt := func() error {
		l, err := r.Reg.Get(ctx, r.Node)
		if err != nil {
			return err
		}
		if _, err := r.Reg.Adopt(ctx, *l, lease.Incarnation{Node: "n2", Epoch: 1}); err != nil {
			return err
		}
		f := shardfile.File{Range: shardfile.Range{Min: sh.Position() + 1, Max: sh.Position() + 1}, Epoch: r.Store.Epoch() + 1}
		adopters = shardfile.FileKey(shardName(shard), h.Manifest.Generation, f)
		_, err = r.Store.Create(ctx, adopters, strings.NewReader("an adopter's delta on its way to the manifest"))
		return err
	}

	if did, err := h.Rebase(ctx, sh, shardfile.Hooks{AfterRebaseSwap: adopt}); !did || err != nil {
		t.Fatalf("rebase %v %v", did, err)
	}
	if n := r.count("drop"); n != 0 {
		t.Fatalf("%d deletes, want none from a node whose lease is taken", n)
	}
	if _, err := r.Store.Head(ctx, adopters); err != nil {
		t.Fatalf("the adopter's file is gone: %v", err)
	}
	if n := len(r.keys(t, shardfile.ShardPrefix(shardName(shard)))); n != 4 {
		t.Fatalf("%d objects, want the base, the two superseded deltas and the adopter's", n)
	}
	if r.leaks.Load() != 0 {
		t.Fatalf("%d leaks, want none", r.leaks.Load())
	}
}

// A sweep whose deletes fail, or whose listing or lease read fails, reports
// each failure as a leak and the rebase still reports its swap as done.
func TestAFailingSweepReportsLeaksAndTheSwapStaysDone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	deltas := func(t *testing.T, r *garbageRig, app *apptest.App, h *shardfile.Handle, sh *capture.Shard) {
		t.Helper()
		for i := 1; i <= 2; i++ {
			ack(t, app, sh, i)
			if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("deletes fail", func(t *testing.T) {
		t.Parallel()
		r := newGarbageRig(t)
		app := r.StartApp(shard)
		h, sh := r.NewShard(shard)
		defer sh.Close(ctx)
		deltas(t, r, app, h, sh)
		r.ops.failDrops = true

		if did, err := h.Rebase(ctx, sh, shardfile.Hooks{}); !did || err != nil {
			t.Fatalf("rebase %v %v, want the swap done whatever the sweep did", did, err)
		}
		r.ops.failDrops = false
		if r.leaks.Load() != 2 {
			t.Fatalf("%d leaks, want one per undeleted file", r.leaks.Load())
		}
		if len(h.Manifest.Files) != 1 {
			t.Fatalf("files %v, want the base alone", h.Manifest.Files)
		}
		if n := len(r.keys(t, shardfile.ShardPrefix(shardName(shard)))); n != 3 {
			t.Fatalf("%d objects, want the base and the two the sweep could not delete", n)
		}
		if msg := r.log.String(); !strings.Contains(msg, "could not delete an LTX file") {
			t.Fatalf("the leaks were not logged:\n%s", msg)
		}
	})

	t.Run("the listing fails", func(t *testing.T) {
		t.Parallel()
		r := newGarbageRig(t)
		app := r.StartApp(shard)
		h, sh := r.NewShard(shard)
		defer sh.Close(ctx)
		deltas(t, r, app, h, sh)
		r.ops.failList = true

		if did, err := h.Rebase(ctx, sh, shardfile.Hooks{}); !did || err != nil {
			t.Fatalf("rebase %v %v, want the swap done whatever the sweep did", did, err)
		}
		r.ops.failList = false
		if r.leaks.Load() != 1 {
			t.Fatalf("%d leaks, want one for the listing", r.leaks.Load())
		}
		if n := len(r.keys(t, shardfile.ShardPrefix(shardName(shard)))); n != 3 {
			t.Fatalf("%d objects, want nothing deleted", n)
		}
		if msg := r.log.String(); !strings.Contains(msg, "could not list the shard's generation") {
			t.Fatalf("the leak was not logged:\n%s", msg)
		}
	})

	t.Run("the lease read fails", func(t *testing.T) {
		t.Parallel()
		r := newGarbageRig(t)
		app := r.StartApp(shard)
		h, sh := r.NewShard(shard)
		defer sh.Close(ctx)
		deltas(t, r, app, h, sh)
		failLease := func() error { r.ops.failLeaseGets.Store(true); return nil }

		if did, err := h.Rebase(ctx, sh, shardfile.Hooks{AfterRebaseSwap: failLease}); !did || err != nil {
			t.Fatalf("rebase %v %v, want the swap done whatever the sweep did", did, err)
		}
		r.ops.failLeaseGets.Store(false)
		if r.leaks.Load() != 1 {
			t.Fatalf("%d leaks, want one for the lease", r.leaks.Load())
		}
		if n := r.count("drop"); n != 0 {
			t.Fatalf("%d deletes, want none on an unread lease", n)
		}
		if msg := r.log.String(); !strings.Contains(msg, "could not read the node's lease") {
			t.Fatalf("the leak was not logged:\n%s", msg)
		}
	})
}
