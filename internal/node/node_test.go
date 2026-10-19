package node_test

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/node"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

type rig struct {
	t     *testing.T
	local *blob.Hooked
	dir   string
	meta  string
	app   *apptest.App
	tails map[*node.Node]*node.TailSet // the shards each node's lease counts
	stops map[*node.Node]func()        // each node's stop of its writer and keeper
}

func newRig(t *testing.T) *rig {
	t.Helper()
	return &rig{t: t, local: blob.WithHook(blob.NewMemory()), dir: t.TempDir(), meta: t.TempDir(), app: apptest.Start(t)}
}

// start is a node whose lease is never renewed by itself (the tests decide
// what the lease says), with an hour of validity.
func (r *rig) start() (*node.Node, error) {
	n, err := node.Start(context.Background(), node.Config{
		Name:    "n1",
		TempDir: r.t.TempDir(),
		Backend: r.local,
		Blob:    blob.Options{Backoff: time.Millisecond},
		Lease:   coord.RegistryOptions{TTL: time.Hour, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper:  coord.KeeperOptions{Interval: 30 * time.Minute, Margin: time.Minute},
	})
	if err == nil {
		if r.tails == nil {
			r.tails = map[*node.Node]*node.TailSet{}
		}
		r.tails[n] = &node.TailSet{}
		n.SetTails(r.tails[n])
	}
	return n, err
}

func (r *rig) mustStart() *node.Node {
	r.t.Helper()
	n, err := r.start()
	if err != nil {
		r.t.Fatal(err)
	}
	if r.stops == nil {
		r.stops = map[*node.Node]func(){}
	}
	r.stops[n] = service.Background(slog.Default(), n.Writer, n.Keeper)
	r.t.Cleanup(r.stops[n])
	return n
}

type liveShard struct {
	name string
	path string
	id   string
	h    *shardfile.Handle
	sh   *capture.Shard
}

// create makes a shard on n: the app creates the file, the node captures it.
func (r *rig) create(n *node.Node, name string) *liveShard {
	r.t.Helper()
	ctx := context.Background()
	s := &liveShard{name: name, path: filepath.Join(r.dir, strings.ReplaceAll(name, "/", "-")+".db"), id: strings.ReplaceAll(name, "/", "-")}
	r.app.Must(r.t, "open", s.id, s.path)
	h, created, err := n.Files.Open(ctx, name)
	if err != nil || !created {
		r.t.Fatalf("open %s: created=%v err=%v", name, created, err)
	}
	sh, err := capture.New(capture.Config{Name: name, Path: s.path, Generation: h.Manifest.Generation, MetaRoot: r.meta, Writer: n.Writer})
	if err != nil {
		r.t.Fatal(err)
	}
	r.tails[n].Track(sh)
	s.h, s.sh = h, sh
	return s
}

func (r *rig) commit(s *liveShard, k int) {
	r.t.Helper()
	for range k {
		r.app.Must(r.t, "insert", s.id, "2", "100")
		if err := s.sh.Sync(context.Background()); err != nil {
			r.t.Fatalf("sync %s: %v", s.name, err)
		}
	}
}

// publishRange writes the node's current journal range into its lease, as a renewal would.
func publishRange(t *testing.T, n *node.Node) coord.NodeLease {
	t.Helper()
	l, err := n.Registry.Renew(context.Background(), n.Keeper.Lease(), n.Info())
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func (r *rig) restoreDump(n *node.Node, name string) string {
	r.t.Helper()
	dst := filepath.Join(r.t.TempDir(), "r.db")
	if _, err := n.Files.Restore(context.Background(), name, dst, shardfile.RestoreOptions{}); err != nil {
		r.t.Fatalf("restore %s: %v", name, err)
	}
	return sqlitetest.Dump(r.t, dst)
}

// The previous process dies with tails in its journal. The next start fences the
// journal, turns every tail into a delta and moves the lease's range past the fence.
func TestRestartAdoptsItsOwnJournal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		publishRange bool // the lease holds a fresh range, or only the range from the start
	}{{"stale range", false}, {"fresh range", true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r := newRig(t)
			n1 := r.mustStart()

			withDelta := r.create(n1, "users/1")
			tailOnly := r.create(n1, "users/2")
			deleted := r.create(n1, "users/3")
			quiet := r.create(n1, "users/4")
			r.commit(withDelta, 4)
			if err := withDelta.h.WriteDelta(ctx, withDelta.sh, shardfile.Hooks{}); err != nil {
				t.Fatal(err)
			}
			r.commit(withDelta, 3)
			r.commit(tailOnly, 5)
			r.commit(deleted, 2)
			if err := deleted.sh.Close(ctx); err != nil {
				t.Fatal(err)
			}
			// A shard deleted since: its manifest is gone, its entries are never read.
			if err := shardfile.NewManifestStore(n1.Store).Delete(ctx, "users/3", deleted.h.ETag); err != nil {
				t.Fatal(err)
			}
			r.commit(quiet, 1)
			if err := quiet.h.WriteDelta(ctx, quiet.sh, shardfile.Hooks{}); err != nil {
				t.Fatal(err)
			}
			if tc.publishRange {
				l := publishRange(t, n1)
				if l.JournalFrom == 0 || l.JournalNext <= l.JournalFrom {
					t.Fatalf("range %d..%d", l.JournalFrom, l.JournalNext)
				}
			}
			want := map[string]string{
				"users/1": apptest.Dump(t, withDelta.path),
				"users/2": apptest.Dump(t, tailOnly.path),
				"users/4": apptest.Dump(t, quiet.path),
			}
			lastSeg := n1.Writer.NextSeq() - 1

			// The process dies (its writer lives on here, as a zombie would).
			n2 := r.mustStart()
			if got := n2.Keeper.Lease().Epoch; got != 2 {
				t.Fatalf("epoch %d, want 2", got)
			}
			rep := n2.Adopted
			if rep.Deltas != 2 || rep.Fence != lastSeg+1 || rep.Expired {
				t.Fatalf("adoption: %+v (last segment %d)", rep, lastSeg)
			}
			// After the fence.
			if n2.Writer.NextSeq() <= rep.Fence {
				t.Fatalf("writer starts at %d, fence is %d", n2.Writer.NextSeq(), rep.Fence)
			}
			// The lease records the new range at once, not at the first renewal.
			l, err := n2.Registry.Get(ctx, "n1")
			if err != nil || l.JournalFrom != rep.Fence+1 || l.JournalNext != rep.Fence+1 || l.Epoch != 2 {
				t.Fatalf("lease after start: %+v %v", l, err)
			}

			for _, name := range []string{"users/1", "users/2"} {
				m, _, err := n2.Files.GetManifest(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				if m.Journal != "" || m.LastPosition() == 0 {
					t.Fatalf("%s after adoption: %+v", name, m)
				}
				if last, _ := m.List(); last[len(last)-1].Epoch != 2 {
					t.Fatalf("%s: the delta should carry the adopter's epoch: %+v", name, m.Files)
				}
			}

			// Everything the journal held is in files now: restore without the journal at all.
			for seq := uint64(1); seq <= lastSeg; seq++ {
				if err := n2.Store.Delete(ctx, segment.Key("n1", seq), ""); err != nil {
					t.Fatal(err)
				}
			}
			for name, dump := range want {
				if got := r.restoreDump(n2, name); got != dump {
					t.Fatalf("%s restored differently", name)
				}
			}

			// The old writer is a zombie: its next segment finds the fence.
			r.app.Must(t, "insert", withDelta.id, "2", "100")
			if err := withDelta.sh.Sync(ctx); !errors.Is(err, segment.ErrFenced) {
				t.Fatalf("zombie's sync = %v, want ErrFenced", err)
			}
			if got := r.restoreDump(n2, "users/1"); got != want["users/1"] {
				t.Fatal("a write after the fence reached the shard's history")
			}

			// Starting again with nothing in the journal writes nothing.
			n3 := r.mustStart()
			if n3.Adopted.Deltas != 0 || n3.Adopted.Fence <= n2.Adopted.Fence {
				t.Fatalf("second restart: %+v", n3.Adopted)
			}
		})
	}
}

// A start that can't finish the adoption leaves the old range in the lease, so
// the next start does it again.
func TestFailedAdoptionKeepsTheRange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newRig(t)
	n1 := r.mustStart()
	s := r.create(n1, "users/1")
	r.commit(s, 4)
	publishRange(t, n1)
	before, err := n1.Registry.Get(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	want := apptest.Dump(t, s.path)

	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && strings.HasPrefix(key, shardfile.FilesPrefix) {
			return errors.New("storage refuses deltas")
		}
		return nil
	})
	if _, err := r.start(); err == nil {
		t.Fatal("start succeeded without writing the delta")
	}
	l, err := n1.Registry.Get(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if l.JournalFrom != before.JournalFrom || l.JournalNext != before.JournalNext || l.Epoch != 3-1 {
		t.Fatalf("lease after a failed start: %+v, before %+v", l, before)
	}

	r.local.SetHook(nil)
	n3 := r.mustStart()
	if n3.Adopted.Deltas != 1 || n3.Keeper.Lease().Epoch != 3 {
		t.Fatalf("second start: %+v epoch %d", n3.Adopted, n3.Keeper.Lease().Epoch)
	}
	if got := r.restoreDump(n3, "users/1"); got != want {
		t.Fatal("restore differs")
	}
}

// If the segments the lease names have expired, the adoption starts where the
// listing says. A shard whose tail has a hole is reported as lost and the node
// still starts.
func TestAdoptionWithExpiredSegments(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newRig(t)
	n1 := r.mustStart()
	s := r.create(n1, "users/1")
	ok := r.create(n1, "users/2")
	r.commit(s, 3) // each sync waits for its own segment
	r.commit(ok, 1)
	r.stops[n1]()
	// The lease says the journal runs from segment 1 up to the writer's next number (a day later, say).
	l1 := n1.Keeper.Lease()
	info := n1.Info()
	info.JournalFrom = 1
	if _, err := n1.Registry.Renew(ctx, l1, info); err != nil {
		t.Fatal(err)
	}
	// The first segment (which the lease still names) has expired: it held users/1's first commit.
	if err := n1.Store.Delete(ctx, segment.Key("n1", 1), ""); err != nil {
		t.Fatal(err)
	}
	n2 := r.mustStart()
	rep := n2.Adopted
	if !rep.Expired || rep.From <= 1 {
		t.Fatalf("adoption: %+v", rep)
	}
	if len(rep.Lost) != 1 || rep.Lost[0] != "users/1" || len(rep.Problems) != 0 {
		t.Fatalf("lost %v problems %v", rep.Lost, rep.Problems)
	}
	// The lease moved on all the same.
	l, err := n2.Registry.Get(ctx, "n1")
	if err != nil || l.JournalFrom != rep.Fence+1 {
		t.Fatalf("lease: %+v %v", l, err)
	}
}

// A segment missing from the middle of the range isn't expiry (that takes the
// oldest first), it is a bug or a damaged journal, so it stops the start.
func TestHoleInTheMiddleOfTheRangeFailsTheStart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newRig(t)
	n1 := r.mustStart()
	s := r.create(n1, "users/1")
	r.commit(s, 4) // one segment each
	l1 := n1.Keeper.Lease()
	if _, err := n1.Registry.Renew(ctx, l1, n1.Info()); err != nil {
		t.Fatal(err)
	}
	info := n1.Info()
	if info.JournalFrom > 1 {
		t.Fatalf("range starts at %d", info.JournalFrom)
	}
	r.stops[n1]()
	// A segment in the middle of the range disappears.
	if err := n1.Store.Delete(ctx, segment.Key("n1", info.JournalFrom+2), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := r.start(); !errors.Is(err, segment.ErrMissing) {
		t.Fatalf("start with a segment missing from the middle of the range: %v", err)
	}
}
