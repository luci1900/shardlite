package shardfile_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/localdb"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// The fetch (docs/internals.md, Takeover): an adoption asks the dead node's
// mirrors what they hold above the run the record landed, copies it in, and
// fences above what landed. The mirror here is the node's own local store
// behind another node's name, which is what a mirror is to the protocol; the
// HTTP path itself is the mirror store's conformance suite.

// fetchEnv is a record with one shard's commits in it, and the means to take a
// segment out of the record and put it on a mirror, as a fan-out the record
// never heard of would have left it.
type fetchEnv struct {
	*testing.T
	e      *harness.Env
	mirror *journal.Store // a mirror of the node's journal
}

func newFetchEnv(t *testing.T) *fetchEnv {
	t.Helper()
	e := harness.New(t)
	app := e.StartApp(shard)
	_, sh := e.NewShard(shard)
	t.Cleanup(func() { sh.Close(context.Background()) })
	write(t, e, app, func() error { return sh.Sync(context.Background()) }, 1)
	write(t, e, app, func() error { return sh.Sync(context.Background()) }, 2)
	db, err := localdb.Memory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	opts := journal.Options{Backoff: time.Millisecond}
	return &fetchEnv{T: t, e: e, mirror: journal.NewStore(journal.NewLocal(db.DB, e.Node), e.Node, 1, opts)}
}

// takeOut deletes the record's segment seq and copies it to the mirror first,
// with the write id the record's create carried, which is what a mirror holds:
// the record's landed run ends one short, and the mirror is where the tail is.
func (x *fetchEnv) takeOut(seq pos.Seg) {
	x.Helper()
	ctx := context.Background()
	head, err := x.e.Journal.Head(ctx, seq)
	if err != nil {
		x.Fatal(err)
	}
	rc, err := x.e.Journal.Get(ctx, seq)
	if err != nil {
		x.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		x.Fatal(err)
	}
	if err := x.mirror.CreateWithID(ctx, seq, bytes.NewReader(data), head.WriteID); err != nil {
		x.Fatal(err)
	}
	if err := x.e.Blob.Delete(ctx, journal.Key(x.e.Node, seq), ""); err != nil {
		x.Fatal(err)
	}
}

// adopter is another node's files over the record, as a peer adoption holds.
func (x *fetchEnv) adopter() *shardfile.Files {
	e := x.e
	return &shardfile.Files{Blob: e.Store, Backend: e.Blob, Node: "n2", Leases: e.Reg,
		TempDir: e.Files.TempDir, JournalOptions: journal.Options{Backoff: time.Millisecond}}
}

// fetch runs the fetch alone, as the adoption's step before its fence.
func (x *fetchEnv) fetch(ctx context.Context, from, to pos.Seg, mirrors ...*journal.Store) (int, error) {
	return x.adopter().FetchTail(ctx, x.e.Node, from, to, mirrors, nil)
}

// adoption is one adopter's pass over the journal, with the mirrors named.
func (x *fetchEnv) adoption(ctx context.Context, next pos.Seg, mirrors ...*journal.Store) (shardfile.AdoptReport, error) {
	return x.adopter().AdoptJournal(ctx, x.e.Node, 1, next, mirrors, nil)
}

// The fetch puts the mirror's copy back in the record, with the write id the
// owner's own attempt carried, and the fence goes above what landed.
func TestFetchLandsTheMirrorsRunBeforeTheFence(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	x.takeOut(2)
	next := x.e.W.NextSeq()

	rep, err := x.adoption(ctx, next, x.mirror)
	if err != nil || rep.Fetched != 1 || rep.Fence != next || rep.Deltas != 1 {
		t.Fatalf("adoption: %+v %v", rep, err)
	}
	mhead, err := x.mirror.Head(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	head, err := x.e.Journal.Head(ctx, 2)
	if err != nil {
		t.Fatal("the record never got the mirror's copy:", err)
	}
	// The record sees the owner's own attempt: the copy carries the write id
	// the first create used, so a retry of the fetch is that attempt again.
	if head.Size != mhead.Size || head.WriteID != mhead.WriteID {
		t.Fatalf("the record's segment %+v is not the mirror's %+v", head, mhead)
	}
	// A fetch repeated after a half-done one copies nothing: the record holds
	// the run to the number the mirror's copy ends at.
	if rep, err := x.adoption(ctx, next, x.mirror); err != nil || rep.Fetched != 0 {
		t.Fatalf("the fetch ran twice: %+v %v", rep, err)
	}
}

// The run stops where the mirrors' run ends: a number none of them holds ends
// the copy, and the fetch reports what it did. The record never holds a hole,
// so this is the un-landed tail, and what is past it is lost, which is the
// window the ensemble's bound states.
func TestFetchStopsWhereTheMirrorsRunEnds(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	x.takeOut(2)
	if err := x.mirror.Delete(ctx, 2); err != nil {
		t.Fatal(err)
	}
	next := x.e.W.NextSeq()
	if n, err := x.fetch(ctx, 1, next, x.mirror); err != nil || n != 0 {
		t.Fatalf("the fetch past an empty mirror: %d %v", n, err)
	}
}

// A mirror that does not answer costs its questions and nothing more: the next
// mirror that holds the segment is asked, and the run lands.
func TestFetchSkipsAMirrorThatDoesNotAnswer(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	x.takeOut(2)
	dead := journal.NewStore(journal.NewMirror(http.DefaultClient, "http://127.0.0.1:1", x.e.Node), x.e.Node, 1,
		journal.Options{Attempts: 1})
	next := x.e.W.NextSeq()
	if n, err := x.fetch(ctx, 1, next, dead, x.mirror); err != nil || n != 1 {
		t.Fatalf("the fetch past a dead mirror: %d %v", n, err)
	}
	// The dead mirror alone ends the run, and says so.
	if n, err := x.fetch(ctx, 1, next, dead); err != nil || n != 0 {
		t.Fatalf("the fetch with only a dead mirror: %d %v", n, err)
	}
}

// A mirror never holds a fence: one that answers an empty segment stops the
// run, and the fetch carries nothing of it in.
func TestFetchStopsInFrontOfAnEmptySegment(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	x.takeOut(2)
	if err := x.mirror.Delete(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if err := x.mirror.Create(ctx, 2, bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	next := x.e.W.NextSeq()
	if n, err := x.fetch(ctx, 1, next, x.mirror); err != nil || n != 0 {
		t.Fatalf("the fetch in front of an empty segment: %d %v", n, err)
	}
}

// The fetch without mirrors is nothing at all: today's adoption, unchanged.
func TestFetchWithoutMirrors(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	next := x.e.W.NextSeq()
	rep, err := x.adoption(ctx, next)
	if err != nil || rep.Fetched != 0 || rep.Deltas != 1 {
		t.Fatalf("adoption without mirrors: %+v %v", rep, err)
	}
}
