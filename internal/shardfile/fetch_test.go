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
// peer what it holds above the run the record landed, copies it in, and
// fences above what landed. The peer here is the node's own local store
// behind another node's name, which is what a peer's disk is to the protocol;
// the HTTP path itself is the mirror store's conformance suite.

// fetchEnv is a record with one shard's commits in it, and the means to take a
// segment out of the record and put it on the peer, as a fan-out the record
// never heard of would have left it.
type fetchEnv struct {
	*testing.T
	e    *harness.Env
	peer *journal.Store // the peer's disk, a mirror of the node's journal
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
	return &fetchEnv{T: t, e: e, peer: journal.NewStore(journal.NewLocal(db.DB, e.Node), e.Node, 1, opts)}
}

// takeOut deletes the record's segment seq and copies it to the peer first,
// with the write id the record's create carried, which is what the peer holds:
// the record's landed run ends one short, and the peer is where the tail is.
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
	if err := x.peer.CreateWithID(ctx, seq, bytes.NewReader(data), head.WriteID); err != nil {
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
		Temp: e.Files.Temp, JournalOptions: journal.Options{Backoff: time.Millisecond}}
}

// fetch runs the fetch alone, as the adoption's step before its fence.
func (x *fetchEnv) fetch(ctx context.Context, from, to pos.Seg, peer *journal.Store) (int, error) {
	return x.adopter().FetchTail(ctx, x.e.Node, from, to, peer, nil)
}

// adoption is one adopter's pass over the journal, with the peer named.
func (x *fetchEnv) adoption(ctx context.Context, next pos.Seg, peer *journal.Store) (shardfile.AdoptReport, error) {
	return x.adopter().AdoptJournal(ctx, x.e.Node, 1, next, peer, nil)
}

// The fetch puts the peer's copy back in the record, with the write id the
// owner's own attempt carried, and the fence goes above what landed.
func TestFetchLandsThePeersRunBeforeTheFence(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	x.takeOut(2)
	next := x.e.W.NextSeq()

	rep, err := x.adoption(ctx, next, x.peer)
	if err != nil || rep.Fetched != 1 || rep.Fence != next || rep.Deltas != 1 {
		t.Fatalf("adoption: %+v %v", rep, err)
	}
	phead, err := x.peer.Head(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	head, err := x.e.Journal.Head(ctx, 2)
	if err != nil {
		t.Fatal("the record never got the peer's copy:", err)
	}
	// The record sees the owner's own attempt: the copy carries the write id
	// the first create used, so a retry of the fetch is that attempt again.
	if head.Size != phead.Size || head.WriteID != phead.WriteID {
		t.Fatalf("the record's segment %+v is not the peer's %+v", head, phead)
	}
	// A fetch repeated after a half-done one copies nothing: the record holds
	// the run to the number the peer's copy ends at.
	if rep, err := x.adoption(ctx, next, x.peer); err != nil || rep.Fetched != 0 {
		t.Fatalf("the fetch ran twice: %+v %v", rep, err)
	}
}

// The run stops where the peer's run ends: a number it does not hold ends the
// copy, and the fetch reports what it did. The record never holds a hole, so
// this is the un-landed tail, and what is past it is lost, which is the
// window the ensemble's bound states.
func TestFetchStopsWhereThePeersRunEnds(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	x.takeOut(2)
	if err := x.peer.Delete(ctx, 2); err != nil {
		t.Fatal(err)
	}
	next := x.e.W.NextSeq()
	if n, err := x.fetch(ctx, 1, next, x.peer); err != nil || n != 0 {
		t.Fatalf("the fetch past an empty peer: %d %v", n, err)
	}
}

// A peer that does not answer costs its question and nothing more: the run
// ends, and the fetch says what it could not learn.
func TestFetchPastAPeerThatDoesNotAnswer(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	x.takeOut(2)
	dead := journal.NewStore(journal.NewMirror(http.DefaultClient, "http://127.0.0.1:1", x.e.Node), x.e.Node, 1,
		journal.Options{Attempts: 1})
	next := x.e.W.NextSeq()
	if n, err := x.fetch(ctx, 1, next, dead); err == nil || n != 0 {
		t.Fatalf("the fetch at a dead peer: %d %v", n, err)
	}
}

// A peer never holds a fence: one that answers an empty segment stops the
// run, and the fetch carries nothing of it in.
func TestFetchStopsInFrontOfAnEmptySegment(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	x.takeOut(2)
	if err := x.peer.Delete(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if err := x.peer.Create(ctx, 2, bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	next := x.e.W.NextSeq()
	if n, err := x.fetch(ctx, 1, next, x.peer); err != nil || n != 0 {
		t.Fatalf("the fetch in front of an empty segment: %d %v", n, err)
	}
}

// The fetch without a peer is nothing at all: the adoption of a node that
// named none, unchanged.
func TestFetchWithoutAPeer(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	next := x.e.W.NextSeq()
	rep, err := x.adoption(ctx, next, nil)
	if err != nil || rep.Fetched != 0 || rep.Deltas != 1 {
		t.Fatalf("adoption without a peer: %+v %v", rep, err)
	}
}

// The fence's own number: a zombie parked at one unresolved create hands the
// peer a segment that a fence then takes the number of. The record holds the
// fence there, the peer the segment, and the fetch stops below the fence's
// number, so it never saw it: the read asks the peer at the journal's fences,
// and the segment's commits join the tail instead of ending the history one
// short.
func TestTheJournalReadAsksThePeerAtTheFences(t *testing.T) {
	t.Parallel()
	x := newFetchEnv(t)
	ctx := context.Background()
	want := restoreDump(t, x.e, "intact.db") // what the journal holds before the loss
	x.takeOut(2)                             // the record's run ends at 1; the peer holds 2, the writer's last segment
	// The lease recorded next=2: the fence goes at the first free number,
	// which is the zombie's own.
	rep, err := x.adoption(ctx, 2, x.peer)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Fence != 2 || rep.Fetched != 0 || rep.AtFence == 0 || rep.Deltas != 1 {
		t.Fatalf("adoption: %+v, want the fence at 2, nothing fetched, and the peer's entries read there", rep)
	}
	// What the delta carried: the adopted journal restores to what it held
	// before the segment was lost.
	if got := restoreDump(t, x.e, "adopted.db"); got != want {
		t.Fatalf("the adopted file lost the segment at the fence:\n%s\nwant\n%s", got, want)
	}
}
