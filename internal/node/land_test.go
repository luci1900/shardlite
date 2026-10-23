package node_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/node"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// logSink is a slog writer that the tests read at the end.
type logSink struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *logSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *logSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// mustStartLogged starts n1 mustStart starts, with a logger that keeps what
// the start said.
func (r *rig) mustStartLogged(t *testing.T) (*node.Node, string) {
	r.t.Helper()
	logs := logSink{}
	n, err := node.Start(context.Background(), node.Config{
		Name:    "n1",
		Data:    r.dir,
		TempDir: r.t.TempDir(),
		Backend: r.local,
		Blob:    blob.Options{Backoff: time.Millisecond},
		Lease:   coord.RegistryOptions{TTL: time.Hour, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper:  coord.KeeperOptions{Interval: 30 * time.Minute, Margin: time.Minute},
		Logger:  slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		r.t.Fatal(err)
	}
	r.tails[n] = &node.TailSet{}
	n.SetTails(r.tails[n])
	r.stops[n] = service.Background(slog.Default(), n.Writer, n.Keeper)
	r.t.Cleanup(r.stops[n])
	return n, logs.String()
}

// die is the process's death: the writer and the keeper stop with the
// context, so whatever send was in flight never resolves and the lease keeps
// the range it had.
func die(t *testing.T, r *rig, n *node.Node) {
	t.Helper()
	r.stops[n]()
	if err := n.Keeper.Check(); err == nil {
		t.Log("the stopped node still thinks its lease is good; the test carries on all the same")
	}
}

// tailHeld holds journal segment creates until release, taking the node's
// stopping as its out: a create the context has left is a write that did not
// resolve.
func tailHeld(t *testing.T, r *rig) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpCreate && strings.HasPrefix(key, journal.Root) {
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	return release
}

// waitLocalCopy waits for the local store's copy of the n-count file to be
// written, which is what the node's local tier holds the segment by.
func waitLocalCopy(t *testing.T, n *node.Node, count uint64) {
	t.Helper()
	select {
	case <-n.Writer.Reached(segment.StoreLocal, count):
	case <-time.After(10 * time.Second):
		t.Fatal("the segment's local copy was not written")
	}
}

// localBytes reads what the local store holds under the number, before any
// landing is to move it.
func localBytes(t *testing.T, n *node.Node, seq pos.Seg) []byte {
	t.Helper()
	rc, err := n.Local.Get(context.Background(), seq)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// journalKeys lists the record's keys for n1's journal, in number order.
func journalKeys(t *testing.T, r *rig) []string {
	t.Helper()
	var keys []string
	for info, err := range r.local.List(context.Background(), journal.Prefix("n1"), "") {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, info.Key)
	}
	return keys
}

// The process dies with a segment its local store holds and the record lacks:
// the record's send was still going when it stopped. The restart lands the
// segment in the record out of the local copy, checked as the bytes leave,
// before it fences above it, so the commits of the segment are there again and
// the next owner gets everything the app wrote.
func TestRestartLandsItsUnlandedTail(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	n1 := r.mustStart()
	s := r.create(n1, "test.users/1")
	r.commit(s, 2)
	next := n1.Writer.NextSeq()
	dead := publishRange(t, n1)
	if dead.JournalNext != next {
		t.Fatalf("the lease's next number is %d, the writer's next is %d", dead.JournalNext, next)
	}

	// The record's create holds. The app commits once more on the same
	// goroutine, the capture queues its file and the sync waits on its
	// segment: the local copy is written beside the held write, and nothing
	// else is.
	release := tailHeld(t, r)
	r.app.Must(r.t, "insert", s.id, "3", "200")
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		if err := s.sh.Sync(context.Background()); err == nil {
			t.Error("the held sync did not wait")
		}
	}()
	waitLocalCopy(t, n1, 3)
	want := apptest.Dump(t, s.path) // every commit the app made, whatever reached the record
	r.stops[n1]()                   // the process dies with its send in flight
	done.Wait()
	release() // nothing of the dead life is left to put: the restart's landing goes through

	// What died with: the local copy is there, the record's number is not.
	if h, err := n1.Local.Head(context.Background(), next); err != nil || h.Size == 0 {
		t.Fatalf("the local copy of segment %d: %+v %v", next, h, err)
	}
	if _, err := n1.Journal.Head(context.Background(), next); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the record at %d: %v, want ErrNotFound", next, err)
	}
	local := localBytes(t, n1, next) // the restart deletes the row once the record has the bytes

	n1b := r.mustStart()
	if n1b.Adopted.Deltas != 1 {
		t.Fatalf("the restart's adoption: %+v", n1b.Adopted)
	}
	fence := n1b.Adopted.Fence
	if fence != next+1 {
		t.Fatalf("the fence took %d, want right above the landed segment at %d", fence, next)
	}
	if n1b.Writer.NextSeq() != fence+1 {
		t.Fatalf("the writer starts at %d, want %d", n1b.Writer.NextSeq(), fence+1)
	}
	// The copy went from the local store to the record, and the local row went
	// with it: the landing is done and cannot be done again with the row gone.
	if _, err := n1b.Local.Head(context.Background(), next); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the landed segment's local copy: %v, want it dropped once the record had it", err)
	}
	if h, err := n1b.Journal.Head(context.Background(), next); err != nil || h.Size == 0 {
		t.Fatalf("the record's segment at %d: %+v %v, want the landed bytes", next, h, err)
	}
	rc, err := n1b.Journal.Get(context.Background(), next)
	if err != nil {
		t.Fatal(err)
	}
	landed, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	rc.Close()
	if !bytes.Equal(landed, local) {
		at := -1
		for i := range landed {
			if i >= len(local) || landed[i] != local[i] {
				at = i
				break
			}
		}
		t.Fatalf("the landed segment's bytes are not the local copy's (%d and %d bytes, first difference at %d)", len(landed), len(local), at)
	}
	// The commits of the held send are served again: the restore reads the
	// manifest's files, which the adoption's delta covers the landed segment
	// through.
	if got := r.restoreDump(n1b, "test.users/1"); got != want {
		t.Fatal("the next life serves the file without the tail that landed")
	}
}

// A segment the landing's check refuses is not landed, and nothing is fenced
// above it: the start fails, the row keeps the copy, and the next start, once
// the segment reads as it was written, lands it where the previous one
// refused to.
func TestABrokenTailFailsTheStartWithoutAFence(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	n1 := r.mustStart()
	s := r.create(n1, "test.users/1")
	r.commit(s, 2)
	next := n1.Writer.NextSeq()
	publishRange(t, n1)

	// The un-landed segment, as the first test holds it.
	release := tailHeld(t, r)
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		r.app.Must(r.t, "insert", s.id, "3", "200")
		if err := s.sh.Sync(context.Background()); err == nil {
			t.Error("the held sync did not wait")
		}
	}()
	waitLocalCopy(t, n1, 3)
	want := apptest.Dump(t, s.path)
	r.stops[n1]()
	done.Wait()
	release() // no create of the dead life is pending: the failing start and the retry go through

	// The segment's bytes go bad in the local store, the way a bit that
	// nothing else touched can.
	saved := chunkData(t, n1, next)
	if _, err := n1.LocalDB.ExecContext(context.Background(),
		"UPDATE segment_chunks SET data = zeroblob(length(data)) WHERE node = 'n1' AND seq = ?", int64(next)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.start(); !errors.Is(err, segment.ErrCorrupt) {
		t.Fatalf("a start over a local copy the landing cannot check: %v, want ErrCorrupt", err)
	}
	// No fence over what the copy holds: the record's journal is what died
	// with the previous life and nothing more.
	if keys := journalKeys(t, r); !slices.Equal(keys, []string{journal.Key("n1", 1), journal.Key("n1", 2)}) {
		t.Fatalf("the record's journal after a refused landing: %v, want the two segments of the old life", keys)
	}
	if h, err := n1.Local.Head(context.Background(), next); err != nil || h.Size == 0 {
		t.Fatalf("the refused landing kept its copy: %+v %v, want the row kept", h, err)
	}

	// Once the segment reads as it was written again, the next start lands it
	// where the refused one left the work.
	restoreChunks(t, n1, next, saved)
	n1b := r.mustStart()
	if n1b.Adopted.Deltas != 1 || n1b.Adopted.Fence != next+1 {
		t.Fatalf("the retry's adoption: %+v (want a fence right above %d)", n1b.Adopted, next)
	}
	if n1b.Writer.NextSeq() != n1b.Adopted.Fence+1 {
		t.Fatalf("the writer starts at %d, want above the fence", n1b.Writer.NextSeq())
	}
	if _, err := n1b.Local.Head(context.Background(), next); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the landed segment's local copy: %v, want it dropped once the record had it", err)
	}
	if got := r.restoreDump(n1b, "test.users/1"); got != want {
		t.Fatal("the retry's start serves the file without the tail it landed")
	}
}

// chunkData is the local store's chunks of the number, from the end first.
func chunkData(t *testing.T, n *node.Node, seq pos.Seg) [][]byte {
	t.Helper()
	rows, err := n.LocalDB.QueryContext(context.Background(),
		"SELECT data FROM segment_chunks WHERE node = 'n1' AND seq = ? ORDER BY idx", int64(seq))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var data [][]byte
	for rows.Next() {
		var b []byte
		if err := rows.Scan(&b); err != nil {
			t.Fatal(err)
		}
		data = append(data, b)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatalf("segment %d has no chunks to break", seq)
	}
	return data
}

// restoreChunks writes the chunks back, so the segment is the order of bytes
// it was again.
func restoreChunks(t *testing.T, n *node.Node, seq pos.Seg, data [][]byte) {
	t.Helper()
	for idx, b := range data {
		if _, err := n.LocalDB.ExecContext(context.Background(),
			"UPDATE segment_chunks SET data = ? WHERE node = 'n1' AND seq = ? AND idx = ?", b, int64(seq), idx); err != nil {
			t.Fatal(err)
		}
	}
}

// An adoption that needed the number the copy was taking had its fence in the
// way: the cutoff was written where the segment would have gone, so the
// landing cannot put anything there and the copy goes with its commits in no
// one's store. The restart says so at error level and starts all the same:
// what the adoption covered is kept, and the loss is the delta it could not
// read, not the way the start went on.
func TestACutoffBeforeTheRestartLosesTheRow(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	n1 := r.mustStart()
	s := r.create(n1, "test.users/1")
	r.commit(s, 2)
	next := n1.Writer.NextSeq()
	dead := publishRange(t, n1) // the range names everything from the first segment on
	before := apptest.Dump(t, s.path)

	release := tailHeld(t, r)
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		r.app.Must(r.t, "insert", s.id, "3", "200")
		if err := s.sh.Sync(context.Background()); err == nil {
			t.Error("the held sync did not wait")
		}
	}()
	waitLocalCopy(t, n1, 3)
	r.stops[n1]()
	done.Wait()
	release() // n2's fence is a create of its own: the hold ends with the death

	// n2 adopts n1's journal while the held write is nowhere. The fence it
	// writes is the first free number, which is the copy's: the cutoff is at
	// the number the landing would have put the segment at.
	reg2 := coord.NewRegistry(r.local, "n2", coord.RegistryOptions{TTL: time.Hour})
	l2, err := reg2.Acquire(context.Background(), coord.NodeInfo{Name: "n2"})
	if err != nil {
		t.Fatal(err)
	}
	files2 := &shardfile.Files{
		Blob:           blob.NewStore(r.local, "n2", l2.Epoch, blob.Options{Backoff: time.Millisecond}),
		Backend:        r.local,
		Node:           "n2",
		Leases:         reg2,
		TempDir:        t.TempDir(),
		JournalOptions: journal.Options{Backoff: time.Millisecond},
	}
	marked, err := reg2.Adopt(context.Background(), dead, l2.Incarnation())
	if err != nil {
		t.Fatal(err)
	}
	rep, err := files2.AdoptJournal(context.Background(), "n1", dead.JournalFrom, dead.JournalNext, nil, nil)
	if err != nil || rep.Deltas != 1 {
		t.Fatalf("n2's adoption of the journal: %+v %v", rep, err)
	}
	if rep.Fence != next {
		t.Fatalf("n2's fence took %d, want the copy's number %d", rep.Fence, next)
	}
	if _, err := reg2.FinishAdoption(context.Background(), marked, marked.Epoch, rep.Fence); err != nil {
		t.Fatal(err)
	}

	// The restart finds the cutoff where its copy's number was: the row goes
	// with a line at error level, the journal starts after the fence, and the
	// copy's bytes stand nowhere.
	n1b, logs := r.mustStartLogged(t)
	for _, want := range []string{"level=ERROR", "not the local copy's", "segment=" + strconv.FormatUint(uint64(next), 10)} {
		if !strings.Contains(logs, want) {
			t.Fatalf("the restart over an adoption's cutoff said nothing about it (missing %q):\n%s", want, logs)
		}
	}
	if n1b.Adopted.Deltas != 0 {
		t.Fatalf("the restart adopted what was adopted: %+v", n1b.Adopted)
	}
	if n1b.Writer.NextSeq() != rep.Fence+2 {
		t.Fatalf("the writer starts at %d, want after the restart's fence above n2's", n1b.Writer.NextSeq())
	}
	if _, err := n1b.Local.Head(context.Background(), next); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the cutoff's row: %v, want it gone with its nowhere-to-go bytes", err)
	}
	if h, err := n1b.Journal.Head(context.Background(), next); err != nil || h.Size != 0 {
		t.Fatalf("the record at %d: %+v %v, want the adoption's fence still there", next, h, err)
	}
	// What the adoption read is what the restore gets: the copy's commit is
	// the loss the cutoff made, and nothing else is.
	if got := r.restoreDump(n1b, "test.users/1"); got != before {
		t.Fatal("the restore gives back more or less than the adoption covered")
	}
}
