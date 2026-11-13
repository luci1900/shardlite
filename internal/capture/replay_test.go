package capture_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/model/pos"
	"github.com/luci1900/shardlite/internal/storage/journal"
	"github.com/luci1900/shardlite/internal/storage/shardfile"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/fault"
	"github.com/luci1900/shardlite/internal/test/harness"
)

// restartAt is the model's RESTART_AT: a sync of a WAL of more frames restarts it.
const restartAt = 2

// appBusy is the busy timeout, in milliseconds, of the app's connection. The
// replay's steps run one at a time and capture takes the write lock only inside
// a call, so the app never waits on it: only a checkpoint other than a passive
// one waits, for capture's read transaction, and always gives up (the guide's
// apps run only passive ones). The 5000 ms an app sets would make each such
// step of a trace take five seconds for the same outcome.
const appBusy = "100"

// snapshotPages is the pages of the replayed database: the reserved tables'
// (which the create command makes, as the node's activation does), the test
// app's own and the two tables' roots. The model's two pages are the tables'
// (the last two), and the pages before them, which no commit changes, are read
// first.
const snapshotPages = 9

// traceStep is one state of a trace of specs/capture.qnt, as `quint run --mbt
// --out-itf` writes it (see testdata/traces): the action that led to it, what it
// picked, and the model's variables after it.
type traceStep struct {
	action string
	pg     int  // the page a commit writes
	trunc  bool // whether a checkpoint truncates
	vars   map[string]json.RawMessage
}

// loadTrace decodes an ITF trace, whose integers are {"#bigint": "n"} and whose
// variables are named by module (captureProtocol::capture::capSeq).
func loadTrace(t *testing.T, path string) []traceStep {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var itf struct {
		States []map[string]json.RawMessage `json:"states"`
	}
	if err := json.Unmarshal(b, &itf); err != nil {
		t.Fatal(err)
	}
	var steps []traceStep
	for _, raw := range itf.States {
		s := traceStep{vars: map[string]json.RawMessage{}}
		for k, v := range raw {
			s.vars[k[strings.LastIndex(k, "::")+2:]] = v
		}
		if err := json.Unmarshal(s.vars["actionTaken"], &s.action); err != nil {
			t.Fatalf("state %d: no action (was the trace made with --mbt?): %v", len(steps), err)
		}
		var picks struct {
			Pg, T struct {
				Tag   string
				Value json.RawMessage
			}
		}
		if err := json.Unmarshal(s.vars["nondetPicks"], &picks); err != nil {
			t.Fatal(err)
		}
		if picks.Pg.Tag == "Some" {
			s.pg = bigint(t, picks.Pg.Value)
		}
		if picks.T.Tag == "Some" {
			if err := json.Unmarshal(picks.T.Value, &s.trunc); err != nil {
				t.Fatal(err)
			}
		}
		steps = append(steps, s)
	}
	return steps
}

func bigint(t testing.TB, raw json.RawMessage) int {
	t.Helper()
	var v struct {
		N string `json:"#bigint"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(v.N)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// int is a variable of the model that is an integer.
func (s traceStep) int(t testing.TB, v string) int {
	t.Helper()
	return bigint(t, s.vars[v])
}

// length is the length of a variable of the model that is a list.
func (s traceStep) length(t testing.TB, v string) int {
	t.Helper()
	var l []json.RawMessage
	if err := json.Unmarshal(s.vars[v], &l); err != nil {
		t.Fatal(err)
	}
	return len(l)
}

// replayer plays a trace against a real SQLite file through an app's connection
// and capture.
//
// The model starts with a database restored at commit 0, captured up to it, and
// a WAL with no frames, which is what the setup restores. The model's captured
// end n is then the position of the restore plus n, except after a resumed
// release that wrote the whole database, which is one file for however many
// commits it holds (see want).
type replayer struct {
	t        testing.TB
	e        *harness.Env
	app      *apptest.App
	appOpen  bool
	sh       *capture.Shard
	pageSize uint32
	cks      ltx.Checksum // the checksum of the database the files give, as of the last check
	synced   pos.Position // the position as of the last check
	want     pos.Position // the position the model's captured end is, in the code's terms

	// The WAL's generation, seen by its header's salt after each commit.
	salt     [8]byte
	haveSalt bool
	gens     int

	// The snapshot being read: the reads are held at fault.SnapshotPage until the
	// model's step reads the next page, or until the snapshot's letGo closes
	// (the replayer does that before it closes a snapshot the trace left open).
	// abandoned closes when the test ends, for a goroutine left by a failure.
	arrived, proceed, abandoned chan struct{}
	letGo                       chan struct{}
	snap                        *snapshot

	// What the app does while Release waits: the model's steps between a
	// release and its resume.
	inflight []traceStep
}

type snapshot struct {
	position pos.Position
	r        io.ReadCloser
	read     int    // the model's pages read
	body     []byte // what it gave, once every page is read
}

func newReplayer(t testing.TB) *replayer {
	t.Helper()
	r := &replayer{t: t, e: harness.New(t), arrived: make(chan struct{}), proceed: make(chan struct{}), abandoned: make(chan struct{})}
	t.Cleanup(func() { close(r.abandoned) })
	ctx := context.Background()
	// The database the model starts from: made, captured, written as a delta,
	// and restored, so that capture takes it up as an activation does, with a WAL
	// that has no frames.
	app := r.e.StartApp(name)
	h, sh := r.e.NewShard(name)
	// Two tables, one per page of the model, each written by a commit of that page.
	for _, q := range []string{
		"CREATE TABLE t0 (id INTEGER PRIMARY KEY, v)",
		"CREATE TABLE t1 (id INTEGER PRIMARY KEY, v)",
		"INSERT INTO t0 VALUES (1, 0)",
		"INSERT INTO t1 VALUES (1, 0)",
	} {
		app.Must(t, "exec", "a", q)
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	app.Must(t, "close", "a")
	if err := sh.Close(ctx); err != nil {
		t.Fatal(err)
	}
	app.Exit()
	for _, p := range []string{"", "-wal", "-shm"} {
		os.Remove(r.e.Path(name) + p)
	}
	res := restore(t, r.e)

	var err error
	r.sh, err = capture.New(capture.Config{
		TempDir: r.e.Files.Temp, Name: res.Handle.Name, Path: r.e.Path(name), Generation: res.Handle.Manifest.Generation, Writer: r.e.W,
		Restored: res.Capture, RestartFrames: restartAt, At: r.at, Durable: r.durable,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.app = apptest.Start(t)
	r.app.Must(t, "create", "a", r.e.Path(name), appBusy)
	r.appOpen = true
	r.pageSize = 4096
	if v := apptest.Query(t, r.e.Path(name), `PRAGMA page_size`); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatal(err)
		}
		r.pageSize = uint32(n)
	}
	if v := apptest.Query(t, r.e.Path(name), `PRAGMA page_count`); v != strconv.Itoa(snapshotPages) {
		t.Fatalf("the setup makes %s pages, the replay expects %d", v, snapshotPages)
	}
	r.synced = r.sh.Position()
	r.want = r.synced
	r.cks = r.sh.Checksums().Checksum()
	return r
}

// at holds a snapshot's reads: each page waits for the model's step to read it.
func (r *replayer) at(p fault.Point) {
	if p != fault.SnapshotPage {
		return
	}
	select {
	case r.arrived <- struct{}{}:
	case <-r.letGo:
		return
	case <-r.abandoned:
		return
	}
	select {
	case <-r.proceed:
	case <-r.letGo:
	case <-r.abandoned:
	}
}

// durable is what Release waits for: the app's work between the node letting go
// and the check of whether anyone else has the shard open.
func (r *replayer) durable(ctx context.Context) error {
	for _, s := range r.inflight {
		r.appStep(s)
	}
	r.inflight = nil
	return r.e.W.Flush(ctx)
}

func (r *replayer) wait(ch chan struct{}, what string) {
	r.t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		r.t.Fatalf("timed out waiting for %s", what)
	}
}

// mismatchPrefix starts the failure of a check of an outcome, as against a
// trace the replayer can't play or a setup that fails, so that a test of a
// broken variant's trace can tell the replay caught it.
const mismatchPrefix = "mismatch: "

// mismatch fails the replay on an outcome the model or the database disagrees
// with.
func (r *replayer) mismatch(format string, args ...any) {
	r.t.Helper()
	r.t.Fatalf(mismatchPrefix+format, args...)
}

// openApp opens the app's connection if it has none.
func (r *replayer) openApp() {
	r.t.Helper()
	if !r.appOpen {
		r.app.Must(r.t, "create", "a", r.e.Path(name), appBusy)
		r.appOpen = true
	}
}

// appStep is a step of the model that is the app's.
func (r *replayer) appStep(s traceStep) {
	t := r.t
	t.Helper()
	r.openApp()
	switch s.action {
	case "commit":
		// A value no earlier commit wrote, so that the page changes.
		r.app.Must(t, "exec", "a", "UPDATE t"+strconv.Itoa(s.pg)+" SET v = "+strconv.Itoa(s.length(t, "hist")))
		r.observeGeneration(s)
	case "checkpoint":
		mode := "PASSIVE"
		if s.trunc {
			mode = "TRUNCATE"
		}
		r.app.Must(t, "ckpt", "a", mode)
	default:
		t.Fatalf("%s is not the app's step", s.action)
	}
}

// observeGeneration compares the WAL's generation after a commit with the
// model's: SQLite restarts the WAL where the model says it does. The first
// commit is the baseline: it is the WAL's first write.
func (r *replayer) observeGeneration(s traceStep) {
	b := make([]byte, 24)
	f, err := os.Open(r.e.Path(name) + "-wal")
	if err != nil {
		return
	}
	defer f.Close()
	if _, err := io.ReadFull(f, b); err != nil {
		return
	}
	var salt [8]byte
	copy(salt[:], b[16:24])
	if r.haveSalt && salt != r.salt {
		r.gens++
	}
	r.salt, r.haveSalt = salt, true
	// The model counts the first write of the WAL, which is this one.
	if got, want := r.gens, s.int(r.t, "gen")-1; got != want {
		r.mismatch("after a commit the WAL has restarted %d times, the model's generation is %d", got, want)
	}
}

// play plays the steps after the initial state, comparing outcomes after each:
// what capture promises and the rest of the node sees, not the model's variables.
func (r *replayer) play(steps []traceStep) {
	t, ctx := r.t, context.Background()
	for i := 1; i < len(steps); i++ {
		s, prev := steps[i], steps[i-1]
		switch s.action {
		case "commit", "checkpoint":
			r.appStep(s)
		case "sync", "lockedRestart":
			r.want += pos.Position(s.int(t, "capSeq") - prev.int(t, "capSeq"))
			if err := r.sh.Sync(ctx); err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
			r.checkFiles(i, s)
		case "snapStart":
			r.want += pos.Position(s.int(t, "capSeq") - prev.int(t, "capSeq"))
			r.snapStart(i, s)
		case "snapRead":
			r.snapRead(i)
		case "snapClose":
			r.snapClose(i)
		case "releaseStart":
			i = r.release(steps, i)
		default:
			t.Fatalf("step %d: action %s is not replayed", i, s.action)
		}
		if got := r.sh.Position(); got != r.want {
			r.mismatch("step %d (%s): captured position %d, the model's is %d", i, s.action, got, r.want)
		}
	}
	r.endSnapshot()
	if r.sh.Open() {
		if err := r.sh.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

// release plays a release from steps[i], which is its start. The app's steps
// that follow it are done while the release waits, and a resume after them is a
// release the check of who has the shard open refuses, which the app's open
// connection does. Without one the app has closed and the release goes through.
// It returns the index of the last step it played.
func (r *replayer) release(steps []traceStep, i int) int {
	t, ctx := r.t, context.Background()
	s := steps[i]
	j := i + 1
	for j < len(steps) && (steps[j].action == "commit" || steps[j].action == "checkpoint") {
		j++
	}
	if j == len(steps) && j > i+1 {
		// The model has not decided the release: drop it and the app's steps
		// (the sync its start makes is dropped with it).
		t.Logf("step %d: dropped a release still waiting at the end of the trace, and its %d app steps", i, j-i-1)
		return j - 1
	}
	r.want += pos.Position(s.int(t, "capSeq") - steps[i-1].int(t, "capSeq"))
	read := func(*sql.DB) error { return nil }
	if j == len(steps) {
		r.app.Must(t, "close", "a")
		r.appOpen = false
		released, f, err := r.sh.Release(ctx, read, capture.ResumeIfHeld)
		if err != nil || !released {
			r.mismatch("step %d: the model releases, capture: released=%v err=%v", i, released, err)
		}
		if k, ok := f.(*capture.Kept); !ok || k.At != r.want {
			r.mismatch("step %d: released as %#v, the model's captured end is %d", i, f, r.want)
		}
		return i
	}
	if steps[j].action != "resume" {
		t.Fatalf("step %d: a release followed by %s", j, steps[j].action)
	}
	r.openApp()
	r.inflight = steps[i+1 : j]
	released, _, err := r.sh.Release(ctx, read, capture.ResumeIfHeld)
	if err != nil || released {
		r.mismatch("step %d: the model resumes, capture: released=%v err=%v", j, released, err)
	}
	// A resume syncs what is left of the same WAL, or writes the whole database
	// as one file.
	before, after := steps[j-1], steps[j]
	if same := before.int(t, "capGen") == before.int(t, "gen") && before.length(t, "wal") >= before.int(t, "capFrames"); same {
		r.want += pos.Position(after.int(t, "capSeq") - before.int(t, "capSeq"))
	} else {
		r.want++
	}
	r.checkFiles(j, after)
	return j
}

// checkFiles checks a step that captured: the files it wrote are one run of
// commits from where capture was to where it is, they carry the checksums
// before and after, and the capture's checksum is that of the database its
// files give.
func (r *replayer) checkFiles(i int, s traceStep) {
	t, ctx := r.t, context.Background()
	t.Helper()
	if err := r.e.W.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	got := r.sh.Position()
	if got != r.want {
		r.mismatch("step %d (%s): captured position %d, the model's is %d", i, s.action, got, r.want)
	}
	var files []capture.TailFile
	for _, f := range tailOf(t, r.sh).Files {
		if f.Position > r.synced {
			files = append(files, f)
		}
	}
	end := r.synced
	for _, f := range files {
		if f.Position != end+1 {
			r.mismatch("step %d: a file starts at %d after %d", i, f.Position, end)
		}
		end = f.Last()
	}
	if end != got {
		r.mismatch("step %d: the files reach %d, the capture is at %d", i, end, got)
	}
	cks := r.sh.Checksums().Checksum()
	if len(files) > 0 {
		first, last := read(t, files[0]), read(t, files[len(files)-1])
		if c := preApplyChecksum(t, first); c != r.cks {
			r.mismatch("step %d: the first file has %s before it, the database was %s", i, c, r.cks)
		}
		if c := trailerChecksum(t, last); c != cks {
			r.mismatch("step %d: the last file has %s after it, the capture's is %s", i, c, cks)
		}
	} else if cks != r.cks {
		r.mismatch("step %d: no file written and the checksum moved from %s to %s", i, r.cks, cks)
	}
	if want := replayChecksum(t, r.e, r.pageSize); cks != want {
		r.mismatch("step %d: the capture's checksum is %s, the database its files give is %s", i, cks, want)
	}
	r.cks, r.synced = cks, got
}

func read(t testing.TB, f capture.TailFile) []byte {
	t.Helper()
	b, err := io.ReadAll(f.Body.Reader())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// snapStart takes the snapshot, which syncs first. Its reader goes to the page
// the model reads first, after the pages no commit changes.
func (r *replayer) snapStart(i int, s traceStep) {
	t := r.t
	if r.snap != nil {
		t.Fatalf("step %d: a snapshot while one is open", i)
	}
	r.letGo = make(chan struct{})
	p, rd, err := r.sh.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("step %d: %v", i, err)
	}
	r.snap = &snapshot{position: p, r: rd}
	if p != r.want {
		r.mismatch("step %d: the snapshot is at %d, the model's captured end is %d", i, p, r.want)
	}
	for range snapshotPages - 2 {
		r.wait(r.arrived, "a page of the snapshot")
		r.proceed <- struct{}{}
	}
	r.wait(r.arrived, "the model's first page of the snapshot")
	r.checkFiles(i, s)
}

// snapRead lets the snapshot read the model's next page. The last one reads
// the rest, which are all read once the snapshot has been written.
func (r *replayer) snapRead(i int) {
	t := r.t
	if r.snap == nil || r.snap.read >= 2 {
		t.Fatalf("step %d: a snapshot read with none to read", i)
	}
	r.proceed <- struct{}{}
	r.snap.read++
	if r.snap.read == 1 {
		r.wait(r.arrived, "the model's second page of the snapshot")
		return
	}
	done := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(r.snap.r)
		done <- b
	}()
	select {
	case r.snap.body = <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("step %d: the snapshot did not finish", i)
	}
}

// snapClose ends the snapshot.
func (r *replayer) snapClose(i int) {
	t := r.t
	if r.snap == nil || r.snap.read != 2 {
		t.Fatalf("step %d: a snapshot closed before the model read it", i)
	}
	r.endSnapshot()
}

// endSnapshot lets go of the snapshot, if any, and if it was read to the end
// checks its pages: they are the database as it was at the position it began at,
// which is the database the captured files give up to there.
func (r *replayer) endSnapshot() {
	t, snap := r.t, r.snap
	if snap == nil {
		return
	}
	r.snap = nil
	// Let go of the reads held in the hook, then close, which waits for the
	// goroutine writing the snapshot.
	close(r.letGo)
	closed := make(chan struct{})
	go func() {
		snap.r.Close()
		close(closed)
	}()
	r.wait(closed, "the snapshot to close")
	if snap.read != 2 {
		return
	}
	if err := r.e.W.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(t.TempDir(), "at.db")
	r.replayUpTo(want, snap.position)
	wb, err := os.ReadFile(want)
	if err != nil {
		t.Fatal(err)
	}
	dec := ltx.NewDecoder(bytes.NewReader(snap.body))
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	if got := int(dec.Header().Commit) * int(r.pageSize); got != len(wb) {
		r.mismatch("the snapshot holds %d bytes of database, the files give %d", got, len(wb))
	}
	buf := make([]byte, r.pageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); err != nil {
			break
		}
		off := int(ph.Pgno-1) * int(r.pageSize)
		if !bytes.Equal(buf, wb[off:off+int(r.pageSize)]) {
			r.mismatch("page %d of the snapshot at %d is not the database at that position", ph.Pgno, snap.position)
		}
	}
}

// replayUpTo applies the shard's journal entries up to a position onto a new
// file.
func (r *replayer) replayUpTo(path string, limit pos.Position) {
	t := r.t
	t.Helper()
	ap, err := shardfile.NewApplier(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var last pos.Position
	err = journal.Scan(ctx, r.e.Journal, 1, 0, func(seq pos.Seg, en journal.Entry) error {
		if en.Shard.String() != name || en.Last() <= last || en.Last() > limit {
			return nil
		}
		b, err := harness.ReadEntry(ctx, r.e.Journal, seq, en)
		if err != nil {
			return err
		}
		last = en.Last()
		return ap.Apply(bytes.NewReader(b), shardfile.Range{Min: en.Position, Max: en.Last()})
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ap.Finish(); err != nil {
		t.Fatal(err)
	}
}

// Each trace of specs/capture.qnt in testdata/traces is played step by step
// against a real SQLite file and capture, and the outcomes of each step are
// those of the model's.
func TestReplayModelTraces(t *testing.T) {
	for _, p := range tracePaths(t) {
		t.Run(strings.TrimSuffix(filepath.Base(p), ".itf.json"), func(t *testing.T) {
			t.Parallel()
			newReplayer(t).play(loadTrace(t, p))
		})
	}
}

func tracePaths(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob("testdata/traces/*.itf.json")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no traces: %v", err)
	}
	return paths
}

// The counterexamples of the model's broken variants (testdata/traces/variants)
// played against the code. lockNoSync's locked restart skips its sync, so the
// model's captured end falls behind the code's and the replay must catch it.
// noSalts and mapAll break what capture reads, not how far it gets, and the
// replay checks what capture reads against the database rather than the model,
// so they must replay clean: the code stays correct along the paths where those
// designs lose commits. (lockGap has none here: its gap has no counterpart in
// the code.)
func TestReplayCounterexamples(t *testing.T) {
	for variant, caught := range map[string]bool{"lockNoSync": true, "noSalts": false, "mapAll": false} {
		t.Run(variant, func(t *testing.T) {
			t.Parallel()
			steps := loadTrace(t, filepath.Join("testdata/traces/variants", variant+".itf.json"))
			if !caught {
				newReplayer(t).play(steps)
				return
			}
			f := &failures{TB: t}
			done := make(chan struct{})
			go func() {
				defer close(done)
				newReplayer(f).play(steps)
			}()
			select {
			case <-done:
			case <-time.After(time.Minute):
				t.Fatal("timed out waiting for the replay")
			}
			got := f.all()
			if len(got) == 0 {
				t.Fatal("the replay passed a trace the variant goes wrong on")
			}
			if !strings.HasPrefix(got[0], mismatchPrefix) {
				t.Fatalf("the replay failed, but not on an outcome: %s", got[0])
			}
			t.Logf("caught: %s", got[0])
		})
	}
}

// failures is a testing.TB that keeps its failures instead of reporting them. A
// Fatal ends the goroutine it is called from, as testing.T's does.
type failures struct {
	testing.TB
	mu  sync.Mutex
	got []string
}

func (f *failures) add(msg string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, msg)
}

func (f *failures) all() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.got)
}

func (f *failures) Failed() bool                      { return len(f.all()) > 0 }
func (f *failures) Fail()                             { f.add("failed") }
func (f *failures) Error(args ...any)                 { f.add(fmt.Sprint(args...)) }
func (f *failures) Errorf(format string, args ...any) { f.add(fmt.Sprintf(format, args...)) }
func (f *failures) FailNow()                          { f.add("failed"); runtime.Goexit() }
func (f *failures) Fatal(args ...any)                 { f.add(fmt.Sprint(args...)); runtime.Goexit() }
func (f *failures) Fatalf(format string, args ...any) {
	f.add(fmt.Sprintf(format, args...))
	runtime.Goexit()
}

// Between them the traces take every action of the model's protocol, so a
// change to the model that adds one is a trace to make.
func TestReplayTracesTakeEveryAction(t *testing.T) {
	seen := map[string]bool{}
	for _, p := range tracePaths(t) {
		for _, s := range loadTrace(t, p) {
			seen[s.action] = true
		}
	}
	for _, a := range []string{"commit", "checkpoint", "sync", "lockedRestart", "snapStart", "snapRead", "snapClose", "releaseStart", "resume"} {
		if !seen[a] {
			t.Errorf("no trace takes %s", a)
		}
	}
}
