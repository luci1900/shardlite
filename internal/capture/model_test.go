//go:build model

package capture_test

import (
	"encoding/json"
	"flag"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/luci1900/shardlite/internal/quinttest"
)

// The tests that need Quint. The model suite runs them (see the testwatch
// README), and each fails rather than skips without Quint (see quinttest), so
// that a missing tool never reads as a pass.

var update = flag.Bool("update", false, "generate the traces in testdata/traces from the model and write the ones that changed")

// A trace is the first one the simulator finds, for a seed, that violates a
// check of the model, so it ends where the check says, with --mbt so that every
// state carries the action taken. The checks of the first table are
// reachability checks (a val named never..., which should fail). The model's
// RESTART_AT is restartAt in replay_test.go.
type witness struct {
	name  string // the file, without .itf.json, under testdata/traces
	check string // the invariant that is violated
	steps int
	seed  int
	main  string // the model's instance, captureProtocol when empty
}

var witnesses = []witness{
	{name: "commit-release", check: "neverReleasedAfterOneCommit", steps: 2, seed: 1},
	{name: "locked-restart", check: "neverThreeFramesCopied", steps: 12, seed: 3},
	{name: "locked-restart-around-a-snapshot", check: "neverThreeFramesCopied", steps: 14, seed: 17},
	{name: "locked-restart-after-a-snapshot", check: "neverThreeFramesCopied", steps: 14, seed: 4242},
	{name: "resumed-release", check: "neverThreeFramesCopied", steps: 14, seed: 101},
	{name: "snapshot-after-restart-1", check: "neverSnapshotAfterRestart", steps: 16, seed: 101},
	{name: "snapshot-after-restart-2", check: "neverSnapshotAfterRestart", steps: 16, seed: 17},
	{name: "snapshot-after-restart-3", check: "neverSnapshotAfterRestart", steps: 16, seed: 4242},
	{name: "snapshot-after-restart-4", check: "neverSnapshotAfterRestart", steps: 16, seed: 99991},
	// The app truncates the WAL while a release waits, and a snapshot follows the resume.
	{name: "snapshot-after-truncate", check: "neverSnapshotAfterRestart", steps: 18, seed: 237570},
	{name: "at-rest-1", check: "neverFourCapturedAtRest", steps: 16, seed: 101},
	{name: "at-rest-2", check: "neverFourCapturedAtRest", steps: 16, seed: 17},
	{name: "at-rest-3", check: "neverFourCapturedAtRest", steps: 16, seed: 3},
	{name: "at-rest-4", check: "neverFourCapturedAtRest", steps: 16, seed: 4242},
	{name: "at-rest-5", check: "neverFourCapturedAtRest", steps: 16, seed: 99991},
	// A trace may end inside a snapshot or a release: the replay lets the snapshot go and drops the release.
	{name: "snapshot-half-read", check: "neverSnapshotHalfRead", steps: 12, seed: 1},
	{name: "release-waiting", check: "neverReleaseWaitingAfterCommit", steps: 12, seed: 1},

	// The first counterexamples to safe of the model's broken variants, which
	// TestReplayCounterexamples plays.
	{name: "variants/lockNoSync", check: "safe", steps: 40, seed: 1, main: "lockNoSync"},
	{name: "variants/noSalts", check: "safe", steps: 40, seed: 1, main: "noSalts"},
	{name: "variants/mapAll", check: "safe", steps: 40, seed: 1, main: "mapAll"},
}

// The committed traces are the ones the witness table generates from the model as
// it is now. Each file's #meta says when it was made, so only the states are
// compared. With -update the traces that changed are written to
// testdata/traces and the ones no row makes are removed: review the diff, a
// trace changes when the model does.
func TestModelTracesCurrent(t *testing.T) {
	dir := t.TempDir()
	t.Run("generate", func(t *testing.T) {
		for _, w := range witnesses {
			t.Run(w.name, func(t *testing.T) {
				t.Parallel()
				w.generate(t, dir)
			})
		}
	})
	if t.Failed() {
		return
	}
	made, committed := itfFiles(t, dir), itfFiles(t, "testdata/traces")
	if *update {
		for _, rel := range made {
			dst := filepath.Join("testdata/traces", rel)
			if slices.Contains(committed, rel) && reflect.DeepEqual(states(t, filepath.Join(dir, rel)), states(t, dst)) {
				continue
			}
			b, err := os.ReadFile(filepath.Join(dir, rel))
			if err == nil {
				err = os.MkdirAll(filepath.Dir(dst), 0o755)
			}
			if err == nil {
				err = os.WriteFile(dst, b, 0o644)
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("wrote %s", dst)
		}
		for _, rel := range committed {
			if !slices.Contains(made, rel) {
				if err := os.Remove(filepath.Join("testdata/traces", rel)); err != nil {
					t.Fatal(err)
				}
				t.Logf("removed %s", rel)
			}
		}
		return
	}
	if !slices.Equal(made, committed) {
		t.Fatalf("the witness table generates %v, testdata/traces holds %v (run with -update)", made, committed)
	}
	for _, rel := range made {
		if !reflect.DeepEqual(states(t, filepath.Join(dir, rel)), states(t, filepath.Join("testdata/traces", rel))) {
			t.Errorf("%s is not what the model generates now: run with -update and review the diff", rel)
		}
	}
}

// generate runs quint for the trace and writes it under dir. It runs in the
// directory of the committed traces so that each trace's #meta names the model
// by the same path. Quint exits non-zero
// when the check is violated, which is the point, so only a missing file is a
// failure.
func (w witness) generate(t *testing.T, dir string) {
	t.Helper()
	main := w.main
	if main == "" {
		main = "captureProtocol"
	}
	out := filepath.Join(dir, w.name+".itf.json")
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		t.Fatal(err)
	}
	status, output := quinttest.Run(t, "testdata/traces", "run", "../../../../specs/capture.qnt", "--main="+main, "--invariant="+w.check,
		"--max-steps="+strconv.Itoa(w.steps), "--seed="+strconv.Itoa(w.seed), "--max-samples=100000", "--mbt", "--out-itf="+out)
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("quint (exit %d) generated no trace: %v\n%s", status, err, output)
	}
}

// itfFiles is the traces under a directory, as paths relative to it.
func itfFiles(t *testing.T, dir string) []string {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".itf.json") {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		paths = append(paths, rel)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func states(t *testing.T, path string) any {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var itf struct{ States any }
	if err := json.Unmarshal(b, &itf); err != nil {
		t.Fatal(err)
	}
	return itf.States
}

// Random traces of the protocol, made from a new seed each run, played against
// the code as the committed ones are. SHARDLITE_SEED repeats a run and
// SHARDLITE_TRACES sets how many traces it makes (100 by default). A trace that
// fails is kept and its path logged, to be added to testdata/traces when the
// failure is fixed.
func TestModelRandomTraces(t *testing.T) {
	seed := rand.Uint64()
	if v := os.Getenv("SHARDLITE_SEED"); v != "" {
		var err error
		if seed, err = strconv.ParseUint(v, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	n := 100
	if v := os.Getenv("SHARDLITE_TRACES"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil || n < 1 {
			t.Fatalf("SHARDLITE_TRACES=%q is not a positive number", v)
		}
	}
	t.Logf("seed: SHARDLITE_SEED=%d, %d traces", seed, n)

	dir := t.TempDir()
	if status, out := quinttest.Run(t, "", "run", "../../specs/capture.qnt", "--main=captureProtocol", "--invariant=safe",
		"--max-steps=40", "--max-samples="+strconv.Itoa(n), "--n-traces="+strconv.Itoa(n),
		"--seed="+strconv.FormatUint(seed, 10), "--mbt", "--out-itf="+filepath.Join(dir, "{seq}.itf.json")); status != 0 {
		t.Fatalf("quint run (safe should hold) exited %d\n%s", status, out)
	}
	paths := itfFiles(t, dir)
	if len(paths) != n {
		t.Fatalf("quint made %d traces, not %d", len(paths), n)
	}
	for _, rel := range paths {
		p := filepath.Join(dir, rel)
		t.Run(strings.TrimSuffix(rel, ".itf.json"), func(t *testing.T) {
			t.Parallel()
			t.Cleanup(func() {
				if t.Failed() {
					keep(t, p)
				}
			})
			newReplayer(t).play(loadTrace(t, p))
		})
	}
}

// keep copies a failed trace out of the test's temporary directory.
func keep(t *testing.T, path string) {
	dir, err := os.MkdirTemp("", "shardlite-trace-")
	if err == nil {
		var b []byte
		if b, err = os.ReadFile(path); err == nil {
			dst := filepath.Join(dir, filepath.Base(path))
			if err = os.WriteFile(dst, b, 0o644); err == nil {
				t.Logf("the trace is kept at %s", dst)
				return
			}
		}
	}
	t.Logf("the trace could not be kept: %v", err)
}
