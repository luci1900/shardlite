//go:build model

package specs_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/luci1900/shardlite/internal/test/quinttest"
)

// The checks of the models in this directory, one test file per model: each
// model's typecheck, its simulation runs and its scripted tests. They need
// Quint, and each row is a parallel subtest, so a run takes about as long as
// the slowest row (2 minutes, dispatch.qnt's run of its invariant).
//
// Each row says what its command must do. Most pass. The broken variants of a
// model must fail, and in a named way: a run must violate the invariant it
// names, and a test must fail on its own `expect` or on a step that can't be
// taken. Any other failure (a parse or type error, a typo in a name, another
// test failing) counts as a mismatch, as does a command that was meant to fail
// and passes. The tail of Quint's output comes with a mismatch (a random run's
// seed is in it, to repeat the run).
//
// `quint verify` is left out: it often doesn't terminate, so simulation is the
// check (capture.qnt's header says how far verification gets there).

// What a row's command must do.
type want string

const (
	pass     want = "pass"     // a typecheck, a run or a test succeeds
	violated want = "violated" // a run violates its --invariant
	expect   want = "expect"   // a test, its --match, fails on its expect
	// stuck is "stuck:QNTnnn": a test fails on a step that can't be taken,
	// with that error code.
)

func stuck(code string) want { return want("stuck:" + code) }

type row struct {
	want want
	args []string
}

var sim = []string{"--max-steps=40", "--max-samples=50000"}

func r(w want, args ...string) row { return row{w, args} }

// withSim is a row with the simulation limits of most runs.
func withSim(w want, args ...string) row { return row{w, append(args, sim...)} }

// check runs each row as a parallel subtest named after its command.
func check(t *testing.T, rows []row) {
	t.Helper()
	for _, row := range rows {
		t.Run(strings.Join(row.args, " "), func(t *testing.T) {
			t.Parallel()
			status, out := quinttest.Run(t, "", row.args...)
			expected := row.expected()
			if got := outcome(row.args, status, out); got != expected {
				t.Errorf("expected %s, got %s\nquint %s\n%s", expected, got, strings.Join(row.args, " "), quinttest.Tail(out))
			}
		})
	}
}

// expected is the outcome the row's command must have, in the words outcome uses.
func (rw row) expected() string {
	inv, test := flagValue(rw.args, "--invariant="), flagValue(rw.args, "--match=")
	switch {
	case rw.want == pass:
		return "pass"
	case rw.want == violated:
		return "invariant " + inv + " violated"
	case rw.want == expect:
		return "test " + test + " fails on its expect"
	case strings.HasPrefix(string(rw.want), "stuck:"):
		return fmt.Sprintf("test %s is stuck (%s)", test, strings.TrimPrefix(string(rw.want), "stuck:"))
	}
	panic("unknown expectation " + string(rw.want))
}

func flagValue(args []string, prefix string) string {
	for _, a := range args {
		if v, ok := strings.CutPrefix(a, prefix); ok {
			return v
		}
	}
	return ""
}

var (
	okLine        = regexp.MustCompile(`(?m)^\[ok\] No violation found`)
	violationLine = regexp.MustCompile(`(?m)^error: Invariant violated`)
	passingLine   = regexp.MustCompile(`(?m)^ +[0-9]+ passing`)
	failedLine    = regexp.MustCompile(`(?m)^.*failed after.*$`)
	failedName    = regexp.MustCompile(`^ *[0-9]*\) *(.*?) failed after.*$`)
	errLine       = regexp.MustCompile(`(?m)^ *Error \[(QNT[0-9]*)\]: (.*)`)
	errorLine     = regexp.MustCompile(`(?m)^error.*`)
)

// outcome is what the command did, in the words the expectations use: pass, an
// invariant violated, a test failing on its expect, a test stuck on a step, or
// an error. A test that passes needs a "N passing" line because a --match that
// matches nothing exits 0 with no tests.
func outcome(args []string, status int, out string) string {
	inv, test := flagValue(args, "--invariant="), flagValue(args, "--match=")
	switch args[0] {
	case "typecheck":
		if status == 0 {
			return "pass"
		}
	case "run":
		if status == 0 && okLine.MatchString(out) {
			return "pass"
		}
		if status != 0 && violationLine.MatchString(out) {
			return "invariant " + inv + " violated"
		}
	case "test":
		failed := failedLine.FindAllString(out, -1)
		if status == 0 && len(failed) == 0 && passingLine.MatchString(out) {
			return "pass"
		}
		if status != 0 && len(failed) == 1 {
			name := failedName.ReplaceAllString(failed[0], "$1")
			code, msg := "", ""
			if m := errLine.FindStringSubmatch(out); m != nil {
				code, msg = m[1], m[2]
			}
			switch {
			case !strings.Contains(name, test):
			case code != "" && strings.Contains(msg, "Expect condition does not hold true"):
				return "test " + orName(test, name) + " fails on its expect"
			case code != "" && strings.Contains(msg, "Cannot continue"):
				return fmt.Sprintf("test %s is stuck (%s)", orName(test, name), code)
			}
			return fmt.Sprintf("test %s fails: %s %s", name, code, msg)
		}
		if status != 0 && len(failed) > 1 {
			return fmt.Sprintf("%d tests fail", len(failed))
		}
	}
	if e := errorLine.FindString(out); e != "" {
		return truncate(e, 160)
	}
	return fmt.Sprintf("exit %d without the expected output", status)
}

func orName(test, name string) string {
	if test == "" {
		return name
	}
	return test
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
