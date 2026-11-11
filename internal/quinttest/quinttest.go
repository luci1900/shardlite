// Package quinttest runs Quint for the tests that check the models in specs/.
// It is imported only from tests, and holds the one copy of the Quint version
// the model suite pins, in the file quint-version (CI reads it too).
package quinttest

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Version is the Quint the model suite pins. Another can make other traces
// from the same seed, which the check of the committed traces would report as
// a change to the model.
var Version = strings.TrimSpace(version)

//go:embed quint-version
var version string

// Quint is the path of quint. It fails the test (never skips it) when quint is
// missing or is not Version, so that a missing tool never reads as a pass.
func Quint(t testing.TB) string {
	t.Helper()
	path, err := found()
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// found looks once for quint and its version, whichever test asks first.
var found = sync.OnceValues(func() (string, error) {
	install := "install it with\n\tnpm install -g @informalsystems/quint@" + Version
	path, err := exec.LookPath("quint")
	if err != nil {
		return "", fmt.Errorf("quint is not on the path, %s", install)
	}
	out, err := exec.Command(path, "--version").Output()
	if err != nil {
		return "", fmt.Errorf("quint --version: %w", err)
	}
	if got := strings.TrimSpace(string(out)); got != Version {
		return "", fmt.Errorf("quint is %s, the model suite needs %s, %s", got, Version, install)
	}
	return path, nil
})

// Timeout is how long one quint command may run (the slowest, a simulation
// of dispatch.qnt's invariant, takes about 2 minutes).
const Timeout = 5 * time.Minute

// Run runs quint with the arguments, in dir if it is not empty. It returns
// quint's exit status (0 when it succeeded) and its output, stdout and stderr
// together. The test fails if quint could not be run at all, or ran past
// Timeout.
func Run(t testing.TB, dir string, args ...string) (status int, output string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), Timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, Quint(t), args...)
	cmd.Dir = dir
	// quint starts an evaluator of its own, so the deadline kills quint's whole
	// process group, as testwatch does a test binary's.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		t.Fatalf("quint %s ran past its %s limit\n%s", strings.Join(args, " "), Timeout, Tail(string(out)))
		return -1, ""
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	default:
		t.Fatalf("quint %s: %v", strings.Join(args, " "), err)
		return -1, ""
	}
}

// Tail is the last 15 lines of the output, each cut to 200 characters and
// marked, for a failure message.
func Tail(out string) string {
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) > 15 {
		lines = lines[len(lines)-15:]
	}
	for i, l := range lines {
		if len(l) > 200 {
			l = l[:200]
		}
		lines[i] = "    | " + l
	}
	return strings.Join(lines, "\n")
}
