// Package apptest starts internal/testapp as a child process and talks to it.
package apptest

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

var (
	buildMu sync.Mutex
	builds  = map[string]string{}
)

// Build builds a main package of this repository (such as ./internal/testapp)
// once per test process and returns the binary's path.
func Build(tb testing.TB, pkg string) string {
	tb.Helper()
	buildMu.Lock()
	defer buildMu.Unlock()
	if p, ok := builds[pkg]; ok {
		return p
	}
	dir, err := os.MkdirTemp("", "shardlite-build")
	if err != nil {
		tb.Fatal(err)
	}
	bin := filepath.Join(dir, filepath.Base(pkg))
	_, file, _, _ := runtime.Caller(0)
	cmd := exec.Command("go", "build", "-o", bin, pkg)
	cmd.Dir = filepath.Join(filepath.Dir(file), "..", "..")
	if out, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("build %s: %v\n%s", pkg, err, out)
	}
	builds[pkg] = bin
	return bin
}

// Binary builds testapp.
func Binary(tb testing.TB) string { return Build(tb, "./internal/testapp") }

// App is one running testapp process.
type App struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Reader
	mu  sync.Mutex
}

// Start launches a testapp process.
func Start(tb testing.TB) *App {
	tb.Helper()
	a, err := StartAt(Binary(tb))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(a.Kill)
	return a
}

// StartAt launches a binary that speaks the same line protocol as testapp.
func StartAt(path string, args ...string) (*App, error) {
	cmd := exec.Command(path, args...)
	cmd.Stderr = os.Stderr
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &App{cmd: cmd, in: in, out: bufio.NewReaderSize(out, 1<<16)}, nil
}

// Do sends one command and returns its reply value.
func (a *App) Do(fields ...string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := io.WriteString(a.in, strings.Join(fields, "\t")+"\n"); err != nil {
		return "", err
	}
	line, err := a.out.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(line, "\n")
	switch {
	case line == "ok":
		return "", nil
	case strings.HasPrefix(line, "ok "):
		return line[3:], nil
	}
	return "", fmt.Errorf("testapp: %s", strings.TrimPrefix(line, "err "))
}

// Must is Do that fails the test on error.
func (a *App) Must(tb testing.TB, fields ...string) string {
	tb.Helper()
	v, err := a.Do(fields...)
	if err != nil {
		tb.Fatal(err)
	}
	return v
}

// Pid is the process id, so a test can signal it.
func (a *App) Pid() int { return a.cmd.Process.Pid }

// Kill stops the process without letting it close anything.
func (a *App) Kill() {
	if a.cmd.Process != nil {
		_ = a.cmd.Process.Kill()
		_ = a.cmd.Wait()
	}
}

// Exit asks the process to exit and waits for it.
func (a *App) Exit() error {
	_, err := a.Do("exit")
	a.in.Close()
	if werr := a.cmd.Wait(); err == nil {
		err = werr
	}
	return err
}

// Dump returns a dump of a database that this process (or its Litestream)
// has open, read by a process of its own. Reading the file here would open
// and close a descriptor on it, and closing any descriptor drops all of the
// process's fcntl locks on the file, Litestream's among them.
func Dump(tb testing.TB, path string) string {
	tb.Helper()
	a, err := StartAt(Binary(tb))
	if err != nil {
		tb.Fatal(err)
	}
	defer a.Kill()
	v := a.Must(tb, "dump", "d", path)
	s, err := strconv.Unquote(v)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

// Query runs a query on a database that this process has open, in a process
// of its own (see Dump), and returns the rows, one per line.
func Query(tb testing.TB, path, query string) string {
	tb.Helper()
	a, err := StartAt(Binary(tb))
	if err != nil {
		tb.Fatal(err)
	}
	defer a.Kill()
	s, err := strconv.Unquote(a.Must(tb, "query", "q", path, query))
	if err != nil {
		tb.Fatal(err)
	}
	return s
}
