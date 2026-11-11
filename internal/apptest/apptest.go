// Package apptest starts internal/testapp as a child process and talks to it,
// along with the waiting helpers tests use (Eventually and Log). It differs
// from harness, which builds the in-process shardlite pieces (the blob store,
// the journal and the files layer) a test runs on and uses apptest for the app
// side of them.
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
// once per test process and returns the binary's path. tags are build tags
// (the fault build of ./cmd/shardlite passes "fault").
func Build(tb testing.TB, pkg string, tags ...string) string {
	tb.Helper()
	_, file, _, _ := runtime.Caller(0)
	return BuildIn(tb, filepath.Join(filepath.Dir(file), "..", ".."), pkg, tags...)
}

// BuildIn is Build for a package of the module in dir, which is how a test in
// another module (such as e2e) builds its own app.
func BuildIn(tb testing.TB, dir, pkg string, tags ...string) string {
	tb.Helper()
	buildMu.Lock()
	defer buildMu.Unlock()
	key := dir + " " + pkg + " " + strings.Join(tags, ",")
	if p, ok := builds[key]; ok {
		return p
	}
	out, err := os.MkdirTemp("", "shardlite-build")
	if err != nil {
		tb.Fatal(err)
	}
	bin := filepath.Join(out, filepath.Base(pkg))
	args := []string{"build", "-o", bin}
	if len(tags) > 0 {
		args = append(args, "-tags", strings.Join(tags, ","))
	}
	cmd := exec.Command("go", append(args, pkg)...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("build %s: %v\n%s", pkg, err, out)
	}
	builds[key] = bin
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
	err *stderr
}

// stderr keeps what a process wrote to its standard error, so a test can look
// at it, and passes it on to the test's own standard error.
type stderr struct {
	mu  sync.Mutex
	buf []byte
	max int
	out io.Writer
}

// maxStderr is how much of a process's standard error is kept.
const maxStderr = 64 << 10

func (w *stderr) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.buf = append(w.buf, p...)
	if len(w.buf) > w.max {
		w.buf = append(w.buf[:0], w.buf[len(w.buf)-w.max:]...)
	}
	w.mu.Unlock()
	return w.out.Write(p)
}

// String is what the process wrote to its standard error.
func (w *stderr) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return string(w.buf)
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
	errs := &stderr{max: maxStderr, out: os.Stderr}
	cmd.Stderr = errs
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
	return &App{cmd: cmd, in: in, out: bufio.NewReaderSize(out, 1<<16), err: errs}, nil
}

// Stderr is what the process wrote to its standard error.
func (a *App) Stderr() string { return a.err.String() }

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

// Dump returns a dump of a database that this process (or its capture)
// has open, read by a process of its own. Reading the file here would open
// and close a descriptor on it, and closing any descriptor drops all of the
// process's fcntl locks on the file, capture's among them.
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
