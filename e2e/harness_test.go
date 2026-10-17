package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/s3test"
	"github.com/luci1900/shardlite/internal/shardfile"
)

const kindsJSON = `{"kinds": {
	"users":  {"public": true, "sync": "off", "idle_timeout": "2s"},
	"wallet": {"public": true, "idle_timeout": "2s"},
	"lobby":  {"public": true, "idle_timeout": "2s"},
	"arena":  {"public": true, "idle_timeout": "300ms"},
	"audit":  {}
}}`

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// proc is a process whose output is kept.
type proc struct {
	name string
	cmd  *exec.Cmd
	out  *lockedBuf
	done chan struct{}
	err  error
}

func startProc(t testing.TB, name, bin string, env []string, args ...string) *proc {
	t.Helper()
	p := &proc{name: name, out: &lockedBuf{}, done: make(chan struct{})}
	p.cmd = exec.Command(bin, args...)
	p.cmd.Env = append(os.Environ(), env...)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { p.kill() })
	return p
}

// waitFor waits for a line of output matching re and returns its submatches.
func (p *proc) waitFor(t testing.TB, re string, timeout time.Duration) []string {
	t.Helper()
	rx := regexp.MustCompile(re)
	deadline := time.Now().Add(timeout)
	for {
		if m := rx.FindStringSubmatch(p.out.String()); m != nil {
			return m
		}
		select {
		case <-p.done:
			t.Fatalf("%s exited (%v) before printing %q:\n%s", p.name, p.err, re, p.out.String())
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: no %q within %v:\n%s", p.name, re, timeout, p.out.String())
		}
	}
}

func (p *proc) kill() {
	if p.cmd.Process != nil {
		p.cmd.Process.Signal(syscall.SIGKILL)
	}
	<-p.done
}

func (p *proc) signal(s syscall.Signal) { p.cmd.Process.Signal(s) }

// term asks the process to stop and waits for its exit status.
func (p *proc) term(timeout time.Duration) error {
	p.signal(syscall.SIGTERM)
	select {
	case <-p.done:
		return p.err
	case <-time.After(timeout):
		p.kill()
		return errors.New("did not stop on SIGTERM")
	}
}

func freeAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

var (
	appOnce sync.Once
	appPath string
	appErr  error
)

// buildApp builds the test app of this module once per test process.
func buildApp(t testing.TB) string {
	t.Helper()
	appOnce.Do(func() {
		dir, err := os.MkdirTemp("", "e2eapp-build")
		if err != nil {
			appErr = err
			return
		}
		appPath = filepath.Join(dir, "e2eapp")
		if out, err := exec.Command("go", "build", "-o", appPath, "./cmd/e2eapp").CombinedOutput(); err != nil {
			appErr = fmt.Errorf("build e2eapp: %v\n%s", err, out)
		}
	})
	if appErr != nil {
		t.Fatal(appErr)
	}
	return appPath
}

// stack is the node and the app, in front of RustFS.
type stack struct {
	t       testing.TB
	dir     string
	nodeBin string
	app     *proc
	appAddr string
	node    *proc
	pub     string // host:port
	ctl     string
	env     []string     // extra environment for the node
	store   s3test.Store // the stack's bucket and prefix on RustFS
}

func newStack(t testing.TB, nodeEnv ...string) *stack {
	t.Helper()
	s := &stack{t: t, store: s3test.Setup(t), dir: t.TempDir(), env: nodeEnv}
	s.nodeBin = apptest.Build(t, "./cmd/shardlite")
	appBin := buildApp(t)
	if err := os.WriteFile(filepath.Join(s.dir, "kinds.json"), []byte(kindsJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	s.pub, s.ctl = freeAddr(t), freeAddr(t)
	s.app = startProc(t, "e2eapp", appBin, nil, "-listen", "127.0.0.1:0", "-control", s.ctl)
	s.appAddr = s.app.waitFor(t, `listening (\S+)`, 20*time.Second)[1]
	s.startNode()
	return s
}

// blobEnv is the node's blob storage settings: RustFS with a prefix of this
// stack's own.
func (s *stack) blobEnv() []string {
	return append([]string{"SHARDLITE_BLOB=s3://" + s.store.Bucket, "SHARDLITE_S3_ENDPOINT=" + s.store.Endpoint, "SHARDLITE_PREFIX=" + s.store.Prefix}, s3test.Env()...)
}

// startNode starts (or restarts) the node and waits until it is ready.
func (s *stack) startNode() {
	s.t.Helper()
	env := append(s.blobEnv(), []string{
		"SHARDLITE_NODE=n1", "SHARDLITE_KINDS=" + filepath.Join(s.dir, "kinds.json"),
		"SHARDLITE_DATA=" + filepath.Join(s.dir, "data"), "SHARDLITE_SCRATCH_DIR=" + filepath.Join(s.dir, "meta"),
		"SHARDLITE_APP=http://" + s.appAddr, "SHARDLITE_LISTEN_PUBLIC=" + s.pub, "SHARDLITE_LISTEN_CONTROL=" + s.ctl, "SHARDLITE_LISTEN_PEER=127.0.0.1:0",
		"SHARDLITE_FLUSH=50ms", "SHARDLITE_LEASE_TTL=5s",
	}...)
	env = append(env, s.env...)
	s.node = startProc(s.t, "shardlite", s.nodeBin, env)
	s.node.waitFor(s.t, `shardlite is up`, 30*time.Second)
	defer func() {
		if s.t.Failed() {
			s.t.Logf("the node's output:\n%s", s.node.out.String())
		}
	}()
	apptest.Eventually(s.t, 20*time.Second, "the node to become ready", func() bool {
		resp, err := http.Get("http://" + s.pub + "/_shardlite/ready")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	})
}

// request sends one request to the public listener and returns the status and body.
func (s *stack) request(method, path, target, body string) (int, string, error) {
	return doRequest(http.DefaultClient, method, "http://"+s.pub+path, target, body)
}

func (s *stack) control(method, path, target, body string) (int, string, error) {
	return doRequest(http.DefaultClient, method, "http://"+s.ctl+path, target, body)
}

func doRequest(c *http.Client, method, url, target, body string) (int, string, error) {
	q, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	if target != "" {
		q.Header.Set("Shardlite-Target", target)
	}
	resp, err := c.Do(q)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), err
}

// verifier reads blob storage directly, as no node: it finds each journal's range in the node's lease.
type verifier struct {
	t     testing.TB
	files *shardfile.Files
}

func newVerifier(t testing.TB, s *stack) *verifier {
	l := s.store.Backend(t)
	return &verifier{t, &shardfile.Files{
		Blob: blob.NewStore(l, "verifier", 1, blob.Options{}), Node: "verifier",
		Leases: coord.NewRegistry(l, "verifier", coord.RegistryOptions{}), TempDir: t.TempDir(),
	}}
}

// tokens restores a shard from blob storage (every file's checksums are
// verified on the way) and returns the tokens in it. ok is false if the shard
// has nothing durable.
func (v *verifier) tokens(shard string) (toks map[string]bool, ok bool, err error) {
	dst := filepath.Join(v.t.TempDir(), "restore.db")
	defer os.Remove(dst)
	if _, err := v.files.Restore(context.Background(), shard, dst, shardfile.RestoreOptions{}); err != nil {
		if errors.Is(err, blob.ErrNotFound) || errors.Is(err, shardfile.ErrNothingToRestore) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("restore %s: %w", shard, err)
	}
	db, err := sql.Open("sqlite", "file:"+dst+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, false, err
	}
	defer db.Close()
	var check string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&check); err != nil || check != "ok" {
		return nil, false, fmt.Errorf("%s: integrity_check: %q %v", shard, check, err)
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return nil, false, err
	}
	toks = map[string]bool{}
	if version == 0 {
		return toks, true, nil
	}
	rs, err := db.Query(`SELECT token FROM rows`)
	if err != nil {
		return nil, false, err
	}
	defer rs.Close()
	for rs.Next() {
		var tok string
		rs.Scan(&tok)
		toks[tok] = true
	}
	return toks, true, rs.Err()
}

// shards lists every shard that has a manifest.
func (v *verifier) shards() []string {
	v.t.Helper()
	var out []string
	for info, err := range v.files.Blob.List(context.Background(), shardfile.ManifestPrefix) {
		if err != nil {
			v.t.Fatal(err)
		}
		shard := strings.TrimPrefix(info.Key, shardfile.ManifestPrefix)
		if _, _, err := v.files.GetManifest(context.Background(), shard); errors.Is(err, blob.ErrNotFound) {
			continue
		}
		out = append(out, shard)
	}
	return out
}

func (v *verifier) has(shard string, tokens ...string) bool {
	toks, ok, err := v.tokens(shard)
	if err != nil || !ok {
		return false
	}
	for _, t := range tokens {
		if !toks[t] {
			return false
		}
	}
	return true
}

func jsonField(t testing.TB, body, field string) any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("not JSON: %q", body)
	}
	return m[field]
}
