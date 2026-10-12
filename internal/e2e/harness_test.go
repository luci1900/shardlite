package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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

	_ "github.com/mattn/go-sqlite3"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/shardfile"
)

const kindsJSON = `{"kinds": {
	"users":  {"public": true, "sync": "off", "idle_timeout": "2s"},
	"wallet": {"public": true, "idle_timeout": "2s"},
	"lobby":  {"public": true, "ephemeral": true, "idle_timeout": "2s"},
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

// stack is the node and the app, in front of a blob directory.
type stack struct {
	t       testing.TB
	dir     string
	nodeBin string
	app     *proc
	appAddr string
	node    *proc
	pub     string // host:port
	ctl     string
	env     []string // extra environment for the node
	prefix  string   // SHARDLITE_PREFIX when the node runs on the S3 emulator
}

func newStack(t testing.TB, nodeEnv ...string) *stack {
	t.Helper()
	s := &stack{t: t, dir: t.TempDir(), env: nodeEnv}
	s.nodeBin = apptest.Build(t, "./cmd/shardlite")
	appBin := apptest.Build(t, "./cmd/e2eapp")
	if err := os.WriteFile(filepath.Join(s.dir, "kinds.json"), []byte(kindsJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	s.pub, s.ctl = freeAddr(t), freeAddr(t)
	s.app = startProc(t, "e2eapp", appBin, nil, "-listen", "127.0.0.1:0", "-control", s.ctl)
	s.appAddr = s.app.waitFor(t, `listening (\S+)`, 20*time.Second)[1]
	s.startNode()
	return s
}

func (s *stack) blobs() string { return filepath.Join(s.dir, "blobs") }

// useS3 reports whether the stack's node runs on the S3 emulator: E2E_BACKEND=s3
// with SHARDLITE_TEST_S3_ENDPOINT set (docker/compose.s3.yaml).
func useS3(t testing.TB) bool {
	t.Helper()
	if os.Getenv("E2E_BACKEND") != "s3" {
		return false
	}
	if os.Getenv("SHARDLITE_TEST_S3_ENDPOINT") == "" {
		t.Skip("E2E_BACKEND=s3 needs SHARDLITE_TEST_S3_ENDPOINT (docker/compose.s3.yaml)")
	}
	return true
}

func s3Bucket() string {
	if b := os.Getenv("SHARDLITE_TEST_S3_BUCKET"); b != "" {
		return b
	}
	return "shardlite-test"
}

var bucketOnce sync.Once

// ensureBucket creates the emulator's bucket once per test binary: the node
// never creates buckets.
func ensureBucket(t testing.TB) {
	t.Helper()
	var err error
	bucketOnce.Do(func() {
		var b *blob.S3
		if b, err = blob.NewS3(context.Background(), blob.S3Options{Bucket: s3Bucket(), Endpoint: os.Getenv("SHARDLITE_TEST_S3_ENDPOINT")}); err == nil {
			err = b.EnsureBucket(context.Background())
		}
	})
	if err != nil {
		t.Fatal(err)
	}
}

// blobEnv is the node's blob storage settings: a directory, or the emulator
// with a prefix of this stack's own.
func (s *stack) blobEnv() []string {
	if !useS3(s.t) {
		return []string{"SHARDLITE_BLOB=" + s.blobs()}
	}
	ensureBucket(s.t)
	if s.prefix == "" {
		var r [6]byte
		rand.Read(r[:])
		s.prefix = "e2e/" + hex.EncodeToString(r[:]) + "/"
	}
	return []string{"SHARDLITE_BLOB=s3://" + s3Bucket(), "SHARDLITE_S3_ENDPOINT=" + os.Getenv("SHARDLITE_TEST_S3_ENDPOINT"), "SHARDLITE_PREFIX=" + s.prefix,
		"AWS_ACCESS_KEY_ID=" + envOr("AWS_ACCESS_KEY_ID", "shardlite"), "AWS_SECRET_ACCESS_KEY=" + envOr("AWS_SECRET_ACCESS_KEY", "shardlite-secret"), "AWS_REGION=" + envOr("AWS_REGION", "us-east-1")}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// startNode starts (or restarts) the node and waits until it is ready.
func (s *stack) startNode() {
	s.t.Helper()
	env := append(s.blobEnv(), []string{
		"SHARDLITE_NODE=n1", "SHARDLITE_KINDS=" + filepath.Join(s.dir, "kinds.json"),
		"SHARDLITE_DATA=" + filepath.Join(s.dir, "data"), "SHARDLITE_META=" + filepath.Join(s.dir, "meta"),
		"SHARDLITE_APP=http://" + s.appAddr, "SHARDLITE_LISTEN=" + s.pub, "SHARDLITE_CONTROL=" + s.ctl,
		"SHARDLITE_FLUSH=50ms", "SHARDLITE_LEASE_TTL=5s",
	}...)
	env = append(env, s.env...)
	s.node = startProc(s.t, "shardlite", s.nodeBin, env)
	s.node.waitFor(s.t, `shardlite is up`, 30*time.Second)
	deadline := time.Now().Add(20 * time.Second)
	for {
		resp, err := http.Get("http://" + s.pub + "/_shardlite/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("node never became ready:\n%s", s.node.out.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
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

func (s *stack) blobEnvPrefix() string { s.blobEnv(); return s.prefix }

// verifier reads blob storage directly, as no node: it finds each journal's range in the node's lease.
type verifier struct {
	t     testing.TB
	files *shardfile.Files
}

func newVerifier(t testing.TB, s *stack) *verifier {
	var l blob.Backend
	if useS3(t) {
		b, err := blob.NewS3(context.Background(), blob.S3Options{Bucket: s3Bucket(), Endpoint: os.Getenv("SHARDLITE_TEST_S3_ENDPOINT")})
		if err != nil {
			t.Fatal(err)
		}
		l = blob.WithPrefix(b, s.blobEnvPrefix())
	} else {
		local, err := blob.NewLocal(s.blobs())
		if err != nil {
			t.Fatal(err)
		}
		l = local
	}
	return &verifier{t, &shardfile.Files{
		Blob: blob.NewStore(l, "verifier", 1, blob.Options{}), Node: "verifier",
		Leases: coord.NewRegistry(l, "verifier", coord.RegistryOptions{}),
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
	db, err := sql.Open("sqlite3", "file:"+dst+"?mode=ro")
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
	for info, err := range v.files.Blob.List(context.Background(), "manifests/") {
		if err != nil {
			v.t.Fatal(err)
		}
		shard := strings.TrimPrefix(info.Key, "manifests/")
		if m, _, err := v.files.GetManifest(context.Background(), shard); errors.Is(err, blob.ErrNotFound) || (err == nil && m.Ephemeral) {
			continue // (ephemeral shards have nothing to restore)
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
