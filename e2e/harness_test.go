package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/e2e/e2eapp"
	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/s3test"
	shardpkg "github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// declarationJSON is the declaration the app serves at its ready route
// (docs/guide.md, Shard kinds), kept in a file in the stack's directory so a
// rewrite stands in for a new deploy. It starts as the app's built-in
// declaration.
const declarationJSON = e2eapp.DefaultDeclaration

// shardName is a shard's name in the tests that need the parsed type.
func shardName(s string) shardpkg.Name { return shardpkg.MustParse(s) }

// startProc starts a process the test kills when it ends.
func startProc(t testing.TB, name, bin string, env []string, args ...string) *apptest.Proc {
	t.Helper()
	p, err := apptest.StartProc(name, bin, env, nil, args...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Kill)
	return p
}

func freeAddr(t testing.TB) string {
	t.Helper()
	a, err := apptest.FreeAddr()
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// waitFor waits for a line of the process's output matching re and returns its
// submatches.
func waitFor(t testing.TB, p *apptest.Proc, re string, timeout time.Duration) []string {
	t.Helper()
	m, err := p.WaitFor(re, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// nodeTags are the build tags of the node's binary: the fault build (fault_rig_test.go)
// adds the fault tag, which is what makes the node read the fault settings.
var nodeTags []string

// stack is the node and the app, in front of RustFS.
type stack struct {
	t       testing.TB
	dir     string
	nodeBin string
	app     *apptest.Proc
	appAddr string
	node    *apptest.Proc
	pub     string // host:port
	ctl     string
	env     []string     // extra environment for the node
	store   s3test.Store // the stack's bucket and prefix on RustFS
}

func newStack(t testing.TB, nodeEnv ...string) *stack {
	t.Helper()
	return newStackOn(t, s3test.Setup(t), nodeEnv...)
}

// newStackOn is newStack on a bucket and prefix that exist already: a second
// node of the same cluster, which sets SHARDLITE_NODE in nodeEnv to a name of
// its own.
func newStackOn(t testing.TB, store s3test.Store, nodeEnv ...string) *stack {
	t.Helper()
	s := &stack{t: t, store: store, dir: t.TempDir(), env: nodeEnv}
	s.nodeBin = apptest.Build(t, "./cmd/shardlite", nodeTags...)
	appBin := apptest.BuildIn(t, ".", "./cmd/e2eapp")
	if err := os.WriteFile(s.declarePath(), []byte(declarationJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	s.pub, s.ctl = freeAddr(t), freeAddr(t)
	s.app = startProc(t, "e2eapp", appBin, nil, "-listen", "127.0.0.1:0", "-control", s.ctl, "-declare", s.declarePath())
	s.appAddr = waitFor(t, s.app, `listening (\S+)`, 20*time.Second)[1]
	s.startNode()
	return s
}

// declarePath is the file the app serves at its ready route: a rewrite of it
// is a new deploy as far as the node's poll is concerned.
func (s *stack) declarePath() string { return filepath.Join(s.dir, "declaration.json") }

// blobEnv is the node's blob storage settings: RustFS with a prefix of this
// stack's own.
func (s *stack) blobEnv() []string {
	return append([]string{"SHARDLITE_BLOB=s3://" + s.store.Bucket, "SHARDLITE_S3_ENDPOINT=" + s.store.Endpoint, "SHARDLITE_PREFIX=" + s.store.Prefix}, s3test.Env()...)
}

// nodeEnv is the node's environment: later entries (the stack's own extras) win.
func (s *stack) nodeEnv() []string {
	env := append(s.blobEnv(), []string{
		"SHARDLITE_NODE=n1",
		"SHARDLITE_DATA=" + filepath.Join(s.dir, "data"),
		"SHARDLITE_APP=http://" + s.appAddr, "SHARDLITE_LISTEN_PUBLIC=" + s.pub, "SHARDLITE_LISTEN_CONTROL=" + s.ctl, "SHARDLITE_LISTEN_PEER=127.0.0.1:0",
		"SHARDLITE_FLUSH=50ms", "SHARDLITE_LEASE_TTL=5s",
	}...)
	return append(env, s.env...)
}

// startNode starts (or restarts) the node and waits until it is ready.
func (s *stack) startNode() {
	s.t.Helper()
	s.node = startProc(s.t, "shardlite", s.nodeBin, s.nodeEnv())
	waitFor(s.t, s.node, `shardlite is up`, 30*time.Second)
	defer func() {
		if s.t.Failed() {
			s.t.Logf("the node's output:\n%s", s.node.Output())
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

// at is the path of an app route that belongs to shard: the app works out which
// shard a request is for from it.
func at(shard, route string) string { return "/" + shard + route }

// shardRequest is request as a client SDK for the app makes it: the shard is in
// the path, and Shardlite-Target names the same shard to save a hop.
func (s *stack) shardRequest(method, shard, route, body string) (int, string, error) {
	return s.request(method, at(shard, route), shard, body)
}

// create creates shards as the app does, with a call to its create route:
// clients can only reach shards that exist.
func (s *stack) create(t testing.TB, shards ...string) {
	t.Helper()
	for _, shard := range shards {
		if st, body, err := s.control("POST", at(shard, "/create"), shard, ""); err != nil || st != 204 {
			t.Fatalf("creating %s: %d %q %v", shard, st, body, err)
		}
	}
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
		Leases: lease.NewRegistry(l, "verifier", lease.RegistryOptions{}), Temp: tempfile.NewDir(t.TempDir()),
	}}
}

// tokens restores a shard from blob storage (every file's checksums are
// verified on the way) and returns the tokens in it. ok is false if the shard
// has nothing durable.
func (v *verifier) tokens(shard string) (toks map[string]bool, ok bool, err error) {
	dst := filepath.Join(v.t.TempDir(), "restore.db")
	defer os.Remove(dst)
	if _, err := v.files.Restore(context.Background(), shardName(shard), dst, shardfile.RestoreOptions{}); err != nil {
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
		_, shard, _ := strings.Cut(strings.TrimPrefix(info.Key, shardfile.ManifestPrefix), "/") // the key leads with the lane
		if _, _, err := v.files.GetManifest(context.Background(), shardName(shard)); errors.Is(err, blob.ErrNotFound) {
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
