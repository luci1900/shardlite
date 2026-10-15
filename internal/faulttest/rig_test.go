//go:build fault

// Package faulttest is the adversarial suite. It is behind a build tag so that
// go test ./... skips it; run it with
//
//	go test -tags fault ./internal/faulttest
//
// It needs RustFS (docker/compose.s3.yaml, with SHARDLITE_TEST_S3_ENDPOINT set)
// since the nodes are separate processes, and skips without it.
//
// It kills the capture side of shardlite at chosen points and
// checks the invariant: every commit whose segment write returned success is
// present after a restore, and every restored database passes integrity_check.
package faulttest

import (
	"context"
	"database/sql"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/s3test"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

const shardName = "users/1"

// rig is one shard with an app process and a node process, either of which
// can be killed and started again.
type rig struct {
	t       *testing.T
	dir     string
	store   s3test.Store // the bucket and a prefix of this test's own
	path    string
	node    *apptest.App
	app     *apptest.App
	n       int          // last commit number written by the app
	pending []int        // commits written since the last successful sync: lost if the node dies first
	acked   map[int]bool // commits a successful sync covered: they must survive
	started int          // how many times the node has been started
}

func newRig(t *testing.T) *rig {
	t.Helper()
	st := s3test.Setup(t)
	dir := t.TempDir()
	r := &rig{t: t, acked: map[int]bool{}, dir: dir, store: st, path: filepath.Join(dir, "users-1.db")}
	r.startNode()
	r.startApp()
	r.app.Must(t, "open", "a", r.path)
	r.node.Must(t, "open", shardName, r.path)
	return r
}

func (r *rig) startNode() {
	r.t.Helper()
	n, err := apptest.StartAt(apptest.Build(r.t, "./internal/testnode"), append(r.store.Args(), "-meta", filepath.Join(r.dir, "meta"))...)
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(n.Kill)
	r.node = n
	r.started++
}

func (r *rig) startApp() {
	r.t.Helper()
	a, err := apptest.StartAt(apptest.Binary(r.t))
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(a.Kill)
	r.app = a
}

// commit writes the next numbered commit and, if sync is set, waits for it to
// reach blob storage. It returns the node's error, if any, and records the
// commit as acknowledged only when the sync succeeded.
func (r *rig) commit(sync bool) error {
	r.t.Helper()
	r.n++
	r.app.Must(r.t, "ack", "a", strconv.Itoa(r.n))
	r.pending = append(r.pending, r.n)
	if !sync {
		return nil
	}
	return r.sync()
}

// sync waits for everything written so far to reach blob storage.
func (r *rig) sync() error {
	if _, err := r.node.Do("sync", shardName); err != nil {
		return err
	}
	r.ackPending()
	return nil
}

func (r *rig) ackPending() {
	for _, n := range r.pending {
		r.acked[n] = true
	}
	r.pending = nil
}

// retryCommit keeps syncing until it succeeds (for storage faults).
func (r *rig) retrySync() {
	r.t.Helper()
	var err error
	for range 200 {
		if err = r.sync(); err == nil {
			return
		}
	}
	r.t.Fatalf("sync never succeeded, last error: %v", err)
}

// crash kills both processes, as losing the host would.
func (r *rig) crash() {
	r.pending = nil
	r.node.Kill()
	r.app.Kill()
}

// recover starts a new node, restores the shard before any app has it open,
// then starts a new app. The node discards local files, since a restart is a crash.
func (r *rig) recoverAll() string {
	r.t.Helper()
	r.crash()
	r.startNode()
	r.startApp()
	how, err := r.node.Do("open", shardName, r.path)
	if err != nil {
		r.t.Fatalf("restore on restart: %v", err)
	}
	r.app.Must(r.t, "open", "a", r.path)
	return how
}

// reactivate lets go of the shard (the app closes it, the node releases it) and
// activates it again from blob storage with a kill armed at point. It returns
// after the node has died, leaving the app and the files as they are.
func (r *rig) reactivate(point string) {
	r.t.Helper()
	r.app.Must(r.t, "close", "a")
	if v, err := r.node.Do("release", shardName); err != nil || v != "released" {
		r.t.Fatalf("release: %q %v", v, err)
	}
	r.ackPending()
	r.node.Must(r.t, "arm", point)
	// A restart restores from blob storage, so the file is discarded first. The
	// WAL Litestream left is removed with it, which the node's own reactivation
	// from its cache does not do (see TestReactivationFoldsTheWALItLeft).
	if _, err := r.node.Do("open", shardName, r.path); err == nil {
		r.t.Fatalf("open succeeded though the node should have died at %s", point)
	}
	r.node.Kill()
}

// reactivateCached is reactivate for a shard activated from the files its release
// left, WAL included (as a cached shard is), not from blob storage. The shard's
// tail is written as a delta when it is released, as before a shard is cached.
func (r *rig) reactivateCached(point string) {
	r.t.Helper()
	r.app.Must(r.t, "close", "a")
	if v, err := r.node.Do("release", shardName, "delta"); err != nil || v != "released" {
		r.t.Fatalf("release: %q %v", v, err)
	}
	r.ackPending()
	r.node.Must(r.t, "arm", point)
	if _, err := r.node.Do("cached", shardName, r.path); err == nil {
		r.t.Fatalf("cached activation succeeded though the node should have died at %s", point)
	}
	r.node.Kill()
}

// verify restores the shard from blob storage into a fresh file and checks the invariant.
func (r *rig) verify(label string) {
	r.t.Helper()
	files := r.readFiles()
	dst := filepath.Join(r.t.TempDir(), "verify.db")
	res, err := files.Restore(context.Background(), shardName, dst, shardfile.RestoreOptions{})
	if err != nil {
		r.t.Fatalf("%s: restore: %v", label, err)
	}
	if _, err := sqlitetest.TryDump(dst); err != nil {
		r.t.Fatalf("%s: restored database is damaged: %v", label, err)
	}
	have := acksIn(r.t, dst)
	for i := range r.acked {
		if !have[i] {
			r.t.Fatalf("%s: commit %d was acknowledged but is missing from the restored shard (restored to txid %d, has %d acks)", label, i, res.State.Position, len(have))
		}
	}
}

// readFiles is a reader of the rig's bucket that is no node: it finds
// each journal's range in the node's lease.
func (r *rig) readFiles() *shardfile.Files {
	r.t.Helper()
	l := r.store.Backend(r.t)
	return &shardfile.Files{Blob: blob.NewStore(l, "verifier", 1, blob.Options{}), Node: "verifier", Leases: coord.NewRegistry(l, "verifier", coord.RegistryOptions{}), TempDir: r.t.TempDir()}
}

func acksIn(t *testing.T, path string) map[int]bool {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT n FROM acks`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int]bool{}
	for rows.Next() {
		var n int
		rows.Scan(&n)
		out[n] = true
	}
	return out
}

// hasCommit reports whether a restore currently contains commit n.
func (r *rig) hasCommit(n int) bool {
	r.t.Helper()
	files := r.readFiles()
	dst := filepath.Join(r.t.TempDir(), "has.db")
	if _, err := files.Restore(context.Background(), shardName, dst, shardfile.RestoreOptions{}); err != nil {
		r.t.Fatal(err)
	}
	return acksIn(r.t, dst)[n]
}

func seedFor(t *testing.T) uint64 {
	if s := os.Getenv("SHARDLITE_SEED"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	return rand.Uint64()
}

func newRand(t *testing.T) *rand.Rand {
	seed := seedFor(t)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed: SHARDLITE_SEED=%d", seed)
		}
	})
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}

func itoa(n int) string { return strconv.Itoa(n) }
