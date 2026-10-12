// Package harness sets up the pieces tests share: a blob directory, a
// segment writer, a files layer and test apps.
package harness

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// Env is one node's worth of storage.
type Env struct {
	T     testing.TB
	Dir   string      // local shard files
	Meta  string      // Litestream metadata directories, one per shard (tmpfs in production)
	Local *blob.Local // the backend, for its fault hook
	Store *blob.Store
	Files *shardfile.Files
	W     *segment.Writer
	Node  string
	Reg   *coord.Registry
	Lease coord.NodeLease
}

// New returns an Env with a segment writer flushing every interval.
func New(t testing.TB, interval time.Duration) *Env {
	t.Helper()
	local, err := blob.NewLocal(filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	reg := coord.NewRegistry(local, "n1", coord.RegistryOptions{})
	lease, err := reg.Acquire(context.Background(), coord.NodeInfo{Name: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	store := blob.NewStore(local, "n1", lease.Epoch, blob.Options{Backoff: time.Millisecond})
	e := &Env{T: t, Dir: t.TempDir(), Meta: t.TempDir(), Local: local, Store: store, Node: "n1", Reg: reg, Lease: lease}
	e.Files = &shardfile.Files{Blob: store, Node: e.Node, Leases: reg}
	e.W = segment.NewWriter(store, e.Node, segment.Options{Interval: interval})
	t.Cleanup(e.W.Close)
	return e
}

// SetJournalRange records in the node's lease which segments of its journal may still
// be needed: from the given one up to the writer's next.
func (e *Env) SetJournalRange(from uint64) {
	e.T.Helper()
	info := e.Lease.NodeInfo
	info.JournalFrom, info.JournalNext = from, e.W.NextSeq()
	l, err := e.Reg.Renew(context.Background(), e.Lease, info)
	if err != nil {
		e.T.Fatal(err)
	}
	e.Lease = l
}

// Path returns the local path for a shard name like users/42.
func (e *Env) Path(shard string) string {
	return filepath.Join(e.Dir, fmt.Sprintf("%s.db", filepath.Base(filepath.Dir(shard))+"-"+filepath.Base(shard)))
}

// NewShard opens a new shard's manifest and starts capturing its (already
// created) file.
func (e *Env) NewShard(shard string) (*shardfile.Handle, *capture.Shard) {
	e.T.Helper()
	h, created, err := e.Files.Open(context.Background(), shard)
	if err != nil {
		e.T.Fatal(err)
	}
	if !created {
		e.T.Fatalf("shard %s already exists", shard)
	}
	return h, e.Activate(h, nil)
}

// Activate starts a new activation of a shard (capture only: nothing is written
// to the manifest) and starts capturing its file. restored is nil for a new shard.
func (e *Env) Activate(h *shardfile.Handle, restored *capture.Restored) *capture.Shard {
	e.T.Helper()
	sh, err := capture.New(capture.Config{Name: h.Name, Path: e.Path(h.Name), Generation: h.Manifest.Generation, MetaRoot: e.Meta, Writer: e.W, Restored: restored})
	if err != nil {
		e.T.Fatal(err)
	}
	return sh
}

// StartApp runs a test app with the shard's file open as id "a".
func (e *Env) StartApp(shard string) *apptest.App {
	e.T.Helper()
	app := apptest.Start(e.T)
	app.Must(e.T, "open", "a", e.Path(shard))
	return app
}
