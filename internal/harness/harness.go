// Package harness sets up the in-process pieces tests share: an in-memory blob
// store, the journal's writer, a files layer and shards on top of them. It
// differs from apptest, which only runs the test app as a child process and
// talks to it (the harness starts the app for a shard through apptest).
package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// ReadEntry returns the LTX file bytes of one journal entry.
func ReadEntry(ctx context.Context, js journal.Store, seq pos.Seg, e journal.Entry) ([]byte, error) {
	rc, err := js.GetRange(ctx, seq, e.Offset, e.Length)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != e.Length {
		return nil, fmt.Errorf("short range read of segment %d: %d of %d bytes", seq, len(b), e.Length)
	}
	return b, nil
}

// Env is one node's worth of storage.
type Env struct {
	T       testing.TB
	Dir     string           // local shard files
	Blob    *blobtest.Hooked // the in-memory backend behind its fault hook
	Store   *blob.Store
	Journal journal.Store // the node's own journal
	Files   *shardfile.Files
	W       *journal.Writer
	Node    cluster.Node
	Reg     *lease.Registry
	Lease   lease.Record
}

// New returns an Env with a journal writer, which writes only when flushed
// (capture.Shard.Sync flushes it).
func New(t testing.TB) *Env {
	t.Helper()
	return newEnv(t, blobtest.WithHook(blob.NewMemory()), "n1")
}

// Peer returns another node's Env on the same storage, with a lease of its own.
// Its first lease has epoch 1, as every node's does.
func (e *Env) Peer(node cluster.Node) *Env {
	e.T.Helper()
	return newEnv(e.T, e.Blob, node)
}

func newEnv(t testing.TB, local *blobtest.Hooked, node cluster.Node) *Env {
	t.Helper()
	reg := lease.NewRegistry(local, node, lease.RegistryOptions{})
	rec, err := reg.Acquire(context.Background(), lease.NodeInfo{Name: node})
	if err != nil {
		t.Fatal(err)
	}
	store := blob.NewStore(local, node, rec.Epoch, blob.Options{Backoff: time.Millisecond})
	e := &Env{T: t, Dir: t.TempDir(), Blob: local, Store: store, Node: node, Reg: reg, Lease: rec,
		Files: &shardfile.Files{Blob: store, Node: node, Leases: reg, Temp: tempfile.NewDir(t.TempDir())}}
	e.Journal = e.Files.Journal(e.Node)
	e.W = journal.NewWriter(e.Journal, journal.WriterOptions{})
	t.Cleanup(service.Background(slog.Default(), e.W))
	return e
}

// SetJournalRange records in the node's lease which segments of its journal may still
// be needed: from the given one up to the writer's next.
func (e *Env) SetJournalRange(from pos.Seg) {
	e.T.Helper()
	info := e.Lease.NodeInfo
	info.JournalFrom, info.JournalNext = from, e.W.NextSeq()
	l, err := e.Reg.Renew(context.Background(), e.Lease, info)
	if err != nil {
		e.T.Fatal(err)
	}
	e.Lease = l
}

// Adopter returns another node's store of a journal: the journal named by
// node, out of Env.Blob, with write ids that name writer. It is what an
// adopter of another node's journal holds.
func (e *Env) Adopter(writer, node cluster.Node) journal.Store {
	return journal.NewBlob(blob.NewStore(e.Blob, writer, e.Store.Epoch(), blob.Options{Backoff: time.Millisecond}), node)
}

// Path returns the local path for a shard name like users/42.
func (e *Env) Path(shard string) string {
	return filepath.Join(e.Dir, fmt.Sprintf("%s.db", filepath.Base(filepath.Dir(shard))+"-"+filepath.Base(shard)))
}

// NewShard opens a new shard's manifest and starts capturing its (already
// created) file.
func (e *Env) NewShard(name string) (*shardfile.Handle, *capture.Shard) {
	e.T.Helper()
	n, err := shard.ParseName(name)
	if err != nil {
		e.T.Fatal(err)
	}
	h, created, err := e.Files.Open(context.Background(), n)
	if err != nil {
		e.T.Fatal(err)
	}
	if !created {
		e.T.Fatalf("shard %s already exists", name)
	}
	return h, e.Activate(h, nil)
}

// Activate starts a new activation of a shard (capture only: nothing is written
// to the manifest) and starts capturing its file. restored is nil for a new shard.
func (e *Env) Activate(h *shardfile.Handle, restored *capture.Restored) *capture.Shard {
	e.T.Helper()
	sh, err := capture.New(capture.Config{Name: h.Name, Path: e.Path(h.Name.String()), Generation: h.Manifest.Generation, Writer: e.W, Restored: restored, TempDir: e.Files.Temp})
	if err != nil {
		e.T.Fatal(err)
	}
	return sh
}

// StartApp runs a test app with the shard's file open as id "a".
func (e *Env) StartApp(shard string) *apptest.App {
	e.T.Helper()
	app := apptest.Start(e.T)
	app.Must(e.T, "create", "a", e.Path(shard))
	return app
}
