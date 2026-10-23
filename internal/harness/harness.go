// Package harness sets up the pieces tests share: an in-memory blob store, a
// segment writer, a files layer and test apps.
package harness

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// ReadEntry returns the LTX file bytes of one journal entry.
func ReadEntry(ctx context.Context, js *journal.Store, seq pos.Seg, e segment.Entry) ([]byte, error) {
	f, err := segment.ReadEntryFile(ctx, js, seq, e, "", int(e.Length))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.Bytes()
}

// Env is one node's worth of storage.
type Env struct {
	T       testing.TB
	Dir     string           // local shard files
	Blob    *blobtest.Hooked // the in-memory backend behind its fault hook
	Store   *blob.Store
	Journal *journal.Store // the node's own journal
	Files   *shardfile.Files
	W       *segment.Writer
	Node    cluster.Node
	Reg     *coord.Registry
	Lease   coord.NodeLease
}

// New returns an Env with a segment writer, which writes only when flushed
// (capture.Shard.Sync flushes it).
func New(t testing.TB) *Env {
	t.Helper()
	local := blobtest.WithHook(blob.NewMemory())
	reg := coord.NewRegistry(local, "n1", coord.RegistryOptions{})
	lease, err := reg.Acquire(context.Background(), coord.NodeInfo{Name: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	store := blob.NewStore(local, "n1", lease.Epoch, blob.Options{Backoff: time.Millisecond})
	jopts := journal.Options{Backoff: time.Millisecond}
	e := &Env{T: t, Dir: t.TempDir(), Blob: local, Store: store, Node: "n1", Reg: reg, Lease: lease,
		Files: &shardfile.Files{Blob: store, Backend: local, Node: "n1", Leases: reg, JournalOptions: jopts, TempDir: t.TempDir()}}
	e.Journal = e.Files.Journal(e.Node)
	e.W = segment.NewWriter(e.Journal, segment.Options{})
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
func (e *Env) Adopter(writer, node cluster.Node) *journal.Store {
	return journal.NewStore(journal.NewBlob(e.Blob, node), writer, e.Store.Epoch(), journal.Options{Backoff: time.Millisecond})
}

// Path returns the local path for a shard name like game.users/42.
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
	sh, err := capture.New(capture.Config{Name: h.Name, Path: e.Path(h.Name.String()), Generation: h.Manifest.Generation, Writer: e.W, Restored: restored, TempDir: e.Files.TempDir, SpoolLimit: uint64(e.Files.SpoolLimit)})
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
