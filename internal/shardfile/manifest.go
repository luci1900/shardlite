package shardfile

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/spool"
)

// Manifest is the per-shard record in blob storage (see docs/internals.md,
// Manifest). Last owner and pin come with the cluster and pinning.
type Manifest struct {
	Generation string   `json:"generation"` // random, set at creation, part of the shard's object paths
	Owner      string   `json:"owner"`      // the node allowed to write the shard, "" if released
	NodeEpoch  uint64   `json:"node_epoch"` // the owner's lease epoch when it claimed the shard
	Files      []string `json:"files"`      // base and deltas, "min-max.epoch" (min-max in the shard's own positions), in order
	Journal    string   `json:"journal"`    // the node journal holding commits after the last file, "" if there are none
	// AutoDelete is set at creation if the shard's kind had auto_delete then, and
	// never changes. The node deletes the shard at deactivation only if both the
	// kind and the manifest have it, so marking a kind auto_delete can't delete
	// shards written before.
	AutoDelete bool `json:"auto_delete,omitempty"`
}

// Range is a range of the shard's own positions covered by one LTX file.
type Range struct{ Min, Max uint64 }

func (r Range) String() string { return fmt.Sprintf("%d-%d", r.Min, r.Max) }

// File names one of a shard's LTX files: the positions it covers and the epoch
// of the node that wrote it, so a crashed node's leftover file never collides
// with a new one.
type File struct {
	Range
	Epoch uint64
}

func (f File) String() string { return fmt.Sprintf("%s.%d", f.Range, f.Epoch) }

// ParseFile parses "min-max.epoch".
func ParseFile(s string) (File, error) {
	bad := fmt.Errorf("bad file name %q", s)
	rg, ep, ok := strings.Cut(s, ".")
	if !ok {
		return File{}, bad
	}
	a, b, ok := strings.Cut(rg, "-")
	if !ok {
		return File{}, bad
	}
	min, err1 := strconv.ParseUint(a, 10, 64)
	max, err2 := strconv.ParseUint(b, 10, 64)
	epoch, err3 := strconv.ParseUint(ep, 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || min == 0 || max < min {
		return File{}, bad
	}
	return File{Range{min, max}, epoch}, nil
}

// List parses the manifest's file list.
func (m *Manifest) List() ([]File, error) {
	out := make([]File, len(m.Files))
	for i, s := range m.Files {
		f, err := ParseFile(s)
		if err != nil {
			return nil, err
		}
		out[i] = f
	}
	return out, nil
}

// LastPosition is the last position in the manifest's files, or 0.
func (m *Manifest) LastPosition() uint64 {
	fs, err := m.List()
	if err != nil || len(fs) == 0 {
		return 0
	}
	return fs[len(fs)-1].Max
}

func (m *Manifest) clone() *Manifest {
	c := *m
	c.Files = append([]string(nil), m.Files...)
	return &c
}

// ManifestKey is the object key of a shard's manifest.
func ManifestKey(shard string) string { return "manifests/" + shard }

// FileKey is the object key of one of a shard's LTX files.
func FileKey(shard, generation string, f File) string {
	return fmt.Sprintf("shards/%s/%s/%s.ltx", shard, generation, f)
}

// NewGeneration returns a random generation name.
func NewGeneration() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

// Version identifies one version of a manifest (its ETag).
type Version = blob.ETag

// ManifestStore reads and changes shards' manifests in blob storage. Every
// change is a compare-and-swap.
type ManifestStore struct{ store *blob.Store }

// NewManifestStore returns a ManifestStore over blob storage.
func NewManifestStore(store *blob.Store) ManifestStore { return ManifestStore{store} }

// Get returns a shard's manifest (blob.ErrNotFound if there is none).
func (b ManifestStore) Get(ctx context.Context, shard string) (*Manifest, Version, error) {
	rc, etag, err := b.store.Get(ctx, ManifestKey(shard))
	if err != nil {
		return nil, "", err
	}
	defer rc.Close()
	var m Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return nil, "", fmt.Errorf("manifest %s: %w", shard, err)
	}
	return &m, etag, nil
}

// Put creates the manifest when expected is nil (blob.ErrExists if somebody
// else has) and otherwise replaces the version expected (blob.ErrConflict if
// somebody else changed it).
func (b ManifestStore) Put(ctx context.Context, shard string, m *Manifest, expected *Version) (Version, error) {
	if expected == nil {
		return b.store.Create(ctx, ManifestKey(shard), encode(m))
	}
	return b.store.Update(ctx, ManifestKey(shard), encode(m), *expected)
}

// Delete removes the manifest if it is still at expected (blob.ErrConflict if
// somebody else changed it, blob.ErrNotFound if it is gone). It is a
// conditional delete, which the startup check requires of the store.
func (b ManifestStore) Delete(ctx context.Context, shard string, expected Version) error {
	return b.store.Delete(ctx, ManifestKey(shard), expected)
}

// Files reads and writes shards' manifests and LTX files in blob storage.
// Lost responses are handled by blob.Store, which tags every write with a
// write id and recognises its own earlier attempts.
type Files struct {
	Blob   *blob.Store
	Node   string // the node whose journal new shards write to
	Leases *coord.Registry
	// TempDir is where files are built and downloaded that are too large to hold
	// in memory (deltas, snapshots, the files of a restore). It is in the data
	// directory, never the scratch directory, which is tmpfs and so memory too.
	TempDir string
	// SpoolLimit is how large a delta gets before it is built in a temporary file,
	// spool.DefaultLimit if zero.
	SpoolLimit int
}

func (f *Files) spoolLimit() int {
	if f.SpoolLimit > 0 {
		return f.SpoolLimit
	}
	return spool.DefaultLimit
}

func (f *Files) manifests() ManifestStore { return ManifestStore{f.Blob} }

// GetManifest returns a shard's manifest and its version (blob.ErrNotFound if
// there is none).
func (f *Files) GetManifest(ctx context.Context, shard string) (*Manifest, blob.ETag, error) {
	return f.manifests().Get(ctx, shard)
}

func encode(m *Manifest) *bytes.Reader {
	b, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return bytes.NewReader(b)
}

// Handle tracks one shard's manifest and its ETag.
type Handle struct {
	F    *Files
	Name string

	Manifest *Manifest
	ETag     blob.ETag

	writes map[string]blob.WriteID // write ids of files put but not yet adopted by the manifest, by key

	// unknown is the manifest of an update whose outcome was not learned (every
	// answer was lost and it couldn't be read back). The handle's version may be
	// behind the stored one until settle has looked.
	unknown *Manifest
}

// Open loads a shard's manifest, creating it with a new generation if the
// shard doesn't exist yet. Created reports which happened.
func (f *Files) Open(ctx context.Context, shard string, autoDelete bool) (h *Handle, created bool, err error) {
	h, err = f.Attach(ctx, shard)
	if errors.Is(err, blob.ErrNotFound) {
		return f.Create(ctx, shard, autoDelete)
	}
	return h, false, err
}

// Attach loads the handle of a shard that must already have a manifest
// (blob.ErrNotFound otherwise).
func (f *Files) Attach(ctx context.Context, shard string) (*Handle, error) {
	m, etag, err := f.GetManifest(ctx, shard)
	if err != nil {
		return nil, err
	}
	return &Handle{F: f, Name: shard, Manifest: m, ETag: etag}, nil
}

// Create creates a shard's manifest, owned by this node, for a caller that has
// just found none. autoDelete records that the shard's kind has auto_delete. created is false if somebody
// else created it first: the handle is then theirs, to claim like any other shard.
func (f *Files) Create(ctx context.Context, shard string, autoDelete bool) (h *Handle, created bool, err error) {
	m := &Manifest{Generation: NewGeneration(), Owner: f.Node, NodeEpoch: f.Blob.Epoch(), Files: []string{}, Journal: f.Node, AutoDelete: autoDelete}
	etag, err := f.manifests().Put(ctx, shard, m, nil)
	if err == nil {
		return &Handle{F: f, Name: shard, Manifest: m, ETag: etag}, true, nil
	}
	if !errors.Is(err, blob.ErrExists) {
		return nil, false, err
	}
	// Somebody got there first: the shard is theirs, unless they have deleted
	// the manifest again since.
	h, err = f.Attach(ctx, shard)
	if errors.Is(err, blob.ErrNotFound) {
		return f.Create(ctx, shard, autoDelete)
	}
	return h, false, err
}

// update writes next with compare-and-swap and adopts it. A conflict is
// somebody else's change: blob.Store has already ruled out an earlier attempt
// of this writer whose response was lost. op names the operation for errors.
//
// If the outcome stays unknown (blob.ErrUnknown: every answer lost and the
// read-back too), the write may have landed: the handle looks again at once, and if that fails too
// it looks before its next operation (settle), so it never keeps writing from a
// version that is gone, which would conflict for good.
func (h *Handle) update(ctx context.Context, op string, next *Manifest) error {
	etag, err := h.F.manifests().Put(ctx, h.Name, next, &h.ETag)
	if err != nil {
		if errors.Is(err, blob.ErrUnknown) {
			// blob.Store has already tried to settle this on a context of its own, so
			// this is an outcome nobody could learn.
			h.unknown = next
			if h.settle(ctx) == nil && h.Manifest == next {
				return nil // it landed
			}
		}
		return fmt.Errorf("%s of %s: %w", op, ManifestKey(h.Name), err)
	}
	h.Manifest, h.ETag = next, etag
	return nil
}

// settle finds out what became of an update whose outcome was unknown: if the
// stored manifest is the one that was written, the handle adopts it. If it is
// anything else the handle keeps its version, and a conflict on the next write
// is somebody else's change. It reads nothing if there is nothing unknown.
func (h *Handle) settle(ctx context.Context) error {
	if h.unknown == nil {
		return nil
	}
	cur, etag, err := h.F.manifests().Get(ctx, h.Name)
	if err != nil {
		return err
	}
	want, _ := json.Marshal(h.unknown)
	got, _ := json.Marshal(cur)
	if bytes.Equal(want, got) {
		h.Manifest, h.ETag = h.unknown, etag
	}
	h.unknown = nil
	return nil
}

// ErrOwnedElsewhere means another node owns the shard and its lease is valid.
var ErrOwnedElsewhere = errors.New("shardfile: shard is owned by another node")

// Claim makes this node the shard's owner at its current epoch and points the
// manifest's journal at the node's journal, with one compare-and-swap. Nothing is
// written if the manifest already says so. A shard owned by another node whose
// lease is valid is not claimed (ErrOwnedElsewhere).
//
// Adopting a journal clears the manifest's journal for every shard it wrote a delta
// for, so a node that resumes such a shard must claim it before the shard's
// first new commit reaches the journal: restore finds the tail through the manifest.
func (h *Handle) Claim(ctx context.Context) error {
	if err := h.settle(ctx); err != nil {
		return err
	}
	m := h.Manifest
	if m.Owner != "" && m.Owner != h.F.Node && h.F.Leases != nil {
		l, err := h.F.Leases.Get(ctx, m.Owner)
		switch {
		case err == nil && l.Epoch == m.NodeEpoch && l.ValidAt(time.Now()):
			return fmt.Errorf("%w: %s (%s)", ErrOwnedElsewhere, h.Name, m.Owner)
		case err != nil && !errors.Is(err, blob.ErrNotFound):
			return err
		}
	}
	next := m.clone()
	next.Owner, next.NodeEpoch, next.Journal = h.F.Node, h.F.Blob.Epoch(), h.F.Node
	if next.Owner == m.Owner && next.NodeEpoch == m.NodeEpoch && next.Journal == m.Journal {
		return nil
	}
	return h.update(ctx, "claim", next)
}

// ReleaseOwnership gives the shard up: no owner and no journal. The caller has
// written a delta for the whole tail first, so nothing is left in any journal, and
// the next node to activate the shard claims it like any unowned shard.
func (h *Handle) ReleaseOwnership(ctx context.Context) error {
	if err := h.settle(ctx); err != nil {
		return err
	}
	next := h.Manifest.clone()
	next.Owner, next.NodeEpoch, next.Journal = "", 0, ""
	return h.update(ctx, "release", next)
}

// Delete removes the shard's manifest by conditional delete at the version the
// handle holds (blob.ErrConflict if somebody else changed it). It is how a shard
// that was never written is dropped at deactivation.
func (h *Handle) Delete(ctx context.Context) error {
	if err := h.settle(ctx); err != nil {
		return err
	}
	return h.F.manifests().Delete(ctx, h.Name, h.ETag)
}

// DeleteShard removes a shard: its manifest is deleted by conditional delete (a
// conflict, if somebody changed it, leaves everything as it is), then the LTX
// files of its generation. A crash in between leaves files nobody refers to. The
// next request creates a new shard with a new generation, so it never reads the
// old one's data. Any tail left in the node journal is never read again (its
// generation no longer matches any manifest). Deleting a shard that doesn't exist
// is done.
func (f *Files) DeleteShard(ctx context.Context, shard string) error {
	h, err := f.Attach(ctx, shard)
	if errors.Is(err, blob.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	return h.DeleteShard(ctx)
}

// DeleteShard is Files.DeleteShard for a shard the caller holds a handle on: the
// manifest is deleted at the handle's version, so a change by anybody else
// (blob.ErrConflict) leaves everything as it is.
func (h *Handle) DeleteShard(ctx context.Context) error {
	f := h.F
	if err := h.Delete(ctx); err != nil && !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	prefix := fmt.Sprintf("shards/%s/%s/", h.Name, h.Manifest.Generation)
	var keys []string
	for info, err := range f.Blob.List(ctx, prefix) {
		if err != nil {
			return err
		}
		keys = append(keys, info.Key)
	}
	for _, k := range keys {
		if err := f.Blob.Delete(ctx, k, ""); err != nil && !errors.Is(err, blob.ErrNotFound) {
			return err
		}
	}
	return nil
}
