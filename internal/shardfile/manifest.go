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
	"sync"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
)

// Manifest is the per-shard record in blob storage (see design.md,
// Manifest). Last owner and pin come with the cluster and pinning.
type Manifest struct {
	Generation string   `json:"generation"` // random, set at creation, part of the shard's object paths
	Owner      string   `json:"owner"`      // the node allowed to write the shard, "" if released
	NodeEpoch  uint64   `json:"node_epoch"` // the owner's lease epoch when it claimed the shard
	Files      []string `json:"files"`      // base and deltas, "min-max.epoch" (min-max in the shard's own positions), in order
	Journal    string   `json:"journal"`    // the node journal holding commits after the last file, "" if there are none
	// Ephemeral marks a shard of an ephemeral kind: nothing of it is shipped, so
	// it never has files or a journal.
	Ephemeral bool `json:"ephemeral,omitempty"`
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

// ManifestStore reads and changes shards' manifests. Every change is a
// compare-and-swap.
type ManifestStore interface {
	// Get returns a shard's manifest (blob.ErrNotFound if there is none).
	Get(ctx context.Context, shard string) (*Manifest, Version, error)
	// Put creates the manifest when expected is nil (blob.ErrExists if somebody
	// else has) and otherwise replaces the version expected (blob.ErrConflict if
	// somebody else changed it).
	Put(ctx context.Context, shard string, m *Manifest, expected *Version) (Version, error)
	// Delete removes the manifest if it is still at expected (blob.ErrConflict if
	// somebody else changed it, blob.ErrNotFound if it is gone). It is a
	// conditional delete, which the startup check requires of the store.
	Delete(ctx context.Context, shard string, expected Version) error
}

type blobManifests struct{ store *blob.Store }

// NewManifestStore returns a ManifestStore over blob storage.
func NewManifestStore(store *blob.Store) ManifestStore { return blobManifests{store} }

func (b blobManifests) Get(ctx context.Context, shard string) (*Manifest, Version, error) {
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

func (b blobManifests) Put(ctx context.Context, shard string, m *Manifest, expected *Version) (Version, error) {
	if expected == nil {
		return b.store.Create(ctx, ManifestKey(shard), encode(m))
	}
	return b.store.Update(ctx, ManifestKey(shard), encode(m), *expected)
}

func (b blobManifests) Delete(ctx context.Context, shard string, expected Version) error {
	return b.store.Delete(ctx, ManifestKey(shard), expected)
}

// Files reads and writes shards' manifests and LTX files in blob storage.
// Lost responses are handled by blob.Store, which tags every write with a
// write id and recognises its own earlier attempts.
type Files struct {
	Blob      *blob.Store
	Node      string        // the node whose journal new shards write to
	Manifests ManifestStore // over Blob if nil
	Leases    coord.NodeRegistry

	once sync.Once
}

func (f *Files) manifests() ManifestStore {
	f.once.Do(func() {
		if f.Manifests == nil {
			f.Manifests = NewManifestStore(f.Blob)
		}
	})
	return f.Manifests
}

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
}

// Open loads a shard's manifest, creating it with a new generation if the
// shard doesn't exist yet. Created reports which happened.
func (f *Files) Open(ctx context.Context, shard string) (h *Handle, created bool, err error) {
	m, etag, err := f.GetManifest(ctx, shard)
	if err == nil {
		return &Handle{F: f, Name: shard, Manifest: m, ETag: etag}, false, nil
	} else if !errors.Is(err, blob.ErrNotFound) {
		return nil, false, err
	}
	return f.Create(ctx, shard)
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

// Restore-time helper for callers that keep a manifest between activations.
func (f *Files) HandleFor(shard string, m *Manifest, etag blob.ETag) *Handle {
	return &Handle{F: f, Name: shard, Manifest: m, ETag: etag}
}

// Create creates a shard's manifest, owned by this node, for a caller that has
// just found none. created is false if somebody else created it first: the
// handle is then theirs, to claim like any other shard.
func (f *Files) Create(ctx context.Context, shard string) (h *Handle, created bool, err error) {
	return f.CreateAs(ctx, shard, false)
}

// CreateAs is Create for a shard of an ephemeral kind if ephemeral is set: its
// manifest says so and names no journal.
func (f *Files) CreateAs(ctx context.Context, shard string, ephemeral bool) (h *Handle, created bool, err error) {
	m := &Manifest{Generation: NewGeneration(), Owner: f.Node, NodeEpoch: f.Blob.Epoch(), Files: []string{}, Journal: f.Node, Ephemeral: ephemeral}
	if ephemeral {
		m.Journal = ""
	}
	etag, err := f.manifests().Put(ctx, shard, m, nil)
	if err == nil {
		return &Handle{F: f, Name: shard, Manifest: m, ETag: etag}, true, nil
	}
	if !errors.Is(err, blob.ErrExists) {
		return nil, false, err
	}
	// Somebody got there first: the shard is theirs, unless they have deleted
	// the manifest again since.
	cur, v, gerr := f.manifests().Get(ctx, shard)
	if gerr != nil {
		if errors.Is(gerr, blob.ErrNotFound) {
			return f.CreateAs(ctx, shard, ephemeral)
		}
		return nil, false, gerr
	}
	return &Handle{F: f, Name: shard, Manifest: cur, ETag: v}, false, nil
}

// update writes next with compare-and-swap and adopts it. A conflict is
// somebody else's change: blob.Store has already ruled out an earlier attempt
// of this writer whose response was lost.
func (h *Handle) update(ctx context.Context, next *Manifest) error {
	etag, err := h.F.manifests().Put(ctx, h.Name, next, &h.ETag)
	if err != nil {
		return err
	}
	h.Manifest, h.ETag = next, etag
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
func (h *Handle) Claim(ctx context.Context) error { return h.ClaimAs(ctx, false) }

// ClaimAs is Claim for a shard of a kind that is ephemeral or not as given: the
// manifest is brought into line with it. An ephemeral shard names no journal.
// (The caller has checked that an ephemeral shard has no data to stop shipping.)
func (h *Handle) ClaimAs(ctx context.Context, ephemeral bool) error {
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
	next.Owner, next.NodeEpoch, next.Journal, next.Ephemeral = h.F.Node, h.F.Blob.Epoch(), h.F.Node, ephemeral
	if ephemeral {
		next.Journal = ""
	}
	if next.Owner == m.Owner && next.NodeEpoch == m.NodeEpoch && next.Journal == m.Journal && next.Ephemeral == m.Ephemeral {
		return nil
	}
	return h.update(ctx, next)
}

// ReleaseOwnership gives the shard up: no owner and no journal. The caller has
// written a delta for the whole tail first, so nothing is left in any journal, and
// the next node to activate the shard claims it like any unowned shard.
func (h *Handle) ReleaseOwnership(ctx context.Context) error {
	next := h.Manifest.clone()
	next.Owner, next.NodeEpoch, next.Journal = "", 0, ""
	return h.update(ctx, next)
}

// Delete removes the shard's manifest by conditional delete at the version the
// handle holds (blob.ErrConflict if somebody else changed it). It is how a shard
// that was never written, or is ephemeral, is dropped at deactivation.
func (h *Handle) Delete(ctx context.Context) error {
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
	if err := h.Delete(ctx); err != nil && !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	prefix := fmt.Sprintf("shards/%s/%s/", shard, h.Manifest.Generation)
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
