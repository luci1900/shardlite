package shardfile

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/model/cluster"
	"github.com/luci1900/shardlite/internal/model/pos"
	"github.com/luci1900/shardlite/internal/model/shard"
	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/storage/journal"
	"github.com/luci1900/shardlite/internal/storage/lease"
	"github.com/luci1900/shardlite/internal/syncs/notify"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// Manifest is the per-shard object in blob storage (see docs/internals.md,
// Manifest).
type Manifest struct {
	Generation shard.Gen    `json:"generation"` // random, set at creation, part of the shard's object paths
	Owner      cluster.Node `json:"owner"`      // the node allowed to write the shard, "" if released
	NodeEpoch  pos.Epoch    `json:"node_epoch"` // the owner's lease epoch when it claimed the shard
	Files      []File       `json:"files"`      // base and deltas ("min-max.epoch" in JSON), in order
	Journal    cluster.Node `json:"journal"`    // the node journal holding commits after the last file, "" if there are none
	// NextReminder is when the shard next needs waking, Unix minutes, omitted when
	// there is none (docs/internals.md, Manifest). The owner's deltas, rebases and
	// deactivations set it, with its entry in the reminder index created before the
	// write. Claims and releases keep it.
	NextReminder Minute `json:"next_reminder,omitempty"`
	// Rev counts the manifest's writes, so no two versions have the same bytes and
	// ETag. The reminder index's pruning rule relies on a changed version staying changed.
	Rev uint64 `json:"rev,omitempty"`
}

// Range is a range of the shard's own positions covered by one LTX file.
type Range struct{ Min, Max pos.Position }

func (r Range) String() string { return fmt.Sprintf("%d-%d", r.Min, r.Max) }

// File names one of a shard's LTX files: the positions it covers and the
// incarnation that wrote it. Epochs count per node, so the epoch alone would let
// two nodes write the same name, and a writer that finds a file at its key takes
// it for its own (Handle.putFile). With the writer's node and epoch in it, no
// other process's file can have the name.
type File struct {
	Range
	By lease.Incarnation
}

func (f File) String() string { return fmt.Sprintf("%s.%s.%d", f.Range, f.By.Node, f.By.Epoch) }

// MarshalText is the file's "min-max.node.epoch" name, so the manifest's JSON
// stays a list of strings.
func (f File) MarshalText() ([]byte, error) { return []byte(f.String()), nil }

// UnmarshalText parses a file's name (ParseFile). A manifest with an entry
// that doesn't parse fails its read: the manifest is then corrupt.
func (f *File) UnmarshalText(b []byte) error {
	parsed, err := ParseFile(string(b))
	if err != nil {
		return err
	}
	*f = parsed
	return nil
}

// ParseFile parses "min-max.node.epoch". A node's name may hold dots, so the
// range ends at the first and the epoch starts after the last.
func ParseFile(s string) (File, error) {
	bad := fmt.Errorf("bad file name %q", s)
	rg, rest, ok := strings.Cut(s, ".")
	if !ok {
		return File{}, bad
	}
	i := strings.LastIndex(rest, ".")
	if i < 0 {
		return File{}, bad
	}
	node, ep := rest[:i], rest[i+1:]
	a, b, ok := strings.Cut(rg, "-")
	if !ok || !cluster.ValidNode(node) {
		return File{}, bad
	}
	min, err1 := strconv.ParseUint(a, 10, 64)
	max, err2 := strconv.ParseUint(b, 10, 64)
	epoch, err3 := strconv.ParseUint(ep, 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || min == 0 || max < min {
		return File{}, bad
	}
	return File{Range{pos.Position(min), pos.Position(max)}, lease.Incarnation{Node: cluster.Node(node), Epoch: pos.Epoch(epoch)}}, nil
}

// LastPosition is the last position in the manifest's files, or 0.
func (m *Manifest) LastPosition() pos.Position {
	if len(m.Files) == 0 {
		return 0
	}
	return m.Files[len(m.Files)-1].Max
}

func (m *Manifest) clone() *Manifest {
	c := *m
	c.Files = append([]File(nil), m.Files...)
	return &c
}

// A shard's manifest and files each have a prefix, under the state and data roles
// (docs/internals.md, Data and state).
const (
	ManifestPrefix = "state/shards/" // the shards' manifests (state, one per shard), by lane
	FilesPrefix    = "data/shards/"  // all shards' LTX files (data), by lane
)

// ManifestKey is the object key of a shard's manifest: state/shards/{lane}/{kind}/{id}.
// It leads with the shard's lane as its files and indexes do, so a spike of claims
// spreads over the store's key ranges.
func ManifestKey(n shard.Name) string { return ManifestPrefix + n.Lane().String() + "/" + n.String() }

// LanePrefix is the key prefix of the LTX files of all the shards in a lane.
func LanePrefix(l shard.Lane) string { return FilesPrefix + l.String() + "/" }

// ShardPrefix is the key prefix of all of a shard's LTX files:
// data/shards/{lane}/{kind}/{id}/.
func ShardPrefix(n shard.Name) string { return LanePrefix(n.Lane()) + n.String() + "/" }

// GenerationPrefix is the key prefix of the LTX files of one generation of a shard.
func GenerationPrefix(n shard.Name, generation shard.Gen) string {
	return ShardPrefix(n) + string(generation) + "/"
}

// FileKey is the object key of one of a shard's LTX files.
func FileKey(n shard.Name, generation shard.Gen, f File) string {
	return fmt.Sprintf("%s%s.ltx", GenerationPrefix(n, generation), f)
}

// NewGeneration returns a random generation name.
func NewGeneration() shard.Gen {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return shard.Gen(b)
}

// Version identifies one version of a manifest (its ETag).
type Version = blob.ETag

// ManifestStore reads and changes shards' manifests in blob storage. Every
// change is a compare-and-swap.
type ManifestStore struct{ store *blob.Store }

// NewManifestStore returns a ManifestStore over blob storage.
func NewManifestStore(store *blob.Store) ManifestStore { return ManifestStore{store} }

// Get returns a shard's manifest (blob.ErrNotFound if there is none).
func (b ManifestStore) Get(ctx context.Context, n shard.Name) (*Manifest, Version, error) {
	rc, etag, err := b.store.Get(ctx, ManifestKey(n))
	if err != nil {
		return nil, "", err
	}
	defer rc.Close()
	var m Manifest
	if err := json.NewDecoder(rc).Decode(&m); err != nil {
		return nil, "", fmt.Errorf("manifest %s: %w", n, err)
	}
	return &m, etag, nil
}

// Put creates the manifest when expected is nil (blob.ErrExists if somebody
// else has) and otherwise replaces the version expected (blob.ErrConflict if
// somebody else changed it).
func (b ManifestStore) Put(ctx context.Context, n shard.Name, m *Manifest, expected *Version) (Version, error) {
	if expected == nil {
		return b.store.Create(ctx, ManifestKey(n), encode(m))
	}
	return b.store.Update(ctx, ManifestKey(n), encode(m), *expected)
}

// Delete removes the manifest if it is still at expected (blob.ErrConflict if
// somebody else changed it, blob.ErrNotFound if it is gone). It is a
// conditional delete, which the startup check requires of the store.
func (b ManifestStore) Delete(ctx context.Context, n shard.Name, expected Version) error {
	return b.store.Delete(ctx, ManifestKey(n), expected)
}

// Files reads and writes shards' manifests and LTX files in blob storage.
// Lost responses are handled by blob.Store, which tags every write with a
// write id and recognises its own earlier attempts.
type Files struct {
	Blob   *blob.Store
	Node   cluster.Node // the node whose journal new shards write to
	Leases *lease.Registry
	// Temp is where files are built and downloaded (deltas, snapshots, the
	// files of a restore, the commits of a restored tail). It is in the data
	// directory, and required.
	Temp *tempfile.Dir
	// Now is the time adoption records its reminders at, time.Now if nil.
	Now func() time.Time
	// Log is where a failed delete of an LTX file the node knows is garbage is
	// logged (see garbage.go). The default logger if nil.
	Log *slog.Logger
	// Leaked counts the LTX files the node knew no manifest lists and could not
	// delete (see garbage.go): the node's Stats.LeakedFiles, nil where nobody
	// counts.
	Leaked *notify.Counter
}

func (f *Files) now() time.Time {
	if f.Now != nil {
		return f.Now()
	}
	return time.Now()
}

func (f *Files) manifests() ManifestStore { return ManifestStore{f.Blob} }

// Journal returns one node's journal, in blob storage through Blob, so its
// retries and write ids are Blob's and a lost response is recognised as this
// node's own. The journal is the one named by node: this Files' own (f.Node),
// or a dead one's being adopted or read for a restore.
//
// The write ids name this node, the writer, whichever journal it writes: an
// adopter's fence must never read as the dead node's own segment, which a
// zombie of that node at the same epoch could have written under the same
// number. Every journal shares Blob's one counter, so the ids never repeat: a
// repeated one would name two attempts as one.
func (f *Files) Journal(node cluster.Node) journal.Store { return journal.NewBlob(f.Blob, node) }

// GetManifest returns a shard's manifest and its version (blob.ErrNotFound if
// there is none).
func (f *Files) GetManifest(ctx context.Context, n shard.Name) (*Manifest, blob.ETag, error) {
	return f.manifests().Get(ctx, n)
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
	Name shard.Name

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
func (f *Files) Open(ctx context.Context, n shard.Name) (h *Handle, created bool, err error) {
	h, err = f.Attach(ctx, n)
	if errors.Is(err, blob.ErrNotFound) {
		return f.Create(ctx, n)
	}
	return h, false, err
}

// Attach loads the handle of a shard that must already have a manifest
// (blob.ErrNotFound otherwise).
func (f *Files) Attach(ctx context.Context, n shard.Name) (*Handle, error) {
	m, etag, err := f.GetManifest(ctx, n)
	if err != nil {
		return nil, err
	}
	return &Handle{F: f, Name: n, Manifest: m, ETag: etag}, nil
}

// Create creates a shard's manifest, owned by this node, for a caller that has
// just found none. created is false if somebody else created it first: the
// handle is then theirs, to claim like any other shard.
func (f *Files) Create(ctx context.Context, n shard.Name) (h *Handle, created bool, err error) {
	m := &Manifest{Generation: NewGeneration(), Owner: f.Node, NodeEpoch: f.Blob.Epoch(), Files: []File{}, Journal: f.Node}
	etag, err := f.manifests().Put(ctx, n, m, nil)
	if err == nil {
		return &Handle{F: f, Name: n, Manifest: m, ETag: etag}, true, nil
	}
	if !errors.Is(err, blob.ErrExists) {
		return nil, false, err
	}
	// Somebody got there first: the shard is theirs, unless they have deleted
	// the manifest again since.
	h, err = f.Attach(ctx, n)
	if errors.Is(err, blob.ErrNotFound) {
		return f.Create(ctx, n)
	}
	return h, false, err
}

// setReminder records next_reminder in a manifest about to be written by update
// (0 for none). The write's entry in the reminder index comes first, keyed by the
// version the write CASes against, and a failure fails the write. A reminder in
// the minute the manifest already has (Unix minutes) needs no new entry: the entry that backs
// the old one backs it too, since an entry is never deleted while it is backed.
func (h *Handle) setReminder(ctx context.Context, next *Manifest, reminder Minute) error {
	if reminder > 0 && reminder != h.Manifest.NextReminder {
		e := Reminder{Minute: reminder, Shard: h.Name, Base: BaseOf(h.ETag)}
		if err := NewReminderIndex(h.F.Blob).Create(ctx, e); err != nil {
			return err
		}
	}
	next.NextReminder = reminder
	return nil
}

// SetReminder writes next_reminder alone (Unix minutes, 0 for none), with its
// entry in the reminder index created first. It is for a deactivation with no
// tail, which has no delta to carry the shard's next reminder: a reminder that went
// stale, or an adoption's, would otherwise leave the manifest naming a time
// the shard has nothing due at, and the dispatcher waking it for nothing. It
// writes nothing if the manifest has that already. With release the same update gives the shard up
// (see Hooks.Release), and nothing is written only if a write of that kind landed already.
func (h *Handle) SetReminder(ctx context.Context, reminder Minute, release bool) error {
	if err := h.settle(ctx); err != nil {
		return err
	}
	if release && h.Manifest.Owner == "" || !release && h.Manifest.NextReminder == reminder {
		return nil
	}
	next := h.Manifest.clone()
	if err := h.setReminder(ctx, next, reminder); err != nil {
		return err
	}
	if release {
		giveUp(next)
	}
	return h.update(ctx, "reminder", next)
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
	next.Rev = h.Manifest.Rev + 1
	etag, err := h.F.manifests().Put(ctx, h.Name, next, &h.ETag)
	if err != nil {
		if errors.Is(err, blob.ErrUnknown) {
			// blob.Store has already tried to settle this on a context of its own, so
			// this is an outcome nobody could learn.
			h.unknown = next
			serr := h.settle(ctx)
			if serr == nil && h.Manifest == next {
				return nil // it landed
			}
			if errors.Is(serr, ErrMovedOn) {
				return fmt.Errorf("%s of %s: %w", op, ManifestKey(h.Name), serr)
			}
		}
		return fmt.Errorf("%s of %s: %w", op, ManifestKey(h.Name), err)
	}
	h.Manifest, h.ETag = next, etag
	return nil
}

// Unsettled reports whether the handle has an update whose outcome it has not
// learned (every answer was lost and the stored manifest could not be read). The
// shard may be unowned then if the update gave it up.
func (h *Handle) Unsettled() bool { return h.unknown != nil }

// settle finds out what became of an update whose outcome was unknown: if the
// stored manifest is the one that was written, the handle adopts it, and if it is
// still the handle's version the update didn't land. Anything else means the
// update landed and the manifest has moved on since (another node claimed a shard
// the update gave up, or adopted one it didn't), so the shard is not this node's
// and settle returns ErrMovedOn: it must be neither served nor resumed. It reads
// nothing if there is nothing unknown.
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
	written := h.unknown
	h.unknown = nil
	switch {
	case bytes.Equal(want, got):
		h.Manifest, h.ETag = written, etag
	case etag != h.ETag:
		return fmt.Errorf("%w: %s", ErrMovedOn, ManifestKey(h.Name))
	}
	return nil
}

var (
	// ErrMovedOn means an update whose outcome was unknown turned out to have
	// landed and the manifest has changed since, by another node: the shard is
	// not this node's any more.
	ErrMovedOn = errors.New("shardfile: the manifest moved on after an update with an unknown outcome")
	// ErrOwnedElsewhere means another node owns the shard: its lease shows
	// nothing that lets the shard be claimed. Claim returns it as an
	// *OwnedElsewhereError, which names the owner.
	ErrOwnedElsewhere = errors.New("shardfile: shard is owned by another node")
	// ErrOwnerNotAdopted means the shard's owner is dead but its journal, which
	// may hold the shard's last commits, hasn't been adopted yet: the shard can't
	// be claimed until it has.
	ErrOwnerNotAdopted = errors.New("shardfile: the shard's owner is dead and not adopted yet")
)

// OwnedElsewhereError is ErrOwnedElsewhere with the owner's name.
type OwnedElsewhereError struct {
	Shard shard.Name
	Owner cluster.Node
}

func (e *OwnedElsewhereError) Error() string {
	return fmt.Sprintf("%v: %s (%s)", ErrOwnedElsewhere, e.Shard, e.Owner)
}

func (e *OwnedElsewhereError) Unwrap() error { return ErrOwnedElsewhere }

// Claim makes this node the shard's owner at its current epoch and points the
// manifest's journal at the node's journal, with one compare-and-swap. Nothing is
// written if the manifest already says so.
//
// A shard another node owns is claimed only once nothing of it can be left in
// that node's journal (docs/internals.md, Takeover): the owner's node is adopted
// up to the owner's epoch (a release counts), or the owner is dead (a newer epoch,
// or another node has marked its lease) and an adopter has cleared the manifest's
// journal. No timestamp is compared: a lease that only looks expired is still
// the owner's (ErrOwnedElsewhere), and a dead owner not yet adopted gives
// ErrOwnerNotAdopted.
//
// Adopting a journal clears the manifest's journal for every shard it wrote a delta
// for, so a node that resumes such a shard must claim it before the shard's
// first new commit reaches the journal: restore finds the tail through the manifest.
func (h *Handle) Claim(ctx context.Context) error {
	if err := h.CheckClaimable(ctx); err != nil {
		return err
	}
	m := h.Manifest
	next := m.clone()
	next.Owner, next.NodeEpoch, next.Journal = h.F.Node, h.F.Blob.Epoch(), h.F.Node
	if next.Owner == m.Owner && next.NodeEpoch == m.NodeEpoch && next.Journal == m.Journal {
		return nil
	}
	return h.update(ctx, "claim", next)
}

// StartOver gives a shard with nothing to restore a new generation, for an
// activation that starts it empty. Its positions start again at 1, and an
// earlier activation that failed after its first sync may have left commits at
// those positions in the journal: in a new generation they are never read.
// Anything the old generation holds that no manifest lists is garbage, which
// the old shard's next rebase would never sweep (it names the new generation)
// and a separate cleanup finds (docs/internals.md, Garbage).
func (h *Handle) StartOver(ctx context.Context) error {
	next := h.Manifest.clone()
	next.Generation = NewGeneration()
	next.NextReminder = 0
	return h.update(ctx, "new generation", next)
}

// CheckClaimable returns what Claim would return for a shard it can't claim
// (ErrOwnedElsewhere or ErrOwnerNotAdopted, or a failed read), without writing
// anything. It lets a node find out that another node owns a shard before it
// starts to activate it.
func (h *Handle) CheckClaimable(ctx context.Context) error {
	if err := h.settle(ctx); err != nil {
		return err
	}
	m := h.Manifest
	if m.Owner != "" && m.Owner != h.F.Node && h.F.Leases != nil {
		l, err := h.F.Leases.Get(ctx, m.Owner)
		switch {
		case errors.Is(err, blob.ErrNotFound):
			// A node with no lease has no journal to adopt.
		case err != nil:
			return err
		case l.Adopted >= m.NodeEpoch:
		case m.Journal == "" && ownerDead(l, m):
		case ownerDead(l, m):
			return fmt.Errorf("%w: %s (%s at epoch %d)", ErrOwnerNotAdopted, h.Name, m.Owner, m.NodeEpoch)
		default:
			return &OwnedElsewhereError{Shard: h.Name, Owner: m.Owner}
		}
	}
	return nil
}

// ownerDead reports whether the lease shows the manifest's owner dead without
// comparing clocks: its node has a newer epoch, or another incarnation has
// marked the lease (which an adopter does only once it has seen the lease go
// unrenewed for a TTL).
func ownerDead(l *lease.Record, m *Manifest) bool {
	owner := lease.Incarnation{Node: m.Owner, Epoch: m.NodeEpoch}
	return l.Epoch > m.NodeEpoch || (l.AdoptedBy != nil && *l.AdoptedBy != owner)
}

// giveUp is the edit that releases a shard: no owner and no journal.
func giveUp(m *Manifest) { m.Owner, m.NodeEpoch, m.Journal = "", 0, "" }

// DeleteShard removes a shard the caller holds a handle on: its manifest is
// deleted by conditional delete at the handle's version (a conflict, if somebody
// changed it, leaves everything as it is), then the LTX files of its generation.
// A crash in between leaves files nobody refers to. The next request creates a
// new shard with a new generation, so it never reads the old one's data, and the
// old files wait for a separate cleanup (docs/internals.md, Garbage). A file
// delete that fails is a leak (Files.leak) and fails the call: every file is
// tried, so one bad delete leaves as little behind as it can.
func (h *Handle) DeleteShard(ctx context.Context) error {
	f := h.F
	if err := h.settle(ctx); err != nil {
		return err
	}
	if err := f.manifests().Delete(ctx, h.Name, h.ETag); err != nil && !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	prefix := GenerationPrefix(h.Name, h.Manifest.Generation)
	var keys []string
	for info, err := range f.Blob.List(ctx, prefix) {
		if err != nil {
			return err
		}
		keys = append(keys, info.Key)
	}
	var firstErr error
	for _, k := range keys {
		if err := f.Blob.Delete(ctx, k, ""); err != nil && !errors.Is(err, blob.ErrNotFound) {
			f.leak(h.Name, k, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
