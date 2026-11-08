// Package lease holds a node's lease: the object in blob storage that all of
// the node's ownerships depend on (docs/internals.md, Node leases).
package lease

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/pos"
)

var (
	// ErrLost means somebody else changed the lease object: another life of
	// the node took it (a newer epoch) or an adopter did. The holder must stop.
	ErrLost = errors.New("lease: lost")
	// ErrExpired means the holder's own safety margin ran out before a
	// renewal got through.
	ErrExpired = errors.New("lease: expired")
	// ErrNotAdoptable means a lease an adopter read has changed since, or was
	// never its to adopt: the node renewed, restarted or was released, or
	// another node marked or finished its adoption. The adopter leaves it.
	ErrNotAdoptable = errors.New("lease: not adoptable")
)

// DefaultTTL is how long a lease is valid after its last renewal.
const DefaultTTL = 10 * time.Second

// NodeInfo is what a node publishes in its lease besides its epoch.
type NodeInfo struct {
	Name    cluster.Node `json:"name"`
	Address string       `json:"address"`
	// JournalFrom is the oldest segment of the node's journal that may still hold a tail
	// no delta covers, and JournalNext the next segment number the node would write
	// when it published this. An adopter reads from JournalFrom, and fences by probing
	// forward from JournalNext, so a JournalNext that is a little stale only costs probes.
	JournalFrom pos.Seg `json:"journal_from"`
	JournalNext pos.Seg `json:"journal_next"`
}

// Incarnation is one life of a node: its name and the epoch of its lease.
type Incarnation struct {
	Node  cluster.Node `json:"node"`
	Epoch pos.Epoch    `json:"epoch"`
}

// Record is a node's lease as of one write.
type Record struct {
	NodeInfo
	Epoch    pos.Epoch     `json:"epoch"` // +1 each time the node acquires its lease
	Renewed  time.Time     `json:"renewed"`
	TTL      time.Duration `json:"ttl"`
	Released bool          `json:"released,omitempty"` // a graceful shutdown: nothing is owned any more
	// AdoptedBy is the incarnation adopting the node's journal, nil if none. A
	// node adopting its own journal at startup names itself.
	AdoptedBy *Incarnation `json:"adopted_by,omitempty"`
	// Adopted is the highest epoch up to which every epoch's journal has been
	// adopted (or the node released its lease at it), 0 if none. A shard whose
	// owner's epoch is at most Adopted has nothing left in that journal.
	Adopted pos.Epoch `json:"adopted,omitempty"`

	ETag blob.ETag `json:"-"`
	// Previous is the lease an Acquire replaced, nil if the node never had one.
	Previous *Record `json:"-"`
}

// Incarnation returns the life this lease is for.
func (l *Record) Incarnation() Incarnation { return Incarnation{l.Name, l.Epoch} }

// ExpiresAt is when the lease runs out by its holder's clock.
func (l *Record) ExpiresAt() time.Time { return l.Renewed.Add(l.TTL) }

// ValidAt reports whether the lease looks valid at t: not released, not
// adopted by another node, not adopted up to its epoch and not past its expiry.
// The expiry compares the holder's clock with t, so it is a hint only (for
// failing fast, say): nothing that decides ownership may rest on it
// (docs/internals.md, Node leases).
func (l *Record) ValidAt(t time.Time) bool {
	return !l.Released && !l.adoptedByOther() && l.Adopted < l.Epoch && t.Before(l.ExpiresAt())
}

// Holds reports whether the lease still shows inc as the node's live life:
// the epoch is its own, the lease is not released and not marked by another
// incarnation, and no adoption has reached its epoch. It leaves the expiry
// out. Every way another node takes shards from a live owner writes the lease
// first (an adoption marks it, a restart takes a new epoch, a release marks it
// adopted), so a lease that Holds means nobody has begun a takeover.
func (l *Record) Holds(inc Incarnation) bool {
	return l.Incarnation() == inc && !l.Released && !l.adoptedByOther() && l.Adopted < l.Epoch
}

// adoptedByOther reports whether another incarnation is adopting the journal.
func (l *Record) adoptedByOther() bool {
	return l.AdoptedBy != nil && *l.AdoptedBy != l.Incarnation()
}

// Prefix is the key prefix of the leases (state, one per node).
const Prefix = "state/leases/"

// Key is the object key of a node's lease.
func Key(node cluster.Node) string { return Prefix + string(node) }

// RegistryOptions tune a Registry.
type RegistryOptions struct {
	TTL  time.Duration // DefaultTTL if zero
	Blob blob.Options
	Now  func() time.Time // time.Now if nil, for tests
	// AdoptionPoll is how often Acquire looks again while another node is
	// adopting the lease, 1 s if zero.
	AdoptionPoll time.Duration
}

// Registry keeps nodes' leases in blob storage.
type Registry struct {
	store *blob.Store
	opts  RegistryOptions
}

// NewRegistry returns a registry for the node named node. Its writes go
// through a blob.Store of their own with a random epoch in their write ids,
// since the node's real epoch isn't known until Acquire has returned.
func NewRegistry(b blob.Backend, node cluster.Node, opts RegistryOptions) *Registry {
	if opts.TTL <= 0 {
		opts.TTL = DefaultTTL
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.AdoptionPoll <= 0 {
		opts.AdoptionPoll = time.Second
	}
	var r [8]byte
	_, _ = rand.Read(r[:])
	return &Registry{store: blob.NewStore(b, node, pos.Epoch(binary.BigEndian.Uint64(r[:])), opts.Blob), opts: opts}
}

// TTL is the lease TTL the registry grants (DefaultTTL if none was set).
func (r *Registry) TTL() time.Duration { return r.opts.TTL }

func encodeLease(l *Record) *bytes.Reader {
	b, err := json.Marshal(l)
	if err != nil {
		panic(err)
	}
	return bytes.NewReader(b)
}

// Get returns a node's lease whether or not it is valid (check ValidAt).
// blob.ErrNotFound if the node never had one.
func (r *Registry) Get(ctx context.Context, node cluster.Node) (*Record, error) {
	rc, etag, err := r.store.Get(ctx, Key(node))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var l Record
	if err := json.NewDecoder(rc).Decode(&l); err != nil {
		return nil, fmt.Errorf("lease %s: %w", node, err)
	}
	l.ETag = etag
	return &l, nil
}

// Acquire takes the node's lease with a new epoch, whether or not the
// previous one is still valid: the caller knows its previous process is gone.
// The lease keeps the previous life's journal range until the adoption of it
// finishes (FinishAdoption), so a crash before then leaves the range in place.
// If an epoch is left to adopt, the new lease names its own incarnation as the
// adopter: the caller adopts its journal before it does anything else.
//
// While another node that looks alive is adopting the lease, Acquire waits for
// it to finish (or to die), polling: taking the lease from under it would let
// its fence land in the new life's journal (docs/internals.md, Takeover).
func (r *Registry) Acquire(ctx context.Context, info NodeInfo) (Record, error) {
	for {
		prev, err := r.Get(ctx, info.Name)
		next := Record{NodeInfo: info, Epoch: 1, TTL: r.opts.TTL}
		next.JournalFrom, next.JournalNext = 1, 1
		if err == nil {
			if busy, err := r.adoptedByLive(ctx, prev); err != nil {
				return Record{}, err
			} else if busy {
				if err := sleep(ctx, r.opts.AdoptionPoll); err != nil {
					return Record{}, err
				}
				continue
			}
			next.Epoch = prev.Epoch + 1
			next.JournalFrom, next.JournalNext = prev.JournalFrom, prev.JournalNext
			next.Adopted = prev.Adopted
			if prev.Adopted < prev.Epoch {
				next.AdoptedBy = &Incarnation{info.Name, next.Epoch}
			}
			next.Previous = prev
		} else if !errors.Is(err, blob.ErrNotFound) {
			return Record{}, err
		}
		next.Renewed = r.opts.Now()
		var etag blob.ETag
		if prev == nil {
			etag, err = r.store.Create(ctx, Key(info.Name), encodeLease(&next))
		} else {
			etag, err = r.store.Update(ctx, Key(info.Name), encodeLease(&next), prev.ETag)
		}
		switch {
		case err == nil:
			next.ETag = etag
			return next, nil
		case errors.Is(err, blob.ErrExists), errors.Is(err, blob.ErrConflict):
			// Somebody else wrote it between our read and our write. Epochs must
			// stay unique, so start again from what is there now.
			continue
		default:
			return Record{}, err
		}
	}
}

// adoptedByLive reports whether another node is adopting l and looks alive. Its
// liveness is judged by its own lease's expiry, which is a hint: waiting on a
// node that is in fact dead only delays the start until its lease looks
// expired, and not waiting on a live one costs a restart, never data.
func (r *Registry) adoptedByLive(ctx context.Context, l *Record) (bool, error) {
	if l.AdoptedBy == nil || l.AdoptedBy.Node == l.Name || l.Adopted >= l.Epoch {
		return false, nil
	}
	a, err := r.Get(ctx, l.AdoptedBy.Node)
	switch {
	case errors.Is(err, blob.ErrNotFound):
		return false, nil
	case err != nil:
		return false, err
	}
	return a.Epoch == l.AdoptedBy.Epoch && a.ValidAt(r.opts.Now()), nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.C:
		return nil
	}
}

// Renew rewrites the lease at the same epoch. info can change (counts and the
// journal range). ErrLost if somebody else changed it.
func (r *Registry) Renew(ctx context.Context, lease Record, info NodeInfo) (Record, error) {
	next := lease
	next.Previous = nil
	next.NodeInfo = info
	next.NodeInfo.Name = lease.Name
	next.Renewed = r.opts.Now()
	etag, err := r.write(ctx, lease, &next)
	if err != nil {
		return lease, err
	}
	next.ETag = etag
	return next, nil
}

// Release marks the lease released, keeping its epoch so epochs never repeat.
// Every shard has been let go of, so nothing is left in the journal: the lease
// records its epoch as adopted, and nobody adopts it.
func (r *Registry) Release(ctx context.Context, lease Record) error {
	next := lease
	next.Previous = nil
	next.Released = true
	next.AdoptedBy = nil
	next.Adopted = max(next.Adopted, next.Epoch)
	_, err := r.write(ctx, lease, &next)
	return err
}

// Listed is a lease as a listing of state/leases/ shows it, without reading it.
type Listed struct {
	Name         cluster.Node
	ETag         blob.ETag
	LastModified time.Time // the store's clock at the lease's last write: a hint of when to look, never a decision
}

// List lists every node's lease from state/leases/, in key order, without reading them.
func (r *Registry) List(ctx context.Context) ([]Listed, error) {
	var out []Listed
	for info, err := range r.store.List(ctx, Prefix) {
		if err != nil {
			return nil, err
		}
		out = append(out, Listed{Name: cluster.Node(strings.TrimPrefix(info.Key, Prefix)), ETag: info.ETag, LastModified: info.LastModified})
	}
	return out, nil
}

// Adopt marks dead, a lease the caller has read and confirmed expired, as
// adopted by the incarnation by, with one compare-and-swap on the ETag it was
// read at. ErrNotAdoptable if it has changed since or was released or adopted
// up to its epoch already. A lease another node is adopting is the caller's to
// judge (its adopter must be dead) before it calls Adopt. It returns the lease
// as written, for FinishAdoption.
func (r *Registry) Adopt(ctx context.Context, dead Record, by Incarnation) (Record, error) {
	if dead.Released || dead.Adopted >= dead.Epoch {
		return Record{}, fmt.Errorf("%w: %s at epoch %d", ErrNotAdoptable, dead.Name, dead.Epoch)
	}
	next := dead
	next.Previous = nil
	next.AdoptedBy = &by
	etag, err := r.store.Update(ctx, Key(dead.Name), encodeLease(&next), dead.ETag)
	if errors.Is(err, blob.ErrConflict) || errors.Is(err, blob.ErrNotFound) {
		return Record{}, fmt.Errorf("%w: %s changed since it was read", ErrNotAdoptable, dead.Name)
	} else if err != nil {
		return Record{}, err
	}
	next.ETag = etag
	return next, nil
}

// FinishAdoption records that every epoch up to epoch has had its journal
// adopted, with a fence at fence, by compare-and-swap on l as the adopter last
// wrote it (Adopt's result, or Acquire's for a node adopting its own journal).
// The adoption mark is cleared, and the journal's range starts after the fence,
// so the node's next life starts its journal there. If the lease has changed
// but is adopted up to epoch already (another adopter took over and finished),
// that counts as done. If it is still marked by the same adopter at the same
// epoch, the change was the adopter's own (a node adopting its own journal
// renews its lease meanwhile, and a write whose answer was lost leaves an old
// ETag), and the finish is written over it. Otherwise ErrNotAdoptable: the node
// restarted or another adopter took over, and this adoption is over.
func (r *Registry) FinishAdoption(ctx context.Context, l Record, epoch pos.Epoch, fence pos.Seg) (Record, error) {
	finish := func(base Record) (Record, error) {
		next := base
		next.Previous = nil
		next.AdoptedBy = nil
		next.Adopted = max(next.Adopted, epoch)
		next.JournalFrom, next.JournalNext = fence+1, fence+1
		etag, err := r.store.Update(ctx, Key(l.Name), encodeLease(&next), base.ETag)
		next.ETag = etag
		return next, err
	}
	changed := func(err error) bool { return errors.Is(err, blob.ErrConflict) || errors.Is(err, blob.ErrNotFound) }
	notAdoptable := fmt.Errorf("%w: %s changed during its adoption", ErrNotAdoptable, l.Name)
	next, err := finish(l)
	if changed(err) {
		cur, gerr := r.Get(ctx, l.Name)
		switch {
		case gerr == nil && cur.Adopted >= epoch:
			return *cur, nil
		case gerr == nil && l.AdoptedBy != nil && cur.AdoptedBy != nil && *cur.AdoptedBy == *l.AdoptedBy && cur.Epoch == l.Epoch && !cur.Released:
			next, err = finish(*cur)
		case gerr != nil && !errors.Is(gerr, blob.ErrNotFound):
			return Record{}, gerr
		default:
			return Record{}, notAdoptable
		}
	}
	switch {
	case changed(err):
		return Record{}, notAdoptable
	case err != nil:
		return Record{}, err
	}
	return next, nil
}

// write replaces lease with next. A write of ours whose outcome stayed unknown
// (a renewal cut short by the keeper stopping, say) can have landed without us
// hearing, which leaves us holding an old ETag. So on a conflict, if what is
// there is still our epoch and nobody has released it, another node marked it
// or it was adopted up to our epoch, it is our own earlier write, and next
// replaces that. Anything else is ErrLost.
func (r *Registry) write(ctx context.Context, lease Record, next *Record) (blob.ETag, error) {
	lost := fmt.Errorf("%w: %s", ErrLost, lease.Name)
	etag, err := r.store.Update(ctx, Key(lease.Name), encodeLease(next), lease.ETag)
	if !errors.Is(err, blob.ErrConflict) && !errors.Is(err, blob.ErrNotFound) {
		return etag, err
	}
	cur, err := r.Get(ctx, lease.Name)
	switch {
	case errors.Is(err, blob.ErrNotFound):
		return "", lost
	case err != nil:
		return "", err
	case cur.Epoch != lease.Epoch || cur.Released || cur.adoptedByOther() || cur.Adopted >= cur.Epoch:
		return "", lost
	}
	etag, err = r.store.Update(ctx, Key(lease.Name), encodeLease(next), cur.ETag)
	if errors.Is(err, blob.ErrConflict) || errors.Is(err, blob.ErrNotFound) {
		return "", lost
	}
	return etag, err
}
