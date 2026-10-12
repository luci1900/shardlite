// Package coord holds a node's lease: the object in blob storage that all of
// the node's ownerships depend on (design.md, Node leases).
package coord

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
)

var (
	// ErrLeaseLost means somebody else changed the lease object: another life of
	// the node took it (a newer epoch) or an adopter did. The holder must stop.
	ErrLeaseLost = errors.New("coord: lease lost")
	// ErrLeaseExpired means the holder's own safety margin ran out before a
	// renewal got through.
	ErrLeaseExpired = errors.New("coord: lease expired")
)

// DefaultTTL is how long a lease is valid after its last renewal.
const DefaultTTL = 10 * time.Second

// NodeInfo is what a node publishes in its lease besides its epoch.
type NodeInfo struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Shards  int    `json:"shards"` // active shards
	Conns   int    `json:"conns"`  // open connections
	// JournalFrom is the oldest segment of the node's journal that may still hold a tail
	// no delta covers, and JournalNext the next segment number the node would write
	// when it published this. An adopter reads from JournalFrom, and fences by probing
	// forward from JournalNext, so a JournalNext that is a little stale only costs probes.
	JournalFrom uint64 `json:"journal_from"`
	JournalNext uint64 `json:"journal_next"`
}

// NodeLease is a node's lease as of one write.
type NodeLease struct {
	NodeInfo
	Epoch     uint64        `json:"epoch"` // +1 each time the node acquires its lease
	Renewed   time.Time     `json:"renewed"`
	TTL       time.Duration `json:"ttl"`
	Released  bool          `json:"released,omitempty"` // a graceful shutdown: nothing is owned any more
	AdoptedBy string        `json:"adopted_by,omitempty"`

	ETag blob.ETag `json:"-"`
	// Previous is the lease an Acquire replaced, nil if the node never had one.
	Previous *NodeLease `json:"-"`
}

// ExpiresAt is when other nodes stop treating the lease as valid.
func (l *NodeLease) ExpiresAt() time.Time { return l.Renewed.Add(l.TTL) }

// ValidAt reports whether other nodes would treat the lease as valid at t.
func (l *NodeLease) ValidAt(t time.Time) bool {
	return !l.Released && l.AdoptedBy == "" && t.Before(l.ExpiresAt())
}

// NodeRegistry is the single-node subset of design.md's interface. List comes
// with the cluster in stage 2.
type NodeRegistry interface {
	// Acquire takes the node's lease with a new epoch, whether or not the
	// previous one is still valid: the caller knows its previous process is gone.
	// The lease keeps the previous life's journal range until the first Renew, so a
	// crash before the journal is adopted leaves the range in place.
	Acquire(ctx context.Context, info NodeInfo) (NodeLease, error)
	// Renew rewrites the lease. info can change (counts and the journal range). ErrLeaseLost if somebody else changed it.
	Renew(ctx context.Context, lease NodeLease, info NodeInfo) (NodeLease, error)
	// Release marks the lease released, keeping its epoch so epochs never repeat.
	Release(ctx context.Context, lease NodeLease) error
	// Get returns a node's lease whether or not it is valid (check ValidAt).
	// blob.ErrNotFound if the node never had one.
	Get(ctx context.Context, node string) (*NodeLease, error)
}

// Key is the object key of a node's lease.
func Key(node string) string { return "nodes/" + node }

// RegistryOptions tune a Registry.
type RegistryOptions struct {
	TTL  time.Duration // DefaultTTL if zero
	Blob blob.Options
	Now  func() time.Time // time.Now if nil, for tests
}

// Registry implements NodeRegistry over blob storage.
type Registry struct {
	store *blob.Store
	opts  RegistryOptions
}

// NewRegistry returns a registry for the node named node. Its writes go
// through a blob.Store of their own with a random epoch in their write ids,
// since the node's real epoch isn't known until Acquire has returned.
func NewRegistry(b blob.Backend, node string, opts RegistryOptions) *Registry {
	if opts.TTL <= 0 {
		opts.TTL = DefaultTTL
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	var r [8]byte
	_, _ = rand.Read(r[:])
	return &Registry{store: blob.NewStore(b, node, binary.BigEndian.Uint64(r[:]), opts.Blob), opts: opts}
}

func encodeLease(l *NodeLease) *bytes.Reader {
	b, err := json.Marshal(l)
	if err != nil {
		panic(err)
	}
	return bytes.NewReader(b)
}

func (r *Registry) get(ctx context.Context, node string) (*NodeLease, error) {
	rc, etag, err := r.store.Get(ctx, Key(node))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	var l NodeLease
	if err := json.NewDecoder(rc).Decode(&l); err != nil {
		return nil, fmt.Errorf("lease %s: %w", node, err)
	}
	l.ETag = etag
	return &l, nil
}

// Get returns a node's lease.
func (r *Registry) Get(ctx context.Context, node string) (*NodeLease, error) {
	return r.get(ctx, node)
}

// Acquire takes the lease with the next epoch.
func (r *Registry) Acquire(ctx context.Context, info NodeInfo) (NodeLease, error) {
	for {
		prev, err := r.get(ctx, info.Name)
		next := NodeLease{NodeInfo: info, Epoch: 1, TTL: r.opts.TTL}
		next.JournalFrom, next.JournalNext = 1, 1
		if err == nil {
			next.Epoch = prev.Epoch + 1
			next.JournalFrom, next.JournalNext = prev.JournalFrom, prev.JournalNext
			next.Previous = prev
		} else if !errors.Is(err, blob.ErrNotFound) {
			return NodeLease{}, err
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
			return NodeLease{}, err
		}
	}
}

// Renew rewrites the lease at the same epoch.
func (r *Registry) Renew(ctx context.Context, lease NodeLease, info NodeInfo) (NodeLease, error) {
	next := lease
	next.Previous = nil
	next.NodeInfo = info
	next.NodeInfo.Name = lease.Name
	next.Renewed = r.opts.Now()
	etag, err := r.store.Update(ctx, Key(lease.Name), encodeLease(&next), lease.ETag)
	if errors.Is(err, blob.ErrConflict) || errors.Is(err, blob.ErrNotFound) {
		// A write of ours whose outcome stayed unknown leaves us holding an old
		// ETag. If what is there is still our epoch and nobody has adopted it, it
		// is our own earlier write: carry on from it.
		cur, gerr := r.get(ctx, lease.Name)
		if gerr != nil || cur.Epoch != lease.Epoch || cur.Released || cur.AdoptedBy != "" {
			if gerr != nil && !errors.Is(gerr, blob.ErrNotFound) {
				return lease, gerr
			}
			return lease, fmt.Errorf("%w: %s", ErrLeaseLost, lease.Name)
		}
		etag, err = r.store.Update(ctx, Key(lease.Name), encodeLease(&next), cur.ETag)
		if errors.Is(err, blob.ErrConflict) {
			return lease, fmt.Errorf("%w: %s", ErrLeaseLost, lease.Name)
		}
	}
	if err != nil {
		return lease, err
	}
	next.ETag = etag
	return next, nil
}

// Release marks the lease released.
func (r *Registry) Release(ctx context.Context, lease NodeLease) error {
	next := lease
	next.Previous = nil
	next.Released = true
	_, err := r.store.Update(ctx, Key(lease.Name), encodeLease(&next), lease.ETag)
	if errors.Is(err, blob.ErrConflict) || errors.Is(err, blob.ErrNotFound) {
		return fmt.Errorf("%w: %s", ErrLeaseLost, lease.Name)
	}
	return err
}

var _ NodeRegistry = (*Registry)(nil)
