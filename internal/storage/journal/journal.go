// Package journal holds a node's journal: where its segments are stored and
// the writer that writes them. The authoritative copy of every journal is in
// blob storage: the fence is created there, and a journal is complete without
// anything else (docs/internals.md, Node journals and snapshots).
//
// The storage side is the narrow Store interface every caller writes through.
// Blob storage is the only implementation today (Blob, which delegates to the
// node's blob.Store), and Store is the seam a faster journal store would plug
// in at. A journal's segments are numbered. A segment is created once and
// never modified, so there are no updates and no conditional deletes: deleting
// one is the caller's choice of garbage, through journal expiry, which works on
// the blob keys.
//
// The writing side is the Writer, which queues the LTX files capture produces
// and writes them as numbered segments, one per flush interval, each holding
// every shard's files from that interval. Scan, ReadFooter and ReadEntryInto
// read segments back, Bounds finds the lowest and highest number present, and
// Fence is what an adopter does to a dead writer's journal.
//
// Layout of a segment:
//
//	LTX file bytes, exactly as capture queued them, back to back
//	footer: JSON array of Entry
//	8 bytes: footer length, big-endian
//	8 bytes: magic "SHLSEG01"
//
// An empty segment is a fence (see docs/internals.md, Fencing). The fence that
// an adopter puts above a dead writer's range lives in the journal's own store:
// one store decides the journal's races.
package journal

import (
	"context"
	"fmt"
	"io"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/model/cluster"
	"github.com/luci1900/shardlite/internal/model/pos"
	"github.com/luci1900/shardlite/internal/storage/blob"
)

// Store is one node's journal: numbered segments, created once and never
// modified. One journal per store, so the name of the node it speaks for belongs
// to what constructs it, not to the calls.
//
// A Store tags each create with a write id (the writer's node, epoch and a
// counter) and, when a create's outcome is not known, reads the number back to
// tell its own earlier attempt from somebody else's segment, as blob.Store
// does. Callers see success, blob.ErrExists (somebody else's segment holds the
// number: a fence, or a newer writer's), or an outcome that stayed unknown
// (blob.ErrUnknown). A lost response costs a retry of the same body and id,
// never a fence. Reads are ErrNotFound for a number with no segment.
//
// An implementation owns the retries and the settling of lost responses: a
// Store does not hand a caller an outcome it could have found out itself. The
// blob one gets both from blob.Store, which it delegates to, and a store of
// another kind has to do the same (the conformance suite checks it, lost
// responses included).
//
// Three contracts a journal leans on:
//
//   - A create is atomic: the segment appears whole or not at all.
//   - A range read reads the range and not the segment.
//   - A segment is written once and never modified.
type Store interface {
	// Name is the node whose journal this is, for error messages: not the
	// writer of an entry, which the write id carries, but the journal itself.
	Name() cluster.Node
	// NewWriteID returns an id for CreateWithID, naming the writer.
	NewWriteID() blob.WriteID
	// Create writes the segment once and returns only when it is durable. It
	// fails with blob.ErrExists if somebody else's segment holds the number.
	Create(ctx context.Context, seq pos.Seg, body blob.Body) error
	// CreateWithID is Create for a caller that keeps retrying one write after
	// it has given up: every call is a retry of the write named by id, so
	// "exists" is checked against that id whatever happened before. The body
	// must be the same each time, and is read from its start on every attempt
	// (it may be larger than memory).
	CreateWithID(ctx context.Context, seq pos.Seg, body blob.Body, id blob.WriteID) error
	// GetRange reads length bytes from offset (to the end if length is not
	// positive or reaches past it). A negative offset asks for the last
	// -offset bytes of the segment, and the length must then be zero: it is
	// how a caller asks for a tail whose end it does not know. An empty
	// segment has no tail, which is an empty read and not an error.
	GetRange(ctx context.Context, seq pos.Seg, offset, length int64) (io.ReadCloser, error)
	// Delete removes the segment. A number with no segment is done, not an
	// error: deleting twice is deleting once.
	Delete(ctx context.Context, seq pos.Seg) error
	// List returns the journal's segments from from (inclusive) on, in number
	// order. It may lag. A failure after the first segment is the caller's to
	// see.
	List(ctx context.Context, from pos.Seg) iter.Seq2[Segment, error]
}

// Segment is a segment as a listing of its journal shows it, which is what
// journal expiry decides by: its number, its size (a fence's is 0) and when the
// store says it was written, by the store's own clock.
type Segment struct {
	Seq     pos.Seg
	Size    int64
	Written time.Time
}

// Root is the key prefix of all the journals in blob storage (the data role,
// docs/internals.md, Data and state).
const Root = "data/journal/"

// seqDigits is how wide a segment number is in a key, so that keys list in
// number order. Numbers have 20 digits: a day's segments per node are four
// digits, and a journal never gets within centuries of 20.
const seqDigits = 20

// Prefix is the key prefix of one node's journal in blob storage.
func Prefix(node cluster.Node) string { return Root + string(node) + "/" }

// Key is the blob key of one numbered segment. The number is zero-padded,
// so a key that sorts after another names a later segment.
func Key(node cluster.Node, seq pos.Seg) string {
	return fmt.Sprintf("%s%0*d", Prefix(node), seqDigits, uint64(seq))
}

// parseKey is the segment number of a key under a node's journal, ok false if
// the key is not a segment's. Keys are zero-padded decimal numbers, and a
// number of 0 has no segment, so anything else is not one either.
func parseKey(node cluster.Node, key string) (pos.Seg, bool) {
	num, ok := strings.CutPrefix(key, Prefix(node))
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(num, 10, 64)
	return pos.Seg(n), err == nil && n > 0
}
