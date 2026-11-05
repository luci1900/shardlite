// Package journal is where a node's journal can live. The authoritative copy of every
// journal is in blob storage: the fence is created there, and a journal is
// complete without anything else (docs/internals.md, Node journals and
// snapshots). The narrow Backend this package holds sits behind the one Store every caller
// writes through, which gives a backend the same handling of a lost response.
// Blob storage is the only backend today, and the place a faster journal store
// would plug in.
//
// A journal's segments are numbered. A segment is created once and never
// modified, so there are no updates and no conditional deletes: deleting one
// is the caller's choice of garbage, through journal expiry.
//
// The fence that an adopter puts above a dead writer's range lives in
// blob storage: one store decides the journal's races.
package journal

import (
	"context"
	"fmt"
	"io"
	"iter"
	"strconv"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/pos"
)

// Backend is one node's journal: numbered segments, created once and never
// modified. One journal per backend, so the name of the node it speaks for
// belongs to what constructs it, not to the calls.
//
// Like blob.Backend, a Backend does single attempts and reports one of a fixed
// set of outcomes (see the outcome table in internal/blob): Create returns
// ErrNotApplied when nothing was written and ErrUnknown when the response was
// lost or the request may have gone through, and Get, GetRange and Head are
// ErrNotFound for a number with no segment. Callers don't use a Backend: they
// use a Store, which retries and settles.
//
// Three contracts a journal leans on:
//
//   - A create is atomic: the segment appears whole or not at all.
//   - A range read reads the range and not the segment.
//   - A segment is written once and never modified.
type Backend interface {
	// Name is the node whose journal this is, for error messages: not the
	// writer of an entry, which the write id carries, but the journal itself.
	Name() cluster.Node
	// Create writes the segment and returns only when it is durable. It fails
	// with blob.ErrExists if the number is already taken. A Backend can't tell
	// whose segment it is (ours from an earlier attempt, somebody else's): the
	// Store reads the write id back to tell them apart.
	//
	// The body is read from its start on every attempt, and may be larger than
	// memory.
	Create(ctx context.Context, seq pos.Seg, body blob.Body, id blob.WriteID) error
	// Get returns the whole segment.
	Get(ctx context.Context, seq pos.Seg) (io.ReadCloser, error)
	// GetRange reads length bytes from offset (to the end if length is not
	// positive or reaches past it). A negative offset asks for the last
	// -offset bytes of the segment, and the length must then be zero: it is
	// how a caller asks for a tail whose end it does not know. An empty
	// segment has no tail, which is an empty read and not an error.
	GetRange(ctx context.Context, seq pos.Seg, offset, length int64) (io.ReadCloser, error)
	// Head says the segment's size and write id. A size of 0 is a fence.
	Head(ctx context.Context, seq pos.Seg) (SegmentInfo, error)
	// List returns the journal's segment numbers from from (inclusive) on, in
	// number order. It may lag. A failure before the first number may be
	// retried by the Store; one afterwards is the caller's to see.
	List(ctx context.Context, from pos.Seg) iter.Seq2[pos.Seg, error]
	// Delete removes the segment. Whether a number with no segment is
	// ErrNotFound or an error-free success depends on the store where it
	// lives: callers accept both.
	Delete(ctx context.Context, seq pos.Seg) error
}

// SegmentInfo is what a journal needs of a segment: its size (0 is a fence)
// and the write id that created it.
type SegmentInfo struct {
	Size    int64
	WriteID blob.WriteID
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
func parseKey(key string) (pos.Seg, bool) {
	n, err := strconv.ParseUint(key, 10, 64)
	return pos.Seg(n), err == nil && n > 0
}
