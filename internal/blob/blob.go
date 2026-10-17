// Package blob is the storage layer shardlite uses for leases, manifests,
// node journal segments and LTX files.
//
// A Backend (S3, or an in-memory one for tests) does single attempts and
// reports one of a fixed set of outcomes. Callers never use a backend
// directly: they use a Store, which wraps any backend, tags every write with
// a write id and retries ambiguous failures, so that a write whose response
// was lost is recognised as the writer's own afterwards (docs/internals.md, Lost
// responses).
package blob

import (
	"context"
	"errors"
	"io"
	"iter"
	"time"
)

// The outcome table. Every Backend follows it for creates, updates and
// conditional deletes, so a caller can trust what an error says:
//
//	Case                                                              Outcome
//	----                                                              -------
//	The store answered that it did nothing                            ErrNotApplied
//	  (409 ConditionalRequestConflict, 429, 503 SlowDown)
//	The request was never fully written (connection refused, DNS,     ErrNotApplied
//	  TLS, the context ended before the body was sent)
//	The request was written and then the response was lost or cut     ErrUnknown
//	  off, 500, 408, a timeout, or the context ended
//	A failed precondition                                             ErrExists (create),
//	                                                                  ErrConflict (update, delete)
//	The setup is wrong (no bucket, no permission, bad credentials)    a plain error, not retried
//
// ErrNotApplied and ErrUnknown wrapping a context error keep it findable with
// errors.Is. A Store settles an unknown outcome itself, with a context that
// outlives its caller's, so what reaches a caller as ErrUnknown is an outcome
// nobody could learn.

// The outcomes a Backend maps its responses onto.
var (
	// ErrExists: Create found an object written by someone else (or by an earlier attempt of ours: a Backend can't tell).
	ErrExists = errors.New("blob: object exists")
	// ErrConflict: Update or Delete found the object changed.
	ErrConflict = errors.New("blob: object changed")
	// ErrNotFound: the object doesn't exist.
	ErrNotFound = errors.New("blob: object not found")
	// ErrNotApplied: the request failed and certainly did not change anything, so it is safe to retry.
	ErrNotApplied = errors.New("blob: not applied")
	// ErrUnknown: the request failed and may have been applied (a timeout, a dropped connection, a server error).
	ErrUnknown = errors.New("blob: outcome unknown")
)

// ETag identifies one version of an object.
type ETag string

// WriteID names one write: the writer's node, its epoch and a counter it
// increments per write. It is stored in the object's metadata with the
// object itself.
type WriteID struct {
	Node  string `json:"node"`
	Epoch uint64 `json:"epoch"`
	N     uint64 `json:"n"`
}

// IsZero reports whether the id is empty (an object written without one).
func (id WriteID) IsZero() bool { return id == WriteID{} }

// ObjectInfo describes one object.
type ObjectInfo struct {
	Key          string
	Size         int64
	ETag         ETag
	LastModified time.Time
	WriteID      WriteID // of the write that created this version (zero in listings from stores whose listings carry no metadata)
}

// Body is what a Store writes: bytes that can be read at any offset and don't
// change (*bytes.Reader, *strings.Reader and *io.SectionReader are). A Store
// reads each attempt through a reader of its own, so attempts share no offset
// with each other or with the caller.
type Body interface {
	io.ReaderAt
	Size() int64
}

// Backend does single attempts and nothing else.
type Backend interface {
	// Create writes the object only if the key doesn't exist (ErrExists otherwise).
	Create(ctx context.Context, key string, body io.ReadSeeker, id WriteID) (ETag, error)
	// Update replaces the object only if its ETag is expected (ErrConflict otherwise, ErrNotFound if missing).
	Update(ctx context.Context, key string, body io.ReadSeeker, expected ETag, id WriteID) (ETag, error)
	Get(ctx context.Context, key string) (io.ReadCloser, ETag, error)
	GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
	// Head returns size, ETag, last modified time and write id. A size of 0 is a fence.
	Head(ctx context.Context, key string) (ObjectInfo, error)
	// Delete removes the object. With expected set it is a conditional delete
	// (ErrConflict if the object changed), which many S3-compatible stores lack or
	// silently ignore: only the cleanup of empty manifests uses it, and only after
	// the startup check showed it works. Whether deleting a missing key is
	// ErrNotFound depends on the store: S3 answers success, so callers accept both.
	Delete(ctx context.Context, key string, expected ETag) error
	// List returns objects whose key starts with prefix, in key order. It may lag.
	// A listing carries no metadata on S3, so the objects it returns have a
	// zero WriteID (the memory backend does the same). Nothing relies on listed write ids.
	List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error]
}
