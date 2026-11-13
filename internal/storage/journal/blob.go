package journal

import (
	"context"
	"errors"
	"io"
	"iter"

	"github.com/luci1900/shardlite/internal/model/cluster"
	"github.com/luci1900/shardlite/internal/model/pos"
	"github.com/luci1900/shardlite/internal/storage/blob"
)

// Blob is one node's journal in blob storage (the fences,
// docs/internals.md, Fencing): a Store that delegates to a blob.Store, which
// mints the write ids, retries and settles a lost response. It only maps a
// number onto the segment's key, the layout journal expiry composes keys
// with, so a caller that goes through the blob package alone sees the same keys.
type Blob struct {
	s    *blob.Store
	node cluster.Node // whose journal this is
}

// NewBlob returns node's journal in s. The write ids name s's own node and
// epoch: the writer, which for an adopter is not the node whose journal it is.
func NewBlob(s *blob.Store, node cluster.Node) *Blob { return &Blob{s: s, node: node} }

func (x *Blob) Name() cluster.Node { return x.node }

func (x *Blob) NewWriteID() blob.WriteID { return x.s.NewWriteID() }

func (x *Blob) Create(ctx context.Context, seq pos.Seg, body blob.Body) error {
	_, err := x.s.Create(ctx, Key(x.node, seq), body)
	return err
}

func (x *Blob) CreateWithID(ctx context.Context, seq pos.Seg, body blob.Body, id blob.WriteID) error {
	_, err := x.s.CreateWithID(ctx, Key(x.node, seq), body, id)
	return err
}

// Delete removes a segment. A missing one is done: deleting twice is deleting once.
func (x *Blob) Delete(ctx context.Context, seq pos.Seg) error {
	if err := x.s.Delete(ctx, Key(x.node, seq), ""); !errors.Is(err, blob.ErrNotFound) {
		return err
	}
	return nil
}

func (x *Blob) GetRange(ctx context.Context, seq pos.Seg, offset, length int64) (io.ReadCloser, error) {
	return x.s.GetRange(ctx, Key(x.node, seq), offset, length)
}

// List lists the journal's segments from from on. Blob storage is asked from the
// key before from, which the zero-padding puts before from's own, so a listing
// begins the segment from asks for. A key that is not a segment's is skipped.
func (x *Blob) List(ctx context.Context, from pos.Seg) iter.Seq2[Segment, error] {
	from = max(from, 1)
	return func(yield func(Segment, error) bool) {
		for info, err := range x.s.ListAfter(ctx, Prefix(x.node), Key(x.node, from-1)) {
			if err != nil {
				yield(Segment{}, err)
				return
			}
			seq, ok := parseKey(x.node, info.Key)
			if !ok {
				continue
			}
			if !yield(Segment{Seq: seq, Size: info.Size, Written: info.LastModified}, nil) {
				return
			}
		}
	}
}
