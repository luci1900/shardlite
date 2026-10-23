package journal

import (
	"context"
	"fmt"
	"io"
	"iter"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/pos"
)

// Blob is a blob backend seen as the record of one node's journal (the fences,
// docs/internals.md, Fencing). It maps a number onto the record's key, which is
// the layout journal expiry composes keys with, so a Store over it and a
// caller that goes through the blob package alone see the same keys.
type Blob struct {
	b    blob.Backend // withPrefix'd, its keys the numbers of one journal
	node cluster.Node // whose journal this is, for the error messages
}

// NewBlob returns the record of node's journal: blob b's objects under the
// journal's prefix, keyed by segment number.
func NewBlob(b blob.Backend, node cluster.Node) *Blob {
	return &Blob{b: blob.WithPrefix(b, Prefix(node)), node: node}
}

// Name is the node whose journal this is.
func (x *Blob) Name() cluster.Node { return x.node }

// padded is the key a number writes under: zero-padded, so that keys list in
// number order.
func padded(seq pos.Seg) string { return fmt.Sprintf("%0*d", seqDigits, uint64(seq)) }

func (x *Blob) Create(ctx context.Context, seq pos.Seg, body blob.Body, id blob.WriteID) error {
	_, err := x.b.Create(ctx, padded(seq), io.NewSectionReader(body, 0, body.Size()), id)
	return err
}

func (x *Blob) Get(ctx context.Context, seq pos.Seg) (io.ReadCloser, error) {
	rc, _, err := x.b.Get(ctx, padded(seq))
	return rc, err
}

func (x *Blob) GetRange(ctx context.Context, seq pos.Seg, offset, length int64) (io.ReadCloser, error) {
	return x.b.GetRange(ctx, padded(seq), offset, length)
}

// Head says the segment's size and write id. The record's own key and
// modification time are of no use to a reader of a journal, so they are left
// behind.
func (x *Blob) Head(ctx context.Context, seq pos.Seg) (SegmentInfo, error) {
	info, err := x.b.Head(ctx, padded(seq))
	if err != nil {
		return SegmentInfo{}, err
	}
	return SegmentInfo{Size: info.Size, WriteID: info.WriteID}, nil
}

func (x *Blob) Delete(ctx context.Context, seq pos.Seg) error {
	return x.b.Delete(ctx, padded(seq), "")
}

// List lists the journal's numbers from from on. The record is asked from the
// key before from, which the zero-padding puts before from's own, so a listing
// begins the segment from asks for. A key that is not a segment's is skipped,
// as a plain listing of the prefix would skip it too.
func (x *Blob) List(ctx context.Context, from pos.Seg) iter.Seq2[pos.Seg, error] {
	if from < 1 {
		from = 1
	}
	return func(yield func(pos.Seg, error) bool) {
		for info, err := range x.b.List(ctx, "", seq0(from)) {
			if err != nil {
				yield(0, err)
				return
			}
			seq, ok := parseKey(info.Key)
			if !ok {
				continue
			}
			if !yield(seq, nil) {
				return
			}
		}
	}
}

// seq0 is the key before from's key, for a listing that starts at from.
func seq0(from pos.Seg) string {
	if from < 1 {
		from = 1
	}
	return padded(from - 1)
}
