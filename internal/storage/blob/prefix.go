package blob

import (
	"context"
	"io"
	"iter"
	"strings"
)

// WithPrefix returns a Backend that keeps every object under prefix
// ("staging/" lets several environments or games share one bucket). The keys
// callers see are unchanged: listings strip the prefix again.
func WithPrefix(b Backend, prefix string) Backend {
	if prefix == "" {
		return b
	}
	return prefixed{b, prefix}
}

type prefixed struct {
	b Backend
	p string
}

func (x prefixed) Create(ctx context.Context, key string, body io.ReadSeeker, id WriteID) (ETag, error) {
	return x.b.Create(ctx, x.p+key, body, id)
}
func (x prefixed) Update(ctx context.Context, key string, body io.ReadSeeker, expected ETag, id WriteID) (ETag, error) {
	return x.b.Update(ctx, x.p+key, body, expected, id)
}
func (x prefixed) Get(ctx context.Context, key string) (io.ReadCloser, ETag, error) {
	return x.b.Get(ctx, x.p+key)
}
func (x prefixed) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return x.b.GetRange(ctx, x.p+key, offset, length)
}
func (x prefixed) Head(ctx context.Context, key string) (ObjectInfo, error) {
	info, err := x.b.Head(ctx, x.p+key)
	info.Key = strings.TrimPrefix(info.Key, x.p)
	return info, err
}
func (x prefixed) Delete(ctx context.Context, key string, expected ETag) error {
	return x.b.Delete(ctx, x.p+key, expected)
}
func (x prefixed) List(ctx context.Context, prefix, after string) iter.Seq2[ObjectInfo, error] {
	if after != "" {
		after = x.p + after
	}
	return func(yield func(ObjectInfo, error) bool) {
		for info, err := range x.b.List(ctx, x.p+prefix, after) {
			info.Key = strings.TrimPrefix(info.Key, x.p)
			if !yield(info, err) {
				return
			}
		}
	}
}
