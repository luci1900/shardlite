package blob_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/luci1900/shardlite/internal/storage/blob"
)

// misbehaving wraps a Memory and gets one thing wrong, the way a store or proxy
// that ignores a header would.
type misbehaving struct {
	*blob.Memory
	ignoreIfNoneMatch bool // Create overwrites
	ignoreIfMatch     bool // Update ignores the expected ETag
	dropMetadata      bool // the write id is not stored
	failCreate        bool // a conditional create is refused outright (501 Not Implemented)
	failUpdate        bool // an update is refused outright
	updateCreates     bool // an update of a missing key creates it (MinIO's last community build)
	ignoreDeleteMatch bool // Delete ignores the expected ETag (many S3-compatible stores)
	failDelete        bool // a conditional delete is refused outright (501 Not Implemented)
}

func (m misbehaving) Delete(ctx context.Context, key string, expected blob.ETag) error {
	if expected != "" {
		if m.failDelete {
			return errors.New("501 Not Implemented")
		}
		if m.ignoreDeleteMatch {
			expected = ""
		}
	}
	return m.Memory.Delete(ctx, key, expected)
}

func (m misbehaving) Create(ctx context.Context, key string, body io.ReadSeeker, id blob.WriteID) (blob.ETag, error) {
	if m.failCreate {
		return "", errors.New("501 Not Implemented")
	}
	if m.dropMetadata {
		id = blob.WriteID{}
	}
	if m.ignoreIfNoneMatch {
		if info, err := m.Memory.Head(ctx, key); err == nil {
			return m.Memory.Update(ctx, key, body, info.ETag, id)
		}
	}
	return m.Memory.Create(ctx, key, body, id)
}

func (m misbehaving) Update(ctx context.Context, key string, body io.ReadSeeker, expected blob.ETag, id blob.WriteID) (blob.ETag, error) {
	if m.failUpdate {
		return "", errors.New("501 Not Implemented")
	}
	if m.dropMetadata {
		id = blob.WriteID{}
	}
	if m.ignoreIfMatch {
		if info, err := m.Memory.Head(ctx, key); err == nil {
			expected = info.ETag
		}
	}
	if m.updateCreates {
		if _, err := m.Memory.Head(ctx, key); errors.Is(err, blob.ErrNotFound) {
			return m.Memory.Create(ctx, key, body, id)
		}
	}
	return m.Memory.Update(ctx, key, body, expected, id)
}

func TestCheckStoreRefusesAMisbehavingStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, tc := range map[string]struct {
		store misbehaving
		step  string
	}{
		"refuses conditional creates": {misbehaving{failCreate: true}, "conditional create"},
		"refuses updates":             {misbehaving{failUpdate: true}, "update with the current ETag"},
		"ignores If-None-Match":       {misbehaving{ignoreIfNoneMatch: true}, "second conditional create"},
		"ignores If-Match":            {misbehaving{ignoreIfMatch: true}, "update with a stale ETag"},
		"drops metadata":              {misbehaving{dropMetadata: true}, "write id"},
		"updates create":              {misbehaving{updateCreates: true}, "update of a missing key"},
		"ignores If-Match on delete":  {misbehaving{ignoreDeleteMatch: true}, "delete with a stale ETag"},
		"refuses conditional deletes": {misbehaving{failDelete: true}, "delete with a stale ETag"},
	} {
		t.Run(name, func(t *testing.T) {
			tc.store.Memory = blob.NewMemory()
			err := blob.CheckStore(ctx, blob.WithPrefix(tc.store, "p/"), "n1")
			if err == nil {
				t.Fatal("the check passed")
			}
			if !strings.Contains(err.Error(), `"`+tc.step+`"`) {
				t.Fatalf("the error doesn't name the step %q: %v", tc.step, err)
			}
			// The scratch key is gone either way.
			for info := range tc.store.Memory.List(ctx, "", "") {
				if strings.Contains(info.Key, "check/") {
					t.Fatalf("scratch key left: %s", info.Key)
				}
			}
		})
	}
}

func TestCheckStorePasses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, mk := range map[string]backendFactory{"memory": memoryFactory, "s3": s3Factory} {
		t.Run(name, func(t *testing.T) {
			b := mk(t)
			if err := blob.CheckStore(ctx, b, "n1"); err != nil {
				t.Fatal(err)
			}
			if err := blob.CheckStore(ctx, b, "n1"); err != nil { // and again: each run uses a key of its own
				t.Fatal(err)
			}
			for info := range b.List(ctx, "check/", "") {
				t.Fatalf("scratch key left: %s", info.Key)
			}
		})
	}
}
