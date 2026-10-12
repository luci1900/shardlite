package blob

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// misbehaving wraps a Local and gets one thing wrong, the way a store or proxy
// that ignores a header would.
type misbehaving struct {
	*Local
	ignoreIfNoneMatch bool // Create overwrites
	ignoreIfMatch     bool // Update ignores the expected ETag
	dropMetadata      bool // the write id is not stored
	failCreate        bool // a conditional create is refused outright (501 Not Implemented)
	failUpdate        bool // an update is refused outright
	updateCreates     bool // an update of a missing key creates it (MinIO's last community build)
	ignoreDeleteMatch bool // Delete ignores the expected ETag (many S3-compatible stores)
	failDelete        bool // a conditional delete is refused outright (501 Not Implemented)
}

func (m misbehaving) Delete(ctx context.Context, key string, expected ETag) error {
	if expected != "" {
		if m.failDelete {
			return errors.New("501 Not Implemented")
		}
		if m.ignoreDeleteMatch {
			expected = ""
		}
	}
	return m.Local.Delete(ctx, key, expected)
}

func (m misbehaving) Create(ctx context.Context, key string, body io.Reader, id WriteID) (ETag, error) {
	if m.failCreate {
		return "", errors.New("501 Not Implemented")
	}
	if m.dropMetadata {
		id = WriteID{}
	}
	if m.ignoreIfNoneMatch {
		if info, err := m.Local.Head(ctx, key); err == nil {
			return m.Local.Update(ctx, key, body, info.ETag, id)
		}
	}
	return m.Local.Create(ctx, key, body, id)
}

func (m misbehaving) Update(ctx context.Context, key string, body io.Reader, expected ETag, id WriteID) (ETag, error) {
	if m.failUpdate {
		return "", errors.New("501 Not Implemented")
	}
	if m.dropMetadata {
		id = WriteID{}
	}
	if m.ignoreIfMatch {
		if info, err := m.Local.Head(ctx, key); err == nil {
			expected = info.ETag
		}
	}
	if m.updateCreates {
		if _, err := m.Local.Head(ctx, key); errors.Is(err, ErrNotFound) {
			return m.Local.Create(ctx, key, body, id)
		}
	}
	return m.Local.Update(ctx, key, body, expected, id)
}

func TestCheckStoreRefusesAMisbehavingStore(t *testing.T) {
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
			tc.store.Local = newLocal(t)
			err := CheckStore(ctx, WithPrefix(tc.store, "p/"), "n1")
			if err == nil {
				t.Fatal("the check passed")
			}
			if !strings.Contains(err.Error(), `"`+tc.step+`"`) {
				t.Fatalf("the error doesn't name the step %q: %v", tc.step, err)
			}
			// The scratch key is gone either way.
			for info := range tc.store.Local.List(ctx, "") {
				if strings.Contains(info.Key, "check/") {
					t.Fatalf("scratch key left: %s", info.Key)
				}
			}
		})
	}
}

func TestCheckStorePasses(t *testing.T) {
	ctx := context.Background()
	for name, mk := range map[string]backendFactory{"local": localFactory, "s3": s3Factory} {
		t.Run(name, func(t *testing.T) {
			b := mk(t)
			if err := CheckStore(ctx, b, "n1"); err != nil {
				t.Fatal(err)
			}
			if err := CheckStore(ctx, b, "n1"); err != nil { // and again: each run uses a key of its own
				t.Fatal(err)
			}
			for info := range b.List(ctx, "check/") {
				t.Fatalf("scratch key left: %s", info.Key)
			}
		})
	}
}
