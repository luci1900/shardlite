package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// CheckStore runs the startup check of a store (design.md, Coordination) on a
// scratch key, check/{node}/{random}, and deletes the key afterwards. It
// refuses a store that misbehaves, naming the step that failed:
//
//  1. a conditional create succeeds;
//  2. a second conditional create fails with ErrExists;
//  3. an update with the right ETag succeeds and gives a new ETag;
//  4. an update with a stale ETag fails with ErrConflict and changes nothing;
//  5. the write id comes back from Head;
//  6. an update of a missing key fails with ErrNotFound and creates nothing;
//  7. a delete with a stale ETag fails with ErrConflict and leaves the key.
//
// A store or proxy that silently ignored If-Match would otherwise break fencing
// without any error. The check goes through a Store, as everything does, so it
// tests the same write path.
func CheckStore(ctx context.Context, b Backend, node string) error {
	var r [8]byte
	if _, err := rand.Read(r[:]); err != nil {
		return err
	}
	key := "check/" + node + "/" + hex.EncodeToString(r[:])
	missing := key + "-missing"
	s := NewStore(b, node, 0, Options{Attempts: 3})
	fail := func(step, format string, args ...any) error {
		return fmt.Errorf("the blob store failed its startup check at %q: %s (it must support conditional writes and deletes: If-None-Match and If-Match on PUT, If-Match on DELETE, and metadata on the object)", step, fmt.Sprintf(format, args...))
	}
	defer func() {
		// Best effort: a scratch object left behind costs nothing.
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = s.Delete(dctx, key, "")
		_ = s.Delete(dctx, missing, "")
	}()

	e1, err := s.Create(ctx, key, strings.NewReader("one"))
	if err != nil {
		return fail("conditional create", "%v", err)
	}
	if _, err := s.Create(ctx, key, strings.NewReader("two")); !errors.Is(err, ErrExists) {
		if err == nil {
			return fail("second conditional create", "it succeeded, so the store ignores If-None-Match: *")
		}
		return fail("second conditional create", "expected ErrExists, got %v", err)
	}
	if got, err := readObject(ctx, s, key); err != nil || got != "one" {
		return fail("second conditional create", "the object holds %q (%v), the second create changed it", got, err)
	}
	e2, err := s.Update(ctx, key, strings.NewReader("three"), e1)
	if err != nil {
		return fail("update with the current ETag", "%v", err)
	}
	if e2 == e1 {
		return fail("update with the current ETag", "the ETag did not change with the contents (%q)", e1)
	}
	if _, err := s.Update(ctx, key, strings.NewReader("four"), e1); !errors.Is(err, ErrConflict) {
		if err == nil {
			return fail("update with a stale ETag", "it succeeded, so the store ignores If-Match")
		}
		return fail("update with a stale ETag", "expected ErrConflict, got %v", err)
	}
	if got, err := readObject(ctx, s, key); err != nil || got != "three" {
		return fail("update with a stale ETag", "the object holds %q (%v), the stale update changed it", got, err)
	}
	info, err := s.Head(ctx, key)
	if err != nil {
		return fail("write id", "head: %v", err)
	}
	if info.WriteID.Node != node || info.WriteID.N == 0 {
		return fail("write id", "head returned %+v, want the write id of the last write by %q (the store drops object metadata)", info.WriteID, node)
	}
	if info.ETag != e2 {
		return fail("write id", "head's ETag %q is not the ETag the update returned (%q)", info.ETag, e2)
	}
	if _, err := s.Update(ctx, missing, strings.NewReader("five"), e2); !errors.Is(err, ErrNotFound) {
		if err == nil {
			return fail("update of a missing key", "it succeeded, so the store creates objects on If-Match")
		}
		return fail("update of a missing key", "expected ErrNotFound, got %v", err)
	}
	if _, err := s.Head(ctx, missing); !errors.Is(err, ErrNotFound) {
		return fail("update of a missing key", "the key exists afterwards (%v), so the update created it", err)
	}
	// The scratch key is at e2: a delete with e1 must leave it alone.
	if err := s.Delete(ctx, key, e1); !errors.Is(err, ErrConflict) {
		if err == nil {
			return fail("delete with a stale ETag", "it succeeded, so the store ignores If-Match on a delete")
		}
		return fail("delete with a stale ETag", "expected ErrConflict, got %v", err)
	}
	if got, err := readObject(ctx, s, key); err != nil || got != "three" {
		return fail("delete with a stale ETag", "the object holds %q (%v), the stale delete removed or changed it", got, err)
	}
	return nil
}

func readObject(ctx context.Context, s *Store, key string) (string, error) {
	rc, _, err := s.Get(ctx, key)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, rc)
	return buf.String(), err
}
