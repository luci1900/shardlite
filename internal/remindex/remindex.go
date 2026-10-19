// Package remindex is the reminder index (docs/internals.md, Reminder index):
// empty objects in blob storage that say a shard may need waking at a minute. An
// entry is created by anyone before the manifest write that records the time
// (the writer's own entry, keyed by the manifest version the write is based on),
// and deleted only when the shard's manifest doesn't back it. It needs no owner
// and no clock.
package remindex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
)

// Prefix is where the index lives.
const Prefix = "index/reminders/"

// minuteLayout is the key's minute, UTC, so keys list in time order.
const minuteLayout = "2006-01-02T15:04Z"

// Times in the index and in the manifest's next_reminder are Unix minutes: nothing
// that reads them needs more, since a shard's own reminders loop fires at the second.

// Entry is one object of the index: index/reminders/{minute}/{p}/{kind}/{id}/{base}.{n}.
type Entry struct {
	Minute int64  // Unix minutes
	Shard  string // kind/id
	Base   string // BaseOf the manifest version the write the entry is for CASes against
	N      int    // how many times the dispatcher has moved the entry
}

// MinuteOf is the minute a Unix time in seconds falls in.
func MinuteOf(unix int64) int64 { return unix / 60 }

// MinuteAt is the minute of t.
func MinuteAt(t time.Time) int64 { return MinuteOf(t.Unix()) }

// BaseOf is the short name of a manifest version.
func BaseOf(v blob.ETag) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:6])
}

// Key is the entry's object key. {p} is one hex digit from a hash of the shard's
// name, so one minute's writes spread over 16 prefixes.
func (e Entry) Key() string {
	h := fnv.New32a()
	h.Write([]byte(e.Shard))
	return fmt.Sprintf("%s%s/%x/%s/%s.%d", Prefix, time.Unix(e.Minute*60, 0).UTC().Format(minuteLayout), h.Sum32()%16, e.Shard, e.Base, e.N)
}

// ParseKey is Key's inverse.
func ParseKey(key string) (Entry, error) {
	bad := fmt.Errorf("remindex: bad key %q", key)
	rest, ok := strings.CutPrefix(key, Prefix)
	if !ok {
		return Entry{}, bad
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 5 {
		return Entry{}, bad
	}
	minute, err := time.Parse(minuteLayout, parts[0])
	if err != nil {
		return Entry{}, bad
	}
	base, n, ok := strings.Cut(parts[4], ".")
	count, err := strconv.Atoi(n)
	if !ok || err != nil || count < 0 {
		return Entry{}, bad
	}
	e := Entry{Minute: minute.Unix() / 60, Shard: parts[2] + "/" + parts[3], Base: base, N: count}
	if e.Key() != key {
		return Entry{}, bad
	}
	return e, nil
}

// Backs reports whether a manifest whose next_reminder is next backs the entry:
// it has one (0 means none) and it is no later than the entry's minute.
func Backs(next int64, e Entry) bool {
	return next > 0 && next <= e.Minute
}

// Prunable is the pruning rule: an entry may be deleted when the manifest doesn't
// back it (there is none, or next_reminder is absent or after the entry's minute)
// and the manifest's current version is not the entry's base. While it is the
// base the write the entry was made for may still be in flight: that write CASes
// against its base, so once the manifest has moved off it the write has landed or
// never will.
func Prunable(exists bool, next int64, version blob.ETag, e Entry) bool {
	if !exists {
		return true
	}
	return !Backs(next, e) && BaseOf(version) != e.Base
}

// Index reads and writes the entries in blob storage.
type Index struct{ store *blob.Store }

// New returns the index over a store.
func New(store *blob.Store) Index { return Index{store} }

// Create makes the entry. One that exists already (a retry of the same write) is
// the same object.
func (x Index) Create(ctx context.Context, e Entry) error {
	_, err := x.store.Create(ctx, e.Key(), bytes.NewReader(nil))
	if err != nil && !errors.Is(err, blob.ErrExists) {
		return fmt.Errorf("creating a reminder entry: %w", err)
	}
	return nil
}

// Delete removes the entry. One that is gone is done.
func (x Index) Delete(ctx context.Context, e Entry) error {
	err := x.store.Delete(ctx, e.Key(), "")
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return fmt.Errorf("deleting a reminder entry: %w", err)
	}
	return nil
}

// Move creates the entry's next move at minute to, then deletes it. A crash
// between the two leaves a duplicate, which costs one more dispatch.
func (x Index) Move(ctx context.Context, e Entry, to int64) error {
	next := Entry{Minute: to, Shard: e.Shard, Base: e.Base, N: e.N + 1}
	if err := x.Create(ctx, next); err != nil {
		return err
	}
	return x.Delete(ctx, e)
}

// Due lists up to limit entries due by now (their minute is no later than now's),
// earliest first. Entries that have been dealt with leave the head of the listing,
// so there is no cursor.
func (x Index) Due(ctx context.Context, now time.Time, limit int) ([]Entry, error) {
	last := MinuteAt(now)
	var out []Entry
	for info, err := range x.store.List(ctx, Prefix) {
		if err != nil {
			return nil, err
		}
		e, err := ParseKey(info.Key)
		if err != nil {
			continue // not ours: left alone
		}
		if e.Minute > last || len(out) >= limit {
			break
		}
		out = append(out, e)
	}
	return out, nil
}
