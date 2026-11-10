package shardfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/shard"
)

// ReminderPrefix is where the reminder index lives (docs/internals.md, Reminder
// index): empty objects whose keys say a shard may need waking at a minute. An entry
// is created by anyone before the manifest write that records the time (the writer's
// own entry, keyed by the manifest version the write is based on), and deleted only
// when the shard's manifest doesn't back it. It needs no owner and no clock.
const ReminderPrefix = "index/reminders/"

// minuteLayout is the key's minute, UTC, so keys list in time order.
const minuteLayout = "2006-01-02T15:04Z"

// Minute is a Unix minute: the unit of a reminder's due time and of a manifest's
// next_reminder. Nothing that reads them needs more, since a shard's own reminders
// loop fires at the second.
type Minute int64

// MinuteOf is the minute a Unix time in seconds falls in.
func MinuteOf(unix int64) Minute { return Minute(unix / 60) }

// MinuteAt is the minute of t.
func MinuteAt(t time.Time) Minute { return MinuteOf(t.Unix()) }

// Reminder is one entry of the reminder index:
// index/reminders/{lane}/{minute}/{kind}/{id}/{base}.{n}. The lane is the shard's,
// so that each lane lists on its own and one minute's writes spread over the lanes.
type Reminder struct {
	Minute Minute     // Unix minutes
	Shard  shard.Name // kind/id
	Base   string     // BaseOf the manifest version the write the entry is for CASes against
	N      int        // how many times the dispatcher has moved the entry
}

// BaseOf is the short name of a manifest version.
func BaseOf(v blob.ETag) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:6])
}

// Key is the reminder's object key.
func (r Reminder) Key() string {
	return fmt.Sprintf("%s%s/%s/%s/%s.%d", ReminderPrefix, r.Shard.Lane(), time.Unix(int64(r.Minute)*60, 0).UTC().Format(minuteLayout), r.Shard, r.Base, r.N)
}

// ParseKey is Key's inverse. A key that isn't a reminder's, or one under another
// shard's lane, isn't a reminder.
func ParseKey(key string) (Reminder, error) {
	bad := fmt.Errorf("shardfile: bad reminder key %q", key)
	rest, ok := strings.CutPrefix(key, ReminderPrefix)
	if !ok {
		return Reminder{}, bad
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 5 {
		return Reminder{}, bad
	}
	minute, err := time.Parse(minuteLayout, parts[1])
	if err != nil {
		return Reminder{}, bad
	}
	base, n, ok := strings.Cut(parts[4], ".")
	count, err := strconv.Atoi(n)
	if !ok || err != nil || count < 0 {
		return Reminder{}, bad
	}
	name, err := shard.ParseName(parts[2] + "/" + parts[3])
	if err != nil {
		return Reminder{}, bad
	}
	r := Reminder{Minute: MinuteOf(minute.Unix()), Shard: name, Base: base, N: count}
	if r.Key() != key {
		return Reminder{}, bad
	}
	return r, nil
}

// ReminderIndex reads and writes the reminders in blob storage.
type ReminderIndex struct{ store *blob.Store }

// NewReminderIndex returns the reminder index over a store.
func NewReminderIndex(store *blob.Store) ReminderIndex { return ReminderIndex{store} }

// Create makes the entry. One that exists already (a retry of the same write) is
// the same object.
func (x ReminderIndex) Create(ctx context.Context, r Reminder) error {
	_, err := x.store.Create(ctx, r.Key(), bytes.NewReader(nil))
	if err != nil && !errors.Is(err, blob.ErrExists) {
		return fmt.Errorf("creating a reminder entry: %w", err)
	}
	return nil
}

// Delete removes the entry. One that is gone is done.
func (x ReminderIndex) Delete(ctx context.Context, r Reminder) error {
	err := x.store.Delete(ctx, r.Key(), "")
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return fmt.Errorf("deleting a reminder entry: %w", err)
	}
	return nil
}

// Move creates the entry next, then deletes r. A crash between the two leaves a
// duplicate, which costs one more pass of the dispatcher over it. The next entry
// must have another key than r (a new minute or a new move count), since it would
// otherwise be deleted again.
func (x ReminderIndex) Move(ctx context.Context, r, next Reminder) error {
	if next.Key() == r.Key() {
		return fmt.Errorf("moving a reminder entry onto itself: %q", r.Key())
	}
	if err := x.Create(ctx, next); err != nil {
		return err
	}
	return x.Delete(ctx, r)
}

// Due lists up to limit reminders of the lane due by now (their minute is no later
// than now's), earliest first. Objects under the prefix that aren't reminders are
// left alone and don't count against the limit. Entries that have been dealt with
// leave the head of the listing, so there is no cursor.
func (x ReminderIndex) Due(ctx context.Context, lane shard.Lane, now time.Time, limit int) ([]Reminder, error) {
	last := MinuteAt(now)
	var out []Reminder
	for info, err := range x.store.List(ctx, ReminderPrefix+lane.String()+"/") {
		if err != nil {
			return nil, err
		}
		r, err := ParseKey(info.Key)
		if err != nil {
			continue // not a reminder: left alone
		}
		if r.Minute > last || len(out) >= limit {
			break
		}
		out = append(out, r)
	}
	return out, nil
}

// Backs reports whether a manifest whose next_reminder is next backs the reminder:
// it has one (0 means none) and it is no later than the reminder's minute.
func Backs(next Minute, r Reminder) bool {
	return next > 0 && next <= r.Minute
}

// Prunable is the pruning rule: a reminder may be deleted when the manifest doesn't
// back it (there is none, or next_reminder is absent or after the reminder's minute)
// and the manifest's current version is not the reminder's base. While it is the
// base the write the reminder was made for may still be in flight: that write CASes
// against its base, so once the manifest has moved off it the write has landed or
// never will.
func Prunable(exists bool, next Minute, version blob.ETag, r Reminder) bool {
	if !exists {
		return true
	}
	return !Backs(next, r) && BaseOf(version) != r.Base
}
