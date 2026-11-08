// Package dueindex is what every due index shares (docs/internals.md, Reconcilers):
// empty objects in blob storage, keyed {prefix}{lane}/{minute}/{subject}, that say
// something may need doing at a minute. An entry is created by anyone, moved to a
// later minute and deleted, and listed per lane up to a minute, earliest first.
// The minute is when the entry is due and the subject is the rest of the key, which
// the index's user encodes and parses. The package knows nothing of what an entry
// is about.
package dueindex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/shard"
)

// minuteLayout is the key's minute, UTC, so keys list in time order.
const minuteLayout = "2006-01-02T15:04Z"

// Minute is a Unix minute: the unit of an entry's due time and of a manifest's
// next_reminder. Nothing that reads them needs more, since a shard's own reminders
// loop fires at the second.
type Minute int64

// MinuteOf is the minute a Unix time in seconds falls in.
func MinuteOf(unix int64) Minute { return Minute(unix / 60) }

// MinuteAt is the minute of t.
func MinuteAt(t time.Time) Minute { return MinuteOf(t.Unix()) }

// Entry is one object of an index: {prefix}{lane}/{minute}/{subject}. The subject
// is never empty and may hold slashes.
type Entry struct {
	Lane    shard.Lane
	Minute  Minute
	Subject string
}

// Key is the entry's object key under prefix. The lane comes first so that each
// lane lists on its own, and one minute's writes spread over the lanes.
func Key(prefix string, e Entry) string {
	return fmt.Sprintf("%s%s/%s/%s", prefix, e.Lane, time.Unix(int64(e.Minute)*60, 0).UTC().Format(minuteLayout), e.Subject)
}

// Parse is Key's inverse.
func Parse(prefix, key string) (Entry, error) {
	bad := fmt.Errorf("dueindex: bad key %q under %q", key, prefix)
	rest, ok := strings.CutPrefix(key, prefix)
	if !ok {
		return Entry{}, bad
	}
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 {
		return Entry{}, bad
	}
	lane, err := strconv.ParseUint(parts[0], 16, 8)
	if err != nil || lane >= shard.Lanes {
		return Entry{}, bad
	}
	minute, err := time.Parse(minuteLayout, parts[1])
	if err != nil {
		return Entry{}, bad
	}
	e := Entry{Lane: shard.Lane(lane), Minute: MinuteOf(minute.Unix()), Subject: parts[2]}
	if e.Subject == "" || Key(prefix, e) != key {
		return Entry{}, bad
	}
	return e, nil
}

// Index reads and writes the entries of one index in blob storage.
type Index struct {
	store  *blob.Store
	prefix string
	what   string
	valid  func(Entry) bool
}

// New returns the index under prefix (which ends in a slash) over a store. what
// names the entries in errors ("reminder" makes "creating a reminder entry"). Due
// skips entries that valid (never nil) rejects, objects under the prefix that the
// index's user doesn't recognise, which are left alone and don't count against a page.
func New(store *blob.Store, prefix, what string, valid func(Entry) bool) Index {
	return Index{store: store, prefix: prefix, what: what, valid: valid}
}

// Create makes the entry. One that exists already (a retry of the same write) is
// the same object.
func (x Index) Create(ctx context.Context, e Entry) error {
	_, err := x.store.Create(ctx, Key(x.prefix, e), bytes.NewReader(nil))
	if err != nil && !errors.Is(err, blob.ErrExists) {
		return fmt.Errorf("creating a %s entry: %w", x.what, err)
	}
	return nil
}

// Delete removes the entry. One that is gone is done.
func (x Index) Delete(ctx context.Context, e Entry) error {
	err := x.store.Delete(ctx, Key(x.prefix, e), "")
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return fmt.Errorf("deleting a %s entry: %w", x.what, err)
	}
	return nil
}

// Move creates the entry's next move, in the same lane at minute to under subject,
// then deletes it. A crash between the two leaves a duplicate, which costs one more
// pass of the entry's handler. The moved entry must have another key than the old
// one (a new minute or a new subject), since it would otherwise be deleted again.
func (x Index) Move(ctx context.Context, e Entry, to Minute, subject string) error {
	next := Entry{Lane: e.Lane, Minute: to, Subject: subject}
	if Key(x.prefix, next) == Key(x.prefix, e) {
		return fmt.Errorf("moving a %s entry onto itself: %q", x.what, Key(x.prefix, e))
	}
	if err := x.Create(ctx, next); err != nil {
		return err
	}
	return x.Delete(ctx, e)
}

// Due lists up to limit entries of the lane due by now (their minute is no later
// than now's), earliest first. Entries that have been dealt with leave the head of
// the listing, so there is no cursor.
func (x Index) Due(ctx context.Context, lane shard.Lane, now time.Time, limit int) ([]Entry, error) {
	last := MinuteAt(now)
	var out []Entry
	for info, err := range x.store.List(ctx, x.prefix+lane.String()+"/") {
		if err != nil {
			return nil, err
		}
		e, err := Parse(x.prefix, info.Key)
		if err != nil || !x.valid(e) {
			continue // not ours: left alone
		}
		if e.Minute > last || len(out) >= limit {
			break
		}
		out = append(out, e)
	}
	return out, nil
}
