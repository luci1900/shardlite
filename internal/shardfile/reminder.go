package shardfile

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/shard"
)

// ReminderPrefix is where the reminder index lives (docs/internals.md, Reminder
// index): a due index (internal/dueindex) whose entries say a shard may need waking
// at a minute. An entry is created by anyone before the manifest write that records
// the time (the writer's own entry, keyed by the manifest version the write is based
// on), and deleted only when the shard's manifest doesn't back it. It needs no owner
// and no clock.
const ReminderPrefix = "index/reminders/"

// Reminder is one entry of the reminder index:
// index/reminders/{lane}/{minute}/{kind}/{id}/{base}.{n}. The lane is the shard's,
// and everything after the minute is the entry's subject.
type Reminder struct {
	Minute dueindex.Minute // Unix minutes
	Shard  shard.Name      // kind/id
	Base   string          // BaseOf the manifest version the write the entry is for CASes against
	N      int             // how many times the dispatcher has moved the entry
}

// BaseOf is the short name of a manifest version.
func BaseOf(v blob.ETag) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:6])
}

// Entry is the reminder as the due index holds it: the shard's lane, the minute and
// the subject kind/id/base.n.
func (r Reminder) Entry() dueindex.Entry {
	return dueindex.Entry{Lane: r.Shard.Lane(), Minute: r.Minute, Subject: fmt.Sprintf("%s/%s.%d", r.Shard, r.Base, r.N)}
}

// ParseReminder is Entry's inverse. A subject that isn't a reminder's, or one under
// another shard's lane, isn't a reminder.
func ParseReminder(d dueindex.Entry) (Reminder, error) {
	bad := fmt.Errorf("shardfile: bad reminder subject %q", d.Subject)
	parts := strings.Split(d.Subject, "/")
	if len(parts) != 3 {
		return Reminder{}, bad
	}
	base, n, ok := strings.Cut(parts[2], ".")
	count, err := strconv.Atoi(n)
	if !ok || err != nil || count < 0 {
		return Reminder{}, bad
	}
	name, err := shard.ParseName(parts[0] + "/" + parts[1])
	if err != nil {
		return Reminder{}, bad
	}
	r := Reminder{Minute: d.Minute, Shard: name, Base: base, N: count}
	if r.Entry() != d {
		return Reminder{}, bad
	}
	return r, nil
}

// ReminderIndex is the reminder index over a store. Listing it skips objects under
// the prefix that aren't reminders.
func ReminderIndex(store *blob.Store) dueindex.Index {
	return dueindex.New(store, ReminderPrefix, "reminder", func(d dueindex.Entry) bool {
		_, err := ParseReminder(d)
		return err == nil
	})
}

// Backs reports whether a manifest whose next_reminder is next backs the reminder:
// it has one (0 means none) and it is no later than the reminder's minute.
func Backs(next dueindex.Minute, r Reminder) bool {
	return next > 0 && next <= r.Minute
}

// Prunable is the pruning rule: a reminder may be deleted when the manifest doesn't
// back it (there is none, or next_reminder is absent or after the reminder's minute)
// and the manifest's current version is not the reminder's base. While it is the
// base the write the reminder was made for may still be in flight: that write CASes
// against its base, so once the manifest has moved off it the write has landed or
// never will.
func Prunable(exists bool, next dueindex.Minute, version blob.ETag, r Reminder) bool {
	if !exists {
		return true
	}
	return !Backs(next, r) && BaseOf(version) != r.Base
}
