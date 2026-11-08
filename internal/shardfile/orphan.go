package shardfile

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/shard"
)

// OrphanPrefix is where the orphan index lives (docs/internals.md, Orphan index): a due
// index (internal/dueindex) whose entries say a generation of a shard may hold LTX
// files that no manifest lists. An entry is created by the writer before any write
// that can leave such files, and its minute is when the age rule first lets the
// collector look (the write's minute plus the age, see Files.AgeRule). Writers never
// delete entries, so all the writes of one generation in one minute share one.
const OrphanPrefix = "index/orphans/"

// DefaultAgeRule is how old an LTX file that no manifest lists must be before it is
// deleted: longer than any upload takes to reach its manifest, and as long as a
// journal segment lives, past which a delta whose manifest update still fails has
// lost its tail anyway.
const DefaultAgeRule = 24 * time.Hour

// Orphan is one entry of the orphan index:
// index/orphans/{lane}/{minute}/{kind}/{id}/{generation}. The lane is the shard's,
// and everything after the minute is the entry's subject.
type Orphan struct {
	Minute     dueindex.Minute // Unix minutes: when the collector may first look
	Shard      shard.Name      // kind/id
	Generation shard.Gen
}

// Entry is the orphan as the due index holds it: the shard's lane, the minute and
// the subject kind/id/generation.
func (o Orphan) Entry() dueindex.Entry {
	return dueindex.Entry{Lane: o.Shard.Lane(), Minute: o.Minute, Subject: fmt.Sprintf("%s/%s", o.Shard, o.Generation)}
}

// ParseOrphan is Entry's inverse. A subject that isn't an orphan's, or one under
// another shard's lane, isn't an orphan.
func ParseOrphan(d dueindex.Entry) (Orphan, error) {
	bad := fmt.Errorf("shardfile: bad orphan subject %q", d.Subject)
	parts := strings.Split(d.Subject, "/")
	if len(parts) != 3 || parts[2] == "" {
		return Orphan{}, bad
	}
	name, err := shard.ParseName(parts[0] + "/" + parts[1])
	if err != nil {
		return Orphan{}, bad
	}
	o := Orphan{Minute: d.Minute, Shard: name, Generation: shard.Gen(parts[2])}
	if o.Entry() != d {
		return Orphan{}, bad
	}
	return o, nil
}

// OrphanIndex is the orphan index over a store. Listing it skips objects under the
// prefix that aren't orphan entries.
func OrphanIndex(store *blob.Store) dueindex.Index {
	return dueindex.New(store, OrphanPrefix, "orphan", func(d dueindex.Entry) bool {
		_, err := ParseOrphan(d)
		return err == nil
	})
}

// AgeRule is how old an LTX file that no manifest lists must be before it is deleted,
// DefaultAgeRule if OrphanAge is zero. The writer's orphan entries and the collector
// both read it, so an entry falls due when the rule first lets the collector look.
func (f *Files) AgeRule() time.Duration {
	if f.OrphanAge > 0 {
		return f.OrphanAge
	}
	return DefaultAgeRule
}

// orphanEntry creates the orphan index's entry for a generation of the shard, due
// when the age rule first lets the collector look at files written now. It comes
// before the write it is for, and a failure fails the write. An entry that exists
// already (an earlier write of the generation in the same minute) is the same object.
func (h *Handle) orphanEntry(ctx context.Context, generation shard.Gen) error {
	o := Orphan{Minute: dueindex.MinuteAt(h.F.now().Add(h.F.AgeRule())), Shard: h.Name, Generation: generation}
	return OrphanIndex(h.F.Blob).Create(ctx, o.Entry())
}
