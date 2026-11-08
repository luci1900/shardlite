package audit

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

const age = time.Hour

var written = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// stand is a time past the age rule for everything the tests write.
var stand = written.Add(2 * age)

type store struct {
	t *testing.T
	*blob.Store
}

func newStore(t *testing.T) store {
	t.Helper()
	mem := blob.NewMemory(blob.WithClock(func() time.Time { return written }))
	return store{t, blob.NewStore(mem, "n1", 1, blob.Options{Backoff: time.Millisecond})}
}

func (s store) put(key string) {
	s.t.Helper()
	if _, err := s.Create(context.Background(), key, strings.NewReader("x")); err != nil {
		s.t.Fatal(err)
	}
}

func key(name, gen, file string) string {
	return shardfile.GenerationPrefix(shard.MustParse(name), shard.Gen(gen)) + file + ".ltx"
}

// manifest lists files of the generation for the shard.
func (s store) manifest(name, gen string, files ...string) {
	s.t.Helper()
	m := &shardfile.Manifest{Generation: shard.Gen(gen)}
	for _, f := range files {
		file, err := shardfile.ParseFile(f)
		if err != nil {
			s.t.Fatal(err)
		}
		m.Files = append(m.Files, file)
	}
	if _, err := shardfile.NewManifestStore(s.Store).Put(context.Background(), shard.MustParse(name), m, nil); err != nil {
		s.t.Fatal(err)
	}
}

// entry creates the orphan index's entry for the shard's generation.
func (s store) entry(name, gen string) {
	s.t.Helper()
	o := shardfile.Orphan{Minute: dueindex.MinuteAt(written.Add(age)), Shard: shard.MustParse(name), Generation: shard.Gen(gen)}
	if err := shardfile.OrphanIndex(s.Store).Create(context.Background(), o.Entry()); err != nil {
		s.t.Fatal(err)
	}
}

func (s store) exists(key string) bool {
	s.t.Helper()
	_, _, err := s.Get(context.Background(), key)
	return err == nil
}

// The walk reports the files past the age that no manifest lists (of a shard whose
// manifest names another generation or is gone as well) and nothing else: not a
// listed file, a young one, a file that isn't LTX.
func TestUnlistedFindsOldFilesNoManifestLists(t *testing.T) {
	s := newStore(t)
	listed := key("users/a", "g1", "1-5.1")
	unlisted := key("users/a", "g1", "6-8.1")
	otherGeneration := key("users/a", "g0", "1-3.1")
	noManifest := key("users/b", "g1", "1-2.1")
	for _, k := range []string{listed, unlisted, otherGeneration, noManifest, shardfile.GenerationPrefix(shard.MustParse("users/a"), "g1") + "notes"} {
		s.put(k)
	}
	s.manifest("users/a", "g1", "1-5.1")

	got, err := Unlisted(context.Background(), s.Store, age, stand)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{noManifest, otherGeneration, unlisted} // by lane (users/b is in lane 2, users/a in lane f), then key
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the walk found\n%v\nwant\n%v", got, want)
	}

	// Nothing is old yet when the audit stands at the write.
	if got, err := Unlisted(context.Background(), s.Store, age, written); err != nil || len(got) != 0 {
		t.Fatalf("at the time of the write the walk found %v, %v", got, err)
	}
}

func TestUnlistedRefusesAKeyThatIsNoShardsFile(t *testing.T) {
	s := newStore(t)
	s.put(shardfile.FilesPrefix + "stray.ltx")
	if _, err := Unlisted(context.Background(), s.Store, age, stand); err == nil {
		t.Fatal("a stray key under the data prefix passed")
	}
	// Another shard's lane is no lane of this one.
	s = newStore(t)
	name := shard.MustParse("users/a")
	other := shard.Lane((uint8(name.Lane()) + 1) % shard.Lanes)
	s.put(shardfile.LanePrefix(other) + "users/a/g1/1-5.1.ltx")
	if _, err := Unlisted(context.Background(), s.Store, age, stand); err == nil {
		t.Fatal("a file in another shard's lane passed")
	}
}

// An unlisted old file that an entry leads to is collected, and the audit passes.
func TestRunPassesWhenEveryOrphanHasAnEntry(t *testing.T) {
	s := newStore(t)
	listed := key("users/a", "g1", "1-5.1")
	s.put(listed)
	s.put(key("users/a", "g1", "6-8.1"))
	s.put(key("users/b", "g1", "1-2.1")) // a deleted shard's
	s.manifest("users/a", "g1", "1-5.1")
	s.entry("users/a", "g1")
	s.entry("users/b", "g1")

	if err := Run(context.Background(), s.Store, age, stand); err != nil {
		t.Fatal(err)
	}
	if !s.exists(listed) {
		t.Fatal("the audit deleted a listed file")
	}
	if left, _ := Unlisted(context.Background(), s.Store, age, stand); len(left) != 0 {
		t.Fatalf("left %v", left)
	}
}

// An unlisted old file in a generation with no entry is one the collector never
// looks at: the audit fails and names it.
func TestRunFailsOnAnOldFileNoEntryLeadsTo(t *testing.T) {
	s := newStore(t)
	s.put(key("users/a", "g1", "1-5.1"))
	orphan := key("users/a", "g1", "6-8.1")
	s.put(orphan)
	s.manifest("users/a", "g1", "1-5.1")
	// An entry for another generation of the shard doesn't cover this one.
	s.entry("users/a", "g0")

	err := Run(context.Background(), s.Store, age, stand)
	if err == nil || !strings.Contains(err.Error(), orphan) {
		t.Fatalf("the audit answered %v, want a failure naming %s", err, orphan)
	}
}
