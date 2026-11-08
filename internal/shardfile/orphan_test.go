package shardfile_test

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/kill"
	shardpkg "github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// The orphan tests below are about the orphan entry's subject and its keys, which are
// what is in storage.

func orphanKey(o shardfile.Orphan) string {
	return dueindex.Key(shardfile.OrphanPrefix, o.Entry())
}

func parseOrphanKey(key string) (shardfile.Orphan, error) {
	d, err := dueindex.Parse(shardfile.OrphanPrefix, key)
	if err != nil {
		return shardfile.Orphan{}, err
	}
	return shardfile.ParseOrphan(d)
}

// The orphan index's keys are fixed: they are what is in storage. The lane comes first
// (one hex digit, the shard's) and then the UTC minute, the shard's name and the
// generation.
func TestOrphanKeysAreFixed(t *testing.T) {
	t.Parallel()
	name := shardpkg.MustParse("users/42")
	if name.Lane().String() != "6" {
		t.Fatalf("lane of %s is %s, the expected key below is for 6", name, name.Lane())
	}
	o := shardfile.Orphan{Minute: minuteAt(14, 5), Shard: name, Generation: "k3x9ab"}
	if got, want := orphanKey(o), "index/orphans/6/2026-10-19T14:05Z/users/42/k3x9ab"; got != want {
		t.Errorf("key %q, want %q", got, want)
	}
}

func TestOrphanKeyRoundTrip(t *testing.T) {
	t.Parallel()
	o := shardfile.Orphan{Minute: minuteAt(14, 5), Shard: shardpkg.MustParse("users/42"), Generation: "k3x9ab"}
	got, err := parseOrphanKey(orphanKey(o))
	if err != nil || got != o {
		t.Fatalf("%q parses to %+v (%v), want %+v", orphanKey(o), got, err, o)
	}
	p := shardfile.OrphanPrefix
	for _, bad := range []string{
		"", p, shardfile.ManifestKey(shardpkg.MustParse("users/42")),
		p + "x/2026-10-19T14:05Z/users/42/g",
		// The generation is missing.
		p + "6/2026-10-19T14:05Z/users/42",
		p + "6/2026-10-19T14:05Z/users/42/",
		// A file under the generation is no entry.
		p + "6/2026-10-19T14:05Z/users/42/g/1-2.1.ltx",
		// Not the shard's lane.
		p + ((o.Shard.Lane() + 1) % shardpkg.Lanes).String() + "/2026-10-19T14:05Z/users/42/g",
		// A reminder's key is no orphan's.
		shardfile.ReminderPrefix + "6/2026-10-19T14:05Z/users/42/g",
	} {
		if _, err := parseOrphanKey(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

// The subject is kind/id/generation, and the lane is the shard's.
func TestOrphanSubjectRoundTrip(t *testing.T) {
	t.Parallel()
	o := shardfile.Orphan{Minute: minuteAt(14, 5), Shard: shardpkg.MustParse("users/42"), Generation: "k3x9ab"}
	d := o.Entry()
	if d.Subject != "users/42/k3x9ab" || d.Lane != o.Shard.Lane() || d.Minute != o.Minute {
		t.Fatalf("due entry %+v, want subject users/42/k3x9ab in the shard's lane", d)
	}
	got, err := shardfile.ParseOrphan(d)
	if err != nil || got != o {
		t.Fatalf("%+v parses to %+v (%v), want %+v", d, got, err, o)
	}
	for _, subject := range []string{"", "a", "users/42", "users/42/", "Users/42/g", "users/42/g/h", "users//g"} {
		bad := d
		bad.Subject = subject
		if _, err := shardfile.ParseOrphan(bad); err == nil {
			t.Errorf("subject %q parsed", subject)
		}
	}
	other := d
	other.Lane = (d.Lane + 1) % shardpkg.Lanes
	if _, err := shardfile.ParseOrphan(other); err == nil {
		t.Error("an entry under another shard's lane parsed")
	}
}

// The orphan index lists its lane's entries earliest first and leaves what isn't an
// entry alone, without it counting against the page.
func TestOrphanIndexListsOnlyOrphans(t *testing.T) {
	t.Parallel()
	store := blob.NewStore(blob.NewMemory(), "n1", 1, blob.Options{Backoff: time.Millisecond})
	x := shardfile.OrphanIndex(store)
	ctx := context.Background()
	names := shardsIn(t, 3, 2)
	late := shardfile.Orphan{Minute: minuteAt(14, 7), Shard: names[0], Generation: "g"}
	early := shardfile.Orphan{Minute: minuteAt(14, 5), Shard: names[1], Generation: "g"}
	for _, o := range []shardfile.Orphan{late, early} {
		if err := x.Create(ctx, o.Entry()); err != nil {
			t.Fatal(err)
		}
	}
	// Junk, and a reminder-shaped subject (two parts after the name).
	for _, key := range []string{"3/2026-10-19T14:00Z/junk", "3/2026-10-19T14:00Z/users/42/g/extra"} {
		if _, err := store.Create(ctx, shardfile.OrphanPrefix+key, bytes.NewReader(nil)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := x.Due(ctx, 3, at(15, 0), 2)
	if err != nil || len(got) != 2 || got[0] != early.Entry() || got[1] != late.Entry() {
		t.Fatalf("due %+v (%v), want %+v then %+v", got, err, early.Entry(), late.Entry())
	}
}

// The age rule is a day unless the Files says otherwise.
func TestAgeRuleDefaultsToADay(t *testing.T) {
	t.Parallel()
	var f shardfile.Files
	if got := f.AgeRule(); got != 24*time.Hour {
		t.Fatalf("default age rule %v, want 24h", got)
	}
	f.OrphanAge = time.Hour
	if got := f.AgeRule(); got != time.Hour {
		t.Fatalf("age rule %v, want 1h", got)
	}
}

// The tests below drive the writers and watch what they do to storage.

// writeLog records, in order, the orphan index's creates, the LTX files' creates, the
// manifest's updates and deletes, the deletes of LTX files and the kill points the
// writers reach.
type writeLog struct {
	mu  sync.Mutex
	ops []string
	// failOrphans makes the creates of orphan entries fail (nothing is recorded for a
	// create that fails).
	failOrphans atomic.Bool
}

func (l *writeLog) add(s string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ops = append(l.ops, s)
}

func (l *writeLog) hook(name string) blobtest.Hook {
	manifest := shardfile.ManifestKey(shardName(name))
	return func(_ context.Context, op blobtest.Op, key string, after bool) error {
		switch {
		case op == blobtest.OpCreate && strings.HasPrefix(key, shardfile.OrphanPrefix):
			if after {
				return nil
			}
			if l.failOrphans.Load() {
				return errors.New("storage is down")
			}
			l.add("entry")
		case op == blobtest.OpCreate && strings.HasPrefix(key, shardfile.FilesPrefix) && !after:
			l.add("file")
		case op == blobtest.OpUpdate && key == manifest && !after:
			l.add("manifest")
		case op == blobtest.OpDelete && key == manifest && !after:
			l.add("manifest-delete")
		case op == blobtest.OpDelete && strings.HasPrefix(key, shardfile.FilesPrefix) && !after:
			l.add("drop")
		}
		return nil
	}
}

func (l *writeLog) hooks() shardfile.Hooks {
	return shardfile.Hooks{At: func(p kill.Point) { l.add("kill:" + string(p)) }}
}

func (l *writeLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := l.ops
	l.ops = nil
	return out
}

// clock is a Files.Now that the test moves.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// orphanRig is an Env whose Files make their orphan entries at a clock the test
// moves, with an age rule of two hours.
type orphanRig struct {
	*harness.Env
	clock *clock
	log   *writeLog
	age   time.Duration
}

func newOrphanRig(t *testing.T) *orphanRig {
	t.Helper()
	r := &orphanRig{Env: harness.New(t), clock: &clock{t: at(14, 5)}, log: &writeLog{}, age: 2 * time.Hour}
	r.Files.Now = r.clock.now
	r.Files.OrphanAge = r.age
	r.Blob.SetHook(r.log.hook(shard))
	return r
}

// want is the entry for a generation made now: due an age from now.
func (r *orphanRig) want(gen shardpkg.Gen) shardfile.Orphan {
	return shardfile.Orphan{Minute: dueindex.MinuteAt(r.clock.now().Add(r.age)), Shard: shardName(shard), Generation: gen}
}

// orphans lists every entry in the orphan index.
func (r *orphanRig) orphans(t *testing.T) []shardfile.Orphan {
	t.Helper()
	var out []shardfile.Orphan
	far := r.clock.now().Add(1000 * time.Hour)
	for i := range shardpkg.Lanes {
		got, err := shardfile.OrphanIndex(r.Store).Due(context.Background(), shardpkg.Lane(i), far, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range got {
			o, err := shardfile.ParseOrphan(d)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, o)
		}
	}
	return out
}

// objects is every object in storage with its version, to compare before and after.
func (r *orphanRig) objects(t *testing.T) map[string]blob.ETag {
	t.Helper()
	out := map[string]blob.ETag{}
	for info, err := range r.Store.List(context.Background(), "") {
		if err != nil {
			t.Fatal(err)
		}
		out[info.Key] = info.ETag
	}
	return out
}

// written is the objects the writers' entries are for and the entries themselves: the
// LTX files, the manifests and the orphan index. The journal and the reminder index
// are other steps of a write (an adoption's fence, its reminder), which come first.
func (r *orphanRig) written(t *testing.T) map[string]blob.ETag {
	t.Helper()
	out := r.objects(t)
	for key := range out {
		if !strings.HasPrefix(key, shardfile.FilesPrefix) && !strings.HasPrefix(key, shardfile.ManifestPrefix) && !strings.HasPrefix(key, shardfile.OrphanPrefix) {
			delete(out, key)
		}
	}
	return out
}

func expect(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: operations %v, want %v", what, got, want)
	}
}

const (
	killFile   = "kill:" + string(kill.OrphanEntryBeforeFile)
	killDelete = "kill:" + string(kill.OrphanEntryBeforeDeleteShard)
)

// A delta's file is preceded by the entry for its generation, due an age after the
// write, and the kill point sits between the two.
func TestDeltaCreatesItsOrphanEntryFirst(t *testing.T) {
	t.Parallel()
	r := newOrphanRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	gen := h.Manifest.Generation

	ack(t, app, sh, 1)
	if err := h.WriteDelta(ctx, sh, r.log.hooks()); err != nil {
		t.Fatal(err)
	}
	expect(t, "delta", r.log.take(), "entry", killFile, "file", "kill:"+string(kill.DeltaBeforeManifest), "manifest")
	if got := r.orphans(t); len(got) != 1 || got[0] != r.want(gen) {
		t.Fatalf("entries %+v, want %+v", got, r.want(gen))
	}
	if want := dueindex.MinuteAt(at(16, 5)); r.orphans(t)[0].Minute != want {
		t.Fatalf("entry due at minute %d, want %d (the write's minute and two hours)", r.orphans(t)[0].Minute, want)
	}
}

// Without an age rule in the Files the entry is due a day after the write.
func TestOrphanEntryIsDueADayAfterTheWriteByDefault(t *testing.T) {
	t.Parallel()
	r := newOrphanRig(t)
	r.Files.OrphanAge = 0
	r.age = 24 * time.Hour
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if got := r.orphans(t); len(got) != 1 || got[0] != r.want(h.Manifest.Generation) {
		t.Fatalf("entries %+v, want %+v", got, r.want(h.Manifest.Generation))
	}
}

// A rebase's base is preceded by the entry, and the deletion of the files it
// supersedes makes none: the base's entry falls due after the swap.
func TestRebaseCreatesItsOrphanEntryFirst(t *testing.T) {
	t.Parallel()
	r := newOrphanRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	gen := h.Manifest.Generation
	for i := 1; i <= 2; i++ {
		ack(t, app, sh, i)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	r.log.take()

	ack(t, app, sh, 3)
	if did, err := h.Rebase(ctx, sh, r.log.hooks()); !did || err != nil {
		t.Fatalf("rebase: %v %v", did, err)
	}
	expect(t, "rebase", r.log.take(),
		"entry", killFile, "file", "kill:"+string(kill.RebaseBeforeManifest), "manifest", "kill:"+string(kill.RebaseAfterSwap),
		"drop", "drop")
	if got := r.orphans(t); len(got) != 1 || got[0] != r.want(gen) {
		t.Fatalf("entries %+v, want the one %+v", got, r.want(gen))
	}
}

// A deletion makes the generation's entry before the manifest is deleted, and
// the files go after it.
func TestDeleteShardCreatesItsOrphanEntryFirst(t *testing.T) {
	t.Parallel()
	r := newOrphanRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	r.log.take()

	if err := h.DeleteShard(ctx, r.log.hooks()); err != nil {
		t.Fatal(err)
	}
	expect(t, "delete", r.log.take(), "entry", killDelete, "manifest-delete", "drop")
	if got := r.orphans(t); len(got) != 1 || got[0] != r.want(h.Manifest.Generation) {
		t.Fatalf("entries %+v, want %+v", got, r.want(h.Manifest.Generation))
	}
}

// Adoption's delta goes through the same write: its entry comes before the file.
func TestAdoptionDeltaCreatesItsOrphanEntryFirst(t *testing.T) {
	t.Parallel()
	r := newOrphanRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	write(t, r.Env, app, func() error { return sh.Sync(ctx) }, 0)
	r.log.take()

	rep, err := r.Files.AdoptJournal(ctx, r.Node, 1, r.W.NextSeq(), nil)
	if err != nil || rep.Deltas != 1 {
		t.Fatalf("adoption: %+v %v", rep, err)
	}
	// The reminder's entry is under another prefix and is not in the log.
	expect(t, "adoption", r.log.take(), "entry", "file", "manifest")
	if got := r.orphans(t); len(got) != 1 || got[0] != r.want(h.Manifest.Generation) {
		t.Fatalf("entries %+v, want %+v", got, r.want(h.Manifest.Generation))
	}
}

// A create of the entry that fails fails the write and leaves storage as it was, for
// each of the writes that make one, and the same write goes through once storage works.
func TestFailedOrphanEntryFailsTheWrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	delta := func(t *testing.T, app *apptest.App, h *shardfile.Handle, sh *capture.Shard, n int) {
		t.Helper()
		ack(t, app, sh, n)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		name    string
		prepare func(t *testing.T, r *orphanRig, app *apptest.App, h *shardfile.Handle, sh *capture.Shard)
		run     func(t *testing.T, r *orphanRig, h *shardfile.Handle, sh *capture.Shard) error
	}{
		{"delta",
			func(t *testing.T, r *orphanRig, app *apptest.App, h *shardfile.Handle, sh *capture.Shard) {
				ack(t, app, sh, 1)
			},
			func(t *testing.T, r *orphanRig, h *shardfile.Handle, sh *capture.Shard) error {
				return h.WriteDelta(ctx, sh, shardfile.Hooks{})
			}},
		{"rebase",
			func(t *testing.T, r *orphanRig, app *apptest.App, h *shardfile.Handle, sh *capture.Shard) {
				delta(t, app, h, sh, 1)
				delta(t, app, h, sh, 2)
				ack(t, app, sh, 3)
			},
			func(t *testing.T, r *orphanRig, h *shardfile.Handle, sh *capture.Shard) error {
				_, err := h.Rebase(ctx, sh, shardfile.Hooks{})
				return err
			}},
		{"delete",
			func(t *testing.T, r *orphanRig, app *apptest.App, h *shardfile.Handle, sh *capture.Shard) {
				delta(t, app, h, sh, 1)
			},
			func(t *testing.T, r *orphanRig, h *shardfile.Handle, sh *capture.Shard) error {
				return h.DeleteShard(ctx, shardfile.Hooks{})
			}},
		{"adoption",
			func(t *testing.T, r *orphanRig, app *apptest.App, h *shardfile.Handle, sh *capture.Shard) {
				write(t, r.Env, app, func() error { return sh.Sync(ctx) }, 0)
			},
			func(t *testing.T, r *orphanRig, h *shardfile.Handle, sh *capture.Shard) error {
				_, err := r.Files.AdoptJournal(ctx, r.Node, 1, r.W.NextSeq(), nil)
				return err
			}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := newOrphanRig(t)
			app := r.StartApp(shard)
			h, sh := r.NewShard(shard)
			defer sh.Close(ctx)
			c.prepare(t, r, app, h, sh)
			before, etag, files := r.written(t), h.ETag, len(h.Manifest.Files)
			r.log.take()

			r.log.failOrphans.Store(true)
			if err := c.run(t, r, h, sh); err == nil {
				t.Fatal("the write went through without its entry")
			}
			if got := r.written(t); !maps.Equal(got, before) {
				t.Fatalf("storage changed by a write whose entry failed: before %v, after %v", before, got)
			}
			if h.ETag != etag || len(h.Manifest.Files) != files {
				t.Fatal("the handle moved on")
			}
			if ops := r.log.take(); len(ops) != 0 {
				t.Fatalf("operations %v after a failed entry, want none", ops)
			}

			r.log.failOrphans.Store(false)
			if err := c.run(t, r, h, sh); err != nil {
				t.Fatalf("the same write once storage works: %v", err)
			}
			if got := r.written(t); maps.Equal(got, before) {
				t.Fatal("the retry wrote nothing")
			}
		})
	}
}

// All the writes of one generation in one minute share one entry, a later minute
// makes another, and a new generation has its own. Writers never delete them.
func TestWritesOfOneGenerationInOneMinuteShareAnEntry(t *testing.T) {
	t.Parallel()
	r := newOrphanRig(t)
	ctx := context.Background()
	app := r.StartApp(shard)
	h, sh := r.NewShard(shard)
	defer sh.Close(ctx)
	gen := h.Manifest.Generation
	delta := func(n int) {
		t.Helper()
		ack(t, app, sh, n)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	entries := func() int {
		n := 0
		for key := range r.objects(t) {
			if strings.HasPrefix(key, shardfile.OrphanPrefix) {
				n++
			}
		}
		return n
	}

	delta(1)
	delta(2)
	r.clock.advance(30 * time.Second) // the same minute
	delta(3)
	if n := entries(); n != 1 {
		t.Fatalf("%d entries after three deltas in a minute, want 1", n)
	}
	first := r.want(gen)

	r.clock.advance(time.Minute)
	delta(4)
	if got := r.orphans(t); len(got) != 2 || got[0] != first || got[1] != r.want(gen) {
		t.Fatalf("entries %+v, want %+v and %+v", got, first, r.want(gen))
	}

	// A rebase deletes the files it supersedes, and no entry.
	ack(t, app, sh, 5)
	if did, err := h.Rebase(ctx, sh, shardfile.Hooks{}); !did || err != nil {
		t.Fatalf("rebase: %v %v", did, err)
	}
	if n := entries(); n != 2 {
		t.Fatalf("%d entries after the rebase, want 2", n)
	}

	// A start over makes none either: its old generation's files each had their
	// upload's entry.
	if err := h.StartOver(ctx); err != nil {
		t.Fatal(err)
	}
	if n := entries(); n != 2 {
		t.Fatalf("%d entries after a start over, want 2", n)
	}
}
