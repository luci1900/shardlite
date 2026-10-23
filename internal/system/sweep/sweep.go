// Package sweep is the _sweep system kind (docs/internals.md, Garbage): one
// shard, _system.sweep/_cluster, that deletes the LTX files no manifest lists.
// Its wake does one batch of a pass over data/shards/ and sets its next
// reminder, so each wake is short, a move or a crash resumes where it stopped
// and the reminders loop paces it.
package sweep

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/system"
)

// Options configure the sweeper. Zero values take the defaults.
type Options struct {
	// Age is how old a file no manifest lists must be before it is deleted:
	// longer than any upload takes to reach its manifest. It matches the journal's
	// expiry, past which a delta whose manifest update still fails has lost its
	// tail anyway. 24 h if zero.
	Age time.Duration
	// Batch is how many keys a wake lists at least (whole shards, so a little
	// more), 1,000 if zero.
	Batch int
	// Pace is the wait between the wakes of a pass, 1 s if zero, which caps the
	// sweeper at about one batch of requests a second.
	Pace time.Duration
	// Every is the wait between the end of a pass and the start of the next, 24 h if zero.
	Every time.Duration
}

// migrations hold the sweeper's state: where the pass in progress has got to.
//
//go:embed migrations/*.sql
var embedded embed.FS

var migrations, _ = fs.Sub(embedded, "migrations") // a fixed, valid name never fails

// New returns the _sweep kind. It deletes from store, checks the node's lease
// with check before each delete, and reads the time from now.
func New(store *blob.Store, check func() error, now func() time.Time, log *slog.Logger, opts Options) kind.Kind {
	if opts.Age <= 0 {
		opts.Age = 24 * time.Hour
	}
	if opts.Batch <= 0 {
		opts.Batch = 1000
	}
	if opts.Pace <= 0 {
		opts.Pace = time.Second
	}
	if opts.Every <= 0 {
		opts.Every = 24 * time.Hour
	}
	sw := &sweeper{store: store, manifests: shardfile.NewManifestStore(store), check: check, now: now, log: log, opts: opts}
	// The local tier: a cursor a host loss loses only repeats a batch, and nothing acts
	// on the sweeper's answers.
	return kind.Kind{Name: kind.SystemKind("sweep"), Sync: kind.SyncLocal, IDs: []shard.ID{"_cluster"}, Migrations: migrations, Handler: sw.handler}
}

type sweeper struct {
	store     *blob.Store
	manifests shardfile.ManifestStore
	check     func() error
	now       func() time.Time
	log       *slog.Logger
	opts      Options
}

func (sw *sweeper) handler(s *system.System) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /_shardlite/reminders", func(w http.ResponseWriter, r *http.Request) {
		if err := sw.wake(r.Context(), s); err != nil {
			sw.log.Warn("sweeping LTX files failed, trying again", "err", err)
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// wake does one batch of the pass and records where it got to, with the next
// reminder: soon while the pass goes on, and Every later once it ends. The
// cursor is the last key listed ("" between passes).
func (sw *sweeper) wake(ctx context.Context, s *system.System) error {
	var cursor string
	var total int
	if err := s.Tx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT cursor, deleted FROM sweep WHERE id = 0`).Scan(&cursor, &total)
	}); err != nil {
		return err
	}
	b, err := sw.list(ctx, cursor)
	if err != nil {
		return err
	}
	deleted, err := sw.sweep(ctx, b.shards)
	total += deleted
	if err != nil {
		return err
	}
	next, wait, kept := b.last, sw.opts.Pace, total
	if b.done {
		next, wait, kept = "", sw.opts.Every, 0
	}
	err = s.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE sweep SET cursor = ?, deleted = ? WHERE id = 0`, next, kept); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO _shardlite_reminders (key, due) VALUES ('run', ?) ON CONFLICT (key) DO UPDATE SET due = excluded.due`,
			sw.now().Add(wait).Unix())
		return err
	})
	if err == nil && b.done {
		sw.log.Info("swept the LTX files no manifest lists", "deleted", total)
	}
	return err
}

// batch is what one wake lists: whole shards, in key order.
type batch struct {
	shards []shardFiles
	last   string // the last key of the last shard
	done   bool   // the listing ended: the pass is over
}

type shardFiles struct {
	name  shard.Name
	files []blob.ObjectInfo
}

// list lists data/shards/ after cursor ("" is the start of a pass) until it has
// Batch keys and the next shard begins, so a batch never splits one.
func (sw *sweeper) list(ctx context.Context, cursor string) (batch, error) {
	var b batch
	n := 0
	for info, err := range sw.store.ListAfter(ctx, shardfile.FilesPrefix, cursor) {
		if err != nil {
			return batch{}, err
		}
		name, ok := shardOf(info.Key)
		if !ok {
			continue // not a shard's file: left alone
		}
		if len(b.shards) == 0 || b.shards[len(b.shards)-1].name != name {
			if n >= sw.opts.Batch {
				return b, nil
			}
			b.shards = append(b.shards, shardFiles{name: name})
		}
		cur := &b.shards[len(b.shards)-1]
		cur.files = append(cur.files, info)
		b.last = info.Key
		n++
	}
	b.done = true
	return b, nil
}

// shardOf is the shard a key belongs to:
// data/shards/{kind}/{id}/{generation}/{file}.ltx.
func shardOf(key string) (shard.Name, bool) {
	parts := strings.Split(strings.TrimPrefix(key, shardfile.FilesPrefix), "/")
	if len(parts) != 4 || !strings.HasSuffix(parts[3], ".ltx") {
		return shard.Name{}, false
	}
	name, err := shard.ParseName(parts[0] + "/" + parts[1])
	return name, err == nil
}

// sweep deletes the garbage among the batch's files and returns how many it
// deleted. It checks the lease before each delete.
func (sw *sweeper) sweep(ctx context.Context, shards []shardFiles) (int, error) {
	deleted := 0
	for _, sh := range shards {
		garbage, err := sw.garbage(ctx, sh)
		if err != nil {
			return deleted, err
		}
		for _, key := range garbage {
			if err := sw.check(); err != nil {
				return deleted, err
			}
			if err := sw.store.Delete(ctx, key, ""); err != nil && !errors.Is(err, blob.ErrNotFound) {
				return deleted, fmt.Errorf("deleting %s: %w", key, err)
			}
			deleted++
		}
	}
	return deleted, nil
}

// garbage is the files of one shard that no manifest lists and that are older
// than Age: in the manifest's generation, the files it doesn't list; in any
// other generation (or every one, with no manifest), all of them once the
// newest is past the age. A shard none of whose files are old enough isn't
// looked at, so active shards cost no manifest read until their base ages.
func (sw *sweeper) garbage(ctx context.Context, sh shardFiles) ([]string, error) {
	old := sw.now().Add(-sw.opts.Age)
	if !slices.ContainsFunc(sh.files, func(f blob.ObjectInfo) bool { return f.LastModified.Before(old) }) {
		return nil, nil
	}
	m, _, err := sw.manifests.Get(ctx, sh.name)
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		return nil, err
	}
	gens := map[shard.Gen][]blob.ObjectInfo{}
	for _, f := range sh.files {
		gen := shard.Gen(strings.Split(strings.TrimPrefix(f.Key, shardfile.ShardPrefix(sh.name)), "/")[0])
		gens[gen] = append(gens[gen], f)
	}
	var out []string
	for gen, files := range gens {
		if m != nil && gen == m.Generation {
			listed := map[string]bool{}
			for _, f := range m.Files {
				listed[shardfile.FileKey(sh.name, gen, f)] = true
			}
			for _, f := range files {
				if !listed[f.Key] && f.LastModified.Before(old) {
					out = append(out, f.Key)
				}
			}
			continue
		}
		if !slices.ContainsFunc(files, func(f blob.ObjectInfo) bool { return !f.LastModified.Before(old) }) {
			for _, f := range files {
				out = append(out, f.Key)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}
