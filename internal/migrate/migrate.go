// Package migrate applies numbered SQL migrations to a SQLite database, inside a
// transaction its caller owns. The files are laid out as golang-migrate lays them
// out: {n}_{name}.up.sql and {n}_{name}.down.sql, numbered from 1 with no gaps,
// and a database's version is its user_version. There is no table of its own and
// no history: a version is the number of up files applied.
//
// The transaction must be immediate (BEGIN IMMEDIATE), since Migrate reads
// user_version and then writes, which a deferred transaction can fail to upgrade
// with SQLITE_BUSY. What has no effect inside a transaction (PRAGMA foreign_keys,
// VACUUM) is set on the connection before it begins.
package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
)

// Latest migrates to the newest version the files have.
const Latest = -1

// ErrNewer means the database's version is newer than the newest file: a newer
// release migrated it. Migrate leaves it untouched.
var ErrNewer = errors.New("migrate: the database is newer than the migrations")

// Migrate brings the database tx is on to version to (Latest for the newest),
// running the up files it lacks or the down files above to, in order, and sets
// user_version. A failure leaves everything to the caller's rollback.
func Migrate(ctx context.Context, tx *sql.Tx, fsys fs.FS, to int) error {
	files, err := load(fsys)
	if err != nil {
		return err
	}
	if to == Latest {
		to = len(files)
	}
	if to < 0 || to > len(files) {
		return fmt.Errorf("migrate: no version %d (the newest is %d)", to, len(files))
	}
	var v int
	if err := tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&v); err != nil {
		return err
	}
	if v > len(files) {
		return fmt.Errorf("%w: it is at version %d and the newest is %d", ErrNewer, v, len(files))
	}
	from := v
	for ; v < to; v++ {
		if err := run(ctx, tx, fsys, files[v].up); err != nil {
			return err
		}
	}
	for ; v > to; v-- {
		if files[v-1].down == "" {
			return fmt.Errorf("migrate: version %d has no down file", v)
		}
		if err := run(ctx, tx, fsys, files[v-1].down); err != nil {
			return err
		}
	}
	if v == from {
		return nil
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, v))
	return err
}

// Check reports what Migrate refuses (a name it can't read, a gap in the
// numbering, two files for one step) and, beyond that, an up file without its
// down file. Tests run it over the migrations a release embeds.
func Check(fsys fs.FS) error {
	files, err := load(fsys)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return errors.New("migrate: no migrations")
	}
	for _, f := range files {
		if f.down == "" {
			return fmt.Errorf("migrate: %s has no down file", f.up)
		}
	}
	return nil
}

// step is one version's files, by name.
type step struct{ up, down string }

// load reads the names of the .sql files in fsys's root, in version order.
func load(fsys fs.FS) ([]step, error) {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, err
	}
	var files []step
	for _, name := range names {
		n, rest, ok := strings.Cut(name, "_")
		v, err := strconv.Atoi(n)
		up := strings.HasSuffix(rest, ".up.sql")
		if !ok || err != nil || v < 1 || !up && !strings.HasSuffix(rest, ".down.sql") {
			return nil, fmt.Errorf("migrate: %q is not {n}_{name}.up.sql or .down.sql", name)
		}
		for len(files) < v {
			files = append(files, step{})
		}
		f := &files[v-1].down
		if up {
			f = &files[v-1].up
		}
		if *f != "" {
			return nil, fmt.Errorf("migrate: %q and %q are both version %d", *f, name, v)
		}
		*f = name
	}
	for i, f := range files {
		if f.up == "" {
			return nil, fmt.Errorf("migrate: version %d has no up file", i+1)
		}
	}
	return files, nil
}

func run(ctx context.Context, tx *sql.Tx, fsys fs.FS, name string) error {
	b, err := fs.ReadFile(fsys, name)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, string(b)); err != nil {
		return fmt.Errorf("migrate: %s: %w", name, err)
	}
	return nil
}
