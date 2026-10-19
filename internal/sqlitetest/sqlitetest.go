// Package sqlitetest compares SQLite files in tests, and checks migrations.
package sqlitetest

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/migrate"
)

// Dump returns every row of every table except Litestream's own, plus the
// schema, as text. It also fails the test if PRAGMA integrity_check fails.
func Dump(tb testing.TB, path string) string {
	tb.Helper()
	s, err := TryDump(path)
	if err != nil {
		tb.Fatalf("dump %s: %v", path, err)
	}
	return s
}

// TryDump is Dump returning an error instead of failing a test.
func TryDump(path string) (string, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return "", err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	var check string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&check); err != nil {
		return "", err
	} else if check != "ok" {
		return "", fmt.Errorf("integrity_check: %s", check)
	}

	var sb strings.Builder
	rows, err := db.Query(`SELECT name, sql FROM sqlite_master WHERE type IN ('table','index') AND name NOT LIKE '_litestream%' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return "", err
	}
	type obj struct{ name, sql string }
	var objs []obj
	for rows.Next() {
		var o obj
		var s sql.NullString
		if err := rows.Scan(&o.name, &s); err != nil {
			return "", err
		}
		o.sql = s.String
		objs = append(objs, o)
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	for _, o := range objs {
		fmt.Fprintf(&sb, "%s\n", o.sql)
		if strings.HasPrefix(o.sql, "CREATE INDEX") || strings.HasPrefix(o.sql, "CREATE UNIQUE INDEX") {
			continue
		}
		r, err := db.Query(fmt.Sprintf(`SELECT * FROM %q ORDER BY rowid`, o.name))
		if err != nil {
			return "", err
		}
		cols, _ := r.Columns()
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		for r.Next() {
			if err := r.Scan(ptrs...); err != nil {
				return "", err
			}
			fmt.Fprintf(&sb, "%s: %v\n", o.name, vals)
		}
		if err := r.Close(); err != nil {
			return "", err
		}
	}
	return sb.String(), nil
}

// Migrations checks a release's migrations (see package migrate): they pass
// migrate.Check, and on a new file migrating up, down to nothing and up again
// leaves the same tables and rows as the first time.
func Migrations(tb testing.TB, fsys fs.FS) {
	tb.Helper()
	if err := migrate.Check(fsys); err != nil {
		tb.Fatal(err)
	}
	path := filepath.Join(tb.TempDir(), "migrations.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		tb.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	to := func(v int) {
		tb.Helper()
		tx, err := db.Begin()
		if err != nil {
			tb.Fatal(err)
		}
		defer tx.Rollback()
		if err := migrate.Migrate(context.Background(), tx, fsys, v); err != nil {
			tb.Fatalf("to %d: %v", v, err)
		}
		if err := tx.Commit(); err != nil {
			tb.Fatal(err)
		}
	}
	to(migrate.Latest)
	first := Dump(tb, path)
	to(0)
	if empty := Dump(tb, path); empty != "" {
		tb.Fatalf("down to 0 leaves:\n%s", empty)
	}
	to(migrate.Latest)
	if again := Dump(tb, path); again != first {
		tb.Fatalf("up again gives:\n%s\nthe first time:\n%s", again, first)
	}
}
