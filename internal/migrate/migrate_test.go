package migrate

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"

	_ "modernc.org/sqlite"
)

var three = fstest.MapFS{
	"1_a.up.sql":   {Data: []byte(`CREATE TABLE a (x); INSERT INTO a VALUES (1);`)},
	"1_a.down.sql": {Data: []byte(`DROP TABLE a;`)},
	"2_b.up.sql":   {Data: []byte(`CREATE TABLE b (x);`)},
	"2_b.down.sql": {Data: []byte(`DROP TABLE b;`)},
	"3_c.up.sql":   {Data: []byte(`ALTER TABLE a ADD COLUMN y;`)},
	"3_c.down.sql": {Data: []byte(`ALTER TABLE a DROP COLUMN y;`)},
	"README.md":    {Data: []byte(`not a migration`)},
}

func open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "m.db")+"?_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	return db
}

// migrate runs Migrate in a transaction of its own, committed if it succeeds.
func migrate(t *testing.T, db *sql.DB, fsys fstest.MapFS, to int) error {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := Migrate(ctx, tx, fsys, to); err != nil {
		return err
	}
	return tx.Commit()
}

// state is the database's version and its schema, in order.
func state(t *testing.T, db *sql.DB) (int, string) {
	t.Helper()
	var v int
	var schema sql.NullString
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT group_concat(sql, ';') FROM (SELECT sql FROM sqlite_schema ORDER BY name)`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	return v, schema.String
}

func TestMigrateUpAndDown(t *testing.T) {
	t.Parallel()
	db := open(t)
	if err := migrate(t, db, three, 1); err != nil {
		t.Fatal(err)
	}
	v1, s1 := state(t, db)
	if v1 != 1 {
		t.Fatalf("version %d", v1)
	}
	// Only the missing files run: a second up from 1 doesn't run 1 again.
	if err := migrate(t, db, three, Latest); err != nil {
		t.Fatal(err)
	}
	if v, _ := state(t, db); v != 3 {
		t.Fatalf("version %d", v)
	}
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM a`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("%d rows, %v", rows, err)
	}
	if err := migrate(t, db, three, 1); err != nil {
		t.Fatal(err)
	}
	if v, s := state(t, db); v != 1 || s != s1 {
		t.Fatalf("down to 1: version %d, schema %q, want %q", v, s, s1)
	}
	if err := migrate(t, db, three, 0); err != nil {
		t.Fatal(err)
	}
	if v, s := state(t, db); v != 0 || s != "" {
		t.Fatalf("down to 0: version %d, schema %q", v, s)
	}
}

func TestMigrateRefusesANewerDatabase(t *testing.T) {
	t.Parallel()
	db := open(t)
	if err := migrate(t, db, three, Latest); err != nil {
		t.Fatal(err)
	}
	v, s := state(t, db)
	older := fstest.MapFS{"1_a.up.sql": three["1_a.up.sql"], "2_b.up.sql": three["2_b.up.sql"]}
	for _, to := range []int{Latest, 1} {
		if err := migrate(t, db, older, to); !errors.Is(err, ErrNewer) {
			t.Fatalf("to %d: %v", to, err)
		}
	}
	if v2, s2 := state(t, db); v2 != v || s2 != s {
		t.Fatal("a refused database was changed")
	}
}

func TestMigrateFailureLeavesTheOldVersion(t *testing.T) {
	t.Parallel()
	db := open(t)
	if err := migrate(t, db, three, 1); err != nil {
		t.Fatal(err)
	}
	v, s := state(t, db)
	bad := fstest.MapFS{}
	for k, f := range three {
		bad[k] = f
	}
	bad["3_c.up.sql"] = &fstest.MapFile{Data: []byte(`ALTER TABLE missing ADD COLUMN y;`)}
	if err := migrate(t, db, bad, Latest); err == nil {
		t.Fatal("a failing migration succeeded")
	}
	// 2 ran before 3 failed, and the rollback took it back.
	if v2, s2 := state(t, db); v2 != v || s2 != s {
		t.Fatalf("version %d, schema %q, want %d %q", v2, s2, v, s)
	}
}

func TestMigrateNeedsADownFileToGoDown(t *testing.T) {
	t.Parallel()
	db := open(t)
	upOnly := fstest.MapFS{"1_a.up.sql": three["1_a.up.sql"]}
	if err := migrate(t, db, upOnly, Latest); err != nil {
		t.Fatal(err)
	}
	if err := migrate(t, db, upOnly, 0); err == nil {
		t.Fatal("went down with no down file")
	}
	if err := Check(upOnly); err == nil {
		t.Fatal("Check passed an up file without its down")
	}
}

func TestCheck(t *testing.T) {
	t.Parallel()
	if err := Check(three); err != nil {
		t.Fatal(err)
	}
	up, down := three["1_a.up.sql"], three["1_a.down.sql"]
	for name, fsys := range map[string]fstest.MapFS{
		"empty":    {},
		"gap":      {"1_a.up.sql": up, "1_a.down.sql": down, "3_c.up.sql": up, "3_c.down.sql": down},
		"from 0":   {"0_a.up.sql": up, "0_a.down.sql": down},
		"no up":    {"1_a.down.sql": down},
		"twice":    {"1_a.up.sql": up, "1_b.up.sql": up, "1_a.down.sql": down},
		"bad name": {"1_a.sql": up},
		"no n":     {"a_b.up.sql": up, "a_b.down.sql": down},
	} {
		if err := Check(fsys); err == nil {
			t.Errorf("%s: passed", name)
		}
	}
}
