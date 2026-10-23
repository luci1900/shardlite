package localdb

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/migrate"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

// TestMigrations checks the migration files: they pass migrate.Check, and up,
// down to nothing and up again leaves the same tables and rows. When a later
// migration changes a table, it must keep the journal's rows: they hold
// acknowledged writes. The first one to change a table is the one that
// proves it, by seeding the rows before it runs and checking them after.
func TestMigrations(t *testing.T) {
	sqlitetest.Migrations(t, migrations)
}

// TestOpenKeepsTheJournalRows opens a database twice and finds the journal's
// rows where they were, at the same version, with the file's own pragmas
// still what Open sets: SQLite keeps none of that without somebody saying so,
// and a delete on an open would do it.
func TestOpenKeepsTheJournalRows(t *testing.T) {
	seed := func(t *testing.T, db *DB) {
		t.Helper()
		for seq, size := range []int64{4, 3, 2, 1} {
			if _, err := db.Exec(`INSERT INTO segments (node, seq, write_id, size) VALUES ('users/7', ?, '{"node":"nodes/a","epoch":1}', ?)`, seq+1, size); err != nil {
				t.Fatal(err)
			}
			for idx := int64(0); idx < size; idx++ {
				if _, err := db.Exec(`INSERT INTO segment_chunks (node, seq, idx, data) VALUES ('users/7', ?, ?, ?)`, seq+1, idx, []byte{0xff}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	rows := func(t *testing.T, db *DB) {
		t.Helper()
		counts := map[string]int{
			"SELECT count(*) FROM segments WHERE node = 'users/7'":       4,
			"SELECT coalesce(sum(size), 0) FROM segments":                10,
			"SELECT count(*) FROM segment_chunks WHERE node = 'users/7'": 10,
		}
		for q, want := range counts {
			var got int
			if err := db.QueryRow(q).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("%q gave %d, want %d", q, got, want)
			}
		}
	}
	pragmas := func(t *testing.T, db *DB) {
		t.Helper()
		for q, wantInt := range map[string]int{"PRAGMA user_version": 1, "PRAGMA page_size": 8192} {
			var got int
			if err := db.QueryRow(q).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if got != wantInt {
				t.Fatalf("%s is %d, want %d", q, got, wantInt)
			}
		}
		var mode string
		if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
			t.Fatal(err)
		}
		if mode != "wal" {
			t.Fatalf("the journal mode is %q, want wal", mode)
		}
	}

	path := filepath.Join(t.TempDir(), Name)
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	pragmas(t, db)
	seed(t, db)
	rows(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pragmas(t, db)
	rows(t, db)
}

// TestOpenLeavesANewerDatabaseAlone gives Open a database whose version is
// beyond the migration files, as a rollback onto a newer node's file would
// leave, and wants it refuse and leave the file as it found it: this node
// cannot read a newer node's chunks, and a newer node's tail may be in the
// file.
func TestOpenLeavesANewerDatabaseAlone(t *testing.T) {
	path := filepath.Join(t.TempDir(), Name)
	older, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := older.Exec(`PRAGMA user_version=2`); err != nil {
		t.Fatal(err)
	}
	if err := older.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(path); !errors.Is(err, migrate.ErrNewer) {
		t.Fatalf("open gave %v, want migrate.ErrNewer", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if s := sqlitetest.Dump(t, path); s != "" {
		t.Fatalf("the newer database's schema changed:\n%s", s)
	}
	again, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	var v int
	if err := again.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 2 {
		t.Fatalf("the version moved to %d", v)
	}
}

// TestMemoryRunsTheMigrations too: the in-memory fallback is not a second set
// of tables, it is the same schema on a database that is not a file.
func TestMemoryRunsTheMigrations(t *testing.T) {
	db, err := Memory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("the version is %d, want 1", v)
	}
}

// TestTablesAreStrict pins the STRICT tables: a value of the wrong type is
// refused, where a lax table would store it as whatever the column's affinity
// makes of it. A number into a TEXT column still converts, so the refusals
// tested are the ones there are: text into an INTEGER column and text into a
// BLOB column.
func TestTablesAreStrict(t *testing.T) {
	db, err := Memory()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{
		`INSERT INTO segments (node, seq, write_id, size) VALUES ('test.users/7', 1, '{"node":"nodes/a","epoch":1}', 'many')`,
		`INSERT INTO segment_chunks (node, seq, idx, data) VALUES ('test.users/7', 1, 0, 'bytes')`,
	} {
		if _, err := db.Exec(q); err == nil {
			t.Errorf("%q was accepted", q)
		}
	}
}
