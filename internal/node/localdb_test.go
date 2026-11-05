package node

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// Open makes the index's table and nothing else.
func TestOpenMakesTheIndex(t *testing.T) {
	db, err := makeLocalDB(filepath.Join(t.TempDir(), LocalDBName))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		got = append(got, n)
	}
	if len(got) != 1 || got[0] != "shards" {
		t.Fatalf("tables %v, want shards alone", got)
	}
}

// Open deletes a database an older node left, journal tables and rows with it,
// and a second Open starts empty again: nothing outlives a start.
func TestOpenStartsFresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), LocalDBName)
	old, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE segments (node TEXT NOT NULL, seq INTEGER NOT NULL, write_id TEXT NOT NULL, size INTEGER NOT NULL, PRIMARY KEY (node, seq))`,
		`INSERT INTO segments VALUES ('n1', 1, '{}', 10)`,
		`PRAGMA user_version = 2`,
	} {
		if _, err := old.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	old.Close()

	db, err := makeLocalDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SELECT * FROM segments`); err == nil {
		t.Fatal("the old journal table survived the start")
	}
	if _, err := db.Exec(`INSERT INTO shards (name, path, generation, position, state, last_used, bytes, size, mtime_ns, wal_size, wal_mtime_ns, manifest, etag)
		VALUES ('test.users/1', 'p', 'g', 1, 'cached', 1, 1, 1, 1, 1, 1, 'm', 'e')`); err != nil {
		t.Fatal(err)
	}
	db.Close()

	db, err = makeLocalDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM shards`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d rows after a second start (%v), want none", n, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

// The table is STRICT: a value of the wrong type is refused, where a lax table
// would store it as whatever the column's affinity makes of it.
func TestTableIsStrict(t *testing.T) {
	db, err := memoryLocalDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`INSERT INTO shards (name, path, generation, position, state, last_used, bytes, size, mtime_ns, wal_size, wal_mtime_ns, manifest, etag)
		VALUES ('test.users/1', 'p', 'g', 1, 'cached', 1, 'many', 1, 1, 1, 1, 'm', 'e')`); err == nil {
		t.Fatal("text in an INTEGER column was accepted")
	}
}
