package node

import (
	"path/filepath"
	"testing"
)

// testDB opens a local database in a temporary directory.
func testDB(t *testing.T) *LocalDB {
	t.Helper()
	db, err := makeLocalDB(filepath.Join(t.TempDir(), LocalDBName))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// all returns every row.
func (x *index) all() ([]indexRow, error) { return x.rows(``) }

// modernc ignores connection-string settings it doesn't know, so check the
// ones the database asks for took effect on a connection.
func TestLocalDatabasePragmasTakeEffect(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	x := newIndex(db.DB)
	for pragma, want := range map[string]string{"journal_mode": "wal", "synchronous": "2", "busy_timeout": "5000", "page_size": "8192"} {
		var got string
		if err := x.db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil {
			t.Fatal(pragma, err)
		}
		if got != want {
			t.Errorf("PRAGMA %s = %q, want %q", pragma, got, want)
		}
	}
}
