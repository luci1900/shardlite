package node

import (
	"path/filepath"
	"testing"
)

// all returns every row.
func (x *index) all() ([]indexRow, error) { return x.rows(``) }

// modernc ignores connection-string settings it doesn't know, so check the
// ones the index asks for took effect on a connection.
func TestIndexPragmasTakeEffect(t *testing.T) {
	t.Parallel()
	x, err := openIndex(filepath.Join(t.TempDir(), "index.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer x.close()
	for pragma, want := range map[string]string{"journal_mode": "wal", "synchronous": "0", "busy_timeout": "5000"} {
		var got string
		if err := x.db.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil {
			t.Fatal(pragma, err)
		}
		if got != want {
			t.Errorf("PRAGMA %s = %q, want %q", pragma, got, want)
		}
	}
}
