package main

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// modernc ignores connection-string settings it doesn't know, so check the
// ones the test app asks for took effect.
func TestOpenPragmasTakeEffect(t *testing.T) {
	a := &app{paths: map[string]string{}, busy: map[string]string{}, dbs: map[string]*sql.DB{}}
	if err := a.open("a", filepath.Join(t.TempDir(), "app.db"), "1234"); err != nil {
		t.Fatal(err)
	}
	defer a.dbs["a"].Close()
	for pragma, want := range map[string]string{"journal_mode": "wal", "synchronous": "1", "busy_timeout": "1234"} {
		var got string
		if err := a.dbs["a"].QueryRow("PRAGMA " + pragma).Scan(&got); err != nil {
			t.Fatal(pragma, err)
		}
		if got != want {
			t.Errorf("PRAGMA %s = %q, want %q", pragma, got, want)
		}
	}
}
