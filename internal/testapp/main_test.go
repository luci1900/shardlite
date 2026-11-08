package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// modernc ignores connection-string settings it doesn't know, so check the
// ones the test app asks for took effect.
func TestOpenPragmasTakeEffect(t *testing.T) {
	a := &app{paths: map[string]string{}, busy: map[string]string{}, dbs: map[string]*sql.DB{}}
	path := filepath.Join(t.TempDir(), "app.db")
	if err := a.open("a", path, "1234"); err == nil {
		t.Fatal("the app made the file it was handed to open")
	}
	if err := os.WriteFile(path, nil, 0o644); err != nil { // the node creates the file first
		t.Fatal(err)
	}
	if err := a.open("a", path, "1234"); err != nil {
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
