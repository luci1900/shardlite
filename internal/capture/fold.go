package capture

import (
	"database/sql"
	"errors"
	"fmt"
)

// foldWAL leaves the database file complete on its own and no WAL sidecars
// beside it. A database that never had either needs nothing.
//
// A pair that a cached or resumed shard left behind may hold commits that are
// not in the database file, so a connection of the node's checkpoints and
// removes it. Nobody else may have the shard open (the caller has just
// checked), and the node has nothing open on it that closing a descriptor
// could drop locks of.
func foldWAL(dbPath string) error {
	sc := WALSidecarsOf(dbPath)
	if !sc.Any() {
		return nil
	}
	sdb, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	sdb.SetMaxOpenConns(1)
	var busy, logFrames, done int
	err = sdb.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &done)
	if cerr := sdb.Close(); err == nil { // the last connection closing removes the pair too
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("capture: checkpoint before capture: %w", err)
	} else if busy != 0 {
		return errors.New("capture: checkpoint before capture was blocked: another process has the shard open")
	}
	return sc.Remove()
}
