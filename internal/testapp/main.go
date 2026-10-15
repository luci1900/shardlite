// Command testapp is a stand-in for an application: it opens SQLite shard
// files with the modernc driver (the one Litestream uses) in WAL mode with
// default locking and writes to them on command, from its own process.
//
// Protocol: one command per line on stdin, fields separated by tabs. Each
// command gets one reply line on stdout: "ok [value]" or "err message".
//
//	open   <id> <path> [busy_ms]  open (creating) a database; busy timeout 5000 ms unless given
//	dump   <id> <path>            every row of every table plus the schema, as one quoted string (read from this process: tests that capture the file in-process must not open it themselves, since closing a descriptor drops all of the process's fcntl locks on it)
//	query  <id> <path> <sql>      run a query on a database opened for the occasion, one quoted string with a row per line
//	openro <id> <path>            open a database in WAL mode and only read it (nothing is created)
//	exec   <id> <sql>             run one statement in its own transaction
//	insert <id> <n> <bytes>       insert n rows of random payload in one transaction; replies with the row count
//	ckpt   <id> <mode>            PRAGMA wal_checkpoint(mode); replies with busy,journal,checkpointed
//	reopen <id>                   close the connection and open it again
//	close  <id>                   close the connection
//	loop   <id> <n> <mode>        n times: insert a row, then PRAGMA wal_checkpoint(mode); replies with rows,busy_results
//	churn  <id> <n>               n times: insert a row, then close and reopen the connection
//	ack    <id> <n>               insert (n) into the acks table, one transaction
//	probe  <id> <path>            ask, with a throwaway descriptor on <path>-shm, whether another process holds the database open (reply "held" or "free")
//	exit
package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

type app struct {
	paths map[string]string
	busy  map[string]string
	dbs   map[string]*sql.DB
}

func main() {
	a := &app{paths: map[string]string{}, busy: map[string]string{}, dbs: map[string]*sql.DB{}}
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	out := bufio.NewWriter(os.Stdout)
	for sc.Scan() {
		f := strings.Split(sc.Text(), "\t")
		if f[0] == "exit" {
			fmt.Fprintln(out, "ok")
			out.Flush()
			return
		}
		v, err := a.do(f)
		if err != nil {
			fmt.Fprintf(out, "err %s\n", strings.ReplaceAll(err.Error(), "\n", " "))
		} else {
			fmt.Fprintf(out, "ok %s\n", v)
		}
		out.Flush()
	}
}

func (a *app) open(id, path, busy string) error {
	if busy == "" {
		busy = "5000"
	}
	a.busy[id] = busy
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout("+busy+")&_pragma=synchronous(NORMAL)")
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY, payload BLOB); CREATE TABLE IF NOT EXISTS acks (n INTEGER PRIMARY KEY)`); err != nil {
		db.Close()
		return err
	}
	a.paths[id], a.dbs[id] = path, db
	return nil
}

func (a *app) do(f []string) (string, error) {
	if len(f) < 2 {
		return "", fmt.Errorf("bad command %q", f)
	}
	cmd, id := f[0], f[1]
	if cmd == "probe" {
		// The app has no sqlite handle on this file, or its own locks are ignored anyway.
		held, err := capture.ProbeOnce(f[2])
		if held {
			return "held", err
		}
		return "free", err
	}
	if cmd == "dump" {
		d, err := sqlitetest.TryDump(f[2])
		return strconv.Quote(d), err
	}
	if cmd == "query" {
		out, err := queryRows(f[2], f[3])
		return strconv.Quote(out), err
	}
	if cmd == "openro" {
		db, err := sql.Open("sqlite", "file:"+f[2]+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
		if err != nil {
			return "", err
		}
		db.SetMaxOpenConns(1)
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM sqlite_master`).Scan(&n); err != nil {
			db.Close()
			return "", err
		}
		a.paths[id], a.dbs[id] = f[2], db
		return strconv.Itoa(n), nil
	}
	if cmd == "open" {
		busy := ""
		if len(f) > 3 {
			busy = f[3]
		}
		return "", a.open(id, f[2], busy)
	}
	if cmd == "reopen" {
		path := a.paths[id]
		if db := a.dbs[id]; db != nil {
			if err := db.Close(); err != nil {
				return "", err
			}
			delete(a.dbs, id)
		}
		return "", a.open(id, path, a.busy[id])
	}
	db := a.dbs[id]
	if db == nil {
		return "", fmt.Errorf("%s not open", id)
	}
	switch cmd {
	case "exec":
		_, err := db.Exec(f[2])
		return "", err
	case "insert":
		n, _ := strconv.Atoi(f[2])
		size, _ := strconv.Atoi(f[3])
		tx, err := db.Begin()
		if err != nil {
			return "", err
		}
		buf := make([]byte, size)
		for range n {
			for i := range buf {
				buf[i] = byte(rand.Uint32())
			}
			if _, err := tx.Exec(`INSERT INTO kv (payload) VALUES (?)`, buf); err != nil {
				tx.Rollback()
				return "", err
			}
		}
		if err := tx.Commit(); err != nil {
			return "", err
		}
		var c int
		err = db.QueryRow(`SELECT count(*) FROM kv`).Scan(&c)
		return strconv.Itoa(c), err
	case "ckpt":
		var busy, log, ck int
		err := db.QueryRow(`PRAGMA wal_checkpoint(`+f[2]+`)`).Scan(&busy, &log, &ck)
		return fmt.Sprintf("%d,%d,%d", busy, log, ck), err
	case "loop":
		n, _ := strconv.Atoi(f[2])
		busyN := 0
		for range n {
			if _, err := db.Exec(`INSERT INTO kv (payload) VALUES (randomblob(200))`); err != nil {
				return "", err
			}
			var busy, log, ck int
			if err := db.QueryRow(`PRAGMA wal_checkpoint(`+f[3]+`)`).Scan(&busy, &log, &ck); err != nil {
				return "", err
			}
			if busy != 0 {
				busyN++
			}
		}
		var c int
		err := db.QueryRow(`SELECT count(*) FROM kv`).Scan(&c)
		return fmt.Sprintf("%d,%d", c, busyN), err
	case "churn":
		n, _ := strconv.Atoi(f[2])
		for range n {
			if _, err := db.Exec(`INSERT INTO kv (payload) VALUES (randomblob(200))`); err != nil {
				return "", err
			}
			if _, err := a.do([]string{"reopen", id}); err != nil {
				return "", err
			}
			db = a.dbs[id]
		}
		var c int
		err := db.QueryRow(`SELECT count(*) FROM kv`).Scan(&c)
		return strconv.Itoa(c), err
	case "ack":
		n, _ := strconv.Atoi(f[2])
		_, err := db.Exec(`INSERT INTO acks (n) VALUES (?)`, n)
		return "", err
	case "close":
		err := db.Close()
		delete(a.dbs, id)
		return "", err
	}
	return "", fmt.Errorf("unknown command %q", cmd)
}

// queryRows runs one query on a connection of its own and formats the rows.
func queryRows(path, query string) (string, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return "", err
	}
	defer db.Close()
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	var out []string
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}
		out = append(out, fmt.Sprint(vals...))
	}
	return strings.Join(out, "\n"), rows.Err()
}
