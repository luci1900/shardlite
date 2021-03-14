package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/lucian1900/shardlite"
)

func migrate(db *sql.DB) error {
	log.Printf("Running migration")

	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS counters (
    	id INTEGER PRIMARY KEY AUTOINCREMENT,
    	count INTEGER
	)`)
	return err
}

type FileStorage struct {
	dirPath string
}

func (f FileStorage) path(kind string, id string) string {
	return path.Join(f.dirPath, kind, fmt.Sprintf("%s.db", id))
}

func (f FileStorage) Upload(kind string, id string, in io.ReadSeeker) error {
	if err := os.MkdirAll(path.Join(f.dirPath, kind), os.ModePerm); err != nil {
		return err
	}
	out, err := os.Create(f.path(kind, id))
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return nil
}

func (f FileStorage) Download(kind string, id string) (io.ReadCloser, error) {
	out, err := os.Open(f.path(kind, id))
	if err != nil {
		return nil, err
	}
	return out, nil
}

type FileLock struct {
	path string
	url  string
}

func (l FileLock) TryLock() error {
	log.Printf("Locking %v", l.path)

	data, err := os.ReadFile(l.path)
	if err == nil {
		return &shardlite.ErrAlreadyActive{Url: string(data)}
	}
	if !os.IsNotExist(err) {
		return err
	}
	if err := os.WriteFile(l.path, []byte(l.url), os.ModePerm); err != nil {
		return err
	}
	return nil
}

func (l FileLock) TryUnlock() error {
	log.Printf("Unlocking %v", l.path)

	if err := os.Remove(l.path); err != nil {
		return err
	}
	return nil
}

type FileLeaser struct {
	pathPrefix string
}

func (l FileLeaser) MakeLock(kind string, id string, url string) shardlite.Locker {
	dirPath := path.Join(l.pathPrefix, kind)
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		panic(err)
	}
	return FileLock{
		path: path.Join(dirPath, kind, fmt.Sprintf("%s.lock", id)),
		url:  url,
	}
}

type API struct {
	users *shardlite.Pile
}

func (a *API) handler(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User")

	shard := a.users.Shard(userID)
	db, err := shard.Activate()
	if err != nil {
		switch e := err.(type) {
		case *shardlite.ErrAlreadyActive:
			redirectUrl, err := url.Parse(e.Url)
			if err != nil {
				panic(err)
			}
			redirectUrl.Path = r.URL.Path
			http.Redirect(w, r, redirectUrl.String(), http.StatusTemporaryRedirect)
			return
		default:
			panic(err)
		}
	}

	tx, err := db.Begin()
	defer tx.Rollback()
	if err != nil {
		panic(err)
	}
	tx.Exec("INSERT INTO counters (count) VALUES (1)")

	total := 0
	row := tx.QueryRow("SELECT SUM(count) FROM counters")
	row.Scan(&total)

	tx.Commit()

	w.WriteHeader(200)
	fmt.Fprintf(w, "%v\n", total)
}

func main() {
	var port string
	if len(os.Args) == 1 {
		port = "8080"
	} else {
		port = os.Args[1]
	}

	shardlite.Debug = true
	config := &shardlite.Config{
		Name:          "users",
		Url:           fmt.Sprintf("http://localhost:%s", port),
		DbPath:        path.Join(os.TempDir(), "simple"),
		SaveInterval:  time.Duration(5 * time.Second),
		ActivationTtl: time.Duration(10 * time.Second),
		MigrateCb:     migrate,
		Storage:       FileStorage{"dbs"},
		Leaser:        FileLeaser{"locks"},
	}
	api := &API{shardlite.NewPile(config)}
	api.users.Start()

	http.HandleFunc("/", api.handler)

	if err := http.ListenAndServe(fmt.Sprintf(":%s", port), nil); err != nil {
		panic(err)
	}
}
