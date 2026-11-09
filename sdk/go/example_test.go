package shardlite_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

func ExampleNode_Ready() {
	node, err := shardlite.New(shardlite.Config{
		Open:    openSQLite,
		Version: "2026.10.0",
		Kinds: []shardlite.Kind{
			{Name: "users", Public: true, IdleTimeout: 10 * time.Minute},
			{Name: "chat", Public: true, Sync: shardlite.SyncNone},
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	mux := http.NewServeMux()
	mux.Handle("GET "+shardlite.ReadyPath, node.Ready())

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, shardlite.ReadyPath, nil))
	fmt.Println(rec.Code, rec.Header().Get("Content-Type"))
	fmt.Println(rec.Body.String())
	// Output:
	// 200 application/json
	// {"version":"2026.10.0","kinds":{"chat":{"public":true,"sync":"none"},"users":{"public":true,"idle_timeout":"10m0s"}}}
}

func ExampleNew() {
	_, err := shardlite.New(shardlite.Config{
		Open:  openSQLite,
		Kinds: []shardlite.Kind{{Name: "users"}, {Name: "Guild"}},
	})
	fmt.Println(err != nil && strings.Contains(err.Error(), `"Guild"`))
	// Output: true
}

func ExampleRequest_Call() {
	// A control API that shows what a call sends.
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("%s %s %s %q %q\n", r.Method, r.URL.Path, r.Header.Get(shardlite.HeaderTarget), r.Header.Get(headerIfExists), r.Header.Get(shardlite.HeaderContext))
	}))
	defer api.Close()
	node, err := shardlite.New(shardlite.Config{Open: openSQLite, Addr: strings.TrimPrefix(api.URL, "http://")})
	if err != nil {
		fmt.Println(err)
		return
	}
	guild := shardlite.Kind{Name: "guild"}

	// While serving a request, the call carries the request's context.
	serving := httptest.NewRequest(http.MethodPost, "/users/42/invite", nil)
	serving.Header.Set(shardlite.HeaderContext, `shard="users/42", hops=0`)
	res, _ := node.Request(serving).Call(guild.Shard("7"), http.MethodPost, "/join", nil, shardlite.IfExists())
	res.Body.Close()

	// A call from the app, with no request behind it, has none.
	res, _ = node.CallAsApp(serving.Context(), guild.Shard("7"), http.MethodGet, "/size", nil)
	res.Body.Close()
	// Output:
	// POST /join guild/7 "true" "shard=\"users/42\", hops=0"
	// GET /size guild/7 "" ""
}

func ExampleNode_Middleware() {
	dir, _ := os.MkdirTemp("", "shardlite-example")
	defer os.RemoveAll(dir)
	file := filepath.Join(dir, "users", "42.k1.db")
	os.Mkdir(filepath.Dir(file), 0o755)
	// The node creates the file before it hands the path out. Here the example
	// does.
	db, _ := openSQLite("file:" + file + "?mode=rwc")
	db.Exec(`PRAGMA journal_mode=WAL; CREATE TABLE greetings (text TEXT)`)
	db.Exec(`INSERT INTO greetings VALUES ('hello from users/42')`)
	db.Close()

	users := shardlite.Kind{Name: "users", Public: true}
	node, err := shardlite.New(shardlite.Config{Open: openSQLite, Kinds: []shardlite.Kind{users}})
	if err != nil {
		fmt.Println(err)
		return
	}
	app := node.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		opened, err := node.Expect(r, users.Shard("42")) // refuses another shard
		if err != nil {
			var ref *shardlite.Refusal
			errors.As(err, &ref)
			http.Error(w, ref.Message, ref.Status)
			return
		}
		db, err := opened.DB() // opened now, closed when this handler returns
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		var text string
		if err := db.QueryRowContext(r.Context(), `SELECT text FROM greetings`).Scan(&text); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprintln(w, text)
	}))

	// What the node sends the app for a request it opened users/42 for.
	req := httptest.NewRequest(http.MethodGet, "/users/42/greeting", nil)
	req.Header.Set(headerOpened, "users/42")
	req.Header.Set(headerFile, file)
	rec := httptest.NewRecorder()
	app.ServeHTTP(rec, req)
	fmt.Print(rec.Body.String())
	// Output: hello from users/42
}
