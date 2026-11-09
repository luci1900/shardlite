package shardlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// DefaultAddr is where the node's control API listens unless told otherwise
// (SHARDLITE_LISTEN_CONTROL).
const DefaultAddr = "127.0.0.1:2282"

// Sync is the durability tier of a kind (the guide's Durability): where an
// acknowledgement waits. The zero value is the node's default, SyncStorage.
type Sync string

const (
	// SyncNone acknowledges without waiting for blob storage.
	SyncNone Sync = "none"
	// SyncStorage acknowledges once the shard's commits are in blob storage.
	SyncStorage Sync = "storage"
)

// Kind is one kind of shard the app serves, with the settings of its
// declaration (the guide's Shard kinds). List it in Config.Kinds. The zero
// value of each setting is the node's default, and false is the safe one.
//
//	var Users = shardlite.Kind{Name: "users", Public: true}
type Kind struct {
	// Name is the whole name of the kind, with no app in front (users, or
	// matchmaker.queue for a library's): segments joined by dots, each a
	// lowercase letter then lowercase letters, digits, - or _, up to 64
	// characters.
	Name string
	// Public lets clients name the kind in Shardlite-Target. It is false for a
	// kind only the app's own calls reach.
	Public bool
	// Sync is the kind's durability tier. The zero value is the node's default.
	Sync Sync
	// IdleTimeout is how long a shard of this kind stays active without a
	// request. Zero is the node's default.
	IdleTimeout time.Duration
}

// Shard names the shard of this kind with id. The id is not checked here: a
// call to a name with a bad id fails before it is sent, and a request never
// matches one.
func (k Kind) Shard(id string) ShardName { return ShardName{kind: k.Name, id: id} }

// Config is what an app tells the SDK about itself, once. Build a Node from it
// with New.
type Config struct {
	// Open opens a shard's database from a URI. It is required.
	Open Opener
	// Addr is host:port of the node's control API, DefaultAddr if empty.
	Addr string
	// HTTP makes the app's calls and its sync, http.DefaultClient if nil. That
	// client has no timeout, so set one on this client or on the contexts of
	// the requests.
	HTTP *http.Client
	// Version is the app's own version, opaque to the node, which shows it in
	// its logs.
	Version string
	// Kinds are the kinds the app serves. Node.Ready declares them to the node.
	Kinds []Kind
	// OnOpen, if set, runs once on a request's database after it opens and the
	// pragmas run, before the handler sees it. It is the place to migrate: the
	// busy timeout is set, so it waits for the node's own bookkeeping commits
	// and does not fail on them. If it returns an error, DB returns that error
	// for the rest of the request.
	OnOpen func(ctx context.Context, shard ShardName, db *sql.DB) error
	// Health, if set, is asked by Node.Ready on every poll. An error makes the
	// app answer not ready (503) with the error as its message, so the node
	// sends it no traffic during a migration or with a dependency down.
	Health func(ctx context.Context) error
}

// Node is the app's handle on its node, built by New. It serves the
// declaration, gives each request a database and a scope to call from, and
// makes the calls an app makes without a request. It is safe for concurrent
// use.
type Node struct {
	open        Opener
	onOpen      func(ctx context.Context, shard ShardName, db *sql.DB) error
	health      func(ctx context.Context) error
	addr        string
	http        *http.Client
	declaration []byte
}

// New checks cfg and builds the Node. It fails on an invalid kind name (or one
// with the node's leading underscore), a kind named twice, a Sync that is
// neither SyncNone nor SyncStorage, a negative IdleTimeout and a nil Open, so a
// declaration the node would refuse never leaves the app.
func New(cfg Config) (*Node, error) {
	if cfg.Open == nil {
		return nil, errors.New("shardlite: Config.Open is required")
	}
	decl := declaration{Version: cfg.Version, Kinds: map[string]declaredKind{}}
	for _, k := range cfg.Kinds {
		switch {
		case strings.HasPrefix(k.Name, "_"):
			return nil, fmt.Errorf("shardlite: the kind %q starts with \"_\", which is the node's own", k.Name)
		case !validKind(k.Name):
			return nil, fmt.Errorf("shardlite: %q is not a valid kind name (lowercase letters, digits, - and _, starting with a letter, in segments joined by dots, up to %d characters)", k.Name, maxKindLen)
		}
		if _, ok := decl.Kinds[k.Name]; ok {
			return nil, fmt.Errorf("shardlite: the kind %q is declared twice", k.Name)
		}
		switch k.Sync {
		case "", SyncNone, SyncStorage:
		default:
			return nil, fmt.Errorf("shardlite: the kind %q has the unknown Sync %q", k.Name, k.Sync)
		}
		if k.IdleTimeout < 0 {
			return nil, fmt.Errorf("shardlite: the kind %q has a negative IdleTimeout", k.Name)
		}
		dk := declaredKind{Public: k.Public, Sync: k.Sync}
		if k.IdleTimeout > 0 {
			dk.IdleTimeout = k.IdleTimeout.String()
		}
		decl.Kinds[k.Name] = dk
	}
	body, err := json.Marshal(decl)
	if err != nil {
		return nil, err
	}
	n := &Node{open: cfg.Open, onOpen: cfg.OnOpen, health: cfg.Health, addr: cfg.Addr, http: cfg.HTTP, declaration: body}
	if n.addr == "" {
		n.addr = DefaultAddr
	}
	if n.http == nil {
		n.http = http.DefaultClient
	}
	return n, nil
}

// declaration and declaredKind are the JSON of the guide's Shard kinds. A zero
// setting is left out, so the node applies its default.
type declaration struct {
	Version string                  `json:"version,omitempty"`
	Kinds   map[string]declaredKind `json:"kinds"`
}

type declaredKind struct {
	Public      bool   `json:"public,omitempty"`
	Sync        Sync   `json:"sync,omitempty"`
	IdleTimeout string `json:"idle_timeout,omitempty"`
}

// Ready serves the app's declaration at ReadyPath, the route the node polls for
// the app's health and kinds. It answers 503 with the error of Config.Health
// while that fails:
//
//	mux.Handle("GET "+shardlite.ReadyPath, node.Ready())
func (n *Node) Ready() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.health != nil {
			if err := n.health(r.Context()); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(n.declaration)
	})
}
