package kind

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/luci1900/shardlite/internal/model/shard"
)

// App is an app's declaration of itself: its version and the kinds it serves
// (docs/guide.md, Shard kinds). The node parses one from the app's ready route.
// The kinds are keyed by their own names (users, matchmaker.queue), as ParseApp
// returns them.
type App struct {
	Version string // the app's own, opaque; empty means it declared none
	Kinds   map[shard.Kind]Kind
}

type appJSON struct {
	Version string          `json:"version"`
	Kinds   json.RawMessage `json:"kinds"` // read token by token, so a kind named twice is seen
}

type kindJSON struct {
	Sync        string `json:"sync"` // "none" or "storage" (the default): a string, like Shardlite-Sync, so more tiers can follow
	IdleTimeout string `json:"idle_timeout"`
	Public      bool   `json:"public"`
}

// ParseApp parses a declaration as the ready route answers it. defaultIdle is
// used for kinds without an idle_timeout of their own: a kind's own setting
// wins. It refuses an unknown field (a declaration that still names an "app"
// among them), a kind named twice (two components claiming one name), a kind
// whose name is invalid and a kind starting with "_", which is the node's own.
func ParseApp(data []byte, defaultIdle time.Duration) (App, error) {
	var f appJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return App{}, fmt.Errorf("declaration: %w", err)
	}
	if len(f.Kinds) == 0 || string(f.Kinds) == "null" {
		return App{}, errors.New(`declaration: no "kinds" object`)
	}
	kd := json.NewDecoder(bytes.NewReader(f.Kinds))
	kd.DisallowUnknownFields() // for a kind's settings
	if tok, err := kd.Token(); err != nil || tok != json.Delim('{') {
		return App{}, errors.New(`declaration: "kinds" is not an object`)
	}
	kinds := map[shard.Kind]Kind{}
	for kd.More() {
		tok, err := kd.Token()
		if err != nil {
			return App{}, fmt.Errorf("declaration: kinds: %w", err)
		}
		name := tok.(string) // the key of an object
		if !shard.ValidKind(name) {
			return App{}, fmt.Errorf("declaration: %q is not a valid kind name (lowercase letters, digits, - and _, starting with a letter, in segments joined by dots, up to %d characters)", name, shard.MaxKindLen)
		}
		kn := shard.Kind(name)
		if kn.Reserved() {
			return App{}, fmt.Errorf("declaration: the kind %q starts with \"_\", which is the node's own", name)
		}
		if _, ok := kinds[kn]; ok {
			return App{}, fmt.Errorf("declaration: the kind %q is declared twice", name)
		}
		var k kindJSON
		if err := kd.Decode(&k); err != nil {
			return App{}, fmt.Errorf("declaration: %s: %w", name, err)
		}
		idle := defaultIdle
		if k.IdleTimeout != "" {
			d, err := time.ParseDuration(k.IdleTimeout)
			if err != nil || d <= 0 {
				return App{}, fmt.Errorf("declaration: %s: bad idle_timeout %q", name, k.IdleTimeout)
			}
			idle = d
		}
		sync, err := ParseSync(k.Sync)
		if err != nil {
			return App{}, fmt.Errorf("declaration: %s: %w", name, err)
		}
		kinds[kn] = Kind{Name: kn, Sync: sync, IdleTimeout: idle, Public: k.Public}
	}
	if _, err := kd.Token(); err != nil { // the closing brace
		return App{}, fmt.Errorf("declaration: kinds: %w", err)
	}
	return App{Version: f.Version, Kinds: kinds}, nil
}
