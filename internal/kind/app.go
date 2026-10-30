package kind

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/luci1900/shardlite/internal/shard"
)

// App is an app's declaration of itself: its name, its version and the kinds
// it serves (docs/guide.md, The ready route). The node parses one from the
// app's ready route, and the node's own app is constructed as one in code
// (docs/internals.md, System kinds), so both kinds tables are the same
// structure. The kinds are keyed by their qualified names (game.users), as
// ParseApp returns them and the system packages construct them.
type App struct {
	Name    shard.App
	Version string // the app's own, opaque; empty means it declared none
	Kinds   map[shard.Kind]Kind
}

type appJSON struct {
	App     string              `json:"app"`
	Version string              `json:"version"`
	Kinds   map[string]kindJSON `json:"kinds"`
}

type kindJSON struct {
	Sync        string `json:"sync"` // "none", "peer" or "storage" (the default): a string, like Shardlite-Sync, so more tiers can follow
	IdleTimeout string `json:"idle_timeout"`
	Public      bool   `json:"public"`
}

// ParseApp parses a declaration as the ready route answers it. defaultIdle is
// used for kinds without an idle_timeout of their own: a kind's own setting
// wins. The kinds are qualified by the app's name, which is how a shard's name
// says who serves it.
func ParseApp(data []byte, defaultIdle time.Duration) (App, error) {
	var f appJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return App{}, fmt.Errorf("declaration: %w", err)
	}
	if !shard.ValidApp(f.App) {
		return App{}, fmt.Errorf("declaration: %q is not a valid app name (lowercase letters, digits, '_' and '.', up to %d characters)", f.App, shard.MaxAppLen)
	}
	if shard.Reserved(f.App) {
		return App{}, fmt.Errorf("declaration: the app's name %q starts with \"_\", which is the node's own", f.App)
	}
	if f.Kinds == nil {
		return App{}, errors.New(`declaration: no "kinds" object`)
	}
	kinds := make(map[shard.Kind]Kind, len(f.Kinds))
	for name, k := range f.Kinds {
		if !shard.ValidKind(name) {
			return App{}, fmt.Errorf("declaration: %q is not a valid kind name (lowercase letters, digits, - and _, starting with a letter)", name)
		}
		idle := defaultIdle
		if k.IdleTimeout != "" {
			d, err := time.ParseDuration(k.IdleTimeout)
			if err != nil || d <= 0 {
				return App{}, fmt.Errorf("declaration: %s: bad idle_timeout %q", name, k.IdleTimeout)
			}
			idle = d
		}
		var sync Sync
		sync, err := ParseSync(k.Sync)
		if err != nil {
			return App{}, fmt.Errorf("declaration: %s: %w", name, err)
		}
		kinds[shard.Qualify(shard.App(f.App), name)] = Kind{Name: shard.Qualify(shard.App(f.App), name), Sync: sync, IdleTimeout: idle, Public: k.Public}
	}
	return App{Name: shard.App(f.App), Version: f.Version, Kinds: kinds}, nil
}
