// Package kind is what a shard kind is (docs/guide.md, Shard kinds): its
// settings. An app's declaration (app.go) parses into it. The zero value of each
// setting is the safe one. The names of kinds and shards are in
// internal/shard.
package kind

import (
	"fmt"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/shard"
)

// Sync is a kind's durability tier (docs/guide.md, Durability): the place
// where the answer waits for the commit before it to be durable. It is a mode
// rather than a boolean, as the declaration's "sync" is a string, so more
// tiers can follow. The zero value is storage, which is the declaration's
// default too: a kind that names no tier, and anything built without naming
// one, waits for blob storage (docs/internals.md, Failing loudly, the
// zero-value policy).
type Sync int

const (
	// SyncStorage waits for blob storage, where the fence is
	// created and where the journal's races are decided.
	SyncStorage Sync = iota
	// SyncNone waits for nothing: the answer leaves once the node's lease is
	// good. Every segment still goes to blob storage, so what the tier gives up
	// is only the wait. A crash of the node can lose the commits not yet in
	// storage, up to a flush interval and a PUT.
	SyncNone
)

// String names a tier, for the logs and the errors.
func (s Sync) String() string {
	switch s {
	case SyncStorage:
		return "storage"
	case SyncNone:
		return "none"
	}
	return "unknown"
}

// Valid reports whether s is one of the tiers. The zero is a tier, storage.
func (s Sync) Valid() bool {
	switch s {
	case SyncStorage, SyncNone:
		return true
	}
	return false
}

// ParseSync parses the name of a durability tier: "none" (no wait) or "storage" (blob storage). An empty name is the declaration's default, storage.
func ParseSync(name string) (Sync, error) {
	switch strings.ToLower(name) {
	case "", "storage":
		return SyncStorage, nil
	case "none":
		return SyncNone, nil
	}
	return 0, fmt.Errorf("sync is %q, want %q or %q", name, "none", "storage")
}

// Kind is a category of shards with shared settings.
type Kind struct {
	Name        shard.Kind
	Sync        Sync
	IdleTimeout time.Duration // how long an unused shard stays active, the node's default if zero
	Public      bool          // clients may name it; otherwise only calls reach it
}
