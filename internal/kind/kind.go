// Package kind is what a shard kind is (docs/guide.md, Shard kinds): its
// settings, and for a system kind (docs/internals.md, System kinds) its fixed
// ids, migrations and system app. An app's declaration (app.go) parses into it
// and each system kind's package constructs it, so both are declared with the
// same settings and the same defaults. The zero value of each setting is the
// safe one. The names of apps, kinds and shards are in internal/shard, which
// the node and the system kinds share.
package kind

import (
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/system"
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

	// IDs are the fixed ids, each starting with "_". The node keeps every one
	// claimed and active somewhere in the cluster, so it never goes idle.
	IDs []shard.ID
	// Migrations hold the kind's schema as numbered files (see package migrate),
	// applied at every activation after the reserved tables.
	Migrations fs.FS
	// Handler builds the system app of one shard. It is called once per
	// activation and does nothing else: a handler starts no goroutines and keeps
	// nothing past a request. The wake is POST /_shardlite/reminders, as for an
	// app. A kind with a Handler is a system kind, served in process, and it is
	// nil for a kind the app serves.
	Handler func(*system.System) http.Handler
}

// SystemKind is the name of one of the node's own kinds: the kind's own name
// under the node's app.
func SystemKind(k string) shard.Kind { return shard.Qualify(shard.SystemApp, k) }
