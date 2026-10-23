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
// tiers can follow. The zero value is the record, which is the safe one: a
// kind that names no tier waits for the record (docs/internals.md, Failing
// loudly, the zero-value policy).
type Sync int

const (
	// SyncStorage waits for the record: blob storage, where the fence is
	// created and where the journal's races are decided.
	SyncStorage Sync = iota
	// SyncLocal waits for the node's own disk, which holds a copy of every
	// segment. The answer survives a crash of the node, but not the loss of
	// the host. The tier decides nothing: the record is still the truth, and
	// the gate falls back to it when the local store is slow or gone.
	SyncLocal
)

// String names a tier, for the logs and the errors.
func (s Sync) String() string {
	switch s {
	case SyncStorage:
		return "storage"
	case SyncLocal:
		return "local"
	}
	return "unknown"
}

// Valid reports whether s is one of the tiers. The zero is a tier, the record.
func (s Sync) Valid() bool {
	switch s {
	case SyncStorage, SyncLocal:
		return true
	}
	return false
}

// ParseSync parses the name of a durability tier: "storage" (the record, blob
// storage) or "local" (the node's own disk). An empty name is the record.
// "peer" (a mirror on another node's disk) comes with stage 3.3.
func ParseSync(name string) (Sync, error) {
	switch strings.ToLower(name) {
	case "", "storage":
		return SyncStorage, nil
	case "local":
		return SyncLocal, nil
	}
	return 0, fmt.Errorf("sync is %q, want %q or %q", name, "local", "storage")
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
