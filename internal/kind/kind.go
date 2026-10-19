// Package kind is what a shard kind is (docs/guide.md, Shard kinds): its
// settings, and for a system kind (docs/internals.md, System kinds) its fixed
// ids, migrations and system app. The kinds file parses into it and each system
// kind's package constructs it, so both are declared with the same settings and
// the same defaults. The zero value of each setting is the safe one.
package kind

import (
	"io/fs"
	"net/http"
	"time"

	"github.com/luci1900/shardlite/internal/system"
)

// Sync is a kind's durability mode (docs/guide.md, Durability). It is a mode
// rather than a boolean, as the kinds file's "sync" is a string, so more modes
// can follow. The zero value is on.
type Sync int

const (
	SyncOn  Sync = iota // gates hold output until the writes before it are in blob storage
	SyncOff             // output leaves at once, and a host loss can lose up to a flush interval
)

// Kind is a category of shards with shared settings.
type Kind struct {
	Name        string
	Sync        Sync
	IdleTimeout time.Duration // how long an unused shard stays active, the node's default if zero
	Public      bool          // clients may name it; otherwise only calls reach it

	// IDs are the fixed ids, each starting with "_". The node keeps every one
	// claimed and active somewhere in the cluster, so it never goes idle.
	IDs []string
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
