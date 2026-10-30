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
// tiers can follow. The zero value is the record, which is the declaration's
// default too: a kind that names no tier, and anything built without naming
// one, waits for the record (docs/internals.md, Failing loudly, the
// zero-value policy).
type Sync int

const (
	// SyncStorage waits for the record: blob storage, where the fence is
	// created and where the journal's races are decided.
	SyncStorage Sync = iota
	// SyncNone waits for nothing: the answer leaves once the node's lease is
	// good. Every segment still goes to the node's own disk and the record, so
	// what the tier gives up is only the wait. A crash of the node can lose
	// the commits not yet in a segment, up to a flush interval and a PUT,
	// though what reached the node's own database before a process crash is
	// kept.
	SyncNone
	// SyncPeer waits for the node's own disk and the journal's peer, whose
	// disk holds a mirror of every segment: the answer is on two machines
	// when it is given, and in the record a flush and a PUT later. A peer
	// that is not usable (none named, or one that has fallen out) fails the
	// answer: the tier is opt-in, and an unmet one is an error, never a
	// weaker durability.
	SyncPeer
)

// String names a tier, for the logs and the errors.
func (s Sync) String() string {
	switch s {
	case SyncStorage:
		return "storage"
	case SyncNone:
		return "none"
	case SyncPeer:
		return "peer"
	}
	return "unknown"
}

// Valid reports whether s is one of the tiers. The zero is a tier, the record.
func (s Sync) Valid() bool {
	switch s {
	case SyncStorage, SyncNone, SyncPeer:
		return true
	}
	return false
}

// ParseSync parses the name of a durability tier: "none" (no wait), "peer"
// (the node's disk and the journal's peer) or "storage" (the record, blob
// storage). An empty name is the declaration's default, the
// record.
func ParseSync(name string) (Sync, error) {
	switch strings.ToLower(name) {
	case "", "storage":
		return SyncStorage, nil
	case "none":
		return SyncNone, nil
	case "peer":
		return SyncPeer, nil
	}
	return 0, fmt.Errorf("sync is %q, want %q, %q or %q", name, "none", "peer", "storage")
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
