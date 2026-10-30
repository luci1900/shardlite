// Package system holds what system kinds are made of (docs/internals.md, System
// kinds): shards the node keeps for itself under reserved kinds, each served by
// a system app built into the node. A system app is an http.Handler, as an app
// is an HTTP server, and it sees only its own shard, through System. Each system
// kind is a package of its own that exports a constructor for its kind.Kind,
// which takes whatever of the node's the kind uses. Those packages never import the
// node or each other.
package system

import (
	"context"
	"database/sql"
	"net/http"

	"github.com/luci1900/shardlite/internal/shard"
)

// System is what a system app gets for its shard: what an app has for its shard.
type System struct {
	Name shard.Name // the shard's kind/id
	// Tx runs fn in a transaction on the shard's database, committed if fn
	// returns nil. It holds the shard's capture lock while fn runs, so fn is
	// short and does no I/O outside the shard: a handler reads what it needs in
	// one Tx, does its I/O outside and records the outcome in another.
	Tx func(ctx context.Context, fn func(*sql.Tx) error) error
	// Call sends req to another system shard, named by its Shardlite-Target
	// header, through the node's routing, and returns the whole answer. It waits
	// first for what this shard committed before it, as an app's call does, so
	// it is never made inside Tx. A path under /_shardlite/ is one only the node
	// may send.
	Call func(ctx context.Context, req *http.Request) (*http.Response, error)
	// Sync waits until what this shard has committed so far is in the record,
	// whatever the kind's tier, and fails if the node has lost its lease, as the control API's sync does for
	// an app, but for this shard's commits only: it returns at once when the
	// shard has nothing new. A system app commits with Tx, then calls Sync before
	// anything outside the shard depends on the commit (a blob storage write, or
	// an answer someone acts on when its kind has sync off). It is never called
	// inside Tx.
	Sync func(ctx context.Context) error
}

// Target is the header that names the shard a call is for, which a system app
// sets on the requests it sends with System.Call.
const Target = "Shardlite-Target"

// Opened is the header that tells a system app which shard the node opened for
// the request, as it tells an app.
const Opened = "Shardlite-Opened"
