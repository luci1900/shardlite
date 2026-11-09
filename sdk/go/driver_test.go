package shardlite_test

import (
	"database/sql"
	"testing"

	// The tests are the one place the SDK names a driver. The packages import
	// none.
	_ "modernc.org/sqlite"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

// openSQLite is the opener an app on modernc would write: the SDK's URI plus
// the driver's option for immediate transactions.
func openSQLite(uri string) (*sql.DB, error) {
	return sql.Open("sqlite", uri+"&_txlock=immediate")
}

// The names of the contract the SDK keeps private, which the tests read and
// set.
const (
	headerOpened   = "Shardlite-Opened"
	headerFile     = "Shardlite-File"
	headerCaller   = "Shardlite-Caller"
	headerIfActive = "Shardlite-If-Active"
	headerIfExists = "Shardlite-If-Exists"
	headerSync     = "Shardlite-Sync"
	syncPath       = "/_shardlite/sync"
)

// The kinds of the tests' apps.
var (
	users = shardlite.Kind{Name: "users", Public: true}
	guild = shardlite.Kind{Name: "guild"}
	lobby = shardlite.Kind{Name: "lobby"}
)

// testConfig is the Config of the tests' apps.
func testConfig() shardlite.Config {
	return shardlite.Config{Open: openSQLite, Kinds: []shardlite.Kind{users, guild, lobby}}
}

// newNode builds the app's Node from cfg, for a test that calls through no
// fake node.
func newNode(t *testing.T, cfg shardlite.Config) *shardlite.Node {
	t.Helper()
	n, err := shardlite.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// who describes a caller as text: "client", "app" or the calling shard's name.
func who(c shardlite.Caller) string {
	switch c := c.(type) {
	case *shardlite.CallerClient:
		return "client"
	case *shardlite.CallerApp:
		return "app"
	case *shardlite.CallerShard:
		return c.Name.String()
	}
	return "nobody"
}
