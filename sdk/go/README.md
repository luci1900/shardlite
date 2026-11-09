# shardlite Go SDK

The Go SDK for apps served by shardlite: a module of its own, `github.com/luci1900/shardlite/sdk/go`, with package `shardlite` and `shardlitetest`, which starts a real node for an app's tests, both on the standard library alone.

The guide is the package documentation (`go doc -all github.com/luci1900/shardlite/sdk/go`, or `doc.go`). It shows a handler, a call, a reminder and a test. The contract it spells in Go is [docs/guide.md](../../docs/guide.md).

The tests use modernc.org/sqlite. Nothing that imports the SDK builds it.
