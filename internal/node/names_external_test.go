package node_test

import "github.com/luci1900/shardlite/internal/model/shard"

// shardName is a shard's name in the tests that need the parsed type.
func shardName(s string) shard.Name { return shard.MustParse(s) }
