//go:build model

package specs_test

import "testing"

// The checks of shard.qnt (see check_test.go for what a row says).
func TestShard(t *testing.T) {
	check(t, []row{
		r(pass, "typecheck", "shard.qnt"),
		r(pass, "run", "shard.qnt", "--invariant=safe", "--max-steps=40", "--max-samples=20000"),
	})
}
