package blob_test

import (
	"os"
	"testing"
)

// TestMain sets the keys and region the S3 client reads from the environment
// once, so that tests need not (t.Setenv would keep them from running side by
// side). The emulator's keys are used when the emulator is, a fake's otherwise.
func TestMain(m *testing.M) {
	keys := [][2]string{{"AWS_ACCESS_KEY_ID", "k"}, {"AWS_SECRET_ACCESS_KEY", "s"}, {"AWS_REGION", "us-east-1"}}
	if os.Getenv("SHARDLITE_TEST_S3_ENDPOINT") != "" {
		keys[0][1], keys[1][1] = "shardlite", "shardlite-secret"
	}
	for _, kv := range keys {
		if os.Getenv(kv[0]) == "" {
			os.Setenv(kv[0], kv[1])
		}
	}
	os.Exit(m.Run())
}
