package blob_test

import (
	"os"
	"testing"

	"github.com/luci1900/shardlite/internal/s3test"
)

// TestMain sets the keys and region the S3 client reads from the environment
// once, so that tests need not (t.Setenv would keep them from running side by
// side). The emulator's keys serve a fake too.
func TestMain(m *testing.M) {
	s3test.Export()
	os.Exit(m.Run())
}
