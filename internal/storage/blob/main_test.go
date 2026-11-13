package blob_test

import (
	"os"
	"testing"
)

// TestMain gives the S3 client keys and a region for the fakes, once, so that
// tests need not (t.Setenv would keep them from running side by side). A fake
// takes any keys. The long suite sets the emulator's, which are kept.
func TestMain(m *testing.M) {
	for k, v := range map[string]string{"AWS_ACCESS_KEY_ID": "fake", "AWS_SECRET_ACCESS_KEY": "fake", "AWS_REGION": "us-east-1"} {
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
	os.Exit(m.Run())
}
