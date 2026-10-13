// Package s3test connects tests to the RustFS emulator that docker/compose.s3.yaml
// runs. Tests that run nodes as separate processes need it, since they can't
// share an in-memory backend.
package s3test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/luci1900/shardlite/internal/blob"
)

// Store is where a test's objects live: a bucket on the emulator and a prefix
// of the test's own.
type Store struct {
	Endpoint string
	Bucket   string
	Prefix   string // ends in "/"
}

// Env is the node-side environment (credentials, region) for processes the test starts.
func Env() []string {
	return []string{
		"AWS_ACCESS_KEY_ID=" + envOr("AWS_ACCESS_KEY_ID", "shardlite"),
		"AWS_SECRET_ACCESS_KEY=" + envOr("AWS_SECRET_ACCESS_KEY", "shardlite-secret"),
		"AWS_REGION=" + envOr("AWS_REGION", "us-east-1"),
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

var bucketOnce, credsOnce sync.Once

// Setup returns a store with a prefix of its own, creating the bucket if
// needed. It skips the test, naming docker/compose.s3.yaml, when
// SHARDLITE_TEST_S3_ENDPOINT is not set. The test's process gets the emulator's
// credentials too, unless the environment has its own.
func Setup(t testing.TB) Store {
	t.Helper()
	endpoint := os.Getenv("SHARDLITE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("SHARDLITE_TEST_S3_ENDPOINT is not set: start RustFS with docker/compose.s3.yaml (this test runs nodes as separate processes)")
	}
	credsOnce.Do(func() {
		// Set for the whole process, not with t.Setenv, which parallel tests can't use.
		for _, kv := range Env() {
			k, v, _ := strings.Cut(kv, "=")
			if os.Getenv(k) == "" {
				os.Setenv(k, v)
			}
		}
	})
	s := Store{Endpoint: endpoint, Bucket: envOr("SHARDLITE_TEST_S3_BUCKET", "shardlite-test")}
	var err error
	bucketOnce.Do(func() {
		var b *blob.S3
		if b, err = blob.NewS3(context.Background(), blob.S3Options{Bucket: s.Bucket, Endpoint: s.Endpoint}); err == nil {
			err = b.EnsureBucket(context.Background())
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	var r [6]byte
	rand.Read(r[:])
	s.Prefix = "test/" + hex.EncodeToString(r[:]) + "/"
	return s
}

// Backend is the test's own view of the store, for a verifier that is no node.
func (s Store) Backend(t testing.TB) blob.Backend {
	t.Helper()
	b, err := blob.NewS3(context.Background(), blob.S3Options{Bucket: s.Bucket, Endpoint: s.Endpoint})
	if err != nil {
		t.Fatal(err)
	}
	return blob.WithPrefix(b, s.Prefix)
}

// Args are the flags testnode takes for this store.
func (s Store) Args() []string {
	return []string{"-s3-bucket", s.Bucket, "-s3-endpoint", s.Endpoint, "-prefix", s.Prefix}
}
