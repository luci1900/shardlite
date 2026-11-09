package node

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestFilesystemNames(t *testing.T) {
	t.Parallel()
	for magic, want := range map[int64]string{
		0x6969: "nfs", 0x517B: "smb", 0xFF534D42: "smb", 0xFE534D42: "smb", 0x65735546: "fuse",
		0x01021994: "tmpfs", 0x858458F6: "ramfs", 0xEF53: "", 0x58465342: "",
	} {
		if got := linuxFSName(magic); got != want {
			t.Errorf("%#x: %q, want %q", magic, got, want)
		}
	}
	for name, want := range map[string]string{"nfs": "nfs", "smbfs": "smb", "macfuse": "fuse", "osxfuse": "fuse", "apfs": "apfs", "webdav": "network"} {
		if got := macFSName(name); got != want {
			t.Errorf("%s: %q, want %q", name, got, want)
		}
	}
	for name, want := range map[string]bool{"nfs": true, "smb": true, "fuse": true, "network": true, "ext4": false, "apfs": false, "tmpfs": false, "": false} {
		if networkFS(name) != want {
			t.Errorf("networkFS(%q) = %v", name, !want)
		}
	}
	// The real thing works on this machine's temporary directory.
	if _, err := filesystemOf(t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestRefusesNetworkFilesystems(t *testing.T) {
	t.Parallel()
	on := func(m map[string]string) fsTypeFunc {
		return func(p string) (string, error) {
			if v, ok := m[p]; ok {
				return v, nil
			}
			return "", errors.New("no such path")
		}
	}
	for name, tc := range map[string]struct {
		fs   map[string]string
		want string // "" for success
	}{
		"local disk":   {map[string]string{"/data": "ext4"}, ""},
		"data on NFS":  {map[string]string{"/data": "nfs"}, "SHARDLITE_DATA (/data) is on a nfs filesystem"},
		"data on FUSE": {map[string]string{"/data": "fuse"}, "SHARDLITE_DATA (/data) is on a fuse filesystem"},
		"data on SMB":  {map[string]string{"/data": "smb"}, "smb filesystem"},
		"unreadable":   {map[string]string{}, "checking the filesystem"},
	} {
		t.Run(name, func(t *testing.T) {
			c := startupChecks{fsType: on(tc.fs), hostLimits: func() hostLimits { return hostLimits{} }, log: slog.New(slog.DiscardHandler)}
			err := c.refuse("/data")
			switch {
			case tc.want == "" && err != nil:
				t.Fatal(err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("got %v, want an error with %q", err, tc.want)
			}
		})
	}
}

func TestStartupWarnings(t *testing.T) {
	t.Parallel()
	run := func(fs string, l hostLimits) string {
		var buf bytes.Buffer
		c := startupChecks{
			fsType:     func(string) (string, error) { return fs, nil },
			hostLimits: func() hostLimits { return l },
			log:        slog.New(slog.NewTextHandler(&buf, nil)),
		}
		c.warn()
		return buf.String()
	}
	roomy := hostLimits{maxMapCount: 1 << 20, fdHard: 1 << 20}
	out := run("tmpfs", roomy)
	if strings.Contains(out, "level=WARN") || !strings.Contains(out, "how many active shards") {
		t.Fatalf("roomy limits on tmpfs:\n%s", out)
	}
	// The number is logged either way.
	byFD, byMaps := roomy.capacity()
	if byFD < minActiveShards || byMaps < minActiveShards {
		t.Fatalf("capacity %d %d", byFD, byMaps)
	}
	if !strings.Contains(out, "by_open_files=") {
		t.Fatalf("no figure logged:\n%s", out)
	}

	// Half of what 10,000 active shards need.
	lowMaps := int64(minActiveShards*mappingsPerShard+baseMappings) / 2
	lowFDs := uint64(minActiveShards*descriptorsPerShard+baseDescriptors) / 2
	out = run("tmpfs", hostLimits{maxMapCount: lowMaps, fdHard: 1 << 20})
	if !strings.Contains(out, "vm.max_map_count allows only about") || strings.Contains(out, "open file limit allows") {
		t.Fatalf("default max_map_count:\n%s", out)
	}
	out = run("tmpfs", hostLimits{maxMapCount: 1 << 20, fdHard: lowFDs})
	if !strings.Contains(out, "open file limit allows only about") || strings.Contains(out, "vm.max_map_count allows") {
		t.Fatalf("low fd limit:\n%s", out)
	}
	// Unknown limits (macOS has no vm.max_map_count) neither warn nor crash.
	out = run("tmpfs", hostLimits{})
	if strings.Contains(out, "level=WARN") || !strings.Contains(out, "unknown") {
		t.Fatalf("unknown limits:\n%s", out)
	}
	// The figure is what the limit divided by the cost per shard gives.
	l := hostLimits{maxMapCount: 100_000, fdHard: 100_000}
	byFD, byMaps = l.capacity()
	if byFD != (100_000-baseDescriptors)/descriptorsPerShard || byMaps != (100_000-baseMappings)/mappingsPerShard {
		t.Fatalf("capacity %d %d", byFD, byMaps)
	}
}

func TestBlobBucket(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		vars          map[string]string
		bucket        string
		wantErr       bool
		errorContains string
	}{
		"directory":              {map[string]string{"SHARDLITE_BLOB": "/var/blobs"}, "", true, "want s3://bucket"},
		"directory, endpoint":    {map[string]string{"SHARDLITE_BLOB": "/var/blobs", "SHARDLITE_S3_ENDPOINT": "http://x"}, "", true, "want s3://bucket"},
		"bucket":                 {map[string]string{"SHARDLITE_BLOB": "s3://my-bucket"}, "my-bucket", false, ""},
		"bucket and prefx":       {map[string]string{"SHARDLITE_BLOB": "s3://my-bucket", "SHARDLITE_PREFIX": "staging"}, "my-bucket", false, ""},
		"endpoint":               {map[string]string{"SHARDLITE_BLOB": "s3://b", "SHARDLITE_S3_ENDPOINT": "http://127.0.0.1:9000"}, "b", false, ""},
		"path in bucket":         {map[string]string{"SHARDLITE_BLOB": "s3://b/x"}, "", true, "SHARDLITE_PREFIX"},
		"no bucket":              {map[string]string{"SHARDLITE_BLOB": "s3://"}, "", true, "s3://bucket"},
		"other scheme":           {map[string]string{"SHARDLITE_BLOB": "gs://bucket"}, "", true, "want s3://bucket"},
		"memory with a path":     {map[string]string{"SHARDLITE_BLOB": "memory://x"}, "", true, "want s3://bucket"},
		"memory without slashes": {map[string]string{"SHARDLITE_BLOB": "memory:"}, "", true, "want s3://bucket"},
	} {
		t.Run(name, func(t *testing.T) {
			e, err := envWith(tc.vars)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), tc.errorContains) {
					t.Fatalf("got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			bucket, err := e.blobBucket()
			if err != nil || bucket != tc.bucket {
				t.Fatalf("bucket %q: %v", bucket, err)
			}
			if e.Prefix != "" && !strings.HasSuffix(e.Prefix, "/") {
				t.Fatalf("prefix %q", e.Prefix)
			}
		})
	}
}

// The real statfs on a path the caller chose, for trying the check on a mount
// the test can't make itself (a Docker bind mount is FUSE on Docker Desktop):
//
//	SHARDLITE_TEST_FS_PATH=/mnt SHARDLITE_TEST_FS_WANT=fuse go test -run RealFilesystem ./internal/node
//
// SHARDLITE_TEST_FS_WANT is the name the node should give the filesystem ("" for
// one it doesn't care about), and the check must refuse exactly the network ones.
func TestRealFilesystem(t *testing.T) {
	t.Parallel()
	path, ok := os.LookupEnv("SHARDLITE_TEST_FS_PATH")
	if !ok {
		t.Skip("SHARDLITE_TEST_FS_PATH is not set")
	}
	want := os.Getenv("SHARDLITE_TEST_FS_WANT")
	got, err := filesystemOf(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s is on %q, want %q", path, got, want)
	}
	err = refuseNetworkFS(filesystemOf, path, "SHARDLITE_DATA")
	if networkFS(want) != (err != nil) {
		t.Fatalf("refusal of a %q filesystem: %v", want, err)
	}
	t.Logf("%s: %q, refused: %v", path, got, err)
}
