package node

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env is the node's configuration from environment variables (design.md,
// Shard kinds): settings about the environment, set by whoever runs the
// cluster. The settings the app's code relies on are in the kinds file.
type Env struct {
	Name          string        // SHARDLITE_NODE: the node's name (the host name by default)
	Address       string        // SHARDLITE_ADDRESS: how other nodes reach this one (unused until there are other nodes)
	Blob          string        // SHARDLITE_BLOB: a directory (the local backend) or s3://bucket (the S3 backend)
	S3Endpoint    string        // SHARDLITE_S3_ENDPOINT: the URL of an S3-compatible store or emulator (AWS if unset)
	Prefix        string        // SHARDLITE_PREFIX: prefix of every object path, for example "staging/"
	KindsFile     string        // SHARDLITE_KINDS: the kinds file
	Flush         time.Duration // SHARDLITE_FLUSH: flush interval, 250ms
	IdleTimeout   time.Duration // SHARDLITE_IDLE_TIMEOUT: default for kinds with no idle_timeout of their own (the built-in default if unset)
	Data          string        // SHARDLITE_DATA: directory of shard files (the local cache)
	Meta          string        // SHARDLITE_META: directory of Litestream's metadata, on tmpfs
	CacheLimit    int64         // SHARDLITE_CACHE_LIMIT: bytes of deactivated shards to keep, 0 for no limit
	LeaseTTL      time.Duration // SHARDLITE_LEASE_TTL: 10s
	DeltaAge      time.Duration // SHARDLITE_DELTA_AGE: how old a shard's tail gets before a delta is written, 3m
	TailAlert     time.Duration // SHARDLITE_TAIL_ALERT: a shard whose oldest tail is older than this raises an alert, 15m
	JournalExpiry time.Duration // SHARDLITE_JOURNAL_EXPIRY: how long the node keeps its journal segments before its sweeper deletes them, 24h
	App           string        // SHARDLITE_APP: the app's base URL, for example http://127.0.0.1:9000
	HealthPath    string        // SHARDLITE_HEALTH_PATH: the app's health endpoint, /health
	Listen        string        // SHARDLITE_LISTEN: the public listener, :8080
	Control       string        // SHARDLITE_CONTROL: the control API on localhost TCP, 127.0.0.1:7000
	Socket        string        // SHARDLITE_SOCKET: the control API on a Unix socket, if set
	MaxCallDepth  int           // SHARDLITE_MAX_CALL_DEPTH: calls nested deeper than this are refused with 508, 32
	Pprof         string        // SHARDLITE_PPROF: address for Go's pprof handlers, for diagnosis (off if unset)
}

// ParseEnv reads the configuration from getenv (os.Getenv in the binary).
func ParseEnv(getenv func(string) string) (Env, error) {
	e := Env{
		Name: getenv("SHARDLITE_NODE"), Address: getenv("SHARDLITE_ADDRESS"), Blob: getenv("SHARDLITE_BLOB"), S3Endpoint: getenv("SHARDLITE_S3_ENDPOINT"),
		Prefix: getenv("SHARDLITE_PREFIX"), KindsFile: getenv("SHARDLITE_KINDS"),
		Data: getenv("SHARDLITE_DATA"), Meta: getenv("SHARDLITE_META"), App: getenv("SHARDLITE_APP"),
		HealthPath: getenv("SHARDLITE_HEALTH_PATH"), Listen: getenv("SHARDLITE_LISTEN"),
		Control: getenv("SHARDLITE_CONTROL"), Socket: getenv("SHARDLITE_SOCKET"), Pprof: getenv("SHARDLITE_PPROF"),
		Flush: 250 * time.Millisecond, LeaseTTL: 10 * time.Second, DeltaAge: 3 * time.Minute, TailAlert: 15 * time.Minute, JournalExpiry: 24 * time.Hour,
	}
	if e.Name == "" {
		h, err := os.Hostname()
		if err != nil {
			return e, fmt.Errorf("SHARDLITE_NODE is not set and the host name is unknown: %w", err)
		}
		e.Name = h
	}
	if e.HealthPath == "" {
		e.HealthPath = "/health"
	}
	if e.Listen == "" {
		e.Listen = ":8080"
	}
	if e.Control == "" {
		e.Control = "127.0.0.1:7000"
	}
	for _, d := range []struct {
		name string
		dst  *time.Duration
	}{{"SHARDLITE_FLUSH", &e.Flush}, {"SHARDLITE_IDLE_TIMEOUT", &e.IdleTimeout}, {"SHARDLITE_LEASE_TTL", &e.LeaseTTL}, {"SHARDLITE_DELTA_AGE", &e.DeltaAge}, {"SHARDLITE_TAIL_ALERT", &e.TailAlert}, {"SHARDLITE_JOURNAL_EXPIRY", &e.JournalExpiry}} {
		if v := getenv(d.name); v != "" {
			x, err := time.ParseDuration(v)
			if err != nil || x <= 0 {
				return e, fmt.Errorf("%s is %q, want a positive duration like 250ms", d.name, v)
			}
			*d.dst = x
		}
	}
	e.MaxCallDepth = DefaultMaxCallDepth
	if v := getenv("SHARDLITE_MAX_CALL_DEPTH"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return e, fmt.Errorf("SHARDLITE_MAX_CALL_DEPTH is %q, want a positive number", v)
		}
		e.MaxCallDepth = n
	}
	if v := getenv("SHARDLITE_CACHE_LIMIT"); v != "" {
		n, err := parseBytes(v)
		if err != nil {
			return e, fmt.Errorf("SHARDLITE_CACHE_LIMIT: %w", err)
		}
		e.CacheLimit = n
	}
	for _, r := range []struct{ name, v string }{{"SHARDLITE_BLOB", e.Blob}, {"SHARDLITE_KINDS", e.KindsFile}, {"SHARDLITE_DATA", e.Data}, {"SHARDLITE_META", e.Meta}} {
		if r.v == "" {
			return e, fmt.Errorf("%s is required", r.name)
		}
	}
	if err := e.checkExpiry(); err != nil {
		return e, err
	}
	if _, _, err := e.blobTarget(); err != nil {
		return e, err
	}
	if e.Prefix != "" && !strings.HasSuffix(e.Prefix, "/") {
		e.Prefix += "/"
	}
	return e, nil
}

// blobTarget says where blob storage is: the bucket of an s3://bucket setting,
// or the directory of a plain path.
func (e Env) blobTarget() (bucket, dir string, err error) {
	if rest, ok := strings.CutPrefix(e.Blob, "s3://"); ok {
		if rest == "" || strings.Contains(rest, "/") {
			return "", "", fmt.Errorf("SHARDLITE_BLOB is %q, want s3://bucket (the bucket's name alone: SHARDLITE_PREFIX sets a prefix inside it)", e.Blob)
		}
		return rest, "", nil
	}
	if e.S3Endpoint != "" {
		return "", "", fmt.Errorf("SHARDLITE_S3_ENDPOINT is set, but SHARDLITE_BLOB (%q) is a directory, not s3://bucket", e.Blob)
	}
	return "", e.Blob, nil
}

// checkExpiry refuses an expiry age that is too short for the tail alert to
// come in time to act: segments must outlive it by at least 4 times.
func (e Env) checkExpiry() error {
	if e.JournalExpiry < 4*e.TailAlert {
		return fmt.Errorf("SHARDLITE_JOURNAL_EXPIRY is %v, want at least 4 times SHARDLITE_TAIL_ALERT (%v) so the alert comes in time to act", e.JournalExpiry, e.TailAlert)
	}
	return nil
}

func parseBytes(s string) (int64, error) {
	mult := int64(1)
	t := strings.ToUpper(strings.TrimSpace(s))
	for suffix, m := range map[string]int64{"KB": 1 << 10, "MB": 1 << 20, "GB": 1 << 30, "TB": 1 << 40, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40} {
		if strings.HasSuffix(t, suffix) {
			mult, t = m, strings.TrimSuffix(t, suffix)
			break
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a size like 500M or 20GB", s)
	}
	return n * mult, nil
}
