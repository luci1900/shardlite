package node

import (
	"cmp"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// Env is the node's configuration from environment variables (docs/guide.md,
// Environment variables): settings about the environment, set by whoever runs
// the cluster. The app declares its own settings (its kinds) at its ready
// route.
type Env struct {
	Name        cluster.Node  // SHARDLITE_NODE: the node's name (the host name by default)
	Advertise   string        // SHARDLITE_ADVERTISE_PEER: how other nodes reach the peer listener (host:port), the peer listener's bound address if unset and not a wildcard
	Blob        string        // SHARDLITE_BLOB: s3://bucket
	S3Endpoint  string        // SHARDLITE_S3_ENDPOINT: the URL of an S3-compatible store or emulator (AWS if unset)
	Prefix      string        // SHARDLITE_PREFIX: prefix of every object path, for example "staging/"
	Flush       time.Duration // SHARDLITE_FLUSH: flush interval, 250ms
	IdleTimeout time.Duration // SHARDLITE_IDLE_TIMEOUT: default for kinds with no idle_timeout of their own (the built-in default if unset)
	Data        string        // SHARDLITE_DATA: directory of shard files (the local cache)
	CacheLimit  int64         // SHARDLITE_CACHE_LIMIT: bytes of deactivated shards to keep, 0 for no limit
	LeaseTTL    time.Duration // SHARDLITE_LEASE_TTL: 10s
	FlushMin    time.Duration // SHARDLITE_FLUSH_MIN: the shortest time between two flush cycle starts, DefaultFlushMin
	DeltaAge    time.Duration // SHARDLITE_DELTA_AGE: how old a shard's tail gets before a delta is written, 15m. A tail 3 times older raises an alert, and journal segments expire after the larger of 24h and 12 times this
	DeltaSize   int64         // SHARDLITE_DELTA_SIZE: how many bytes of tail make a delta due, 256KB
	App         string        // SHARDLITE_APP: the app's base URL, for example http://127.0.0.1:9000
	Public      string        // SHARDLITE_LISTEN_PUBLIC: the public listener, :2280
	Peer        string        // SHARDLITE_LISTEN_PEER: the listener for traffic from other nodes, :2281
	Control     string        // SHARDLITE_LISTEN_CONTROL: the control API on localhost TCP, 127.0.0.1:2282
	Socket      string        // SHARDLITE_LISTEN_CONTROL_SOCKET: the control API on a Unix socket, if set
	MaxHops     int           // SHARDLITE_MAX_HOPS: calls chained past this many hops are refused with 508, 32
	Pprof       string        // SHARDLITE_LISTEN_PPROF: address for Go's pprof handlers, for diagnosis (off if unset)
	Standbys    int           // SHARDLITE_STANDBYS: whether the node stands by for others and names a peer for its journal, 0 or 1, 1
}

// ParseEnv reads the configuration from getenv (os.Getenv in the binary).
func ParseEnv(getenv func(string) string) (Env, error) {
	e := Env{
		Name: cluster.Node(getenv("SHARDLITE_NODE")), Advertise: getenv("SHARDLITE_ADVERTISE_PEER"), Blob: getenv("SHARDLITE_BLOB"), S3Endpoint: getenv("SHARDLITE_S3_ENDPOINT"),
		Prefix: getenv("SHARDLITE_PREFIX"),
		Data:   getenv("SHARDLITE_DATA"), App: getenv("SHARDLITE_APP"),
		Public:  getenv("SHARDLITE_LISTEN_PUBLIC"),
		Control: getenv("SHARDLITE_LISTEN_CONTROL"), Peer: getenv("SHARDLITE_LISTEN_PEER"), Socket: getenv("SHARDLITE_LISTEN_CONTROL_SOCKET"), Pprof: getenv("SHARDLITE_LISTEN_PPROF"),
		Flush: 250 * time.Millisecond, LeaseTTL: 10 * time.Second, FlushMin: DefaultFlushMin, DeltaAge: shardfile.DefaultDeltaAge, DeltaSize: shardfile.DefaultDeltaSize,
	}
	if e.Name == "" {
		h, err := os.Hostname()
		if err != nil {
			return e, fmt.Errorf("SHARDLITE_NODE is not set and the host name is unknown: %w", err)
		}
		e.Name = cluster.Node(h)
	}
	// The name goes into object keys as it is (docs/guide.md, the reference).
	if !cluster.ValidNode(string(e.Name)) {
		return e, fmt.Errorf("SHARDLITE_NODE is %q: a node's name is 1 to 128 ASCII letters, digits, '.', '-' and '_'", e.Name)
	}
	e.Public = cmp.Or(e.Public, ":2280")
	e.Control = cmp.Or(e.Control, "127.0.0.1:2282")
	e.Peer = cmp.Or(e.Peer, ":2281")
	for _, d := range []struct {
		name string
		dst  *time.Duration
	}{{"SHARDLITE_FLUSH", &e.Flush}, {"SHARDLITE_IDLE_TIMEOUT", &e.IdleTimeout}, {"SHARDLITE_LEASE_TTL", &e.LeaseTTL}, {"SHARDLITE_FLUSH_MIN", &e.FlushMin}, {"SHARDLITE_DELTA_AGE", &e.DeltaAge}} {
		if v := getenv(d.name); v != "" {
			x, err := time.ParseDuration(v)
			if err != nil || x <= 0 {
				return e, fmt.Errorf("%s is %q, want a positive duration like 250ms", d.name, v)
			}
			*d.dst = x
		}
	}
	e.MaxHops = DefaultMaxHops
	if v := getenv("SHARDLITE_MAX_HOPS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return e, fmt.Errorf("SHARDLITE_MAX_HOPS is %q, want a positive number", v)
		}
		e.MaxHops = n
	}
	e.Standbys = 1
	if v := getenv("SHARDLITE_STANDBYS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 1 {
			return e, fmt.Errorf("SHARDLITE_STANDBYS is %q, want 0 or 1", v)
		}
		e.Standbys = n
	}
	if v := getenv("SHARDLITE_DELTA_SIZE"); v != "" {
		n, err := parseBytes(v)
		if err != nil || n <= 0 {
			return e, fmt.Errorf("SHARDLITE_DELTA_SIZE is %q, want a size like 1MB", v)
		}
		e.DeltaSize = n
	}
	if v := getenv("SHARDLITE_CACHE_LIMIT"); v != "" {
		n, err := parseBytes(v)
		if err != nil {
			return e, fmt.Errorf("SHARDLITE_CACHE_LIMIT: %w", err)
		}
		e.CacheLimit = n
	}
	for _, r := range []struct{ name, v string }{{"SHARDLITE_BLOB", e.Blob}, {"SHARDLITE_DATA", e.Data}} {
		if r.v == "" {
			return e, fmt.Errorf("%s is required", r.name)
		}
	}
	if _, err := e.blobBucket(); err != nil {
		return e, err
	}
	if e.Prefix != "" && !strings.HasSuffix(e.Prefix, "/") {
		e.Prefix += "/"
	}
	return e, nil
}

// blobBucket is the bucket of the s3://bucket setting.
func (e Env) blobBucket() (string, error) {
	rest, ok := strings.CutPrefix(e.Blob, "s3://")
	if !ok {
		return "", fmt.Errorf("SHARDLITE_BLOB is %q, want s3://bucket", e.Blob)
	}
	if rest == "" || strings.Contains(rest, "/") {
		return "", fmt.Errorf("SHARDLITE_BLOB is %q, want s3://bucket (the bucket's name alone: SHARDLITE_PREFIX sets a prefix inside it)", e.Blob)
	}
	return rest, nil
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
