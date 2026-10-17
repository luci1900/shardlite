// Command bench measures the node in front of the e2e app: requests per
// second and latency for active shards, with sync off (users) and sync on
// (wallet). It starts its own node and app for each kind, activates the
// shards, then drives closed-loop load. The numbers are relative: the load
// generator, the node and the app share the machine.
//
// The node needs an S3 store: run RustFS (docker/compose.s3.yaml) and pass its
// endpoint. Each run uses a prefix of its own.
// The app is built from the e2e module: (cd e2e && go build -o ../e2eapp ./cmd/e2eapp).
//
//	bench -s3-endpoint http://127.0.0.1:19000 -node ./shardlite -app ./e2eapp -shards 1000 -clients 64 -duration 15s
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
)

var (
	nodeBin  = flag.String("node", "shardlite", "the node binary")
	appBin   = flag.String("app", "e2eapp", "the e2e app binary")
	nShards  = flag.Int("shards", 1000, "active shards")
	clients  = flag.Int("clients", 64, "concurrent clients")
	duration = flag.Duration("duration", 15*time.Second, "length of each measurement")
	flush    = flag.Duration("flush", 250*time.Millisecond, "flush interval")
	dir      = flag.String("dir", "", "working directory (a temporary one by default)")
	hold     = flag.Duration("hold", 0, "after activating, keep the node and app up this long for inspection (pprof is on 127.0.0.1:6060)")
	kinds    = flag.String("kinds", "users,wallet", "kinds to measure")
	s3URL    = flag.String("s3-endpoint", "", "endpoint of the S3 store (RustFS from docker/compose.s3.yaml)")
	s3Bucket = flag.String("s3-bucket", "shardlite-test", "bucket in the S3 store")
)

type proc struct {
	cmd *exec.Cmd
	out bytes.Buffer
	mu  sync.Mutex
}

func (p *proc) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.Write(b)
}

func (p *proc) String() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.String()
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func start(bin string, env []string, args ...string) *proc {
	p := &proc{}
	p.cmd = exec.Command(bin, args...)
	p.cmd.Env = append(os.Environ(), env...)
	p.cmd.Stdout, p.cmd.Stderr = p, p
	if os.Getenv("BENCH_LOG") != "" {
		p.cmd.Stdout, p.cmd.Stderr = io.MultiWriter(p, os.Stderr), io.MultiWriter(p, os.Stderr)
	}
	if err := p.cmd.Start(); err != nil {
		log.Fatal(err)
	}
	return p
}

func (p *proc) waitFor(re string, d time.Duration) []string {
	rx := regexp.MustCompile(re)
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if m := rx.FindStringSubmatch(p.String()); m != nil {
			return m
		}
	}
	log.Fatalf("no %q in:\n%s", re, p.String())
	return nil
}

func freeAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

type result struct {
	kind, op           string
	rps                float64
	p50, p95, p99, max time.Duration
	errors             int64
	cpu                float64 // the node's CPU as a percentage of one core, -1 if unknown
}

func main() {
	flag.Parse()
	if *s3URL == "" {
		log.Fatal("-s3-endpoint is required: the node needs an S3 store, so start RustFS with docker/compose.s3.yaml")
	}
	for _, k := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"} {
		os.Setenv(k, envOr(k, map[string]string{"AWS_ACCESS_KEY_ID": "shardlite", "AWS_SECRET_ACCESS_KEY": "shardlite-secret", "AWS_REGION": "us-east-1"}[k]))
	}
	if b, err := blob.NewS3(context.Background(), blob.S3Options{Bucket: *s3Bucket, Endpoint: *s3URL}); err != nil {
		log.Fatal(err)
	} else if err := b.EnsureBucket(context.Background()); err != nil {
		log.Fatal(err)
	}
	// Every active shard costs file descriptors in the node and the app.
	var rl syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl) == nil {
		rl.Cur = rl.Max
		syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rl)
	}
	root := *dir
	if root == "" {
		var err error
		if root, err = os.MkdirTemp("", "shardlite-bench"); err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(root)
	}
	var results []result
	for _, kind := range strings.Split(*kinds, ",") {
		results = append(results, run(root, kind)...)
	}
	fmt.Printf("\n%d active shards, %d clients, %v per measurement, flush interval %v\n\n", *nShards, *clients, *duration, *flush)
	fmt.Println("| kind | sync | operation | requests/s | p50 | p95 | p99 | max | errors | node CPU |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|---|")
	for _, r := range results {
		sync := "off"
		if r.kind == "wallet" {
			sync = "on"
		}
		cpu := "n/a"
		if r.cpu >= 0 {
			cpu = fmt.Sprintf("%.0f%%", r.cpu)
		}
		fmt.Printf("| %s | %s | %s | %.0f | %s | %s | %s | %s | %d | %s |\n", r.kind, sync, r.op, r.rps, ms(r.p50), ms(r.p95), ms(r.p99), ms(r.max), r.errors, cpu)
	}
}

// cpuTicks is the CPU time (user plus system, in clock ticks of 10 ms) the
// process has used so far, from /proc (Linux only).
func cpuTicks(pid int) (int64, bool) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, false
	}
	// The command name is in parentheses and may hold spaces: fields count from after it.
	rest := string(b[strings.LastIndexByte(string(b), ')')+2:])
	f := strings.Fields(rest)
	if len(f) < 13 {
		return 0, false
	}
	var u, sy int64
	fmt.Sscan(f[11], &u)
	fmt.Sscan(f[12], &sy)
	return u + sy, true
}

func ms(d time.Duration) string { return fmt.Sprintf("%.2f ms", float64(d.Microseconds())/1000) }

// run starts a node and an app, activates the shards of one kind and measures.
func run(root, kind string) []result {
	d := filepath.Join(root, kind)
	os.RemoveAll(d)
	os.MkdirAll(d, 0o755)
	kf := filepath.Join(d, "kinds.json")
	os.WriteFile(kf, []byte(`{"kinds": {"users": {"public": true, "sync": "off", "idle_timeout": "1h"}, "wallet": {"public": true, "idle_timeout": "1h"}}}`), 0o644)
	pub, ctl := freeAddr(), freeAddr()
	// The app keeps each shard's database open (its idle close is long) so the measurement is of serving, not of reopening.
	app := start(*appBin, nil, "-listen", "127.0.0.1:0", "-control", ctl, "-idle-close", "10m")
	defer app.cmd.Process.Kill()
	appAddr := app.waitFor(`listening (\S+)`, 20*time.Second)[1]
	node := start(*nodeBin, []string{
		"SHARDLITE_NODE=bench", "SHARDLITE_BLOB=s3://" + *s3Bucket, "SHARDLITE_S3_ENDPOINT=" + *s3URL, fmt.Sprintf("SHARDLITE_PREFIX=bench/%s/%d/", kind, time.Now().UnixNano()),
		"AWS_ACCESS_KEY_ID=" + envOr("AWS_ACCESS_KEY_ID", "shardlite"), "AWS_SECRET_ACCESS_KEY=" + envOr("AWS_SECRET_ACCESS_KEY", "shardlite-secret"), "AWS_REGION=" + envOr("AWS_REGION", "us-east-1"), "SHARDLITE_KINDS=" + kf,
		"SHARDLITE_DATA=" + filepath.Join(d, "data"), "SHARDLITE_SCRATCH_DIR=" + filepath.Join(d, "meta"),
		"SHARDLITE_APP=http://" + appAddr, "SHARDLITE_LISTEN_PUBLIC=" + pub, "SHARDLITE_LISTEN_CONTROL=" + ctl, "SHARDLITE_LISTEN_PEER=127.0.0.1:0",
		"SHARDLITE_FLUSH=" + flush.String(), "SHARDLITE_LISTEN_PPROF=127.0.0.1:6060",
	})
	defer node.cmd.Process.Kill()
	node.waitFor(`shardlite is up`, 30*time.Second)

	client := &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: *clients * 2, MaxIdleConns: *clients * 2}}
	do := func(method, path, target, body string) error {
		q, _ := http.NewRequest(method, "http://"+pub+path, strings.NewReader(body))
		q.Header.Set("Shardlite-Target", target)
		resp, err := client.Do(q)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		return nil
	}
	// Wait for readiness, then activate every shard with one write.
	for end := time.Now().Add(20 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if resp, err := http.Get("http://" + pub + "/_shardlite/ready"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(end) {
			log.Fatal("node not ready")
		}
	}
	log.Printf("%s: activating %d shards", kind, *nShards)
	t0 := time.Now()
	var next atomic.Int64
	var wg errgroup.Group
	for range *clients {
		wg.Go(func() error {
			for {
				i := int(next.Add(1)) - 1
				if i >= *nShards {
					return nil
				}
				if err := do("POST", "/add", fmt.Sprintf("%s/s%d", kind, i), "warm"); err != nil {
					log.Fatalf("activating %s/s%d: %v\n%s", kind, i, err, node.String())
				}
			}
		})
	}
	wg.Wait()
	log.Printf("%s: %d shards active in %v", kind, *nShards, time.Since(t0).Round(time.Millisecond))

	mappings := func(when string) {
		if b, err := os.ReadFile(fmt.Sprintf("/proc/%d/maps", node.cmd.Process.Pid)); err == nil {
			var shm, anon, guard, big int
			for _, l := range strings.Split(string(b), "\n") {
				f := strings.Fields(l)
				switch {
				case len(f) < 2:
				case strings.HasSuffix(l, "-shm"):
					shm++
				case f[1] == "---p":
					guard++
				case f[1] == "rw-p" && len(f) == 5:
					var lo, hi uint64
					fmt.Sscanf(f[0], "%x-%x", &lo, &hi)
					if hi-lo >= 1<<20 {
						big++
					} else {
						anon++
					}
				}
			}
			log.Printf("%s: the node has %d memory mappings %s (%d active shards): %d of -shm, %d small anonymous, %d anonymous of 1 MB or more, %d guard", kind, bytes.Count(b, []byte("\n")), when, *nShards, shm, anon, big, guard)
		}
	}
	mappings("after activation")
	if *hold > 0 {
		log.Printf("%s: holding for %v, node pid %d", kind, *hold, node.cmd.Process.Pid)
		time.Sleep(*hold)
		return nil
	}
	var out []result
	for _, op := range []string{"idle", "read", "write", "trickle"} {
		log.Printf("%s: measuring %s", kind, op)
		ticks0, ok := cpuTicks(node.cmd.Process.Pid)
		t0 := time.Now()
		r := measure(kind, op, do)
		r.cpu = -1
		if ticks1, ok1 := cpuTicks(node.cmd.Process.Pid); ok && ok1 {
			r.cpu = float64(ticks1-ticks0) / 100 / time.Since(t0).Seconds() * 100
		}
		out = append(out, r)
		mappings("after " + op)
	}
	return out
}

func measure(kind, op string, do func(method, path, target, body string) error) result {
	var (
		wg     errgroup.Group
		mu     sync.Mutex
		all    []time.Duration
		errs   atomic.Int64
		serial atomic.Int64
	)
	if op == "idle" {
		// Every shard active, no requests: the flush loop looking for changes.
		time.Sleep(*duration)
		return result{kind: kind, op: op}
	}
	end := time.Now().Add(*duration)
	start := time.Now()
	nClients := *clients
	if op == "trickle" {
		// 200 writes a second from one client: most of the shards are idle, so the
		// cost is mostly the flush loop looking at them.
		nClients = 1
	}
	for c := range nClients {
		wg.Go(func() error {
			rng := rand.New(rand.NewPCG(uint64(c), 7))
			var lat []time.Duration
			for time.Now().Before(end) {
				target := fmt.Sprintf("%s/s%d", kind, rng.IntN(*nShards))
				t := time.Now()
				var err error
				if op == "trickle" {
					time.Sleep(5 * time.Millisecond)
					t = time.Now()
				}
				if op == "read" {
					err = do("GET", "/count", target, "")
				} else {
					err = do("POST", "/add", target, fmt.Sprintf("b%d", serial.Add(1)))
				}
				if err != nil {
					errs.Add(1)
					continue
				}
				lat = append(lat, time.Since(t))
			}
			mu.Lock()
			all = append(all, lat...)
			mu.Unlock()
			return nil
		})
	}
	wg.Wait()
	el := time.Since(start)
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pick := func(q float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[min(len(all)-1, int(q*float64(len(all))))]
	}
	return result{kind: kind, op: op, rps: float64(len(all)) / el.Seconds(), p50: pick(0.5), p95: pick(0.95), p99: pick(0.99), max: pick(1), errors: errs.Load()}
}
