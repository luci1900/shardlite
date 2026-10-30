//go:build fault

package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ledger records what clients were told was durable.
type ledger struct {
	mu      sync.Mutex
	acked   map[string]map[string]bool // shard → tokens that must survive
	pending []ack                      // sync-off writes whose response came back, waiting for a completed sync
	counts  map[string]int
}

// ack is a sync-off write whose response came back. A sync call covers it only if the same
// process (life) of the node answered both: a restarted node knows nothing about writes
// the previous one acknowledged and never flushed.
type ack struct {
	shard, token string
	life         int64
}

func (l *ledger) add(kind, shard, token string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.acked[shard] == nil {
		l.acked[shard] = map[string]bool{}
	}
	l.acked[shard][token] = true
	l.counts[kind]++
}

func (l *ledger) snapshot() map[string][]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string][]string{}
	for shard, toks := range l.acked {
		for t := range toks {
			out[shard] = append(out[shard], t)
		}
	}
	return out
}

// SIGKILL the node under load, over and over. Nothing a sync-on gate released,
// and nothing a completed sync call covered, may be missing from blob storage
// at any kill, and every shard must restore with its checksums verified.
func TestKillNodeUnderLoad(t *testing.T) {
	t.Parallel()
	seed := rand.Uint64()
	if v := os.Getenv("SHARDLITE_SEED"); v != "" {
		seed, _ = strconv.ParseUint(v, 10, 64)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed: SHARDLITE_SEED=%d", seed)
		}
	})
	seconds := 30
	if v := os.Getenv("E2E_SECONDS"); v != "" {
		seconds, _ = strconv.Atoi(v)
	}

	// A short delta age and a small cache, so deltas, rebases, deactivation and eviction happen under the load too.
	s := newStack(t, "SHARDLITE_DELTA_AGE=300ms", "SHARDLITE_CACHE_LIMIT=150K")
	v := newVerifier(t, s)
	led := &ledger{acked: map[string]map[string]bool{}, counts: map[string]int{}}

	const nShards = 12
	wallets := make([]string, nShards)
	users := make([]string, nShards)
	for i := range nShards {
		wallets[i], users[i] = fmt.Sprintf("e2e.wallet/w%d", i), fmt.Sprintf("e2e.users/u%d", i)
	}
	s.create(t, append(wallets, users...)...)

	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var attempts, failures atomic.Int64
	var whyMu sync.Mutex
	why := map[string]int{}
	fail := func(st int, err error) {
		failures.Add(1)
		k := fmt.Sprint(st)
		if err != nil {
			k = "error"
			if strings.Contains(err.Error(), "refused") {
				k = "refused"
			} else if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "reset") {
				k = "dropped"
			} else if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline") {
				k = "timeout"
			}
		}
		whyMu.Lock()
		why[k]++
		whyMu.Unlock()
	}
	var life atomic.Int64 // counts the node's processes; bumped just before each kill
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	cc := dialGRPC(t, s.pub)

	post := func(path, target, body string) bool {
		attempts.Add(1)
		st, _, err := doRequest(client, "POST", "http://"+s.pub+path, target, body)
		if err != nil || st != 200 {
			fail(st, err)
			time.Sleep(20 * time.Millisecond) // the node is down or the shard is not ready yet
			return false
		}
		return true
	}

	for w := range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(w)))
			for n := 0; ctx.Err() == nil; n++ {
				tok := fmt.Sprintf("w%d-%d", w, n)
				wallet, user := wallets[rng.IntN(nShards)], users[rng.IntN(nShards)]
				switch op := rng.IntN(10); {
				case op < 3: // sync on, plain HTTP
					if post(at(wallet, "/add"), wallet, tok) {
						led.add("http", wallet, tok)
					}
				case op < 4: // sync on, WebSocket: the echo is the acknowledgement
					c, cancel := context.WithTimeout(ctx, 5*time.Second)
					ws, err := dialWS(c, s.pub, wallet)
					if err != nil {
						cancel()
						failures.Add(1)
						break
					}
					for i := range 4 {
						t := fmt.Sprintf("%s-ws%d", tok, i)
						if echo, err := wsEcho(c, ws, t); err == nil && echo == "ok:"+t {
							led.add("ws", wallet, t)
						} else {
							failures.Add(1)
							break
						}
					}
					ws.CloseNow()
					cancel()
				case op < 5: // sync on, gRPC unary
					c, cancel := context.WithTimeout(ctx, 5*time.Second)
					out, err := unary(onShard(c, wallet), cc, "Add", tok)
					cancel()
					if err == nil && out == "ok:"+tok {
						led.add("grpc", wallet, tok)
					} else {
						failures.Add(1)
					}
				case op < 6: // sync on, a call to another sync-on shard: both writes are durable when it returns
					to := wallets[rng.IntN(nShards)]
					if post(at(wallet, "/transfer")+"?to="+to+"&token="+tok, wallet, "") {
						led.add("transfer", wallet, "debit:"+tok)
						led.add("transfer", to, "credit:"+tok)
					}
				default: // sync off: durable once a later sync call completes
					l0 := life.Load()
					if post(at(user, "/add"), user, tok) && life.Load() == l0 {
						led.mu.Lock()
						led.pending = append(led.pending, ack{user, tok, l0})
						led.mu.Unlock()
					}
				}
			}
		}()
	}

	// The sync calls.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(seed, 1000))
		for ctx.Err() == nil {
			time.Sleep(time.Duration(50+rng.IntN(250)) * time.Millisecond)
			l0 := life.Load()
			led.mu.Lock()
			snap := append([]ack(nil), led.pending...)
			led.mu.Unlock()
			attempts.Add(1)
			st, _, err := doRequest(client, "POST", "http://"+s.ctl+"/_shardlite/sync", "", "")
			if err != nil || st != 204 {
				failures.Add(1)
				continue
			}
			// Everything whose response had come back before the call began, from the
			// process that answered the call, is durable.
			led.mu.Lock()
			for _, a := range snap {
				if a.life != l0 || life.Load() != l0 {
					continue
				}
				if led.acked[a.shard] == nil {
					led.acked[a.shard] = map[string]bool{}
				}
				led.acked[a.shard][a.token] = true
				led.counts["synced"]++
			}
			// Writes from earlier processes that no sync covered may be lost; forget them.
			led.pending = led.pending[len(snap):]
			led.mu.Unlock()
		}
	}()

	check := func(label string) {
		t.Helper()
		for shard, toks := range led.snapshot() {
			have, ok, err := v.tokens(shard)
			if err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			if !ok {
				t.Fatalf("%s: %s has acknowledged writes (%d) but nothing durable", label, shard, len(toks))
			}
			for _, tok := range toks {
				if !have[tok] {
					t.Fatalf("%s: %s lost the acknowledged write %q (%d acknowledged for this shard, %d in blob storage)", label, shard, tok, len(toks), len(have))
				}
			}
		}
		for _, shard := range v.shards() {
			if _, _, err := v.tokens(shard); err != nil {
				t.Fatalf("%s: %v", label, err)
			}
		}
	}

	rng := rand.New(rand.NewPCG(seed, 2000))
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	kills := 0
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(300+rng.IntN(2200)) * time.Millisecond)
		life.Add(1)
		s.node.kill()
		kills++
		time.Sleep(50 * time.Millisecond) // responses that left before the kill reach their clients
		check(fmt.Sprintf("after kill %d", kills))
		s.startNode()
	}

	// Stop the load, let the app close its handles, and shut down gracefully.
	stop()
	wg.Wait()
	eventually(t, "the app to close its databases", func() bool { return s.appDebug().OpenDBs == 0 })
	if err := s.node.term(60 * time.Second); err != nil {
		t.Fatalf("graceful stop: %v\n%s", err, s.node.out.String())
	}
	check("after the graceful stop")
	if l, err := v.files.Leases.Get(context.Background(), "n1"); err != nil || !l.Released {
		t.Fatalf("lease after the graceful stop: %+v %v\n%s", l, err, s.node.out.String())
	}
	led.mu.Lock()
	defer led.mu.Unlock()
	t.Logf("%d kills; %d requests, %d failed; acknowledged writes by kind: %v; %d sync-off writes never covered by a sync",
		kills, attempts.Load(), failures.Load(), led.counts, len(led.pending))
	// The node's own error lines tell what the crashes exercised.
	errLines := map[string]int{}
	for _, line := range strings.Split(s.node.out.String(), "\n") {
		if strings.Contains(line, "level=ERROR") || strings.Contains(line, "level=WARN") {
			if i := strings.Index(line, "msg="); i >= 0 {
				msg := line[i:]
				if j := strings.Index(msg, " shard="); j > 0 {
					msg = msg[:j]
				}
				errLines[msg]++
			}
		}
	}
	t.Logf("node warnings and errors (last life): %v", errLines)
	whyMu.Lock()
	t.Logf("failed plain requests by reason: %v", why)
	whyMu.Unlock()
	if kills < 5 {
		t.Fatalf("only %d kills in %d s", kills, seconds)
	}
	for _, kind := range []string{"http", "ws", "grpc", "transfer", "synced"} {
		if led.counts[kind] == 0 {
			t.Errorf("no acknowledged %s writes: the load did not exercise that path", kind)
		}
	}
}
