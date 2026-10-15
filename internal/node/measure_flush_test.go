package node

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/s3test"
)

// The measurements behind the flush and delta defaults (plans/stage-1.2-report.md).
// They run against RustFS (SHARDLITE_TEST_S3_ENDPOINT) with the latency of S3
// Standard added to the puts that matter (30 to 60 ms, as the emulator on a
// local disk answers in a few), and print their figures with -v:
//
//	SHARDLITE_MEASURE=flush go test -tags fault -run TestMeasureFlush -v -timeout 30m ./internal/node
//	SHARDLITE_MEASURE=delta go test -tags fault -run TestMeasureDeltas -v -timeout 30m ./internal/node
//	SHARDLITE_MEASURE=replay go test -tags fault -run TestMeasureReplay -v -timeout 30m ./internal/node
func measuring(t *testing.T, what string) {
	if !strings.Contains(","+os.Getenv("SHARDLITE_MEASURE")+",", ","+what+",") {
		t.Skip("set SHARDLITE_MEASURE=" + what + " to run")
	}
}

// s3Latency makes puts of segments, LTX files and manifests take as long as on S3 Standard.
func s3Latency(r *mrig, puts *counter) {
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if after || (op != blob.OpCreate && op != blob.OpUpdate) {
			return nil
		}
		switch {
		case strings.HasPrefix(key, "journal/"):
			puts.segments.Add(1)
		case strings.HasPrefix(key, "shards/"):
			puts.files.Add(1)
		case strings.HasPrefix(key, "manifests/"):
			puts.manifests.Add(1)
		default:
			return nil
		}
		time.Sleep(30*time.Millisecond + time.Duration(rand.Int64N(int64(30*time.Millisecond))))
		return nil
	})
}

type counter struct{ segments, files, manifests atomic.Int64 }

type flushCfg struct {
	name                  string
	interval, min, coales time.Duration
	noEarly               bool
}

type flushResult struct {
	p50, p99, max time.Duration
	puts          float64 // segment puts a second
	cycles        int64
	requests      int
}

// loadFlush has n writers each commit to one of 20 shards and wait for the gate,
// every 100 to 300 ms, for d.
func loadFlush(t *testing.T, cfg flushCfg, n int, d time.Duration) flushResult {
	t.Helper()
	s := s3test.Setup(t)
	r := newMRigOn(t, func(o *ShardsOptions) {
		o.Flush.Interval, o.Flush.Min, o.Flush.Coalesce, o.Flush.NoEarly = cfg.interval, cfg.min, cfg.coales, cfg.noEarly
		o.Cache.Every = time.Hour
	}, s.Backend(t))
	var puts counter
	const shards = 20
	refs := make([]*Ref, shards)
	for i := range refs {
		refs[i] = r.acquire(fmt.Sprintf("wallet/m%d", i))
		r.write(refs[i], fmt.Sprint(i))
	}
	if err := r.m.Gate(context.Background(), refs[0], true); err != nil {
		t.Fatal(err)
	}
	s3Latency(r, &puts)
	var log apptest.Log[time.Duration]
	var wg sync.WaitGroup
	c0, start := r.m.Flusher().Completed(), time.Now()
	end := start.Add(d)
	for w := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 1))
			k := w % shards
			time.Sleep(time.Duration(rng.Int64N(int64(300 * time.Millisecond)))) // spread the first requests
			for time.Now().Before(end) {
				if _, err := r.app.Do("insert", fmt.Sprint(k), "1", "100"); err != nil {
					t.Error(err)
					return
				}
				t0 := time.Now()
				if err := r.m.Gate(context.Background(), refs[k], true); err != nil {
					t.Error(err)
					return
				}
				log.Add(time.Since(t0))
				time.Sleep(100*time.Millisecond + time.Duration(rng.Int64N(int64(200*time.Millisecond))))
			}
		}()
	}
	wg.Wait()
	el := time.Since(start)
	lats := log.All()
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	pick := func(q float64) time.Duration {
		if len(lats) == 0 {
			return 0
		}
		return lats[min(len(lats)-1, int(q*float64(len(lats))))]
	}
	return flushResult{p50: pick(0.5), p99: pick(0.99), max: pick(1), puts: float64(puts.segments.Load()) / el.Seconds(), cycles: int64(r.m.Flusher().Completed() - c0), requests: len(lats)}
}

// monthly is what segment puts cost a month on S3 Standard ($0.005 per 1,000 PUTs).
func monthly(putsPerSecond float64) float64 { return putsPerSecond * 86400 * 30 * 0.005 / 1000 }

func TestMeasureFlush(t *testing.T) {
	measuring(t, "flush")
	const d = 8 * time.Second
	const reps = 2 // each row is the mean of this many runs
	row := func(cfg flushCfg, n int) {
		var p50, p99, max, puts float64
		var requests int
		for range reps {
			res := loadFlush(t, cfg, n, d)
			p50 += float64(res.p50) / reps
			p99 += float64(res.p99) / reps
			max += float64(res.max) / reps
			puts += res.puts / reps
			requests += res.requests / reps
		}
		ms := func(f float64) time.Duration { return time.Duration(f).Round(time.Millisecond) }
		t.Logf("| %s | %d | %d | %v | %v | %v | %.1f | $%.0f |", cfg.name, n, requests, ms(p50), ms(p99), ms(max), puts, monthly(puts))
	}
	t.Log("| config | writers | requests | p50 | p99 | max | segment PUTs/s | $/month |")
	before := flushCfg{name: "before (every 250 ms)", interval: 250 * time.Millisecond, noEarly: true}
	for _, n := range []int{1, 10, 100, 1000} {
		row(before, n)
	}
	for _, n := range []int{10, 100} {
		// Coalescing delay, with the minimum spacing out of the way.
		for _, c := range []time.Duration{0, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond} {
			row(flushCfg{name: fmt.Sprintf("coalesce %v, min 0", c), interval: 250 * time.Millisecond, coales: c, min: time.Nanosecond}, n)
		}
		// Minimum spacing, with a coalescing delay of 5 ms.
		for _, m := range []time.Duration{time.Nanosecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond} {
			row(flushCfg{name: fmt.Sprintf("coalesce 5ms, min %v", m), interval: 250 * time.Millisecond, coales: 5 * time.Millisecond, min: m}, n)
		}
	}
	// The defaults.
	def := flushCfg{name: fmt.Sprintf("after (coalesce %v, min %v)", DefaultFlushCoalesce, DefaultFlushMin), interval: 250 * time.Millisecond, coales: DefaultFlushCoalesce, min: DefaultFlushMin}
	for _, n := range []int{1, 10, 100, 1000} {
		row(def, n)
	}
}

// TestMeasureDeltas counts the background delta puts of shards that each commit
// a small write regularly, at the old and the new defaults. The run is
// time-scaled by 20 (a commit every 250 ms stands for one every 5 s, and a delta
// age of 9 s for 3 minutes), so a run of a few minutes stands for an hour or so.
func TestMeasureDeltas(t *testing.T) {
	measuring(t, "delta")
	const (
		scale  = 20
		shards = 100
		run    = 150 * time.Second
	)
	one := func(name string, age time.Duration, size int64) {
		s := s3test.Setup(t)
		r := newMRigOn(t, func(o *ShardsOptions) {
			o.Lifecycle.DeltaAge, o.Lifecycle.DeltaSize, o.Lifecycle.SnapshotEvery, o.Cache.Every = age, size, 200*time.Millisecond, time.Hour
		}, s.Backend(t))
		var puts counter
		refs := make([]*Ref, shards)
		for i := range refs {
			refs[i] = r.acquire(fmt.Sprintf("slow/m%d", i)) // (sync off: nothing waits, the tick flushes)
			r.write(refs[i], fmt.Sprint(i))
		}
		r.afterCycle()
		r.afterCycle()
		before := r.m.Stats.TailBytesMax.Load()
		_ = before
		s3Latency(r, &puts)
		var wg sync.WaitGroup
		end := time.Now().Add(run)
		var commits atomic.Int64
		for i := range shards {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(uint64(i), 2))
				time.Sleep(time.Duration(rng.Int64N(int64(250 * time.Millisecond))))
				for time.Now().Before(end) {
					if _, err := r.app.Do("insert", fmt.Sprint(i), "1", "100"); err != nil {
						t.Error(err)
						return
					}
					commits.Add(1)
					time.Sleep(250 * time.Millisecond)
				}
			}()
		}
		wg.Wait()
		el := run.Seconds()
		deltas := float64(puts.manifests.Load())                       // one manifest update per delta (and none else in the run: shards were created before)
		perHour := deltas / float64(shards) * 1000 / el * 3600 / scale // for 1,000 shards, per simulated hour
		t.Logf("| %s | %d | %d | %.0f | %.0f | $%.0f | %d KB |", name, commits.Load(), int(deltas), perHour*2, perHour*2/3600, perHour*2*720*0.005/1000, r.m.Stats.TailBytesMax.Load()>>10)
	}
	t.Log("| config (scaled) | commits | deltas | PUTs/hour for 1,000 shards (file + manifest) | PUTs/s | $/month | highest tail bytes held (100 shards) |")
	one("old: age 3 min (9 s here), no size trigger", 3*time.Minute/scale, 1<<40)
	one("new: age 15 min (45 s here), size 1 MB", 15*time.Minute/scale, 1<<20)
	one("new age only (45 s here), no size trigger", 15*time.Minute/scale, 1<<40)
}

// TestMeasureReplay times the adoption of a dead node's journal after a busy run.
func TestMeasureReplay(t *testing.T) {
	measuring(t, "replay")
	for _, d := range []time.Duration{10 * time.Second, 30 * time.Second} {
		s := s3test.Setup(t)
		r := newMRigOn(t, func(o *ShardsOptions) { o.Cache.Every = time.Hour; o.Lifecycle.DeltaAge = time.Hour }, s.Backend(t))
		var puts counter
		const shards = 20
		refs := make([]*Ref, shards)
		for i := range refs {
			refs[i] = r.acquire(fmt.Sprintf("wallet/m%d", i))
			r.write(refs[i], fmt.Sprint(i))
		}
		s3Latency(r, &puts)
		var wg sync.WaitGroup
		end := time.Now().Add(d)
		for w := range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(uint64(w), 3))
				k := w % shards
				for time.Now().Before(end) {
					r.app.Do("insert", fmt.Sprint(k), "1", "100")
					r.m.Gate(context.Background(), refs[k], true)
					time.Sleep(100*time.Millisecond + time.Duration(rng.Int64N(int64(200*time.Millisecond))))
				}
			}()
		}
		wg.Wait()
		segs := puts.segments.Load()
		for i := range shards {
			r.app.Must(t, "close", fmt.Sprint(i))
		}
		r.crash()
		r.stopJournal()
		r.local.SetHook(nil)
		start := time.Now()
		r.startNode()
		took := time.Since(start)
		t.Logf("%v of load: %d segments, adopted %d shards' tails in %v (%.1f ms a segment)", d, segs, r.n.Adopted.Deltas, took.Round(time.Millisecond), float64(took.Microseconds())/1000/float64(segs))
	}
}
