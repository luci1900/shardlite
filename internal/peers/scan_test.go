package peers

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

// realClock is the time itself.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)
	return t.C, t.Stop
}

// runScan serves a lease scan for node "w" (epoch 1) on b until the test ends, and
// returns it with the count of the leases it asked to have adopted. Its adopter
// answers that the node is adopted and does nothing else. tune changes the scan
// before it runs.
func runScan(t *testing.T, b blob.Backend, opts ScanOptions, tune ...func(*Scan)) (*Scan, *notify.Counter) {
	t.Helper()
	adopts := &notify.Counter{}
	s := &Scan{
		Registry: lease.NewRegistry(b, "w", lease.RegistryOptions{}),
		Self:     func() lease.Incarnation { return lease.Incarnation{Node: "w", Epoch: 1} },
		Clock:    realClock{}, Options: opts, Log: slog.New(slog.DiscardHandler), Scans: &notify.Counter{},
		Adopt: func(context.Context, *lease.Record) bool { adopts.Inc(); return true },
	}
	for _, f := range tune {
		f(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var g errgroup.Group
	g.Go(func() error { return s.Serve(ctx) })
	t.Cleanup(func() { cancel(); g.Wait() })
	return s, adopts
}

// deaths records the nodes a scan tells died about, in order.
type deaths struct {
	mu    sync.Mutex
	nodes []cluster.Node
}

func (d *deaths) tell(n cluster.Node) {
	d.mu.Lock()
	d.nodes = append(d.nodes, n)
	d.mu.Unlock()
}

func (d *deaths) count(n cluster.Node) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	count := 0
	for _, x := range d.nodes {
		if x == n {
			count++
		}
	}
	return count
}

// listingAfter waits for the scan's next listing and returns it.
func listingAfter(t *testing.T, s *Scan) map[string]Peer {
	t.Helper()
	if err := s.Scans.Wait(context.Background(), s.Scans.Load()+1); err != nil {
		t.Fatal(err)
	}
	synctest.Wait() // the listing is published, and any adoption it started is over
	return view(s)
}

// keep takes node's lease and renews it every interval until the test ends. If
// stall is given, the node's second renewal comes that much later than usual
// (a pause, after which it renews as before).
func keep(t *testing.T, b blob.Backend, node cluster.Node, interval time.Duration, stall ...time.Duration) {
	t.Helper()
	reg := lease.NewRegistry(b, node, lease.RegistryOptions{})
	l, err := reg.Acquire(context.Background(), lease.NodeInfo{Name: node})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var g errgroup.Group
	g.Go(func() error {
		for i := 0; ; i++ {
			wait := interval
			if i == 1 && len(stall) > 0 {
				wait += stall[0]
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(wait):
			}
			if l, err = reg.Renew(ctx, l, l.NodeInfo); err != nil && ctx.Err() == nil {
				return err
			}
		}
	})
	t.Cleanup(func() {
		cancel()
		if err := g.Wait(); err != nil {
			t.Error(err)
		}
	})
}

func view(s *Scan) map[string]Peer {
	out := map[string]Peer{}
	for _, v := range s.Nodes().nodes() {
		out[string(v.Name)] = v
	}
	return out
}

// Each node lists every N × Every on average, so the cluster as a whole lists
// about every Every.
func TestLeaseScanSpreadsWithTheNodes(t *testing.T) {
	t.Parallel()
	const every = 2 * time.Second
	count := func(nodes int) uint64 {
		var n uint64
		synctest.Test(t, func(t *testing.T) {
			b := blob.NewMemory()
			for i := range nodes {
				keep(t, b, cluster.Node(string(rune('a'+i))), time.Minute)
			}
			s, _ := runScan(t, b, ScanOptions{Every: every, TTL: time.Hour})
			time.Sleep(20 * every)
			synctest.Wait()
			n = s.Scans.Load()
		})
		return n
	}
	// Waits are between half and one and a half times N × Every, plus the first
	// listing at once.
	if n := count(1); n < 14 || n > 41 {
		t.Fatalf("one node listed %d times in 20 intervals", n)
	}
	if n := count(4); n < 4 || n > 11 {
		t.Fatalf("one of four nodes listed %d times in 20 intervals", n)
	}
}

// A renewal that lands while the scan reads a lease that looked expired keeps
// the node alive until its holder's own deadline: the scan's second look comes a
// TTL after the read returned, not a TTL after the listing.
func TestLeaseScanTimesItsWatchFromTheRead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := blobtest.WithHook(blob.NewMemory())
		reg := lease.NewRegistry(b, "z", lease.RegistryOptions{})
		l, err := reg.Acquire(context.Background(), lease.NodeInfo{Name: "z"})
		if err != nil {
			t.Fatal(err)
		}
		// The scan lists at 11 s, when the lease looks expired, and its read takes
		// 3 s. z renews 2.5 s into the read and never again, so by its own clock
		// (TTL less the 2 s margin) its lease is good until 21.5 s.
		var once atomic.Bool
		b.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
			if op == blobtest.OpGet && key == lease.Key("z") && !after && once.CompareAndSwap(false, true) {
				time.Sleep(2500 * time.Millisecond)
				if _, err := reg.Renew(ctx, l, l.NodeInfo); err != nil {
					t.Error(err)
				}
				time.Sleep(500 * time.Millisecond)
			}
			return nil
		})
		time.Sleep(11 * time.Second)
		s, _ := runScan(t, b, ScanOptions{Every: time.Hour, TTL: 10 * time.Second})

		time.Sleep(10*time.Second + 400*time.Millisecond) // 21.4 s
		synctest.Wait()
		if view(s)["z"].Dead {
			t.Fatal("z is dead before its own deadline")
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if !view(s)["z"].Dead {
			t.Fatalf("z isn't dead a TTL after the read: %+v", view(s))
		}
	})
}

// The listing shows new nodes, and a node is dead only once its lease has gone
// unchanged for its TTL: one that pauses for longer than the TTL looks expired
// but renews before the scan's second look, and is never dead.
func TestLeaseScanFindsTheDead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := blob.NewMemory()
		keep(t, b, "w", 3*time.Second)
		keep(t, b, "live", 3*time.Second)
		// Renews at 3 s and 16 s, so for 13 s its lease looks expired (the TTL is
		// 10 s) and it renews within 10 s of the scan's reading it. A node that
		// renewed every 15 s would be dead whenever a watch of the scan fell between
		// two of its renewals, which depends on when the scan happens to list.
		keep(t, b, "slow", 3*time.Second, 10*time.Second)
		if _, err := lease.NewRegistry(b, "gone", lease.RegistryOptions{}).Acquire(context.Background(), lease.NodeInfo{Name: "gone"}); err != nil {
			t.Fatal(err)
		}
		left := lease.NewRegistry(b, "left", lease.RegistryOptions{})
		l, err := left.Acquire(context.Background(), lease.NodeInfo{Name: "left"})
		if err != nil {
			t.Fatal(err)
		}
		if err := left.Release(context.Background(), l); err != nil {
			t.Fatal(err)
		}
		s, _ := runScan(t, b, ScanOptions{Every: 2 * time.Second, TTL: 10 * time.Second})
		synctest.Wait()
		if got := view(s); len(got) != 5 || got["gone"].Dead || got["left"].Dead {
			t.Fatalf("first listing: %+v", got)
		}

		keep(t, b, "new", 3*time.Second)
		time.Sleep(15 * time.Second)
		synctest.Wait()
		if _, ok := view(s)["new"]; !ok {
			t.Fatalf("a new node isn't listed: %+v", view(s))
		}

		time.Sleep(time.Minute)
		synctest.Wait()
		got := view(s)
		if !got["gone"].Dead || !got["left"].Dead {
			t.Fatalf("a node that never renewed, or released its lease, isn't dead: %+v", got)
		}
		for _, name := range []string{"w", "live", "slow", "new"} {
			if got[name].Dead {
				t.Fatalf("%s is dead: %+v", name, got)
			}
		}
	})
}

// A node whose lease is released is dead and stays dead, with nothing to adopt,
// and died runs once.
func TestLeaseScanKeepsAReleasedNodeDead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		b := blob.NewMemory()
		left := lease.NewRegistry(b, "left", lease.RegistryOptions{})
		l, err := left.Acquire(context.Background(), lease.NodeInfo{Name: "left"})
		if err != nil {
			t.Fatal(err)
		}
		if err := left.Release(context.Background(), l); err != nil {
			t.Fatal(err)
		}
		var d deaths
		s, adopts := runScan(t, b, ScanOptions{Every: 2 * time.Second, TTL: 10 * time.Second}, func(s *Scan) { s.Died = d.tell })
		synctest.Wait()
		for !view(s)["left"].Dead {
			listingAfter(t, s)
		}
		for range 15 {
			if got := listingAfter(t, s); !got["left"].Dead {
				t.Fatalf("a released node is listed live: %+v", got)
			}
		}
		if got := d.count("left"); got != 1 {
			t.Fatalf("died ran %d times for one release", got)
		}
		if got := adopts.Load(); got != 0 {
			t.Fatalf("%d asks to adopt a released lease", got)
		}
	})
}

// A node adopted is asked about no more. One not adopted is asked about again,
// with the lease as it was if nothing wrote it, and with the lease as it is now
// if the attempt changed it (the adopter here marks the lease and then fails, as
// an adoption whose journal step fails does): the next listing shows the ETag
// changed and reads the lease again.
func TestLeaseScanFollowsTheAdoption(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name       string
		done, mark bool
	}{{"done", true, false}, {"not done", false, false}, {"marked and not done", false, true}} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				b := blob.NewMemory()
				if _, err := lease.NewRegistry(b, "gone", lease.RegistryOptions{}).Acquire(ctx, lease.NodeInfo{Name: "gone"}); err != nil {
					t.Fatal(err)
				}
				var (
					mu    sync.Mutex
					seen  []lease.Record // the leases the scan asked to have adopted, in order
					calls notify.Counter
				)
				s, _ := runScan(t, b, ScanOptions{Every: 2 * time.Second, TTL: 10 * time.Second}, func(s *Scan) {
					s.Adopt = func(ctx context.Context, l *lease.Record) bool {
						defer calls.Inc()
						mu.Lock()
						seen = append(seen, *l)
						first := len(seen) == 1
						mu.Unlock()
						if !first {
							return false
						}
						if c.mark {
							if _, err := s.Registry.Adopt(ctx, *l, s.Self()); err != nil {
								t.Error(err)
							}
						}
						return c.done
					}
				})
				if err := calls.Wait(ctx, 1); err != nil {
					t.Fatal(err)
				}
				for range 5 {
					listingAfter(t, s)
				}
				mu.Lock()
				defer mu.Unlock()
				switch {
				case c.done:
					if len(seen) != 1 {
						t.Fatalf("asked %d times to adopt a node already adopted", len(seen))
					}
				case len(seen) < 2:
					t.Fatalf("asked %d times, not again after an adoption that wasn't done", len(seen))
				case c.mark:
					if last := seen[len(seen)-1]; last.ETag == seen[0].ETag || last.AdoptedBy == nil {
						t.Fatalf("still asked with the lease as it was before the mark: %+v", last)
					}
				default:
					for _, l := range seen[1:] {
						if l.ETag != seen[0].ETag {
							t.Fatalf("asked with another lease, though nothing wrote it: %+v then %+v", seen[0], l)
						}
					}
				}
			})
		})
	}
}
