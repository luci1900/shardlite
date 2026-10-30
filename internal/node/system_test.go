package node

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/migrate"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/system"
)

// probe is a system kind for tests. Its wake counts its runs in its own table
// and deletes its "run" reminder, so it runs once per activation that seeds it.
// POST /write commits a row, POST /sync commits a row unless ?empty is set and
// then waits for System.Sync, and POST /call calls the shard its ?to names with
// POST /write, through System.Call.
type probe struct {
	wakes notify.Counter              // wakes the system app answered
	runs  atomic.Int64                // the count of runs the last wake read, which the shard's file keeps
	wrote notify.Counter              // writes committed
	seen  atomic.Pointer[http.Header] // the headers of the last POST /write
}

const probeShard = "_system.probe/_cluster"

// probeV1 is the probe's schema, and probeV2 a release that adds a table.
var (
	probeV1 = fstest.MapFS{
		"1_init.up.sql":   {Data: []byte(`CREATE TABLE runs (n INTEGER NOT NULL); CREATE TABLE writes (n INTEGER NOT NULL)`)},
		"1_init.down.sql": {Data: []byte(`DROP TABLE runs; DROP TABLE writes`)},
	}
	probeV2 = fstest.MapFS{
		"1_init.up.sql":    probeV1["1_init.up.sql"],
		"1_init.down.sql":  probeV1["1_init.down.sql"],
		"2_extra.up.sql":   {Data: []byte(`CREATE TABLE extra (n INTEGER NOT NULL)`)},
		"2_extra.down.sql": {Data: []byte(`DROP TABLE extra`)},
	}
)

func (p *probe) kind() kind.Kind { return p.kindWith(probeV1) }

// kindWith is the probe kind with the migrations of one release.
func (p *probe) kindWith(migrations fstest.MapFS) kind.Kind {
	return p.kindAs("_system.probe", kind.SyncStorage, migrations)
}

// kindAs is a probe kind under another name or with another sync setting.
func (p *probe) kindAs(name string, sync kind.Sync, migrations fstest.MapFS) kind.Kind {
	return kind.Kind{
		Name: shard.Kind(name), Sync: sync, IDs: []shard.ID{"_cluster"}, Migrations: migrations,
		Handler: func(s *system.System) http.Handler {
			mux := http.NewServeMux()
			mux.HandleFunc("POST /_shardlite/reminders", func(w http.ResponseWriter, r *http.Request) {
				var n int64
				err := s.Tx(r.Context(), func(tx *sql.Tx) error {
					if _, err := tx.Exec(`INSERT INTO runs VALUES (1)`); err != nil {
						return err
					}
					if _, err := tx.Exec(`DELETE FROM _shardlite_reminders WHERE key = 'run'`); err != nil {
						return err
					}
					return tx.QueryRow(`SELECT count(*) FROM runs`).Scan(&n)
				})
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				p.runs.Store(n)
				p.wakes.Inc()
			})
			mux.HandleFunc("POST /write", func(w http.ResponseWriter, r *http.Request) {
				hdr := r.Header.Clone()
				p.seen.Store(&hdr)
				err := s.Tx(r.Context(), func(tx *sql.Tx) error {
					_, err := tx.Exec(`INSERT INTO writes VALUES (1)`)
					return err
				})
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				p.wrote.Inc()
				w.WriteHeader(http.StatusCreated)
			})
			mux.HandleFunc("POST /sync", func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("empty") == "" {
					if err := s.Tx(r.Context(), func(tx *sql.Tx) error {
						_, err := tx.Exec(`INSERT INTO writes VALUES (1)`)
						return err
					}); err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
					p.wrote.Inc()
				}
				if err := s.Sync(r.Context()); err != nil {
					http.Error(w, err.Error(), http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusCreated)
			})
			mux.HandleFunc("POST /call", func(w http.ResponseWriter, r *http.Request) {
				req, _ := http.NewRequest(http.MethodPost, "/write", nil)
				req.Header.Set(system.Target, r.URL.Query().Get("to"))
				resp, err := s.Call(r.Context(), req)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadGateway)
					return
				}
				resp.Body.Close()
				w.WriteHeader(resp.StatusCode)
			})
			return mux
		},
	}
}

// probeCluster starts n nodes running the probe kind, with leases that expire
// in real time and quick passes of the claimer and the lease scan.
func probeCluster(t *testing.T, n int, vars map[string]string, tune ...func(*CatalogOptions)) (*testCluster, *probe) {
	t.Helper()
	p := &probe{}
	env := map[string]string{"SHARDLITE_LEASE_TTL": "1s", "SHARDLITE_IDLE_TIMEOUT": "50ms"}
	for k, v := range vars {
		env[k] = v
	}
	c := newClusterEnv(t, n, env, func(o *CatalogOptions) {
		o.System = []kind.Kind{p.kind()}
		o.Claims = ClaimOptions{Every: 50 * time.Millisecond, Retry: 20 * time.Millisecond}
		o.Leases.Every = 50 * time.Millisecond
		for _, f := range tune {
			f(o)
		}
	})
	return c, p
}

// owners are the running nodes the probe shard is active on.
func owners(c *testCluster) []*clusterNode {
	var out []*clusterNode
	for _, nd := range c.nodes {
		if m := nd.m.Load(); nd.cancel != nil && m != nil && m.IsActive(shardName(probeShard)) {
			out = append(out, nd)
		}
	}
	return out
}

// afterClaimPasses returns once nd's claimer has made n more passes.
func afterClaimPasses(t *testing.T, nd *clusterNode, n uint64) {
	t.Helper()
	st := &nd.m.Load().Stats
	waitCounter(t, "passes of the claimer", &st.ClaimPasses, st.ClaimPasses.Load()+n)
}

func waitRuns(t *testing.T, p *probe, wakes uint64, runs int64) {
	t.Helper()
	waitCounter(t, "wakes of the probe", &p.wakes, wakes)
	if got := p.runs.Load(); got != runs {
		t.Fatalf("the wake counted %d runs, want %d", got, runs)
	}
}

// A system shard with a fixed id is claimed by one node, seeded and woken at
// once, and stays active past the idle timeout. When its owner stops, and when
// its owner is killed, another node claims it and its state carries over: the
// seed is inserted again only because the wake deleted it.
func TestSystemShardIsClaimedAndMoves(t *testing.T) {
	c, p := probeCluster(t, 2, nil)
	waitRuns(t, p, 1, 1)
	for _, nd := range c.nodes {
		afterClaimPasses(t, nd, 3) // well past the idle timeout
	}
	own := owners(c)
	if len(own) != 1 {
		t.Fatalf("the probe is active on %d nodes, want 1", len(own))
	}
	first := own[0]
	other := c.nodes[0]
	if other == first {
		other = c.nodes[1]
	}

	first.Stop()
	waitRuns(t, p, 2, 2)
	eventually(t, "the probe on the other node", func() bool { return other.m.Load().IsActive(shardName(probeShard)) })

	first.Start()
	other.cycle() // the wake's commit is in the journal: the probe counts before the gate
	other.Kill()
	waitRuns(t, p, 3, 3)
	if own := owners(c); len(own) != 1 || own[0] != first {
		t.Fatalf("after the kill the probe is active on %v, want only %s", own, first.env.Name)
	}
}

// kickClaims kicks nd's claimer n times, waiting for each pass.
func kickClaims(t *testing.T, nd *clusterNode, n int) {
	t.Helper()
	m := nd.m.Load()
	for range n {
		next := m.Stats.ClaimPasses.Load() + 1
		m.claim.kick.Kick()
		waitCounter(t, "a kicked pass of the claimer", &m.Stats.ClaimPasses, next)
	}
}

// In a steady cluster a kicked pass of the claimer reads nothing: the owner
// keeps the shard active, and the other node's owner cache names a live owner.
// (The slow pass reads the manifest anyway: TestClaimerSlowPassChecksTheOwnerCache.)
func TestClaimerReadsNothingInASteadyCluster(t *testing.T) {
	c, p := probeCluster(t, 2, nil, func(o *CatalogOptions) { o.Claims.Every = time.Hour })
	waitRuns(t, p, 1, 1)
	for _, nd := range c.nodes {
		st := &nd.m.Load().Stats
		waitCounter(t, "listings of the leases", &st.LeaseScans, st.LeaseScans.Load()+2) // both nodes listed alive
		kickClaims(t, nd, 1)                                                             // and the owner in the cache
	}
	var reads atomic.Int32
	manifest := shardfile.ManifestKey(shardName(probeShard))
	for _, nd := range c.nodes {
		nd.onStore(func(_ context.Context, op blobtest.Op, key string, after bool) error {
			if !after && strings.HasSuffix(key, manifest) {
				reads.Add(1)
			}
			return nil
		})
	}
	for _, nd := range c.nodes {
		kickClaims(t, nd, 3)
	}
	if n := reads.Load(); n != 0 {
		t.Fatalf("%d requests for the probe's manifest in a steady cluster", n)
	}
}

// A pass at a kick or a retry trusts the owner cache, and the slow pass doesn't:
// it acquires a fixed id the cache places on a live node, so an entry naming a
// node that gave the shard up is corrected within the slow schedule.
func TestClaimerSlowPassChecksTheOwnerCache(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	var acquires atomic.Int32
	c := &claimer{
		targets: []shard.Name{shardName(probeShard)},
		active:  func(shard.Name) bool { return false },
		owner:   func(shard.Name) (cluster.Node, bool) { return "n1", true },
		live:    func(cluster.Node) bool { return true },
		acquire: func(context.Context, shard.Name) (*Hold, error) {
			acquires.Add(1)
			return nil, fmt.Errorf("%w: n1", shardfile.ErrOwnedElsewhere)
		},
		clock: clk, opts: ClaimOptions{Every: 10 * time.Minute, Retry: time.Second}, log: slog.Default(),
		kick: notify.NewKick(), adopted: &notify.Counter{}, passes: &notify.Counter{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	pass := func(what string, n uint64, do func(), want int32) {
		t.Helper()
		do()
		waitCounter(t, what, c.passes, n)
		if got := acquires.Load(); got != want {
			t.Fatalf("after %s: %d acquires, want %d", what, got, want)
		}
	}
	pass("the startup pass", 1, func() {}, 0)
	pass("a kicked pass", 2, c.kick.Kick, 0)
	pass("the slow pass", 3, func() { clk.Advance(10 * time.Minute) }, 1)
	pass("another kicked pass", 4, c.kick.Kick, 1)
}

// A system app's answer waits at the gate: nothing leaves before what the
// request committed is in the node's journal.
func TestSystemAnswerWaitsForDurability(t *testing.T) {
	p := &probe{}
	r := newSRigWith(t, func(o *CatalogOptions) {
		o.System = []kind.Kind{p.kind()}
		o.Claims = ClaimOptions{Every: time.Hour}
	})
	waitRuns(t, p, 1, 1)

	var block atomic.Bool
	entered, release := make(chan struct{}, 1), make(chan struct{})
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpCreate && strings.HasPrefix(key, journal.Root) && block.Load() {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		return nil
	})
	block.Store(true)
	done := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, "/write", nil)
		req.Header.Set(hdrTarget, probeShard)
		resp, err := r.srv.call(req)
		if err != nil {
			t.Error(err)
			done <- 0
			return
		}
		done <- resp.StatusCode
	}()
	waitCounter(t, "the write", &p.wrote, 1)
	<-entered
	select {
	case s := <-done:
		t.Fatalf("answered %d while the journal upload was held", s)
	default:
	}
	block.Store(false)
	close(release)
	if s := <-done; s != http.StatusCreated {
		t.Fatalf("status %d, want 201", s)
	}
}

// probeCall sends the probe shard named target a request for path through the
// node's routing, as the node's own call, and returns its status.
func probeCall(t *testing.T, srv *Server, target, path string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, path, nil)
	req.Header.Set(hdrTarget, target)
	resp, err := srv.call(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// holdUploads holds the node journal's uploads while hold is set, and reports
// on entered when one is held.
func holdUploads(r *srig) (hold *atomic.Bool, entered chan struct{}, release func()) {
	hold = new(atomic.Bool)
	entered, gate := make(chan struct{}, 1), make(chan struct{})
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpCreate && strings.HasPrefix(key, journal.Root) && hold.Load() {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	return hold, entered, sync.OnceFunc(func() { close(gate) })
}

// A system kind with sync off answers without waiting for the journal, and
// System.Sync waits for the shard's own commits: until they are in the journal,
// at once when it has nothing new, and it fails once the lease is lost.
func TestSystemSyncOff(t *testing.T) {
	p := &probe{}
	const shard = "_system.lazy/_cluster"
	r := newSRigWith(t, func(o *CatalogOptions) {
		o.System = []kind.Kind{p.kindAs("_system.lazy", kind.SyncNone, probeV1)}
		o.Claims = ClaimOptions{Every: time.Hour}
	})
	waitRuns(t, p, 1, 1)
	hold, entered, release := holdUploads(r)
	defer release()
	hold.Store(true)
	wrote := make(chan int, 1)
	go func() { wrote <- probeCall(t, r.srv, shard, "/write") }()
	<-entered // the write's segment is held
	select {
	case s := <-wrote:
		if s != http.StatusCreated {
			t.Fatalf("a write answered %d", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a sync-off system kind's answer waited for the journal")
	}

	done := make(chan int, 1)
	go func() { done <- probeCall(t, r.srv, shard, "/sync") }()
	waitCounter(t, "the second write", &p.wrote, 2)
	select {
	case s := <-done:
		t.Fatalf("a sync answered %d while the journal upload was held", s)
	default:
	}
	hold.Store(false)
	release()
	if s := <-done; s != http.StatusCreated {
		t.Fatalf("a sync answered %d", s)
	}

	// Nothing new: Sync returns at once, even with uploads held.
	hold, _, release = holdUploads(r)
	defer release()
	hold.Store(true)
	if s := probeCall(t, r.srv, shard, "/sync?empty=1"); s != http.StatusCreated {
		t.Fatalf("a sync with nothing new answered %d", s)
	}
	hold.Store(false)

	// Another life of the node takes the lease: Sync fails.
	other := coord.NewRegistry(r.local, "n1", coord.RegistryOptions{TTL: time.Hour})
	if _, err := other.Acquire(context.Background(), coord.NodeInfo{Name: "n1"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the lease to be lost", func() bool { return r.n.Keeper.Check() != nil })
	sh := r.m.tab.load(shardName(shard)).a
	if err := sh.gate(context.Background(), kind.SyncStorage, false); err == nil {
		t.Fatal("System.Sync passed after the lease was lost")
	}
}

// A system app reaches another system shard with System.Call, through the
// node's routing.
func TestSystemCallReachesAnotherSystemShard(t *testing.T) {
	caller, callee := &probe{}, &probe{}
	r := newSRigWith(t, func(o *CatalogOptions) {
		o.System = []kind.Kind{caller.kind(), callee.kindAs("_system.callee", kind.SyncStorage, probeV1)}
		o.Claims = ClaimOptions{Every: time.Hour}
	})
	waitRuns(t, caller, 1, 1)
	waitRuns(t, callee, 1, 1)
	if s := probeCall(t, r.srv, probeShard, "/call?to=_system.callee/_cluster"); s != http.StatusCreated {
		t.Fatalf("the call answered %d", s)
	}
	if n := callee.wrote.Load(); n != 1 {
		t.Fatalf("the callee took %d writes, want 1", n)
	}
	if n := caller.wrote.Load(); n != 0 {
		t.Fatalf("the caller took %d writes, want 0", n)
	}
	// The system app is told which shard the node opened, not a target.
	hdr := callee.seen.Load()
	if hdr == nil || hdr.Get(system.Opened) != "_system.callee/_cluster" || hdr.Get(system.Target) != "" || hdr.Get(hdrCaller) != probeShard {
		t.Fatalf("the callee's system app saw %v, want Shardlite-Opened _system.callee/_cluster, no Shardlite-Target and Shardlite-Caller %s", hdr, probeShard)
	}
}

// A system kind is a kind that isn't public: a client (and a client's request
// forwarded from another node) gets 404 as for an undeclared kind, a call from
// the app reaches its system app, but only for a fixed id, and a path under
// /_shardlite/ never gets through a listener.
func TestSystemKindsAreReachedOnlyByCalls(t *testing.T) {
	c, p := probeCluster(t, 1, nil)
	waitRuns(t, p, 1, 1)
	nd := c.nodes[0]
	h2c := new(http.Protocols)
	h2c.SetUnencryptedHTTP2(true)
	for _, tc := range []struct {
		name, addr, target, path string
		hdr                      map[string]string
		client                   *http.Client
		want                     int
	}{
		{"client", nd.Public, probeShard, "/write", nil, http.DefaultClient, http.StatusNotFound},
		{"forwarded client", nd.Peer, probeShard, "/write", map[string]string{hdrOrigin: originClient}, &http.Client{Transport: &http.Transport{Protocols: h2c}}, http.StatusNotFound},
		{"call to an id that isn't fixed", nd.Control, "_system.probe/other", "/write", nil, http.DefaultClient, http.StatusBadRequest},
		{"call to the node's path", nd.Control, probeShard, "/_shardlite/reminders", nil, http.DefaultClient, http.StatusNotFound},
		{"call", nd.Control, probeShard, "/write", nil, http.DefaultClient, http.StatusCreated},
	} {
		req, _ := http.NewRequest(http.MethodPost, "http://"+tc.addr+tc.path, nil)
		req.Header.Set(hdrTarget, tc.target)
		for k, v := range tc.hdr {
			req.Header.Set(k, v)
		}
		resp, err := tc.client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s: status %d, want %d", tc.name, resp.StatusCode, tc.want)
		}
	}
	if n := p.wrote.Load(); n != 1 {
		t.Fatalf("%d writes reached the system app, want the call's 1", n)
	}
	if n := p.wakes.Load(); n != 1 {
		t.Fatalf("%d wakes, want only the node's", n)
	}
}

// An app that answers its ready route with 500 makes the node not ready, and
// stops nothing of a system kind's.
func TestUnhealthyAppDoesNotStopSystemWakes(t *testing.T) {
	c, p := probeCluster(t, 1, nil)
	nd := c.nodes[0]
	nd.sick.Store(true)
	waitRuns(t, p, 1, 1)
	var st int
	eventually(t, "the node to report not ready", func() bool {
		resp, err := http.Get("http://" + nd.Public + readyPath)
		if err != nil {
			return false
		}
		resp.Body.Close()
		st = resp.StatusCode
		return st == http.StatusServiceUnavailable
	})
	if st != http.StatusServiceUnavailable {
		t.Fatalf("ready answered %d with the app unhealthy", st)
	}
}

// The registry refuses system kinds that break the rules of names and fixed ids.
func TestSystemKindRules(t *testing.T) {
	h := func(*system.System) http.Handler { return http.NotFoundHandler() }
	for _, tc := range []struct {
		name string
		list []kind.Kind
	}{
		{"name without _", []kind.Kind{{Name: "probe", IDs: []shard.ID{"_cluster"}, Handler: h}}},
		{"id without _", []kind.Kind{{Name: "_system.probe", IDs: []shard.ID{"cluster"}, Handler: h}}},
		{"no ids", []kind.Kind{{Name: "_system.probe", Handler: h}}},
		{"no handler", []kind.Kind{{Name: "_system.probe", IDs: []shard.ID{"_cluster"}}}},
		{"id twice", []kind.Kind{{Name: "_system.probe", IDs: []shard.ID{"_0", "_0"}, Handler: h}}},
		{"kind twice", []kind.Kind{{Name: "_system.probe", IDs: []shard.ID{"_cluster"}, Handler: h}, {Name: "_system.probe", IDs: []shard.ID{"_cluster"}, Handler: h}}},
	} {
		if _, _, err := newSystemApp(tc.list, 0); err == nil {
			t.Errorf("%s: accepted", tc.name)
		}
	}
}

// Activation seeds a system shard's "run" reminder only if it has none: one the
// system app set is kept.
func TestSystemSeedKeepsTheAppsReminder(t *testing.T) {
	p := &probe{}
	k := p.kind()
	r := newSRigWith(t, func(o *CatalogOptions) {
		o.System = []kind.Kind{k}
		o.Claims = ClaimOptions{Every: time.Hour}
	})
	waitRuns(t, p, 1, 1)
	a := r.shard(probeShard)
	ctx := context.Background()
	const later = 4102444800 // 2100
	set := func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO _shardlite_reminders (key, due) VALUES ('run', ?)`, later)
		return err
	}
	if err := a.tx(ctx, set); err != nil {
		t.Fatal(err)
	}
	if err := ensureTables(ctx, a.cap.load(), &k, time.Now().Unix(), 0); err != nil {
		t.Fatal(err)
	}
	var due int64
	if err := a.tx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT due FROM _shardlite_reminders WHERE key = 'run'`).Scan(&due)
	}); err != nil {
		t.Fatal(err)
	}
	if due != later {
		t.Fatalf("the run reminder is due at %d, want %d", due, later)
	}
}

// A "run" reminder that went stale while the cluster was down (due longer ago than
// reminders are kept) is due now again at activation: the loop ignores a stale
// one, so a system shard would never run again.
func TestSystemSeedRefreshesAStaleReminder(t *testing.T) {
	p := &probe{}
	k := p.kind()
	r := newSRigWith(t, func(o *CatalogOptions) {
		o.System = []kind.Kind{k}
		o.Claims = ClaimOptions{Every: time.Hour}
	})
	waitRuns(t, p, 1, 1)
	a := r.shard(probeShard)
	ctx := context.Background()
	now := time.Now().Unix()
	for _, c := range []struct {
		name  string
		due   int64
		fresh bool
	}{{"stale", now - 100, true}, {"not stale", now - 10, false}} {
		if err := a.tx(ctx, func(tx *sql.Tx) error {
			_, err := tx.Exec(`UPDATE _shardlite_reminders SET due = ? WHERE key = 'run'`, c.due)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := ensureTables(ctx, a.cap.load(), &k, now, now-50); err != nil {
			t.Fatal(err)
		}
		var due int64
		if err := a.tx(ctx, func(tx *sql.Tx) error {
			return tx.QueryRow(`SELECT due FROM _shardlite_reminders WHERE key = 'run'`).Scan(&due)
		}); err != nil {
			t.Fatal(err)
		}
		if want := map[bool]int64{true: now, false: c.due}[c.fresh]; due != want {
			t.Fatalf("%s: the run reminder is due at %d, want %d", c.name, due, want)
		}
	}
}

// userVersion is the system shard's version, through its connection.
func userVersion(t *testing.T, a *actor) int {
	t.Helper()
	var v int
	if err := a.tx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`PRAGMA user_version`).Scan(&v)
	}); err != nil {
		t.Fatal(err)
	}
	return v
}

// Activation applies a system kind's migrations: a new shard gets every one,
// a shard at an older version only the missing ones, and a shard at a newer
// version is refused with nothing changed.
func TestSystemMigrationsAtActivation(t *testing.T) {
	p := &probe{}
	v1, v2 := p.kindWith(probeV1), p.kindWith(probeV2)
	r := newSRigWith(t, func(o *CatalogOptions) {
		o.System = []kind.Kind{v2}
		o.Claims = ClaimOptions{Every: time.Hour}
	})
	waitRuns(t, p, 1, 1)
	a := r.shard(probeShard)
	if v := userVersion(t, a); v != 2 {
		t.Fatalf("a new shard is at version %d, want 2", v)
	}
	ctx := context.Background()
	// Back to version 1, as an older release left it: activating again runs only
	// the second migration (the first would fail, its tables being there).
	if err := a.tx(ctx, func(tx *sql.Tx) error {
		return migrate.Migrate(ctx, tx, probeV2, 1)
	}); err != nil {
		t.Fatal(err)
	}
	if err := ensureTables(ctx, a.cap.load(), &v2, time.Now().Unix(), 0); err != nil {
		t.Fatal(err)
	}
	if v := userVersion(t, a); v != 2 {
		t.Fatalf("an older shard is at version %d after activation, want 2", v)
	}
	if err := ensureTables(ctx, a.cap.load(), &v1, time.Now().Unix(), 0); !errors.Is(err, migrate.ErrNewer) {
		t.Fatalf("an older release activated a newer shard: %v", err)
	}
	if v := userVersion(t, a); v != 2 {
		t.Fatalf("a refused shard is at version %d, want it untouched at 2", v)
	}
}

// A node of an older release refuses a system shard a newer one migrated, and
// gives it up rather than keeping it claimed, so the newer node claims it again.
func TestSystemShardNewerThanTheNodeIsGivenUp(t *testing.T) {
	p := &probe{}
	vars := map[string]string{"SHARDLITE_LEASE_TTL": "1s", "SHARDLITE_IDLE_TIMEOUT": "50ms"}
	tune := func(migrations fstest.MapFS) func(*CatalogOptions) {
		return func(o *CatalogOptions) {
			o.System = []kind.Kind{p.kindWith(migrations)}
			o.Claims = ClaimOptions{Every: 50 * time.Millisecond, Retry: 20 * time.Millisecond}
			o.Leases.Every = 50 * time.Millisecond
		}
	}
	c := newClusterEnv(t, 1, vars, tune(probeV2))
	newer := c.nodes[0]
	waitRuns(t, p, 1, 1)
	older := c.newNode("n1")
	older.tune = tune(probeV1)
	c.nodes = append(c.nodes, older)
	older.Start()

	// A stop leaves the newer node's name in the manifest, so an owner of none
	// means the older node claimed the probe and gave it up.
	newer.Stop()
	eventually(t, "the probe claimed and given up by the older node", func() bool {
		m, _, err := shardfile.NewManifestStore(blob.NewStore(c.mem, "check", 1, blob.Options{})).Get(context.Background(), shardName(probeShard))
		return err == nil && m.Owner == ""
	})
	afterClaimPasses(t, older, 3) // and doesn't claim it again
	if older.m.Load().IsActive(shardName(probeShard)) || p.wakes.Load() != 1 {
		t.Fatal("the older node served the probe")
	}
	m, _, err := shardfile.NewManifestStore(blob.NewStore(c.mem, "check", 1, blob.Options{})).Get(context.Background(), shardName(probeShard))
	if err != nil || m.Owner != "" {
		t.Fatalf("the older node claimed the probe again: %+v %v", m, err)
	}

	newer.Start()
	waitRuns(t, p, 2, 2)
	eventually(t, "the probe on the newer node", func() bool { return newer.m.Load().IsActive(shardName(probeShard)) })
}
