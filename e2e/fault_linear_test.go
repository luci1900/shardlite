//go:build fault

package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/linear"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/s3test"
)

// The cluster check: three nodes on one prefix, each reaching the store through
// a proxy of its own, with clients appending to lists and reading them through
// random nodes while a nemesis kills, pauses, cuts off and drains one node at a
// time. Every node runs with no lease expiry, so every pause is one it cannot
// see. Other clients set reminders throughout, some on shards left idle.
// Afterwards every list is read through a node and from a restore, the history
// of each list must be linearizable (internal/linear) and every reminder must
// have fired at least once in the restores.

const (
	linearTTL     = 5 * time.Second
	linearAdopt   = 6 * linearTTL // the longest a fault waits for the others to adopt the node
	linearSettle  = time.Minute   // the longest the clients take to reach every shard after a change
	linearAwake   = time.Second   // how long a paused or cut node stays awake and cut off before the heal
	linearShards  = 4
	linearClients = 6
	linearPerList = 100 // appends a list takes before its shard's clients move to the next

	linearIdleShards      = 2 // shards of a short-idle kind the reminder clients leave idle
	linearReminderClients = 2
	linearRemindWait      = 2 * time.Minute // the longest the reminders take to fire once the clients end
	linearRemindIn        = 2               // seconds until a reminder the clients set is due
	linearRemindEvery     = time.Second     // how long a reminder client waits between two sets
)

// faultKind is one fault of the nemesis.
type faultKind string

const (
	faultKill  faultKind = "kill"
	faultPause faultKind = "pause"
	faultCut   faultKind = "cut"
	faultDrain faultKind = "drain"
)

var faultKinds = []faultKind{faultKill, faultPause, faultCut, faultDrain}

// linearNode is a node of the cluster, with its app and its storage proxy.
type linearNode struct {
	s     *stack
	proxy *storageProxy
}

// linearCluster is the check's three nodes and what the test knows about them.
type linearCluster struct {
	t      *testing.T
	nodes  []*linearNode
	v      *verifier
	shards []string

	mu    sync.Mutex
	lists map[string]*listState // per shard: the list its clients append to now

	avoid atomic.Int32 // 1 + the node the clients stay off, 0 for none

	idle []string // shards of a short-idle kind (arena), set reminders on and then left alone

	lives [][]string             // per node, the output of each life that ended
	kept  map[*apptest.Proc]bool // the lives whose output is in lives
}

type listState struct{ idx, appends int }

func newCluster(t *testing.T, env ...string) *linearCluster {
	t.Helper()
	store := s3test.Setup(t)
	u, err := url.Parse(store.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	c := &linearCluster{t: t, lists: map[string]*listState{}, lives: make([][]string, 3), kept: map[*apptest.Proc]bool{}}
	base := append(append([]string(nil), faultEnv...),
		"SHARDLITE_LEASE_TTL="+linearTTL.String(),
		"SHARDLITE_FAULT_NO_LEASE_EXPIRY=true",
		"SHARDLITE_FAULT_STORAGE=lost:20:0s:segments,lost:20:0s:files")
	base = append(base, env...)
	for i := range 3 {
		p := newStorageProxy(t, u.Host)
		nodeEnv := append(append([]string(nil), base...), "SHARDLITE_NODE=n"+strconv.Itoa(i+1), "SHARDLITE_S3_ENDPOINT="+u.Scheme+"://"+p.addr())
		c.nodes = append(c.nodes, &linearNode{s: newStackOn(t, store, nodeEnv...), proxy: p})
	}
	c.v = newVerifier(t, c.nodes[0].s)
	for i := range linearShards {
		c.shards = append(c.shards, fmt.Sprintf("linear/s%d", i))
	}
	// The reminder clients' idle shards: arena idles in 300 ms, so a shard is
	// inactive long before its reminder is due and only the dispatcher's wake
	// fires it.
	for i := range linearIdleShards {
		c.idle = append(c.idle, fmt.Sprintf("arena/r%d", i))
	}
	c.nodes[0].s.create(t, slices.Concat(c.shards, c.idle)...)
	return c
}

// list is the list of shard that clients append to and read now. An append
// counts towards moving to the next.
func (c *linearCluster) list(shard string, isAppend bool) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.lists[shard]
	if st == nil {
		st = &listState{}
		c.lists[shard] = st
	}
	name := "l" + strconv.Itoa(st.idx)
	if isAppend {
		if st.appends++; st.appends >= linearPerList {
			st.idx, st.appends = st.idx+1, 0
		}
	}
	return name
}

// up restarts node i if it has ended, once its app has closed the databases of
// the requests the dead node had in flight.
func (c *linearCluster) up(i int) {
	c.t.Helper()
	n := c.nodes[i].s
	select {
	case <-n.node.Done:
	default:
		return
	}
	c.ended(i)
	apptest.Eventually(c.t, apptest.Timeout, "the app to finish its requests and close their databases", func() bool { return n.appDebug().Serving == 0 })
	n.startNode()
}

// ended keeps what node i's current life logged, and fails the test if it
// logged a checksum error.
func (c *linearCluster) ended(i int) {
	c.t.Helper()
	p := c.nodes[i].s.node
	if c.kept[p] {
		return
	}
	c.kept[p] = true
	out := p.Output()
	c.lives[i] = append(c.lives[i], out)
	if strings.Contains(out, "the capture went wrong somewhere") {
		c.t.Fatalf("n%d logged a checksum error:\n%s", i+1, out)
	}
}

// logLives logs the output of every life of every node.
func (c *linearCluster) logLives() {
	for i := range c.nodes {
		lives := c.lives[i]
		if p := c.nodes[i].s.node; !c.kept[p] {
			lives = append(lives, p.Output())
		}
		for j, out := range lives {
			c.t.Logf("n%d, life %d of %d:\n%s", i+1, j+1, len(lives), out)
		}
	}
}

// answer is what a client learnt from one request.
type answer struct {
	outcome linear.Outcome
	body    string
	why     string // the status and Shardlite-Error, or the transport error, for a report
}

func ask(client *http.Client, method, u, target, body string) answer {
	q, err := http.NewRequest(method, u, strings.NewReader(body))
	if err != nil {
		panic(err) // the test built a bad request
	}
	q.Header.Set("Shardlite-Target", target)
	resp, err := client.Do(q)
	if err != nil {
		return answer{outcome: linear.Answer(nil, err), why: err.Error()}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return answer{outcome: linear.Answer(nil, err), why: err.Error()}
	}
	return answer{outcome: linear.Answer(resp, nil), body: string(b), why: fmt.Sprintf("%d %s", resp.StatusCode, resp.Header.Get("Shardlite-Error"))}
}

// counters are the clients' running counts. The nemesis reads them before and
// after a phase (tallies) to see what the phase achieved.
type counters struct {
	outcomes [3]atomic.Int64 // by linear.Outcome
	relayed  atomic.Int64    // known relayed appends
	appends  map[string]*atomic.Int64
	reads    map[string]*atomic.Int64 // per shard: known appends and known reads
}

func newCounters(shards []string) *counters {
	k := &counters{appends: map[string]*atomic.Int64{}, reads: map[string]*atomic.Int64{}}
	for _, s := range shards {
		k.appends[s], k.reads[s] = new(atomic.Int64), new(atomic.Int64)
	}
	return k
}

func (k *counters) count(op *linear.Op) {
	k.outcomes[op.Outcome].Add(1)
	if op.Outcome != linear.Known {
		return
	}
	switch {
	case !op.Append:
		k.reads[op.Key.Shard].Add(1)
	case op.Via != "":
		k.relayed.Add(1)
		fallthrough
	default:
		k.appends[op.Key.Shard].Add(1)
	}
}

// tallies are the counters at one moment.
type tallies struct {
	outcomes [3]int64
	relayed  int64
	appends  map[string]int64
	reads    map[string]int64
}

func (k *counters) now() tallies {
	t := tallies{relayed: k.relayed.Load(), appends: map[string]int64{}, reads: map[string]int64{}}
	for o := range t.outcomes {
		t.outcomes[o] = k.outcomes[o].Load()
	}
	for s := range k.appends {
		t.appends[s], t.reads[s] = k.appends[s].Load(), k.reads[s].Load()
	}
	return t
}

// everyShard waits until every shard has a known append (and a known read, if
// reads) since mark, and reports false if that took longer than linearSettle.
func (k *counters) everyShard(mark tallies, reads bool) bool {
	deadline := time.Now().Add(linearSettle)
	for {
		now, all := k.now(), true
		for s := range now.appends {
			if now.appends[s] == mark.appends[s] || reads && now.reads[s] == mark.reads[s] {
				all = false
			}
		}
		if all {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// pick is the address of a node for a client's next request, a random one that
// is not the faulted node the clients stay off (see avoid).
func (c *linearCluster) pick(rng *rand.Rand) string {
	n := rng.IntN(len(c.nodes))
	if a := int(c.avoid.Load()) - 1; n == a {
		n = (a + 1 + rng.IntN(len(c.nodes)-1)) % len(c.nodes)
	}
	return c.nodes[n].s.pub
}

// run is one client: it appends to lists, directly and relayed by another
// shard, and reads them, each time through a random node, until ctx ends.
func (c *linearCluster) run(ctx context.Context, id int, seed uint64, rec *linear.Recorder, k *counters) error {
	rng := rand.New(rand.NewPCG(seed, uint64(id)))
	// Connections are kept for reuse: a fresh one per request runs a machine out
	// of local ports within a run.
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 8}}
	for n := 0; ctx.Err() == nil; n++ {
		// The clients stay off a paused or cut node, as a load balancer's
		// health check would, so it commits nothing while it is faulted.
		node := c.pick(rng)
		shard := c.shards[rng.IntN(len(c.shards))]
		value := fmt.Sprintf("c%d.%d", id, n)
		var op *linear.Op
		var a answer
		switch r := rng.IntN(10); {
		case r < 4:
			list := c.list(shard, true)
			op = rec.Begin(id, linear.Key{Shard: shard, List: list}, "", true, value)
			a = ask(client, "POST", "http://"+node+at(shard, "/append")+"?list="+list, shard, value)
		case r < 6:
			via := c.shards[(slices.Index(c.shards, shard)+1+rng.IntN(len(c.shards)-1))%len(c.shards)]
			list := c.list(shard, true)
			op = rec.Begin(id, linear.Key{Shard: shard, List: list}, via, true, value)
			a = ask(client, "POST", "http://"+node+at(via, "/relay")+"?to="+url.QueryEscape(shard)+"&list="+list, via, value)
		default:
			list := c.list(shard, false)
			op = rec.Begin(id, linear.Key{Shard: shard, List: list}, "", false, "")
			a = ask(client, "GET", "http://"+node+at(shard, "/list")+"?list="+list, shard, "")
		}
		var seen []string
		if !op.Append && a.outcome == linear.Known {
			if err := json.Unmarshal([]byte(a.body), &seen); err != nil {
				return fmt.Errorf("client %d: a read answered 200 with %q: %v", id, a.body, err)
			}
		}
		rec.End(op, a.outcome, seen)
		k.count(op)
		if a.outcome != linear.Known {
			time.Sleep(20 * time.Millisecond) // back off after an error, as a client honouring Retry-After would
		}
	}
	return nil
}

// reminderSets is what the reminder clients set: every key whose set a node
// acknowledged, by shard.
type reminderSets struct {
	mu   sync.Mutex
	keys map[string][]string
}

func newReminderSets() *reminderSets { return &reminderSets{keys: map[string][]string{}} }

func (s *reminderSets) add(shard, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[shard] = append(s.keys[shard], key)
}

// list returns every acknowledged set as a Reminder for the reminder rule.
func (s *reminderSets) list() []linear.Reminder {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []linear.Reminder
	for shard, keys := range s.keys {
		for _, k := range keys {
			out = append(out, linear.Reminder{Shard: shard, Key: k})
		}
	}
	return out
}

// runReminder is one reminder client: it sets reminders until ctx ends, each
// through a random node and due linearRemindIn later, on the lists' shards
// (whose kind idles slowly, so the owner's own loop fires them) and on the idle
// shards (which nothing else touches, so the dispatcher wakes an inactive shard
// for them). It records every key whose set a node acknowledged.
func (c *linearCluster) runReminder(ctx context.Context, id int, seed uint64, sets *reminderSets) error {
	rng := rand.New(rand.NewPCG(seed, 1<<40+uint64(id))) // a stream of its own, off the clients' and the nemesis's
	shards := slices.Concat(c.shards, c.idle)
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 8}}
	for n := 0; ctx.Err() == nil; n++ {
		shard := shards[rng.IntN(len(shards))]
		key := fmt.Sprintf("r%d.%d", id, n)
		a := ask(client, "POST", "http://"+c.pick(rng)+at(shard, "/remind")+"?key="+key+"&in="+strconv.Itoa(linearRemindIn), shard, "")
		if a.outcome == linear.Known {
			sets.add(shard, key)
		}
		time.Sleep(linearRemindEvery)
	}
	return nil
}

// fault injects kind on node i, heals it and brings the node back, and reports
// whether the fault took effect.
//
// A kill is healed once another node has begun adopting the dead one, and took
// effect if the node died of SIGKILL and was adopted. A drain took effect if
// the node stopped cleanly. A pause or a cut is held until the adoption has
// finished and every shard has an acknowledged append since, so the new owners
// have newer writes than the faulted node read. Then the node is awake and cut
// off from storage for linearAwake (a paused node's proxy is cut just before it
// resumes), and probe reads every shard through it. The clients stay off the
// node from the start of the fault, so it has committed nothing since, and the
// lists' kind has a long idle timeout, so its shards are still active: letting a
// shard go and waiting for a flush both need storage, and either would hold the
// read. The node believes it still owns its shards, and only the confirmation
// stops it answering a stale read, since neither its renewal nor its lease read
// reaches storage until the heal. The pause or cut took effect if all of that
// happened and the node stopped after the heal, finding its lease taken.
func (c *linearCluster) fault(kind faultKind, i int, k *counters, rec *linear.Recorder) bool {
	c.t.Helper()
	n := c.nodes[i]
	took := false
	switch kind {
	case faultKill:
		n.s.node.Kill()
		took = killed(n.s.node.Err) && c.adopting(i)
	case faultDrain:
		err := n.s.node.Term(60 * time.Second)
		if err != nil {
			c.t.Logf("n%d did not drain cleanly: %v", i+1, err)
		}
		took = err == nil
	case faultPause, faultCut:
		c.avoid.Store(int32(i + 1))
		defer c.avoid.Store(0)
		epoch := c.lease(i).Epoch
		held := n.proxy.held.Load()
		if kind == faultPause {
			n.s.node.Signal(syscall.SIGSTOP)
		} else {
			n.proxy.cut()
		}
		took = c.adoptedUpTo(i, epoch)
		if took {
			took = k.everyShard(k.now(), false)
			if !took {
				c.t.Logf("n%d's shards took no acknowledged append within %s of its adoption", i+1, linearSettle)
			}
		}
		if kind == faultPause {
			n.proxy.cut()
			n.s.node.Signal(syscall.SIGCONT)
		}
		probed := c.probe(i, rec)
		time.Sleep(linearAwake) // the fault: the node is awake and cut off this long
		c.t.Logf("n%d awake and cut off, every shard read through it: %s", i+1, probed)
		n.proxy.heal()
		if took {
			c.ends(i)
		}
		took = took && n.proxy.held.Load() > held
	}
	c.up(i)
	return took
}

// owner picks a node that owns a shard now, by the manifests: only a node that
// owns a shard can answer a stale read once it is paused or cut off and woken.
// A node that owns none reads the manifest for every request, which the cut
// holds. If no node owns a shard, it picks any.
func (c *linearCluster) owner(rng *rand.Rand) int {
	c.t.Helper()
	var owners []int
	for _, shard := range c.shards {
		m, _, err := c.v.files.GetManifest(context.Background(), shardName(shard))
		if err != nil {
			c.t.Fatal(err)
		}
		var n int // the nodes are n1, n2 and n3; a released shard has no owner
		if _, err := fmt.Sscanf(string(m.Owner), "n%d", &n); err == nil && !slices.Contains(owners, n-1) {
			owners = append(owners, n-1)
		}
	}
	if len(owners) == 0 {
		return rng.IntN(len(c.nodes))
	}
	slices.Sort(owners)
	return owners[rng.IntN(len(owners))]
}

// probe reads every shard's current list through node i, at once, into the
// history while the node is awake and cut off. The clients' own reads reach it
// only by chance, and this makes sure a stale owner is asked. Each read is a
// client of its own, since the reads are concurrent and Porcupine takes a
// client to be sequential. It returns what the reads got, for the round's log.
func (c *linearCluster) probe(i int, rec *linear.Recorder) string {
	client := &http.Client{Timeout: linearAwake}
	answers := make([]string, len(c.shards))
	var g errgroup.Group
	for j, shard := range c.shards {
		g.Go(func() error {
			list := c.list(shard, false)
			op := rec.Begin(linearClients+2+j, linear.Key{Shard: shard, List: list}, "", false, "")
			a := ask(client, "GET", "http://"+c.nodes[i].s.pub+at(shard, "/list")+"?list="+list, shard, "")
			var seen []string
			if a.outcome == linear.Known && json.Unmarshal([]byte(a.body), &seen) != nil {
				a.outcome, a.why = linear.Indeterminate, "200 with a body that is not a list" // noted in the log, not checked
			}
			rec.End(op, a.outcome, seen)
			answers[j] = shard + " " + a.why
			return nil
		})
	}
	g.Wait()
	return strings.Join(answers, ", ")
}

// lease is node i's lease as storage has it now.
func (c *linearCluster) lease(i int) *lease.Record {
	c.t.Helper()
	l, err := c.v.files.Leases.Get(context.Background(), cluster.Node("n"+strconv.Itoa(i+1)))
	if err != nil {
		c.t.Fatal(err)
	}
	return l
}

// adopting waits until another node has begun adopting node i, as it does a
// dead node, and reports false if that took longer than linearAdopt.
func (c *linearCluster) adopting(i int) bool {
	return c.waitLease(i, "begun adopting", func(l *lease.Record) bool {
		// Adopting names the adopter, and finishing clears it and marks the
		// node's epoch adopted, which a fast adoption does between two looks.
		return l.AdoptedBy != nil && l.AdoptedBy.Node != l.Name || l.Adopted >= l.Epoch
	})
}

// adoptedUpTo waits until node i's journal has been adopted up to epoch, and
// reports false if that took longer than linearAdopt.
func (c *linearCluster) adoptedUpTo(i int, epoch pos.Epoch) bool {
	return c.waitLease(i, "finished adopting", func(l *lease.Record) bool { return l.Adopted >= epoch })
}

func (c *linearCluster) waitLease(i int, what string, done func(*lease.Record) bool) bool {
	c.t.Helper()
	for deadline := time.Now().Add(linearAdopt); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if done(c.lease(i)) {
			return true
		}
	}
	c.t.Logf("no node had %s n%d within %s", what, i+1, linearAdopt)
	return false
}

// ends waits for node i to end, as a node that finds its lease taken does.
func (c *linearCluster) ends(i int) {
	c.t.Helper()
	select {
	case <-c.nodes[i].s.node.Done:
	case <-time.After(3 * linearTTL):
		c.t.Fatalf("n%d was adopted while it could not see, and has not stopped %s after it could", i+1, 3*linearTTL)
	}
}

// ownership says who blob storage says owns shard and what each node's lease
// says, for a report.
func (c *linearCluster) ownership(shard string) string {
	var b strings.Builder
	m, _, err := c.v.files.GetManifest(context.Background(), shardName(shard))
	if err != nil {
		fmt.Fprintf(&b, "%s's manifest: %v\n", shard, err)
	} else {
		fmt.Fprintf(&b, "%s's manifest: owner %q at epoch %d, journal %q, rev %d\n", shard, m.Owner, m.NodeEpoch, m.Journal, m.Rev)
	}
	for i := range c.nodes {
		name := cluster.Node("n" + strconv.Itoa(i+1))
		l, err := c.v.files.Leases.Get(context.Background(), name)
		if err != nil {
			fmt.Fprintf(&b, "%s's lease: %v\n", name, err)
			continue
		}
		fmt.Fprintf(&b, "%s's lease: epoch %d, released %v, adopted by %v, adopted to %d\n", name, l.Epoch, l.Released, l.AdoptedBy, l.Adopted)
	}
	return b.String()
}

// restored reads a shard's lists, the values its relay route recorded and its
// reminder sets and firings from a restore of blob storage.
func (c *linearCluster) restored(shard string) (lists map[string][]string, relayed []string, rem linear.Restored) {
	c.t.Helper()
	lists = map[string][]string{}
	ok, err := c.v.restored(shard, func(db *sql.DB, version int) error {
		if version == 0 {
			return nil
		}
		rs, err := db.Query(`SELECT token FROM rows`)
		if err != nil {
			return err
		}
		for rs.Next() {
			var tok string
			if err := rs.Scan(&tok); err != nil {
				return err
			}
			// The tokens this test's routes record: a relayed value, a reminder
			// set and a reminder firing.
			switch {
			case strings.HasPrefix(tok, "relay:"):
				relayed = append(relayed, strings.TrimPrefix(tok, "relay:"))
			case strings.HasPrefix(tok, "set:"):
				rem.Set = append(rem.Set, strings.TrimPrefix(tok, "set:"))
			case strings.HasPrefix(tok, "fired:"):
				rem.Fired = append(rem.Fired, strings.TrimPrefix(tok, "fired:"))
			}
		}
		rs.Close()
		if version < 2 { // no lists table yet
			return rs.Err()
		}
		rs, err = db.Query(`SELECT list, value FROM lists ORDER BY list, seq`)
		if err != nil {
			return err
		}
		defer rs.Close()
		for rs.Next() {
			var list, value string
			if err := rs.Scan(&list, &value); err != nil {
				return err
			}
			lists[list] = append(lists[list], value)
		}
		return rs.Err()
	})
	if err != nil {
		c.t.Fatalf("restoring %s: %v", shard, err)
	}
	if !ok {
		c.t.Fatalf("%s has nothing durable", shard)
	}
	return lists, relayed, rem
}

// clusterRun is what a run of the cluster check found: the history, how many
// lists it has, and what the checker and the relay rule made of it.
type clusterRun struct {
	ops   []linear.Op
	lists int
	check error
	relay error
}

// runCluster runs the cluster check for SHARDLITE_ROUNDS rounds (8 if unset),
// with env added to every node's. Each round picks one of kinds and a node from
// a seeded generator (SHARDLITE_SEED repeats a run's choices), every kind once
// in each len(kinds) rounds. It fails t if the run proves too little (a kind
// that never took effect, a clean phase with nothing acknowledged, no relayed
// append or reminder set acknowledged, no reminder on an idle shard fired) or
// if a reminder never fires, and leaves the verdict on the history to the
// caller.
func runCluster(t *testing.T, kinds []faultKind, env ...string) clusterRun {
	t.Helper()
	seed := faultSeed(t)
	rounds := 8
	if v := os.Getenv("SHARDLITE_ROUNDS"); v != "" {
		var err error
		if rounds, err = strconv.Atoi(v); err != nil || rounds < len(kinds) {
			t.Fatalf("SHARDLITE_ROUNDS is %q, want at least %d", v, len(kinds))
		}
	}
	c := newCluster(t, env...)
	t.Cleanup(func() {
		if t.Failed() {
			c.logLives()
		}
	})
	rec := linear.NewRecorder()
	k := newCounters(c.shards)
	sets := newReminderSets()

	ctx, stop := context.WithCancel(context.Background())
	defer stop() // a test that fails early stops its clients too
	var clients errgroup.Group
	for id := range linearClients {
		clients.Go(func() error { return c.run(ctx, id, seed, rec, k) })
	}
	for id := range linearReminderClients {
		clients.Go(func() error { return c.runReminder(ctx, id, seed, sets) })
	}

	// The nemesis.
	rng := rand.New(rand.NewPCG(seed, 1<<32))
	var order []faultKind
	took := map[faultKind]int{}
	for r := range rounds {
		if len(order) == 0 {
			order = append([]faultKind(nil), kinds...)
			rng.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })
		}
		kind, i := order[0], rng.IntN(len(c.nodes))
		if kind == faultPause || kind == faultCut {
			i = c.owner(rng)
		}
		order = order[1:]
		start := k.now()
		effect := c.fault(kind, i, k, rec)
		if effect {
			took[kind]++
		}
		// The clean phase: every node is up, and it lasts until every shard has
		// an acknowledged append and a known read since the heal.
		healed := k.now()
		if !k.everyShard(healed, true) {
			t.Fatalf("round %d: not every shard had an acknowledged append and a known read within %s of the heal: the cluster did not recover", r, linearSettle)
		}
		end := k.now()
		t.Logf("round %d: %s n%d (took effect: %v): %d known, %d failed, %d indeterminate",
			r, kind, i+1, effect, end.outcomes[linear.Known]-start.outcomes[linear.Known],
			end.outcomes[linear.Failed]-start.outcomes[linear.Failed], end.outcomes[linear.Indeterminate]-start.outcomes[linear.Indeterminate])
	}
	stop()
	if err := clients.Wait(); err != nil {
		t.Fatal(err)
	}
	for _, kind := range kinds {
		if took[kind] == 0 {
			t.Fatalf("no %s took effect in %d rounds", kind, rounds)
		}
	}
	if k.relayed.Load() == 0 {
		t.Fatal("no relayed append was acknowledged")
	}
	acked := sets.list()
	if len(acked) == 0 {
		t.Fatal("no reminder set was acknowledged")
	}

	// The final reads: every list once through a node, then once from a restore,
	// each starting after everything before it returned.
	keys := map[linear.Key]bool{}
	for _, op := range rec.Ops() {
		keys[op.Key] = true
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for k := range keys {
		var last answer
		for deadline := time.Now().Add(apptest.Timeout); last.outcome != linear.Known; {
			if time.Now().After(deadline) {
				t.Fatalf("a final read of %s did not succeed in %s, the last answer: %s %q\n%s", k, apptest.Timeout, last.why, last.body, c.ownership(k.Shard))
			}
			op := rec.Begin(linearClients, k, "", false, "")
			last = ask(client, "GET", "http://"+c.nodes[0].s.pub+at(k.Shard, "/list")+"?list="+k.List, k.Shard, "")
			var seen []string
			if last.outcome == linear.Known {
				if err := json.Unmarshal([]byte(last.body), &seen); err != nil {
					t.Fatalf("a final read answered 200 with %q: %v", last.body, err)
				}
			}
			rec.End(op, last.outcome, seen)
			if last.outcome != linear.Known {
				time.Sleep(100 * time.Millisecond) // back off, as the clients do
			}
		}
	}
	// The restores: each shard's lists and relayed values (one more read that
	// starts after everything before it returned) and its reminder sets and
	// firings.
	all := slices.Concat(c.shards, c.idle)
	finals := map[linear.Key][]string{}
	relays := map[string][]string{}
	rems := map[string]linear.Restored{}
	for _, shard := range all {
		lists, rel, rem := c.restored(shard)
		relays[shard], rems[shard] = rel, rem
		for k := range keys {
			if k.Shard != shard {
				continue
			}
			op := rec.Begin(linearClients+1, k, "", false, "")
			rec.End(op, linear.Known, lists[k.List])
			finals[k] = lists[k.List]
		}
	}

	// Every reminder must have fired at least once: every set acknowledged and
	// every set in a restore (a set the client never heard back about may still
	// have happened). The wait is bounded at linearRemindWait, since a wake's
	// backoff reaches a minute and the dispatcher wakes an inactive shard within
	// the minute its reminder is due in: running out of time is a failure, not a
	// slow cluster.
	for deadline := time.Now().Add(linearRemindWait); ; {
		err := linear.CheckReminders(acked, rems)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not every reminder fired within %s of the clients ending:\n%v", linearRemindWait, err)
		}
		time.Sleep(2 * time.Second)
		for _, shard := range all {
			_, _, rems[shard] = c.restored(shard)
		}
	}
	// A firing on an idle shard, or the run never exercised the dispatcher's
	// wake of an inactive shard.
	idle := false
	for _, shard := range c.idle {
		if len(rems[shard].Fired) > 0 {
			idle = true
		}
	}
	if !idle {
		t.Fatalf("no reminder on an idle shard fired within %s", linearRemindWait)
	}
	fired := 0
	for _, rem := range rems {
		fired += len(rem.Fired)
	}
	t.Logf("%d reminder sets acknowledged, %d firings in the restores", len(acked), fired)

	for i := range c.nodes {
		c.ended(i) // the nodes still up: their output is checked too
	}
	ops := rec.Ops()
	html := filepath.Join(os.TempDir(), fmt.Sprintf("shardlite-linear-%s-%d.html", t.Name(), seed))
	return clusterRun{ops: ops, lists: len(keys), check: linear.Check(ops, html, 2*time.Minute), relay: linear.CheckRelay(ops, finals, relays)}
}

// TestFaultCluster runs the cluster check with every fault kind. The history
// and the relay rule must pass, and every reminder the clients set must have
// fired (runCluster fails the test if one never does).
func TestFaultCluster(t *testing.T) {
	t.Parallel()
	r := runCluster(t, faultKinds)
	if r.check != nil {
		t.Fatal(r.check)
	}
	if r.relay != nil {
		t.Fatal(r.relay)
	}
	t.Logf("%d operations on %d lists are linearizable", len(r.ops), r.lists)
}

// TestFaultLinearCatchesUnconfirmedReads runs the cluster check with storage
// answers that leave without the confirmation (SHARDLITE_FAULT_UNCONFIRMED) and
// only the faults that leave a stale owner awake, pauses and cuts. A woken owner
// then serves stale reads, and the check must report a violation: it shows the
// check can fail, end to end. The relay rule must still hold: the sabotage
// skips the ownership confirmation, not the durability a relayed append's call
// waits for.
func TestFaultLinearCatchesUnconfirmedReads(t *testing.T) {
	t.Parallel()
	r := runCluster(t, []faultKind{faultPause, faultCut}, "SHARDLITE_FAULT_UNCONFIRMED=true")
	switch {
	case r.check == nil:
		t.Fatalf("%d operations on %d lists are linearizable with storage answers unconfirmed: the run never let a stale owner answer", len(r.ops), r.lists)
	case !errors.Is(r.check, linear.ErrViolation):
		t.Fatalf("the check failed without finding a violation: %v", r.check) // running out of time finds nothing
	}
	if r.relay != nil {
		t.Fatal(r.relay)
	}
	t.Logf("the check caught the unconfirmed reads: %v", r.check)
}
