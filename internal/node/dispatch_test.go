package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/dispatch"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/blobtest"
	"github.com/luci1900/shardlite/internal/test/harness"
)

// ringer makes n's app hand out its shards' paths (for requests that aren't
// wakes) and answer each wake by deleting the reminders, counting the wakes.
func ringer(t *testing.T, n *clusterNode, app *apptest.App) (paths chan string, rung *atomic.Int32) {
	paths, rung = make(chan string, 1), &atomic.Int32{}
	n.serve(func(w http.ResponseWriter, q *http.Request) {
		path := q.Header.Get("Shardlite-File")
		if q.URL.Path != remindersPath {
			paths <- remindIn(t, app, q)
			return
		}
		app.Must(t, "open", path, path)
		app.Must(t, "exec", path, `DELETE FROM _shardlite_reminders`)
		app.Must(t, "close", path)
		rung.Add(1)
	})
	return paths, rung
}

// remindIn is what an app does with a request that isn't a wake: one with a
// Remind-Due header puts a timer row due then into its shard, while the request
// holds the shard (so it can't go idle first). It returns the shard's path.
func remindIn(t *testing.T, app *apptest.App, q *http.Request) string {
	path := q.Header.Get("Shardlite-File")
	if due := q.Header.Get("Remind-Due"); due != "" {
		app.Must(t, "open", path, path)
		app.Must(t, "exec", path, `INSERT INTO _shardlite_reminders (key, due) VALUES ('k', `+due+`)`)
		app.Must(t, "close", path)
	}
	return path
}

// remind has n's app put a timer row due at due into shard, creating the shard
// if it doesn't exist (clients can only reach shards that exist).
func remind(t *testing.T, n *clusterNode, paths chan string, shard string, due int64) {
	t.Helper()
	if resp, body := send(t, "GET", "http://"+n.Control+"/", map[string]string{"Shardlite-Target": shard, "Remind-Due": fmt.Sprint(due)}, ""); resp.StatusCode != 200 {
		t.Fatalf("%s through %s: %d %q", shard, n.env.Name, resp.StatusCode, body)
	}
	<-paths
}

// dispatchCluster is a cluster on one fake clock whose leases never look old and
// whose nodes wake inactive shards through the dispatcher.
func dispatchCluster(t *testing.T, clk *harness.FakeClock) *testCluster {
	return newClusterWith(t, 2, func(o *ShardsetOptions) {
		o.Clock = clk
		o.Leases.TTL = 1000 * time.Hour
	})
}

// idleWithReminder puts a timer row due at due into shard on n and waits until the
// shard is let go of there. Its manifest then names the reminder and the reminder
// index has the entry for it.
func idleWithReminder(t *testing.T, clk *harness.FakeClock, n *clusterNode, paths chan string, shard string, due int64) {
	t.Helper()
	remind(t, n, paths, shard, due)
	m := n.m.Load()
	advanceUntil(t, clk, 100*time.Millisecond, "deactivation", func() bool { return !m.IsActive(shardName(shard)) })
	mf, _, err := m.n.Files.GetManifest(context.Background(), shardName(shard))
	if err != nil {
		t.Fatal(err)
	}
	if mf.NextReminder != shardfile.MinuteOf(due) {
		t.Fatalf("%s's manifest has next_reminder %d, want %d", shard, mf.NextReminder, shardfile.MinuteOf(due))
	}
}

// allEntries lists every entry in the reminder index, whatever its minute and lane.
func allEntries(t *testing.T, store *blob.Store) []shardfile.Reminder {
	t.Helper()
	var out []shardfile.Reminder
	for i := range shard.Lanes {
		got, err := shardfile.NewReminderIndex(store).Due(context.Background(), shard.Lane(i), time.Now().AddDate(10, 0, 0), 100000)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, got...)
	}
	return out
}

// entriesOf lists the shard's entries in the reminder index.
func entriesOf(t *testing.T, n *clusterNode, name string) []shardfile.Reminder {
	t.Helper()
	var out []shardfile.Reminder
	for _, e := range allEntries(t, n.m.Load().n.Store) {
		if e.Shard.String() == name {
			out = append(out, e)
		}
	}
	return out
}

// A shard deactivated with a reminder an hour out is dispatched in that minute
// and woken, with no wake loop: the dispatch claims the released shard on the node
// the placer picks, and that node's app answers the wake.
func TestDispatchWakesAnIdleShard(t *testing.T) {
	clk := harness.NewFakeClock()
	c := dispatchCluster(t, clk)
	n0, n1 := c.nodes[0], c.nodes[1]
	app := apptest.Start(t)
	paths, rung0 := ringer(t, n0, app)
	_, rung1 := ringer(t, n1, app)
	rung := func() int32 { return rung0.Load() + rung1.Load() }
	due := clk.Now().Unix() + 3600
	idleWithReminder(t, clk, n0, paths, "brief/d", due)
	if got := entriesOf(t, n0, "brief/d"); len(got) != 1 || got[0].Minute != shardfile.MinuteOf(due) {
		t.Fatalf("entries %+v, want one for the minute of %d", got, due)
	}

	clk.Advance(time.Duration(due-clk.Now().Unix()-120) * time.Second)
	if rung() != 0 {
		t.Fatal("the shard was woken before it was due")
	}
	advanceUntil(t, clk, 100*time.Millisecond, "the row to be delivered", func() bool { return rung() == 1 })
	if late := clk.Now().Unix() - due; late > 150 {
		t.Fatalf("delivered %d s late, want within about a minute", late)
	}
	// The shard's next manifest write finds no reminder, so the entry is not kept
	// beyond its dispatch: it is moved while the shard is active, and pruned after.
	advanceUntil(t, clk, 100*time.Millisecond, "deactivation", func() bool {
		return !n0.m.Load().IsActive(shardName("brief/d")) && !n1.m.Load().IsActive(shardName("brief/d"))
	})
	if mf, _, err := n0.m.Load().n.Files.GetManifest(context.Background(), shardName("brief/d")); err != nil || mf.NextReminder != 0 {
		t.Fatalf("manifest %+v (%v), want no next_reminder after the wake", mf, err)
	}
	if rung() != 1 {
		t.Fatalf("the handler ran %d times", rung())
	}
}

// A deactivation whose entry can't be created fails and the shard stays active, as
// a failed entry keeps it active, and it deactivates once storage answers.
func TestFailedEntryKeepsTheShardActive(t *testing.T) {
	clk := harness.NewFakeClock()
	c := dispatchCluster(t, clk)
	n0 := c.nodes[0]
	app := apptest.Start(t)
	paths, _ := ringer(t, n0, app)
	var refused atomic.Int32
	n0.onStore(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && strings.HasPrefix(key, shardfile.ReminderPrefix) && !after {
			refused.Add(1)
			return errors.New("storage is down")
		}
		return nil
	})
	remind(t, n0, paths, "brief/f", clk.Now().Unix()+3600)
	advanceUntil(t, clk, 100*time.Millisecond, "the entry to be refused", func() bool { return refused.Load() > 0 })
	if !n0.m.Load().IsActive(shardName("brief/f")) {
		t.Fatal("the shard went idle without its entry")
	}
	n0.onStore(func(context.Context, blobtest.Op, string, bool) error { return nil })
	advanceUntil(t, clk, 100*time.Millisecond, "deactivation", func() bool { return !n0.m.Load().IsActive(shardName("brief/f")) })
	if got := entriesOf(t, n0, "brief/f"); len(got) != 1 {
		t.Fatalf("entries %+v, want one", got)
	}
}

// A due shard that nobody owns, and that the dispatching node doesn't have, is
// claimed by the call that dispatches it on the other node, which the placer
// picks, and woken there. The dispatching node is the one that ranks first for
// the shard's lane, and only it dispatches. (The shard of a node that stopped is
// claimed once the lease scan counts the node dead, which
// TestAdopterEntryWakesAKilledOwnersActiveShard covers.)
func TestDispatchClaimsAShardNobodyOwns(t *testing.T) {
	clk := harness.NewFakeClock()
	c := dispatchCluster(t, clk)
	app := apptest.Start(t)
	rungOn := map[*clusterNode]*atomic.Int32{}
	pathsOn := map[*clusterNode]chan string{}
	for _, n := range c.nodes {
		pathsOn[n], rungOn[n] = ringer(t, n, app)
	}
	const name = "brief/s"
	// The nodes agree on who ranks first for the shard's lane once each has listed the
	// cluster, and the shard goes idle on the other node.
	advanceUntil(t, clk, 100*time.Millisecond, "both nodes to list the cluster", func() bool {
		for _, n := range c.nodes {
			if len(n.m.Load().leases.Nodes().Live()) != len(c.nodes) {
				return false
			}
		}
		return true
	})
	runner := dispatch.Runner(c.nodes[0].m.Load().leases.Nodes().Live(), shardName(name).Lane())
	var dispatcher, other *clusterNode
	for _, n := range c.nodes {
		if n.env.Name == runner {
			dispatcher = n
		} else {
			other = n
		}
	}
	// The placer picks at random among the live nodes, itself included, so this
	// one is made to pick the other node.
	dispatcher.placeOn(other.env.Name)
	for _, n := range c.nodes {
		waitCounter(t, "the dispatcher to have run on "+string(n.env.Name), &n.m.Load().dispatcher.Passes, 1)
	}
	due := clk.Now().Unix() + 3600
	idleWithReminder(t, clk, other, pathsOn[other], name, due)
	clk.Advance(time.Duration(due-clk.Now().Unix()) * time.Second)
	advanceUntil(t, clk, 100*time.Millisecond, "the row to be delivered on the other node", func() bool { return rungOn[other].Load() == 1 })
	if rungOn[dispatcher].Load() != 0 {
		t.Fatal("the dispatching node claimed a shard the placer put elsewhere")
	}
	if got := dispatcher.m.Load().dispatcher.Dispatched.Load(); got == 0 {
		t.Fatal("the node that ranks first for the lane sent no dispatch")
	}
	if got := other.m.Load().dispatcher.Dispatched.Load(); got != 0 {
		t.Fatalf("the other node sent %d dispatches for a lane it doesn't run", got)
	}
}

// After its owner is killed, the adopter records an entry due now for each tail it
// writes a delta for, and _dispatch wakes an active shard whose reminder was only
// in the dead node's journal. Leases expire in real time here.
func TestAdopterEntryWakesAKilledOwnersActiveShard(t *testing.T) {
	c := newClusterEnv(t, 2, map[string]string{"SHARDLITE_LEASE_TTL": "1s"}, func(o *ShardsetOptions) {
		o.Leases.Every = 50 * time.Millisecond
		o.Dispatch = &dispatch.Options{Every: time.Second}
	})
	n0, n1 := c.nodes[0], c.nodes[1]
	app := apptest.Start(t)
	paths := make(chan string, 1)
	n0.serve(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == remindersPath {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		paths <- remindIn(t, app, q)
	})
	_, rung1 := ringer(t, n1, app)
	remind(t, n0, paths, "users/k", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "the reminder's commit to be in the journal", func() bool {
		a := n0.m.Load().tab.load(shardName("users/k"))
		return a != nil && a.a != nil && !a.a.cap.hasNewFrames()
	})
	n0.Kill()
	waitCounter(t, "the adoption of n0", &n1.m.Load().Stats.Adoptions, 1)
	apptest.Eventually(t, apptest.Timeout, "the row to be delivered on n1", func() bool { return rung1.Load() == 1 })
}
