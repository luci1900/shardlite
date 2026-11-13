//go:build fault

package e2e

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/model/pos"
	"github.com/luci1900/shardlite/internal/storage/audit"
	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/fault"
)

// The fault rig drives the real binary, built with the fault tag, through the
// e2e app, and kills it at a chosen point (docs/internals.md, Testing). The
// invariant after every kill: each write a sync-on request acknowledged is in
// blob storage, and every shard restores with its checksums verified and
// passes integrity_check.

func init() { nodeTags = []string{"fault"} }

const (
	faultWallet = "wallet/f1" // sync on: a 200 means the write is durable
	faultUsers  = "users/f1"  // sync none: a 200 means nothing yet
)

// faultEnv is what every node of the rig runs with: tails become deltas within
// a few snapshot passes, and a manifest with three files is due a rebase.
var faultEnv = []string{"SHARDLITE_DELTA_AGE=" + faultDeltaAge.String(), "SHARDLITE_FAULT_MAX_FILES=" + strconv.Itoa(faultMaxFiles)}

// faultMaxFiles is how many files a manifest of the rig lists before a rebase.
const faultMaxFiles = 2

// faultDeltaAge is the delta age of every node of the rig. A shard looks for a
// delta every 2 s (LifecycleOptions.SnapshotEvery), so a healthy tail reaches
// about this plus 2 s. At 1 s the tail alert (3 times this) stays quiet unless
// deltas really stall.
const faultDeltaAge = time.Second

// activationPoints are where an activation of a restored shard can be killed
// between the restore and the first sync: after the restore, as the node folds
// a leftover WAL into the database file, once the node holds its read
// transaction over the database, and once the first sync is done.
var activationPoints = []fault.Point{fault.Restored, fault.Folded, fault.ReadHeld, fault.FirstSyncDone}

type faultRig struct {
	t     *testing.T
	s     *stack
	v     *verifier
	n     int             // commits written
	acked map[string]bool // tokens a 200 from the sync-on shard covered: they must survive
	fires int
	fire  string   // the file whose existence arms the kill point of this life
	base  []string // the environment of every node of the rig

	killed *apptest.Proc   // the last node crash counted
	kills  int             // nodes the rig has killed, the final crash included
	logs   strings.Builder // what every node killed so far logged
	// reportedLeaks is for a scenario that fails every delete of an LTX file:
	// its nodes stop gracefully instead of being killed, the faulty node must
	// report a failed delete, and every file left must be in a node's log.
	reportedLeaks bool
}

// newFaultRig starts a node with faultEnv and extra, and creates the rig's shards.
// Creating retries, since a fault setting in extra can fail it.
func newFaultRig(t *testing.T, extra ...string) *faultRig {
	t.Helper()
	base := append(append([]string(nil), faultEnv...), extra...)
	s := newStack(t, base...)
	r := &faultRig{t: t, s: s, v: newVerifier(t, s), acked: map[string]bool{}, base: base}
	for _, shard := range []string{faultWallet, faultUsers} {
		for attempt := 0; ; attempt++ {
			st, body, err := s.control("POST", at(shard, "/create"), shard, "")
			if err == nil && st == 204 {
				break
			}
			if attempt == 200 {
				t.Fatalf("creating %s: %d %q %v", shard, st, body, err)
			}
		}
	}
	t.Cleanup(r.audit)
	return r
}

// audit is the end of every scenario, run as the rig's cleanup before the processes are
// killed (they are older cleanups): it stops the node, so no upload is on its way to a
// manifest, and walks the store for the LTX files no manifest lists (internal/storage/audit).
// A file a node logged is a leak it reported (a failed delete). The rest are allowed up
// to what the kills explain: per node killed, each of the rig's two shards can leave one
// upload on its way to the manifest (a delta, or a rebase's base) or, killed just after a
// rebase's swap, the files the swap superseded (faultMaxFiles+1). A scenario whose nodes
// stop gracefully (reportedLeaks) is allowed none. A scenario that failed is left as it is.
func (r *faultRig) audit() {
	if r.t.Failed() {
		return
	}
	r.crash()
	left, err := audit.Unlisted(context.Background(), r.v.files.Blob, 0, time.Now().Add(time.Hour))
	if err != nil {
		r.t.Errorf("the walk of the store after the scenario: %v", err)
	}
	logged := r.logs.String()
	unlogged := slices.DeleteFunc(left, func(key string) bool { return strings.Contains(logged, key) })
	limit := 2 * (faultMaxFiles + 1) * r.kills
	if r.reportedLeaks {
		limit = 0
	}
	if len(unlogged) > limit {
		r.t.Errorf("%d LTX files no manifest lists and no node logged after the scenario, and %d kills explain %d:\n%s", len(unlogged), r.kills, limit, strings.Join(unlogged, "\n"))
	}
}

// restart kills the node, as losing the host would, and starts a new one with
// extra in its environment.
func (r *faultRig) restart(extra ...string) {
	r.t.Helper()
	r.crash()
	// A request that was in flight at the kill still has its shard's file open until
	// it returns, and a new node refuses a shard another process holds.
	apptest.Eventually(r.t, apptest.Timeout, "the app to finish its requests and close their databases", func() bool { return r.s.appDebug().Serving == 0 })
	r.s.env = append(append([]string(nil), r.base...), extra...)
	r.s.startNode()
}

// crash kills the node and checks what it logged.
func (r *faultRig) crash() {
	r.t.Helper()
	if r.reportedLeaks {
		// A scenario that fails deletes runs no kills: a kill mid-sweep leaks files
		// silently, which is the crash's leak and not the failed delete's. The node
		// stops the way a supervisor stops it, so a sweep ends and reports.
		if err := r.s.node.Term(30 * time.Second); err != nil {
			r.t.Logf("the node did not stop cleanly: %v", err)
		}
	}
	r.s.node.Kill()
	if r.killed != r.s.node {
		r.killed = r.s.node
		r.kills++
		r.logs.WriteString(r.s.node.Output())
	}
	// The capture went wrong somewhere: the fault tests must never see it.
	if out := r.s.node.Output(); strings.Contains(out, "the capture went wrong somewhere") {
		r.t.Fatalf("the node logged a checksum error:\n%s", out)
	}
}

// arm restarts the node with the kill point set but not yet armed: the point
// fires once fireNow is called. The restart is a crash, so the next activation
// of a shard restores it from blob storage.
func (r *faultRig) arm(point fault.Point) {
	r.t.Helper()
	r.fires++
	r.fire = filepath.Join(r.s.dir, fmt.Sprintf("fire-%d", r.fires))
	r.restart("SHARDLITE_FAULT_KILL_AT="+string(point), "SHARDLITE_FAULT_KILL_WHEN="+r.fire)
}

// fireNow arms the point: from here the node dies when it reaches it.
func (r *faultRig) fireNow() {
	r.t.Helper()
	if err := os.WriteFile(r.fire, nil, 0o644); err != nil {
		r.t.Fatal(err)
	}
}

// dies waits for the node to die at its armed point.
func (r *faultRig) dies(point fault.Point) {
	r.t.Helper()
	select {
	case <-r.s.node.Done:
	case <-time.After(60 * time.Second):
		r.t.Fatalf("the node did not die at %s", point)
	}
}

var faultClient = &http.Client{Timeout: 10 * time.Second}

// commit writes the next numbered token to a shard. A 200 from the sync-on shard
// is the acknowledgement: the token is then in blob storage.
func (r *faultRig) commit(shard string) error {
	r.n++
	tok := "t" + strconv.Itoa(r.n)
	st, body, err := doRequest(faultClient, "POST", "http://"+r.s.pub+at(shard, "/add"), shard, tok)
	if err == nil && st != 200 {
		err = fmt.Errorf("status %d: %s", st, body)
	}
	if err == nil && shard == faultWallet {
		r.acked[tok] = true
	}
	return err
}

func (r *faultRig) mustCommit(shard string) {
	r.t.Helper()
	if err := r.commit(shard); err != nil {
		r.t.Fatalf("commit: %v", err)
	}
}

// touch asks for the shard, which activates it. It reports whether the node answered.
func (r *faultRig) touch(shard string) bool {
	st, _, err := doRequest(faultClient, "GET", "http://"+r.s.pub+at(shard, "/count"), shard, "")
	return err == nil && st == 200
}

// position is where the shard's manifest in blob storage has got to.
func (r *faultRig) position() pos.Position {
	m, _, err := r.v.files.GetManifest(context.Background(), shardName(faultWallet))
	if errors.Is(err, blob.ErrNotFound) {
		return 0
	} else if err != nil {
		r.t.Fatal(err)
	}
	return m.LastPosition()
}

// settle gives the shard some durable commits and n deltas (or rebases, as the
// manifest fills), so a crash has history to lose.
func (r *faultRig) settle(n int) {
	r.t.Helper()
	r.mustCommit(faultWallet)
	r.mustCommit(faultWallet)
	for range n {
		before := r.position()
		r.mustCommit(faultWallet)
		apptest.Eventually(r.t, apptest.Timeout, "a delta", func() bool { return r.position() > before })
		r.mustCommit(faultWallet)
	}
}

// writeUntilDeath keeps writing until the node dies at its armed point.
func (r *faultRig) writeUntilDeath(point fault.Point) {
	r.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-r.s.node.Done:
			return
		default:
		}
		_ = r.commit(faultWallet) // fails once the node is dying
		time.Sleep(50 * time.Millisecond)
	}
	r.t.Fatalf("the node did not die at %s", point)
}

// waitReleased waits until the node has let go of the shard: it is released in
// blob storage and its files are gone.
func (r *faultRig) waitReleased(shard string) {
	r.t.Helper()
	apptest.Eventually(r.t, apptest.Timeout, shard+" to be let go of", func() bool {
		files, err := filepath.Glob(filepath.Join(r.s.dir, "data", shard+".*"))
		return err == nil && len(files) == 0
	})
}

// verify restores the shard from blob storage and checks the invariant.
func (r *faultRig) verify(label string) {
	r.t.Helper()
	have, ok, err := r.v.tokens(faultWallet)
	if err != nil {
		r.t.Fatalf("%s: %v", label, err)
	}
	if !ok && len(r.acked) > 0 {
		r.t.Fatalf("%s: %d acknowledged writes and nothing durable", label, len(r.acked))
	}
	for tok := range r.acked {
		if !have[tok] {
			r.t.Fatalf("%s: %q was acknowledged but is missing from the restored shard (%d tokens in it, %d acknowledged)", label, tok, len(have), len(r.acked))
		}
	}
	for _, shard := range r.v.shards() {
		if _, _, err := r.v.tokens(shard); err != nil {
			r.t.Fatalf("%s: %v", label, err)
		}
	}
}

// recoverAndCheck is what follows every kill: the invariant holds on what the
// dead node left, a new node starts clean, and the shard keeps working and keeps
// its history.
func (r *faultRig) recoverAndCheck(label string) {
	r.t.Helper()
	r.crash()
	r.verify(label + ": after the kill")
	r.restart()
	for range 3 {
		if err := r.commit(faultWallet); err != nil {
			r.t.Fatalf("%s: commit after recovery: %v", label, err)
		}
	}
	r.verify(label + ": after recovery")
}

func faultSeed(t *testing.T) uint64 {
	seed := rand.Uint64()
	if v := os.Getenv("SHARDLITE_SEED"); v != "" {
		var err error
		if seed, err = strconv.ParseUint(v, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed: SHARDLITE_SEED=%d", seed)
		}
	})
	return seed
}
