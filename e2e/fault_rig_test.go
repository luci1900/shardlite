//go:build fault

package e2e

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/node"
	"github.com/luci1900/shardlite/internal/pos"
)

// The fault rig drives the real binary, built with the fault tag, through the
// e2e app, and kills it at a chosen point (docs/internals.md, Testing). The
// invariant after every kill: each write a sync-on request acknowledged is in
// blob storage, and every shard restores with its checksums verified and
// passes integrity_check.

func init() { nodeTags = []string{"fault"} }

const (
	faultWallet = "e2e.wallet/f1" // sync on: a 200 means the write is durable
	faultUsers  = "e2e.users/f1"  // sync none: a 200 means nothing yet
)

// faultEnv is what every node of the rig runs with: tails become deltas within
// a few snapshot passes, and a manifest with three files is due a rebase.
var faultEnv = []string{"SHARDLITE_DELTA_AGE=300ms", "SHARDLITE_MAX_FILES=2"}

// activationPoints are where an activation of a restored shard can be killed
// between the restore and the first sync: after the restore, as the node folds
// a leftover WAL into the database file, once the node holds its read
// transaction over the database, and once the first sync is done.
var activationPoints = []kill.Point{kill.Restored, kill.Folded, kill.ReadHeld, kill.FirstSyncDone}

type faultRig struct {
	t     *testing.T
	s     *stack
	v     *verifier
	n     int             // commits written
	acked map[string]bool // tokens a 200 from the sync-on shard covered: they must survive
	fires int
	fire  string   // the file whose existence arms the kill point of this life
	base  []string // the environment of every node of the rig
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
	return r
}

// restart kills the node, as losing the host would, and starts a new one with
// extra in its environment.
func (r *faultRig) restart(extra ...string) {
	r.t.Helper()
	r.crash()
	// The app still has the shard's file open from before the kill, and a new node
	// refuses a shard another process holds. The app closes idle files within moments.
	apptest.Eventually(r.t, apptest.Timeout, "the app to close its databases", func() bool { return r.s.appDebug().OpenDBs == 0 })
	r.s.env = append(append([]string(nil), r.base...), extra...)
	r.s.startNode()
}

// crash kills the node and checks what it logged.
func (r *faultRig) crash() {
	r.t.Helper()
	r.s.node.kill()
	// The capture went wrong somewhere: the fault tests must never see it.
	if out := r.s.node.out.String(); strings.Contains(out, "the capture went wrong somewhere") {
		r.t.Fatalf("the node logged a checksum error:\n%s", out)
	}
}

// arm restarts the node with the kill point set but not yet armed: the point
// fires once fireNow is called. The restart is a crash, so the next activation
// of a shard restores it from blob storage.
func (r *faultRig) arm(point kill.Point) {
	r.t.Helper()
	r.fires++
	r.fire = filepath.Join(r.s.dir, fmt.Sprintf("fire-%d", r.fires))
	r.restart("SHARDLITE_KILL_AT="+string(point), "SHARDLITE_KILL_WHEN="+r.fire)
}

// fireNow arms the point: from here the node dies when it reaches it.
func (r *faultRig) fireNow() {
	r.t.Helper()
	if err := os.WriteFile(r.fire, nil, 0o644); err != nil {
		r.t.Fatal(err)
	}
}

// dies waits for the node to die at its armed point.
func (r *faultRig) dies(point kill.Point) {
	r.t.Helper()
	select {
	case <-r.s.node.done:
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
func (r *faultRig) writeUntilDeath(point kill.Point) {
	r.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-r.s.node.done:
			return
		default:
		}
		_ = r.commit(faultWallet) // fails once the node is dying
		time.Sleep(50 * time.Millisecond)
	}
	r.t.Fatalf("the node did not die at %s", point)
}

// waitCached waits until the node has let go of the shard and kept its files.
func (r *faultRig) waitCached(shard string) {
	r.t.Helper()
	path := filepath.Join(r.s.dir, "data", node.LocalDBName)
	apptest.Eventually(r.t, apptest.Timeout, shard+" to be cached", func() bool {
		db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
		if err != nil {
			return false
		}
		defer db.Close()
		var state string
		return db.QueryRow(`SELECT state FROM shards WHERE name = ?`, shard).Scan(&state) == nil && state == "cached"
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
