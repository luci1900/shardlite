package node

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/migrate"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// System kinds (docs/internals.md, System kinds): shards the node keeps for
// itself under reserved kinds, each served by a system app in process. They are
// kinds like any other that aren't public: calls reach them, clients don't, and
// what only the node may send goes under /_shardlite/, which no listener lets
// through. Their names are qualified by the node's own app (_system), like
// every other kind's.

var fixedIDName = regexp.MustCompile(`^_[A-Za-z0-9_-]{1,127}$`)

// systemKinds is the registry of system kinds, by name.
type systemKinds map[shard.Kind]*kind.Kind

// newSystemApp checks the kinds the node runs and builds the system app's
// declaration: the same structure an app's ready route answers with,
// constructed here in code (docs/internals.md, System kinds). It also returns the
// registry the catalog and the claimer work from, whose entries alias the
// list. Kinds that leave their idle timeout at zero get the node's default,
// as an app's kinds without one do.
func newSystemApp(list []kind.Kind, defaultIdle time.Duration) (kind.App, systemKinds, error) {
	s := make(systemKinds, len(list))
	for i := range list {
		k := &list[i]
		appName, _, ok := shard.Split(k.Name)
		switch {
		case !ok || !shard.ValidKindName(string(k.Name)):
			return kind.App{}, nil, fmt.Errorf("system kind %q: the name must be an app's name and a kind's name", k.Name)
		case appName != shard.SystemApp:
			return kind.App{}, nil, fmt.Errorf("system kind %q: the node's own kinds belong to the %q app", k.Name, shard.SystemApp)
		case s[k.Name] != nil:
			return kind.App{}, nil, fmt.Errorf("system kind %q is declared twice", k.Name)
		case len(k.IDs) == 0:
			return kind.App{}, nil, fmt.Errorf("system kind %q has no fixed ids", k.Name)
		case k.Handler == nil:
			return kind.App{}, nil, fmt.Errorf("system kind %q has no handler", k.Name)
		}
		for j, id := range k.IDs {
			if !fixedIDName.MatchString(string(id)) {
				return kind.App{}, nil, fmt.Errorf("system kind %q: fixed id %q must be _ and an id", k.Name, id)
			}
			if slices.Contains(k.IDs[:j], id) {
				return kind.App{}, nil, fmt.Errorf("system kind %q: fixed id %q is listed twice", k.Name, id)
			}
		}
		if k.IdleTimeout == 0 {
			k.IdleTimeout = defaultIdle
		}
		s[k.Name] = k
	}
	app := kind.App{Name: shard.SystemApp, Kinds: make(map[shard.Kind]kind.Kind, len(s))}
	for name, k := range s {
		app.Kinds[name] = *k
	}
	return app, s, nil
}

// targets are the names of every fixed id, which the claimer keeps claimed.
func (s systemKinds) targets() []shard.Name {
	var out []shard.Name
	for _, k := range s {
		for _, id := range k.IDs {
			out = append(out, shard.New(k.Name, id))
		}
	}
	slices.SortFunc(out, func(a, b shard.Name) int { return cmp.Compare(a.String(), b.String()) })
	return out
}

// tx is System.Tx: fn in a transaction on capture's connection, under the
// shard's capture lock. Nothing but the shard's system app writes a system
// shard, and capture.Shard.Use runs one caller at a time, so a deferred BEGIN
// never meets another writer.
func (a *actor) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	return a.useDB(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit()
	})
}

// call is System.Call: a request of the system app's to another system shard,
// through the node's routing with the shard as its caller, so it waits for what
// the shard committed before it as an app's call does.
func (a *actor) call(ctx context.Context, req *http.Request) (*http.Response, error) {
	req = req.WithContext(ctx)
	req.Header.Set(hdrCaller, a.Name.String())
	return a.d.call(req)
}

// serveSystem ends route for a system shard, which info holds: the system app
// gets the request with the headers an app would, and its response is buffered
// and let out only once the gate has passed: nothing leaves before what the
// request committed is durable in the kind's tier, and the lease is always
// checked (a route whose answer someone acts on calls System.Sync).
// System apps answer small.
func (s *Server) serveSystem(w http.ResponseWriter, r *http.Request, info *reqInfo) {
	stripShardliteHeaders(r.Header)
	r.Header.Set(hdrOpened, info.name.String())
	r.Header.Set(hdrHops, strconv.Itoa(info.hops))
	if info.caller != "" {
		r.Header.Set(hdrCaller, info.caller)
	}
	buf := &bufferWriter{header: http.Header{}}
	info.hold.a.app.ServeHTTP(buf, r)
	if err := s.m.Gate(r.Context(), info.hold, info.hold.a.sys.Sync); err != nil {
		s.answer(w, r, info.own, err)
		return
	}
	stripShardliteHeaders(buf.header)
	for k, v := range buf.header {
		w.Header()[k] = v
	}
	w.WriteHeader(cmp.Or(buf.status, http.StatusOK))
	w.Write(buf.body.Bytes())
}

// bufferWriter keeps a system app's whole response until the gate lets it out.
type bufferWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *bufferWriter) Header() http.Header { return w.header }

func (w *bufferWriter) WriteHeader(code int) {
	if w.status == 0 && code >= 200 { // a forward's 102 is the owner's, not the answer
		w.status = code
	}
}

func (w *bufferWriter) code() int { return w.status }

func (w *bufferWriter) Flush() {}

func (w *bufferWriter) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.body.Write(p)
}

// ClaimOptions configure the claimer.
type ClaimOptions struct {
	Every time.Duration // the slow pass, a safety net, 10 min if zero
	Retry time.Duration // the first wait after a pass that left something to try again, doubling up to Every, 2 s if zero
}

// claimer keeps every fixed id of the system kinds claimed: nothing else would
// bring such a shard back once its owner is gone, since it is never woken
// through a hint. It passes at startup (after adoption), whenever the lease
// scan finds a node dead (a released lease counts) or this node finishes an
// adoption, and on a slow schedule. A pass acquires each fixed id that isn't
// active here, creating it if it doesn't exist, unless the owner cache names a
// live node for it: a failed claim records the owner there, and a dead node's
// entries leave it, so in a steady cluster a kicked pass reads nothing. The slow
// pass doesn't trust the cache, since an entry can name a live node that no
// longer owns the shard (one that claimed it and gave it up, see activate), and
// nothing else would correct it while that node lives: it costs a manifest read
// for each fixed id active elsewhere. The claim rule makes the acquires
// idempotent across the cluster.
type claimer struct {
	targets []shard.Name
	active  func(shard.Name) bool
	owner   func(shard.Name) (cluster.Node, bool)            // the owner cache
	live    func(cluster.Node) bool                          // whether the lease scan's last listing has the node alive
	acquire func(context.Context, shard.Name) (*Hold, error) // Catalog.Acquire
	clock   Clock
	opts    ClaimOptions
	log     *slog.Logger
	kick    *notify.Kick        // a node died
	adopted *notify.Counter     // adoptions this node finished
	passes  *notify.Counter     // for tests
	newer   map[shard.Name]bool // fixed ids a newer release migrated, which this node never claims again (only the claimer's loop touches it)
}

func (*claimer) String() string { return "claimer" }

func (c *claimer) Serve(ctx context.Context) error {
	adopted := c.adopted.Reached(c.adopted.Load() + 1)
	var retry time.Duration
	slow := false // the startup pass has an empty owner cache
	for {
		wait := c.opts.Every
		if c.pass(ctx, slow) {
			retry = 0
		} else {
			retry = min(max(2*retry, c.opts.Retry), c.opts.Every)
			wait = retry
		}
		t, stop := c.clock.NewTimer(wait)
		c.passes.Inc() // once the next pass's timer is set, so a test moving the clock after it reaches that pass
		slow = false
		select {
		case <-ctx.Done():
			stop()
			return nil
		case <-c.kick.C():
			stop()
		case <-adopted:
			stop()
			adopted = c.adopted.Reached(c.adopted.Load() + 1)
		case <-t:
			slow = wait == c.opts.Every
		}
	}
}

// pass claims what isn't active here or on a live owner (by the owner cache,
// unless the pass is slow), and reports whether nothing is left to try again
// soon: a shard whose owner is dead and not adopted yet, or a failure.
func (c *claimer) pass(ctx context.Context, slow bool) (done bool) {
	done = true
	for _, target := range c.targets {
		if c.active(target) || c.newer[target] {
			continue
		}
		if node, ok := c.owner(target); ok && c.live(node) && !slow {
			continue
		}
		hold, err := c.acquire(ctx, target)
		switch {
		case err == nil:
			hold.Release() // it never goes idle, so it stays active
		case errors.Is(err, shardfile.ErrOwnedElsewhere):
		case errors.Is(err, shardfile.ErrOwnerNotAdopted):
			done = false
		case errors.Is(err, migrate.ErrNewer):
			// The activation gave the shard up for a node of the newer release, which
			// claims it at its start or when it finds a node dead. Claiming it again
			// here would only take it from them for another refusal.
			if c.newer == nil {
				c.newer = map[shard.Name]bool{}
			}
			c.newer[target] = true
			c.log.Error("a newer release migrated a system shard, leaving it to the nodes that can read it", "shard", target, "err", err)
		default:
			done = false
			if ctx.Err() == nil {
				c.log.Warn("could not claim a system shard, trying again", "shard", target, "err", err)
			}
		}
	}
	return done
}
