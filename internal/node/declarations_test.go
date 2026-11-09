package node

import (
	"context"
	"errors"
	"github.com/luci1900/shardlite/internal/apptest"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/lease"
)

// The poll is both the health check and the source of the app's kinds: the
// node is ready only while the app answers 2xx with a declaration, and keeps
// the kinds of the last declaration that was good.
func TestDeclarationPoll(t *testing.T) {
	t.Parallel()
	var decl atomic.Value // string
	decl.Store(testKinds)
	var status atomic.Int32
	status.Store(200)
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if status.Load() != 200 {
			w.WriteHeader(int(status.Load()))
			return
		}
		io.WriteString(w, decl.Load().(string))
	}))
	defer fake.Close()
	u, _ := url.Parse(fake.URL)
	kinds := NewKinds(0)
	keeper := lease.NewKeeper(nil, lease.Record{NodeInfo: lease.NodeInfo{Name: "test"}, Renewed: time.Now(), TTL: time.Minute}, lease.KeeperOptions{})
	srv := NewServer(new(Shardset), kinds, keeper, ServerOptions{App: u, ReadyEvery: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var g errgroup.Group
	g.Go(func() error { return srv.declarations.Serve(ctx) })
	ready := func() bool { ok, _ := srv.Ready(); return ok }
	apptest.Eventually(t, apptest.Timeout, "the first declaration", ready)

	if _, err := kinds.Lookup("users"); err != nil {
		t.Fatalf("the declaration's kinds: %v", err)
	}

	// A failed poll keeps the kinds and marks the node not ready, with the
	// reason the ready route answers.
	status.Store(503)
	apptest.Eventually(t, apptest.Timeout, "not ready with the app failing", func() bool { return !ready() })
	if _, why := srv.Ready(); !strings.Contains(why, "503") {
		t.Fatalf("why: %q", why)
	}
	if _, err := kinds.Lookup("users"); err != nil {
		t.Fatal("kinds lost while the app was failing")
	}

	// A declaration that isn't valid counts as a failure, not as kinds.
	status.Store(200)
	decl.Store(`{"kinds"`)
	apptest.Eventually(t, apptest.Timeout, "not ready with a bad declaration", func() bool { return !ready() })
	if _, err := kinds.Lookup("users"); err != nil {
		t.Fatal("kinds lost on a bad declaration")
	}

	// The app coming back with a new declaration makes the node ready and the
	// kinds current again.
	decl.Store(`{"version": "2", "kinds": {"users": {"public": true}, "wallet": {}}}`)
	apptest.Eventually(t, apptest.Timeout, "ready again", ready)
	if got, err := kinds.Lookup("wallet"); err != nil || got.Name != "wallet" {
		t.Fatalf("%+v %v", got, err)
	}

	// A kind starting with "_" is the node's own, which no app may declare: the
	// declaration counts as a failure, and the kinds stay.
	decl.Store(`{"kinds": {"users": {"public": true}, "_wake": {}}}`)
	apptest.Eventually(t, apptest.Timeout, "not ready with a reserved kind", func() bool { return !ready() })
	if _, why := srv.Ready(); !strings.Contains(why, "starts with") {
		t.Fatalf("why: %q", why)
	}
	if _, err := kinds.Lookup("_wake"); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("a reserved kind: %v", err)
	}
	if _, err := kinds.Lookup("wallet"); err != nil {
		t.Fatal("kinds lost on a reserved kind")
	}
}

// A node before its first good declaration serves nothing. A kind starting
// with "_" is the node's and none exists yet, so a target naming one parses and
// is answered as undeclared.
func TestKindsBeforeADeclaration(t *testing.T) {
	t.Parallel()
	k := NewKinds(0)
	if _, err := k.Lookup("users"); !errors.Is(err, ErrUnknownKind) {
		t.Fatal(err)
	}
	n, err := ParseTarget("_wake/1")
	if err != nil {
		t.Fatal(err)
	}
	if !n.Kind().Reserved() {
		t.Fatalf("%q is not reserved", n.Kind())
	}
	if _, err := k.Lookup(n.Kind()); !errors.Is(err, ErrUnknownKind) {
		t.Fatal(err)
	}
}

// lostRig is a declaration poll on a fake clock in front of an app the test
// can make answer well, answer badly or not answer at all.
type lostRig struct {
	t     *testing.T
	clock *harness.FakeClock
	srv   *Server
	mode  atomic.Int32 // an appMode
	lost  atomic.Int32 // how many times the poll called the app lost
	polls uint64       // the polls the test has waited for
}

type appMode int32

const (
	appGood      appMode = iota // 200 with a declaration
	appFailing                  // 500
	appBadDecl                  // 200 with a declaration that doesn't parse
	appUnreached                // the connection is closed without an answer
)

func newLostRig(t *testing.T, first appMode) *lostRig {
	t.Helper()
	r := &lostRig{t: t, clock: harness.NewFakeClock()}
	r.mode.Store(int32(first))
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch appMode(r.mode.Load()) {
		case appGood:
			io.WriteString(w, testKinds)
		case appFailing:
			w.WriteHeader(http.StatusInternalServerError)
		case appBadDecl:
			io.WriteString(w, `{"kinds"`)
		case appUnreached:
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
		}
	}))
	t.Cleanup(fake.Close)
	u, _ := url.Parse(fake.URL)
	keeper := lease.NewKeeper(nil, lease.Record{NodeInfo: lease.NodeInfo{Name: "test"}, Renewed: time.Now(), TTL: time.Minute}, lease.KeeperOptions{})
	r.srv = NewServer(new(Shardset), NewKinds(0), keeper, ServerOptions{
		App: u, ReadyEvery: time.Second, Clock: r.clock, AppLost: func() { r.lost.Add(1) },
	})
	ctx, cancel := context.WithCancel(context.Background())
	var g errgroup.Group
	g.Go(func() error { return r.srv.declarations.Serve(ctx) })
	t.Cleanup(func() {
		cancel()
		g.Wait()
	})
	r.waitPoll() // the first poll runs at once
	return r
}

// waitPoll waits for one more poll to finish.
func (r *lostRig) waitPoll() {
	r.t.Helper()
	r.polls++
	select {
	case <-r.srv.declarations.polls.Reached(r.polls):
	case <-time.After(apptest.Timeout):
		r.t.Fatalf("poll %d did not finish", r.polls)
	}
}

// poll moves the clock by d, which includes one period of the poll, and waits
// for the poll that follows.
func (r *lostRig) poll(d time.Duration) {
	r.t.Helper()
	r.clock.Advance(d)
	r.waitPoll()
}

// set changes how the app answers and polls once, a period later.
func (r *lostRig) set(m appMode) {
	r.t.Helper()
	r.mode.Store(int32(m))
	r.poll(time.Second)
}

// A node whose app answered once and then went appLostAfter without any answer
// calls it lost, once. The run is measured from the first poll that went
// unanswered, and a poll before the end of it signals nothing.
func TestDeclarationPollSignalsALostApp(t *testing.T) {
	t.Parallel()
	r := newLostRig(t, appGood)
	if ok, _ := r.srv.Ready(); !ok {
		t.Fatal("not ready after a good answer")
	}
	r.set(appUnreached) // the run begins here
	if ok, _ := r.srv.Ready(); ok {
		t.Fatal("ready with the app unreachable")
	}
	r.poll(appLostAfter - time.Second) // just short of the end of the run
	if n := r.lost.Load(); n != 0 {
		t.Fatalf("lost after less than appLostAfter: %d", n)
	}
	r.poll(time.Second)
	if n := r.lost.Load(); n != 1 {
		t.Fatalf("lost %d times after appLostAfter without an answer, want 1", n)
	}
	r.poll(appLostAfter) // the poll goes on until the node stops it, and says nothing more
	r.poll(appLostAfter)
	if n := r.lost.Load(); n != 1 {
		t.Fatalf("lost %d times, want once", n)
	}
}

// A node that never heard from its app waits for it, however long that takes.
func TestDeclarationPollNeverCallsAnAppLostThatNeverAnswered(t *testing.T) {
	t.Parallel()
	r := newLostRig(t, appUnreached)
	for range 3 {
		r.poll(appLostAfter)
	}
	if n := r.lost.Load(); n != 0 {
		t.Fatalf("lost %d times without the app ever answering", n)
	}
	// Once it answers, even badly, the run counts from there.
	r.set(appFailing)
	r.set(appUnreached)
	r.poll(appLostAfter)
	if n := r.lost.Load(); n != 1 {
		t.Fatalf("lost %d times, want 1 once the app had answered and then gone", n)
	}
}

// An answer is an app that is there, however bad: it ends the run of polls
// without one, a non-2xx status and a declaration that doesn't parse as well as
// a good one.
func TestDeclarationPollAnyAnswerEndsTheRun(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		mode appMode
	}{{"a good declaration", appGood}, {"a non-2xx answer", appFailing}, {"a declaration that doesn't parse", appBadDecl}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newLostRig(t, appGood)
			r.set(appUnreached)
			r.poll(appLostAfter - time.Second) // nearly the whole run
			r.set(tc.mode)                     // the answer ends it
			r.set(appUnreached)                // and a new run begins
			r.poll(appLostAfter - time.Second)
			if n := r.lost.Load(); n != 0 {
				t.Fatalf("lost %d times though %s came within appLostAfter", n, tc.name)
			}
			r.poll(time.Second)
			if n := r.lost.Load(); n != 1 {
				t.Fatalf("lost %d times, want 1 appLostAfter after the answer", n)
			}
		})
	}
}

// An app that is only failing or only badly declared is never lost: the node
// goes not ready, and drains nothing.
func TestDeclarationPollAFailingAppIsNotLost(t *testing.T) {
	t.Parallel()
	for _, m := range []appMode{appFailing, appBadDecl} {
		r := newLostRig(t, appGood)
		r.set(m)
		for range 3 {
			r.poll(appLostAfter)
		}
		if n := r.lost.Load(); n != 0 {
			t.Fatalf("mode %d: lost %d times with the app answering", m, n)
		}
		if ok, _ := r.srv.Ready(); ok {
			t.Fatalf("mode %d: ready", m)
		}
	}
}
