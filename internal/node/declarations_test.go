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
