package node

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/system"
)

// The poll is both the health check and the source of the app's kinds: the
// node is ready only while the app answers 2xx with a declaration, and keeps
// the kinds of the last declaration that was good.
func TestReadyPoll(t *testing.T) {
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
	keeper := coord.NewKeeper(nil, coord.NodeLease{NodeInfo: coord.NodeInfo{Name: "test"}, Renewed: time.Now(), TTL: time.Minute}, coord.KeeperOptions{})
	srv := NewServer(nil, kinds, keeper, ServerOptions{App: u, ReadyEvery: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var g errgroup.Group
	g.Go(func() error { return srv.ready.Serve(ctx) })
	ready := func() bool { ok, _ := srv.Ready(); return ok }
	eventually(t, "the first declaration", ready)

	if _, err := kinds.Lookup("test.users"); err != nil {
		t.Fatalf("the declaration's kinds: %v", err)
	}

	// A failed poll keeps the kinds and marks the node not ready, with the
	// reason the ready route answers.
	status.Store(503)
	eventually(t, "not ready with the app failing", func() bool { return !ready() })
	if _, why := srv.Ready(); !strings.Contains(why, "503") {
		t.Fatalf("why: %q", why)
	}
	if _, err := kinds.Lookup("test.users"); err != nil {
		t.Fatal("kinds lost while the app was failing")
	}

	// A declaration that isn't valid counts as a failure, not as kinds.
	status.Store(200)
	decl.Store(`{"app": "test"`)
	eventually(t, "not ready with a bad declaration", func() bool { return !ready() })
	if _, err := kinds.Lookup("test.users"); err != nil {
		t.Fatal("kinds lost on a bad declaration")
	}

	// The app coming back with a new declaration makes the node ready and the
	// kinds current again.
	decl.Store(`{"app": "test", "version": "2", "kinds": {"users": {"public": true}, "wallet": {}}}`)
	eventually(t, "ready again", ready)
	if got, err := kinds.Lookup("test.wallet"); err != nil || got.Name != "test.wallet" {
		t.Fatalf("%+v %v", got, err)
	}
}

// A node with no app kinds yet (before the first poll, or without an app)
// serves only the system kinds.
func TestKindsWithoutAnApp(t *testing.T) {
	t.Parallel()
	k := NewKinds(0)
	if _, err := k.Lookup("test.users"); !errors.Is(err, ErrUnknownKind) {
		t.Fatal(err)
	}
	app, _, err := newSystemApp([]kind.Kind{{Name: "_system.probe", IDs: []shard.ID{"_cluster"}, Handler: func(*system.System) http.Handler { return http.NotFoundHandler() }}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	k.setSystem(app)
	if got, err := k.Lookup("_system.probe"); err != nil || got.Name != "_system.probe" {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := k.Lookup("test.users"); !errors.Is(err, ErrUnknownKind) {
		t.Fatal(err)
	}
}
