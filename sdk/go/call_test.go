package shardlite_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

func closeBody(t *testing.T, res *http.Response, err error) *http.Response {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	return res
}

func TestCallCopiesTheContextAndTheTrace(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	const trace = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	from := fake.Request(users.Shard("42"), http.MethodPost, "/users/42/invite", nil, withHops(3))
	from.Header.Set("traceparent", trace)
	// Both scopes carry the request's context.
	for name, call := range map[string]func() (*http.Response, error){
		"the request": func() (*http.Response, error) {
			return app.Request(from).Call(guild.Shard("7"), http.MethodPost, "/invite?by=42", strings.NewReader("hi"))
		},
		"the opened shard": func() (*http.Response, error) {
			var res *http.Response
			var err error
			run(app, func(w http.ResponseWriter, r *http.Request) {
				opened, refused := app.Expect(r, users.Shard("42"))
				if refused == nil {
					res, err = opened.Call(guild.Shard("7"), http.MethodPost, "/invite?by=42", strings.NewReader("hi"))
				}
			}, from)
			return res, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			before := len(fake.Calls())
			res, err := call()
			closeBody(t, res, err)

			calls := fake.Calls()
			if len(calls) != before+1 {
				t.Fatalf("recorded %d calls, want 1 more", len(calls)-before)
			}
			got := calls[before]
			if got.Target != "guild/7" || got.Method != http.MethodPost || got.Path != "/invite?by=42" || string(got.Body) != "hi" {
				t.Errorf("call = %+v, want a POST /invite?by=42 \"hi\" to guild/7", got)
			}
			if h := got.Header.Get(shardlite.HeaderContext); h != from.Header.Get(shardlite.HeaderContext) || h == "" {
				t.Errorf("Shardlite-Context = %q, want the request's %q unchanged", h, from.Header.Get(shardlite.HeaderContext))
			}
			if h := got.Header.Get("traceparent"); h != trace {
				t.Errorf("traceparent = %q", h)
			}
			if h := got.Header.Get(headerCaller); h != "" {
				t.Errorf("the call sets Shardlite-Caller %q: the node works out the caller from the context", h)
			}
		})
	}
}

func TestCallFromAShardlessRequestIsFromTheApp(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	res, err := app.Request(fake.ShardlessRequest(http.MethodPost, "/login", nil)).Call(users.Shard("42"), http.MethodGet, "/me", nil)
	closeBody(t, res, err)
	got := fake.Calls()[0]
	if got.Header.Get(shardlite.HeaderContext) != "" {
		t.Errorf("call = %+v, want one from the app with no context", got)
	}
}

func TestCallAsAppSendsNoContextAndNoTrace(t *testing.T) {
	fake := newFake(t, testConfig())
	res, err := fake.App().CallAsApp(t.Context(), users.Shard("42"), http.MethodPut, "/refresh", strings.NewReader("x"), shardlite.Header("Idempotency-Key", "k1"))
	closeBody(t, res, err)
	got := fake.Calls()[0]
	if got.Target != "users/42" || got.Method != http.MethodPut || string(got.Body) != "x" {
		t.Errorf("call = %+v", got)
	}
	for _, h := range []string{shardlite.HeaderContext, "traceparent"} {
		if v := got.Header.Get(h); v != "" {
			t.Errorf("a call from the app sent %s %q", h, v)
		}
	}
	if got.Header.Get("Idempotency-Key") != "k1" {
		t.Errorf("the app's header was lost: %v", got.Header)
	}
}

func TestCallEndsWithItsContext(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	// A call made on a request ends with the request's context.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	from := fake.Request(users.Shard("42"), http.MethodGet, "/", nil).WithContext(ctx)
	if res, err := app.Request(from).Call(guild.Shard("7"), http.MethodGet, "/", nil); err == nil {
		res.Body.Close()
		t.Error("a call outlived its request")
	}
	if res, err := app.CallAsApp(ctx, guild.Shard("7"), http.MethodGet, "/", nil); err == nil {
		res.Body.Close()
		t.Error("a call outlived its context")
	}
	if n := len(fake.Calls()); n != 0 {
		t.Errorf("%d calls reached the node", n)
	}
}

func TestEachOptionSetsItsHeader(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	for name, tc := range map[string]struct {
		opts []shardlite.CallOption
		want map[string]string
	}{
		"none":              {nil, map[string]string{headerIfActive: "", headerIfExists: "", headerSync: ""}},
		"IfActive":          {[]shardlite.CallOption{shardlite.IfActive()}, map[string]string{headerIfActive: "true", headerIfExists: ""}},
		"IfExists":          {[]shardlite.CallOption{shardlite.IfExists()}, map[string]string{headerIfActive: "", headerIfExists: "true"}},
		"WithSync none":     {[]shardlite.CallOption{shardlite.WithSync(shardlite.SyncNone)}, map[string]string{headerSync: "none"}},
		"WithSync storage":  {[]shardlite.CallOption{shardlite.WithSync(shardlite.SyncStorage)}, map[string]string{headerSync: "storage"}},
		"WithSync the zero": {[]shardlite.CallOption{shardlite.WithSync("")}, map[string]string{headerSync: ""}},
		"Header":            {[]shardlite.CallOption{shardlite.Header("Idempotency-Key", "k"), shardlite.Header("Content-Type", "text/plain")}, map[string]string{"Idempotency-Key": "k", "Content-Type": "text/plain"}},
		"all": {[]shardlite.CallOption{shardlite.IfActive(), shardlite.IfExists(), shardlite.WithSync(shardlite.SyncNone), shardlite.Header("X-A", "1")},
			map[string]string{headerIfActive: "true", headerIfExists: "true", headerSync: "none", "X-A": "1"}},
	} {
		t.Run(name, func(t *testing.T) {
			before := len(fake.Calls())
			res, err := app.CallAsApp(t.Context(), guild.Shard("7"), http.MethodGet, "/", nil, tc.opts...)
			closeBody(t, res, err)
			sent := fake.Calls()[before].Header
			for h, want := range tc.want {
				if got := sent.Get(h); got != want {
					t.Errorf("%s = %q, want %q", h, got, want)
				}
			}
		})
	}
}

func TestACallThatIsMalformedFailsBeforeItIsSent(t *testing.T) {
	fake := newFake(t, testConfig())
	app := fake.App()
	from := app.Request(fake.Request(users.Shard("42"), http.MethodGet, "/", nil))
	for name, tc := range map[string]struct {
		path string
		opts []shardlite.CallOption
	}{
		"Shardlite-Context":      {"/", []shardlite.CallOption{shardlite.Header(shardlite.HeaderContext, `shard="users/1", hops=0`)}},
		"Shardlite-Caller":       {"/", []shardlite.CallOption{shardlite.Header("shardlite-caller", "app")}},
		"Shardlite-Target":       {"/", []shardlite.CallOption{shardlite.Header(shardlite.HeaderTarget, "users/1")}},
		"Shardlite-If-Active":    {"/", []shardlite.CallOption{shardlite.Header(headerIfActive, "yes")}},
		"underscores":            {"/", []shardlite.CallOption{shardlite.Header("Shardlite_Caller", "app")}},
		"a sync tier that isn't": {"/", []shardlite.CallOption{shardlite.WithSync("disk")}},
		"a path with no slash":   {"me", nil},
	} {
		t.Run(name, func(t *testing.T) {
			if res, err := from.Call(guild.Shard("7"), http.MethodGet, tc.path, nil, tc.opts...); err == nil {
				res.Body.Close()
				t.Error("the call was sent")
			}
			if res, err := app.CallAsApp(t.Context(), guild.Shard("7"), http.MethodGet, tc.path, nil, tc.opts...); err == nil {
				res.Body.Close()
				t.Error("the call was sent from the app")
			}
		})
	}
	if n := len(fake.Calls()); n != 0 {
		t.Errorf("%d calls reached the node", n)
	}
}

// roundTripFunc is an http.RoundTripper that answers from a function.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTheNodeIsAtDefaultAddrWithTheAppsClient(t *testing.T) {
	var urls []string
	cfg := testConfig()
	cfg.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		urls = append(urls, r.URL.String())
		return &http.Response{StatusCode: 200, Body: http.NoBody, Header: http.Header{}}, nil
	})}
	app := newNode(t, cfg)
	res, err := app.CallAsApp(t.Context(), users.Shard("42"), http.MethodGet, "/me", nil)
	closeBody(t, res, err)
	if err := app.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []string{"http://127.0.0.1:2282/me", "http://127.0.0.1:2282/_shardlite/sync"}
	if strings.Join(urls, " ") != strings.Join(want, " ") {
		t.Errorf("URLs = %v, want the control API's default address through the app's client: %v", urls, want)
	}
	if shardlite.DefaultAddr != "127.0.0.1:2282" {
		t.Errorf("DefaultAddr = %q", shardlite.DefaultAddr)
	}
}

func TestSync(t *testing.T) {
	fake := newFake(t, testConfig())
	if err := fake.App().Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := fake.Syncs(); got != 1 {
		t.Errorf("Syncs = %d, want 1", got)
	}
	if got := len(fake.Calls()); got != 0 {
		t.Errorf("a sync was recorded as a call")
	}

	// It is a POST to the sync endpoint.
	app := newNode(t, answering(func(r *http.Request) *http.Response {
		if r.Method != http.MethodPost || r.URL.Path != syncPath {
			t.Errorf("sync sent %s %s", r.Method, r.URL.Path)
		}
		return reply(http.StatusNoContent, "", nil)
	}))
	if err := app.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSetSync(t *testing.T) {
	for _, tier := range []shardlite.Sync{shardlite.SyncNone, shardlite.SyncStorage} {
		h := http.Header{}
		if err := shardlite.SetSync(h, tier); err != nil || h.Get(headerSync) != string(tier) {
			t.Errorf("SetSync(%q) = %v, set %q", tier, err, h.Get(headerSync))
		}
	}
	// Anything else is refused and sets nothing, even over an earlier value.
	for _, tier := range []shardlite.Sync{"", "disk", "None"} {
		h := http.Header{}
		h.Set(headerSync, "storage")
		if err := shardlite.SetSync(h, tier); err == nil {
			t.Errorf("SetSync(%q) was accepted", tier)
		}
		if got := h.Get(headerSync); got != "storage" {
			t.Errorf("SetSync(%q) changed the header to %q", tier, got)
		}
	}
	// On a response, it is the header the node reads.
	rec := httptest.NewRecorder()
	if err := shardlite.SetSync(rec.Header(), shardlite.SyncStorage); err != nil {
		t.Fatal(err)
	}
	rec.WriteHeader(http.StatusAccepted)
	if got := rec.Result().Header.Get("Shardlite-Sync"); got != "storage" {
		t.Errorf("the response has Shardlite-Sync %q", got)
	}
}
