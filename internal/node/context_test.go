package node

import (
	"net/http"
	"testing"
)

// ctxOf is the Shardlite-Context of a request opened for name that is hops into a chain of calls, as the node writes it for an app.
func ctxOf(name string, hops int64) string {
	v, err := callContext{shard: shardName(name), hops: hops}.format()
	if err != nil {
		panic(err)
	}
	return v
}

func TestContextRoundTrip(t *testing.T) {
	t.Parallel()
	v := ctxOf("users/42", 3)
	if v != `shard="users/42", hops=3` {
		t.Fatalf("written as %q", v)
	}
	c, ok, err := parseContext(http.Header{hdrContext: {v}})
	if err != nil || !ok || c.shard.String() != "users/42" || c.hops != 3 {
		t.Fatalf("read back %+v %v %v", c, ok, err)
	}
}

func TestParseContext(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		values  []string
		ok, bad bool
		shard   string
		hops    int64
	}{
		{name: "none"},
		{name: "good", values: []string{`shard="users/1", hops=0`}, ok: true, shard: "users/1"},
		{name: "in either order", values: []string{`hops=4, shard="users/1"`}, ok: true, shard: "users/1", hops: 4},
		{name: "on two lines", values: []string{`shard="users/1"`, `hops=2`}, ok: true, shard: "users/1", hops: 2},
		{name: "a key it does not know", values: []string{`shard="users/1", hops=2, trace="x", flag`}, ok: true, shard: "users/1", hops: 2},
		{name: "parameters it does not know", values: []string{`shard="users/1";a=1, hops=2;b`}, ok: true, shard: "users/1", hops: 2},
		{name: "not a dictionary", values: []string{`"users/1"`}, bad: true},
		{name: "garbage", values: []string{`shard=`}, bad: true},
		{name: "no shard", values: []string{`hops=1`}, bad: true},
		{name: "no hops", values: []string{`shard="users/1"`}, bad: true},
		{name: "a shard that is no string", values: []string{`shard=users, hops=1`}, bad: true},
		{name: "a shard that is no name", values: []string{`shard="nokind", hops=1`}, bad: true},
		{name: "an empty shard", values: []string{`shard="", hops=1`}, bad: true},
		{name: "hops that are no integer", values: []string{`shard="users/1", hops="1"`}, bad: true},
		{name: "hops that are a decimal", values: []string{`shard="users/1", hops=1.5`}, bad: true},
		{name: "negative hops", values: []string{`shard="users/1", hops=-1`}, bad: true},
		{name: "an inner list", values: []string{`shard=("users/1"), hops=1`}, bad: true},
	} {
		c, ok, err := parseContext(http.Header{hdrContext: tc.values})
		if (err != nil) != tc.bad || ok != tc.ok {
			t.Errorf("%s: ok=%v err=%v", tc.name, ok, err)
			continue
		}
		if tc.ok && (c.shard.String() != tc.shard || c.hops != tc.hops) {
			t.Errorf("%s: read %+v", tc.name, c)
		}
	}
}
