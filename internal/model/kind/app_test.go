package kind

import (
	"strings"
	"testing"
	"time"
)

func TestParseApp(t *testing.T) {
	t.Parallel()
	a, err := ParseApp([]byte(`{"version": "2026.10.0", "kinds": {"users": {"public": true, "sync": "none", "idle_timeout": "10m"}, "wallet": {}, "chat": {"sync": "storage"}, "audit": {}}}`), 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if a.Version != "2026.10.0" || len(a.Kinds) != 4 {
		t.Fatalf("%+v", a)
	}
	// The kinds are keyed by their own names.
	if a.Kinds["users"].IdleTimeout != 10*time.Minute || a.Kinds["users"].Sync != SyncNone || !a.Kinds["users"].Public || a.Kinds["chat"].Sync != SyncStorage || a.Kinds["audit"].Public {
		t.Fatalf("%+v", a.Kinds)
	}
	if a.Kinds["users"].Name != "users" {
		t.Fatalf("%+v", a.Kinds["users"])
	}
	// Leaving sync out gives storage, the conservative tier; the other
	// defaults are the safe ones: not public.
	if w := a.Kinds["wallet"]; w.Sync != SyncStorage || w.Public || w.IdleTimeout != 3*time.Minute {
		t.Fatalf("defaults: %+v", w)
	}
	// The node's idle timeout is a default: a kind's own setting wins.
	a, _ = ParseApp([]byte(`{"kinds": {"users": {"idle_timeout": "10m"}, "wallet": {}}}`), time.Second)
	if a.Kinds["users"].IdleTimeout != 10*time.Minute || a.Kinds["wallet"].IdleTimeout != time.Second {
		t.Fatalf("%+v", a.Kinds)
	}
	// The version is the app's own and may be left out for now.
	if a.Version != "" {
		t.Fatalf("%+v", a)
	}
	// A dot is an ordinary character in a kind's name, so a library can name
	// its kinds matchmaker.queue and the node keeps it as one kind.
	a, err = ParseApp([]byte(`{"kinds": {"matchmaker.queue": {"public": true}, "matchmaker.ticket": {}, "users": {}}}`), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if k, ok := a.Kinds["matchmaker.queue"]; !ok || k.Name != "matchmaker.queue" || !k.Public || len(a.Kinds) != 3 {
		t.Fatalf("%+v", a.Kinds)
	}
	for name, bad := range map[string]string{
		"unknown field":    `{"kinds": {"users": {"synk": true}}}`,
		"auto_delete":      `{"kinds": {"users": {"auto_delete": true}}}`,
		"sync value":       `{"kinds": {"users": {"sync": "maybe"}}}`,
		"sync peer":        `{"kinds": {"users": {"sync": "peer"}}}`,
		"sync boolean":     `{"kinds": {"users": {"sync": true}}}`,
		"bad duration":     `{"kinds": {"users": {"idle_timeout": "soon"}}}`,
		"zero duration":    `{"kinds": {"users": {"idle_timeout": "0s"}}}`,
		"bad name":         `{"kinds": {"Users": {}}}`,
		"slash":            `{"kinds": {"a/b": {}}}`,
		"empty segment":    `{"kinds": {"a..b": {}}}`,
		"leading dot":      `{"kinds": {".a": {}}}`,
		"trailing dot":     `{"kinds": {"a.": {}}}`,
		"digit segment":    `{"kinds": {"a.1b": {}}}`,
		"too long":         `{"kinds": {"` + strings.Repeat("a", 65) + `": {}}}`,
		"an app":           `{"app": "test", "kinds": {"users": {}}}`,
		"an empty app":     `{"app": "", "kinds": {"users": {}}}`,
		"duplicate":        `{"kinds": {"users": {}, "wallet": {}, "users": {"public": true}}}`,
		"duplicate dotted": `{"kinds": {"a.b": {}, "a.b": {}}}`,
		"reserved":         `{"kinds": {"users": {}, "_wake": {}}}`,
		"reserved dotted":  `{"kinds": {"_node.probe": {}}}`,
		"no kinds":         `{"version": "1"}`,
		"null kinds":       `{"kinds": null}`,
		"kinds list":       `{"kinds": []}`,
		"kinds string":     `{"kinds": "users"}`,
		"kind not object":  `{"kinds": {"users": true}}`,
		"not json":         `kinds`,
		"truncated":        `{"kinds": {"users": {}`,
	} {
		if _, err := ParseApp([]byte(bad), time.Minute); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	// The errors name what is wrong.
	for bad, want := range map[string]string{
		`{"app": "test", "kinds": {}}`:                        `"app"`,
		`{"kinds": {"users": {}, "wallet": {}, "users": {}}}`: `"users"`,
		`{"kinds": {"_wake": {}}}`:                            `"_"`,
	} {
		_, err := ParseApp([]byte(bad), time.Minute)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: error %v, want it to mention %s", bad, err, want)
		}
	}
	if _, err := ParseApp([]byte(`{"version": "", "kinds": {}}`), time.Minute); err != nil {
		t.Errorf("an empty version: %v", err)
	}
}
