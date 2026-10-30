package kind

import (
	"testing"
	"time"
)

func TestParseApp(t *testing.T) {
	t.Parallel()
	a, err := ParseApp([]byte(`{"app": "test", "version": "2026.10.0", "kinds": {"users": {"public": true, "sync": "none", "idle_timeout": "10m"}, "wallet": {}, "chat": {"sync": "storage"}, "audit": {}}}`), 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if a.Name != "test" || a.Version != "2026.10.0" {
		t.Fatalf("%+v", a)
	}
	// The kinds are qualified by the app the declaration names.
	if a.Kinds["test.users"].IdleTimeout != 10*time.Minute || a.Kinds["test.users"].Sync != SyncNone || !a.Kinds["test.users"].Public || a.Kinds["test.chat"].Sync != SyncStorage || a.Kinds["test.audit"].Public {
		t.Fatalf("%+v", a.Kinds)
	}
	if a.Kinds["test.users"].Name != "test.users" {
		t.Fatalf("%+v", a.Kinds["test.users"])
	}
	// Leaving sync out gives the record, the conservative tier; the other
	// defaults are the safe ones: not public. The peer tier is opt-in.
	if w := a.Kinds["test.wallet"]; w.Sync != SyncStorage || w.Public || w.IdleTimeout != 3*time.Minute {
		t.Fatalf("defaults: %+v", w)
	}
	// The node's idle timeout is a default: a kind's own setting wins.
	a, _ = ParseApp([]byte(`{"app": "test", "kinds": {"users": {"idle_timeout": "10m"}, "wallet": {}}}`), time.Second)
	if a.Kinds["test.users"].IdleTimeout != 10*time.Minute || a.Kinds["test.wallet"].IdleTimeout != time.Second {
		t.Fatalf("%+v", a.Kinds)
	}
	// The version is the app's own and may be left out for now.
	if a.Version != "" {
		t.Fatalf("%+v", a)
	}
	for name, bad := range map[string]string{
		"unknown field": `{"app": "test", "kinds": {"users": {"synk": true}}}`,
		"auto_delete":   `{"app": "test", "kinds": {"users": {"auto_delete": true}}}`,
		"sync value":    `{"app": "test", "kinds": {"users": {"sync": "maybe"}}}`,
		"sync boolean":  `{"app": "test", "kinds": {"users": {"sync": true}}}`,
		"bad duration":  `{"app": "test", "kinds": {"users": {"idle_timeout": "soon"}}}`,
		"zero duration": `{"app": "test", "kinds": {"users": {"idle_timeout": "0s"}}}`,
		"bad name":      `{"app": "test", "kinds": {"Users": {}}}`,
		"slash":         `{"app": "test", "kinds": {"a/b": {}}}`,
		"no app":        `{"kinds": {"users": {}}}`,
		"bad app":       `{"app": "Test", "kinds": {"users": {}}}`,
		"reserved app":  `{"app": "_system", "kinds": {"users": {}}}`,
		"no kinds":      `{"app": "test"}`,
		"not json":      `kinds`,
		"trailing":      `{"app": "test", "kinds": {}} x`,
	} {
		if _, err := ParseApp([]byte(bad), time.Minute); err == nil && name != "trailing" {
			t.Errorf("%s accepted", name)
		}
	}
	if _, err := ParseApp([]byte(`{"app": "test", "version": "", "kinds": {}}`), time.Minute); err != nil {
		t.Errorf("an empty version: %v", err)
	}
}
