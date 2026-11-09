package shardlite_test

import (
	"net/http"
	"strings"
	"testing"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

func TestKindShardAndParseShardAgree(t *testing.T) {
	if got := users.Shard("42").String(); got != "users/42" {
		t.Errorf("Shard = %q", got)
	}
	dotted := shardlite.Kind{Name: "matchmaker.queue"}
	if got := dotted.Shard("eu-1_A").String(); got != "matchmaker.queue/eu-1_A" {
		t.Errorf("Shard = %q", got)
	}
	for _, s := range []string{"users/42", "matchmaker.queue/eu", "a/" + strings.Repeat("x", 128), strings.Repeat("a", 64) + "/1"} {
		name, err := shardlite.ParseShard(s)
		if err != nil || name.String() != s {
			t.Errorf("ParseShard(%q) = %v, %v", s, name, err)
		}
	}
	if parsed, _ := shardlite.ParseShard("users/42"); parsed != users.Shard("42") {
		t.Error("the same name parsed and built differs, so it can't be compared or used as a map key")
	}
}

// ParseShard checks the grammar and never whether the kind is declared.
func TestParseShardChecksTheGrammarOnly(t *testing.T) {
	for _, s := range []string{
		"", "users", "users/", "/42", "Users/42", "users/4 2", "users/4/2", "users/ä", "1users/1", "_users/1",
		"users.1/1", "users../1", ".users/1", "users./1",
		strings.Repeat("a", 65) + "/1", "a/" + strings.Repeat("x", 129),
	} {
		if name, err := shardlite.ParseShard(s); err == nil {
			t.Errorf("ParseShard(%q) = %v, want an error", s, name)
		}
	}
	// A kind nobody declared parses.
	if _, err := shardlite.ParseShard("anything-at-all/1"); err != nil {
		t.Error(err)
	}
}

func TestTheZeroShardNameNamesNothing(t *testing.T) {
	var zero shardlite.ShardName
	if zero.String() != "" {
		t.Errorf("the zero name is %q", zero.String())
	}
	node := newFake(t, testConfig())
	app := node.App()
	if res, err := app.CallAsApp(t.Context(), zero, http.MethodGet, "/", nil); err == nil {
		res.Body.Close()
		t.Error("a call to the zero name was sent")
	}
	// A name with an id or kind outside the grammar is refused too, before
	// anything is sent.
	for _, bad := range []shardlite.ShardName{users.Shard(""), users.Shard("a b"), shardlite.Kind{Name: "Users"}.Shard("1"), shardlite.Kind{}.Shard("1")} {
		if res, err := app.CallAsApp(t.Context(), bad, http.MethodGet, "/", nil); err == nil {
			res.Body.Close()
			t.Errorf("a call to %q was sent", bad)
		}
	}
	if n := len(node.Calls()); n != 0 {
		t.Errorf("%d calls reached the node", n)
	}
}
