//go:build fault

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/dunglas/httpsfv"

	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

// A fault setting that does not parse fails the start, so a test never runs
// against a node that armed nothing.
func TestUnknownFaultValueFailsTheStart(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ name, value, want string }{
		{"SHARDLITE_FAULT_KILL_AT", "segment-befor", "no kill point"},
		{"SHARDLITE_FAULT_STORAGE", "break:10:1ms", "mode is fail"},
		{"SHARDLITE_FAULT_STORAGE", "fail:10", "three fields"},
		{"SHARDLITE_FAULT_STORAGE", "fail:10:1ms:index", "target is segments, files"},
		{"SHARDLITE_FAULT_STORAGE", "fail:10:1ms:orphans", "target is segments, files"},
		{"SHARDLITE_FAULT_STORAGE", "fail:10:1ms:segments:x", "three fields"},
		{"SHARDLITE_FAULT_STORAGE", "fail:2000:1ms", "permille"},
		{"SHARDLITE_FAULT_STORAGE", "fail:10:soon", "duration"},
		{"SHARDLITE_FAULT_STORAGE", "lost:5:1ms:segments,break:10:1ms", "mode is fail"},
		{"SHARDLITE_FAULT_STORAGE", "lost:5:1ms:segments,", "three fields"},
		{"SHARDLITE_FAULT_NO_LEASE_EXPIRY", "maybe", "true or false"},
		{"SHARDLITE_FAULT_UNCONFIRMED", "yes", "true or false"},
		{"SHARDLITE_FAULT_TEMP", "rename:10", "op is create"},
		{"SHARDLITE_FAULT_TEMP", "append", "two fields"},
		{"SHARDLITE_FAULT_TEMP", "append:x", "permille"},
		{"SHARDLITE_FAULT_PEER_HEADER", "mangle:10", "mode is drop"},
		{"SHARDLITE_FAULT_PEER_HEADER", "drop", "two fields"},
		{"SHARDLITE_FAULT_PEER_HEADER", "drop:2000", "permille"},
		{"SHARDLITE_FAULT_MAX_FILES", "0", "positive"},
	} {
		_, err := faultOptions(func(k string) string {
			if k == c.name {
				return c.value
			}
			return ""
		})
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s=%q: %v, want an error with %q", c.name, c.value, err, c.want)
		}
	}
}

func TestKnownFaultValuesArm(t *testing.T) {
	t.Parallel()
	env := map[string]string{"SHARDLITE_FAULT_KILL_AT": "segment-after", "SHARDLITE_FAULT_STORAGE": "lost:5:2ms:segments,lost:5:0s:files", "SHARDLITE_FAULT_NO_LEASE_EXPIRY": "true", "SHARDLITE_FAULT_UNCONFIRMED": "false", "SHARDLITE_FAULT_TEMP": "dup:7", "SHARDLITE_FAULT_PEER_HEADER": "garble:300", "SHARDLITE_FAULT_MAX_FILES": "2"}
	opts, err := faultOptions(func(k string) string { return env[k] })
	if err != nil || len(opts) != 1 {
		t.Fatalf("faultOptions: %d options, %v", len(opts), err)
	}
}

// Every garbled value is one the receiver ignores: a value of Shardlite-Node
// that is no node's name, and a value of Shardlite-Placement that does not read
// or does not say avoid. And a peer header fault at a rate of 1000 changes every
// value.
func TestPeerHeaderFaultGarblesAndDrops(t *testing.T) {
	t.Parallel()
	garble, err := peerHeaderFault("garble:1000")
	if err != nil {
		t.Fatal(err)
	}
	// A value of Shardlite-Node the receiver ignores: no node's name. One of
	// Shardlite-Placement it ignores: not an Item, or not the token avoid.
	notAName := func(form string) bool { return !cluster.ValidNode(form) }
	notAvoid := func(form string) bool {
		item, err := httpsfv.UnmarshalItem([]string{form})
		if err != nil {
			return true
		}
		tok, ok := item.Value.(httpsfv.Token)
		return !ok || tok != "avoid"
	}
	for _, c := range []struct {
		name   string
		pool   []string
		reject func(string) bool
	}{
		{"Shardlite-Node", garbledNode, notAName},
		{"Shardlite-Placement", garbledPlacement, notAvoid},
	} {
		seen := map[string]bool{}
		for range 500 {
			got := garble(c.name, `x`)
			if got == `x` {
				t.Fatalf("garble:1000 left the value alone")
			}
			seen[got] = true
		}
		if len(seen) != len(c.pool) {
			t.Errorf("%s: %d garbled forms sent, want all %d", c.name, len(seen), len(c.pool))
		}
		for _, form := range c.pool {
			if !c.reject(form) {
				t.Errorf("%s: garbage %q reads, and the receiver would not ignore it", c.name, form)
			}
		}
	}
	drop, _ := peerHeaderFault("drop:1000")
	if got := drop("Shardlite-Node", "n1"); got != "" {
		t.Fatalf("drop:1000 sent %q", got)
	}
	none, _ := peerHeaderFault("drop:0")
	if got := none("Shardlite-Node", "n1"); got != "n1" {
		t.Fatalf("drop:0 sent %q", got)
	}
}

// The storage fault's target says which calls fail: the creates of the journal's
// segments (the default), the creates of LTX files or the deletes of LTX files, and
// only those. Any other target is refused.
func TestStorageFaultTargetsItsCalls(t *testing.T) {
	t.Parallel()
	seg := journal.Root + "n1/00000000000000000001"
	file := shardfile.FilesPrefix + "6/users/42/abc123/0000000000000001-0000000000000002.ltx"
	for _, c := range []struct {
		value string
		fails map[blobtest.Op]string // the key whose call of that op fails
	}{
		{"fail:1000:0s", map[blobtest.Op]string{blobtest.OpCreate: seg}},
		{"fail:1000:0s:segments", map[blobtest.Op]string{blobtest.OpCreate: seg}},
		{"fail:1000:0s:files", map[blobtest.Op]string{blobtest.OpCreate: file}},
		{"fail:1000:0s:file-deletes", map[blobtest.Op]string{blobtest.OpDelete: file}},
	} {
		hook, err := storageFault(c.value)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		for _, op := range []blobtest.Op{blobtest.OpCreate, blobtest.OpUpdate, blobtest.OpDelete} {
			for _, key := range []string{seg, file} {
				want := c.fails[op] == key
				if got := hook(ctx, op, key, false) != nil; got != want {
					t.Errorf("%s: %s of %s fails: %v, want %v", c.value, op, key, got, want)
				}
			}
		}
	}
}
