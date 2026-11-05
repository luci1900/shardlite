//go:build fault

package main

import (
	"strings"
	"testing"
)

// A fault setting that does not parse fails the start, so a test never runs
// against a node that armed nothing.
func TestUnknownFaultValueFailsTheStart(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ name, value, want string }{
		{"SHARDLITE_KILL_AT", "segment-befor", "no kill point"},
		{"SHARDLITE_STORAGE_FAULT", "break:10:1ms", "mode is fail"},
		{"SHARDLITE_STORAGE_FAULT", "fail:10", "three fields"},
		{"SHARDLITE_STORAGE_FAULT", "fail:2000:1ms", "permille"},
		{"SHARDLITE_STORAGE_FAULT", "fail:10:soon", "duration"},
		{"SHARDLITE_TEMP_FAULT", "rename:10", "op is create"},
		{"SHARDLITE_TEMP_FAULT", "append", "two fields"},
		{"SHARDLITE_TEMP_FAULT", "append:x", "permille"},
		{"SHARDLITE_MAX_FILES", "0", "positive"},
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
	env := map[string]string{"SHARDLITE_KILL_AT": "segment-after", "SHARDLITE_STORAGE_FAULT": "lost:5:2ms", "SHARDLITE_TEMP_FAULT": "dup:7", "SHARDLITE_MAX_FILES": "2"}
	opts, err := faultOptions(func(k string) string { return env[k] })
	if err != nil || len(opts) != 1 {
		t.Fatalf("faultOptions: %d options, %v", len(opts), err)
	}
}
