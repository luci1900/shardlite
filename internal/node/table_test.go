package node

import (
	"errors"
	"testing"
	"time"
)

// A request that finds an entry takes a reference when the shard is active,
// waits when it is being activated or let go of, and fails when it is gone or
// stopped. The stopped case matters most: the shards have stopped, so nothing
// will ever change that entry, and a request that waits for a change there
// waits for a change that never comes.
func TestEntryTake(t *testing.T) {
	t.Parallel()
	now := time.Now()
	why := errors.New("the activation failed")
	for _, tc := range []struct {
		name  string
		state entryState
		err   error // what an activation that failed leaves the entry with
		ok    bool
		wait  bool
		fail  error
	}{
		{"active", entryActive, nil, true, false, nil},
		{"activating", entryActivating, nil, false, true, nil},
		{"busy", entryBusy, nil, false, true, nil},
		{"stopped", entryStopped, nil, false, false, errShardsStopped},
		{"gone", entryGone, why, false, false, why},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newEntry(shardName("wallet/1"), tc.state)
			e.err = tc.err
			ok, wait, err := e.take(now)
			if ok != tc.ok || (wait != nil) != tc.wait || !errors.Is(err, tc.fail) {
				t.Fatalf("ok %v, wait %v, err %v, want ok %v, wait %v, err %v", ok, wait != nil, err, tc.ok, tc.wait, tc.fail)
			}
			want := 0
			if tc.ok {
				want = 1
			}
			if e.refs != want {
				t.Fatalf("the entry took %d references, want %d", e.refs, want)
			}
			if !tc.wait {
				return
			}
			// The wait ends when the entry changes, and the request then takes a
			// reference.
			e.set(entryActive)
			select {
			case <-wait:
			default:
				t.Fatal("the wait did not end when the entry changed")
			}
			if ok, _, err := e.take(now); !ok || err != nil {
				t.Fatalf("the request after the change: ok %v, err %v, want a reference", ok, err)
			}
		})
	}
}
