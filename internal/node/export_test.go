package node

import (
	"sync"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/model/pos"
)

// TailSet is a set of captured shards for SetTails, for tests that capture
// shards without the shardset.
type TailSet struct {
	mu sync.Mutex
	m  map[*capture.Shard]struct{}
}

// Track remembers a shard, before its capture's first sync.
func (s *TailSet) Track(sh *capture.Shard) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[*capture.Shard]struct{}{}
	}
	s.m[sh] = struct{}{}
}

// Untrack forgets a shard that is no longer active.
func (s *TailSet) Untrack(sh *capture.Shard) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, sh)
}

// Tails is the lowest segment a tracked shard's tail still needs, 0 if none.
func (s *TailSet) Tails() pos.Seg {
	var from pos.Seg
	s.mu.Lock()
	defer s.mu.Unlock()
	for sh := range s.m {
		if f := sh.TailFrom(); f != 0 && (from == 0 || f < from) {
			from = f
		}
	}
	return from
}
