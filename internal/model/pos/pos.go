// Package pos is the numbers of the shardlite protocol: a shard's positions,
// a lease's epochs and a journal's segments (docs/internals.md). One type per
// numbering, so the compiler keeps them apart.
package pos

type Position uint64 // a shard's own commit number, 1-based; 0 is nothing yet
type Epoch uint64    // a node lease's epoch
type Seg uint64      // a node journal segment number; 0 is none
type Count uint64    // a number of commits

// Add is the position of the last of c commits from p.
func (p Position) Add(c Count) Position { return p + Position(c) - 1 }
