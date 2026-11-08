// Package kill names the places a fault test can kill a node. The names are
// one vocabulary spread over capture, the node and the fault rig, and they cross
// the fault build's environment (SHARDLITE_FAULT_KILL_AT), so they do not change.
package kill

// Point is a place in a node's work where a fault test kills it.
type Point string

const (
	Restored             Point = "restored"
	Folded               Point = "folded"
	ReadHeld             Point = "read-held"
	FirstSyncDone        Point = "first-sync-done"
	SegmentBefore        Point = "segment-before"
	SegmentAfter         Point = "segment-after"
	RestoreBeforeRename  Point = "restore-before-rename"
	DeltaBeforeManifest  Point = "delta-before-manifest"
	RebaseBeforeManifest Point = "rebase-before-manifest"
	RebaseAfterSwap      Point = "rebase-after-swap"
	// The orphan index's entry comes before every write that can leave files no
	// manifest lists (docs/internals.md, Orphan index). These two are right after an
	// entry is created and before the write it is for: a file's upload (a delta,
	// a rebase's base or an adoption's delta) and the manifest's deletion.
	OrphanEntryBeforeFile        Point = "orphan-entry-before-file"
	OrphanEntryBeforeDeleteShard Point = "orphan-entry-before-delete-shard"
	// SnapshotPage is before each page a snapshot reads, in the goroutine that
	// reads them. A test holds the reads there to put the app's work between two
	// of them.
	SnapshotPage Point = "snapshot-page"
	// StopLetGo is after a shard has been let go of at a stop, with the
	// lease still held and the other shards on their way out.
	StopLetGo Point = "stop-let-go"
)

// ByName names the point a rig arms by name. ok is false for a name that is no
// point's: the fault build refuses it at the start rather than arms a point that
// never fires, so a misspelled name fails there instead of leaving the test to
// pass without killing anything.
func ByName(name string) (Point, bool) {
	switch Point(name) {
	case Restored, Folded, ReadHeld, FirstSyncDone, SegmentBefore, SegmentAfter,
		RestoreBeforeRename, DeltaBeforeManifest, RebaseBeforeManifest, RebaseAfterSwap, SnapshotPage, StopLetGo,
		OrphanEntryBeforeFile, OrphanEntryBeforeDeleteShard:
		return Point(name), true
	}
	return "", false
}
