// Package kill names the places a fault test can kill a node. The names are
// one vocabulary spread over capture, the test node and the fault rig, and
// they cross the test node's command line, so they do not change.
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
)

// ByName names the point a rig arms by name. ok is false for a name that is no
// point's: the test node refuses it rather than arms a point that never fires,
// so a misspelled name fails at the arm instead of leaving the test to pass
// without killing anything.
func ByName(name string) (Point, bool) {
	switch Point(name) {
	case Restored, Folded, ReadHeld, FirstSyncDone, SegmentBefore, SegmentAfter,
		RestoreBeforeRename, DeltaBeforeManifest, RebaseBeforeManifest, RebaseAfterSwap:
		return Point(name), true
	}
	return "", false
}
