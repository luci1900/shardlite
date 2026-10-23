package capture

import (
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/spool"
)

// TailFile is one captured commit file not yet in a delta.
type TailFile struct {
	Position pos.Position // the shard's position of its first commit
	Count    pos.Count    // the number of commits in the file
	// Body is the LTX file: in memory if it is small, in a temporary file if it is
	// larger than the spool limit. Whoever holds a TailFile holds a reference to
	// it, and the TailFiles holding it close it.
	Body *spool.File
	At   time.Time
	// Seg is a lower bound on the number of the node journal segment holding the
	// file: the writer's next number when the file was queued. The node's lease
	// records the oldest one still needed.
	Seg pos.Seg
}

// Last is the shard's position of its last commit.
func (t TailFile) Last() pos.Position { return t.Position.Add(t.Count) }

// TailFiles is a shard's captured commit files not yet in a delta, with the
// references they hold. It has one owner at a time, which closes it exactly
// once; Take hands the ownership to another value.
type TailFiles []TailFile

// Close gives up every file's reference.
func (fs TailFiles) Close() {
	for _, f := range fs {
		f.Body.Close()
	}
}

// Retained returns a copy holding a reference of its own to each file.
func (fs TailFiles) Retained() TailFiles {
	out := make(TailFiles, 0, len(fs))
	for _, f := range fs {
		f.Body.Retain()
		out = append(out, f)
	}
	return out
}

// Take returns the files and leaves none: the ownership has moved.
func (fs *TailFiles) Take() TailFiles {
	t := *fs
	*fs = nil
	return t
}

// tail is the files captured since the shard's last delta, in position order,
// and the position after them. The shard's lock guards it. Others read view.
type tail struct {
	files   TailFiles    // the files restored from the journal first, then this activation's
	nextPos pos.Position // the position the next commit will have

	view atomic.Pointer[tailView] // published under the shard's lock whenever the tail or the position changes
}

// tailView is what the shard tells others without its lock: TailFrom (the
// lease reads it), TailInfo and TailBytes (the watchdog and the delta timer)
// and Position. It is never changed once published.
type tailView struct {
	position pos.Position
	from     pos.Seg
	files    int
	oldest   time.Time
	bytes    int64
}

func newTail(r *Restored) *tail {
	t := &tail{nextPos: 1}
	if r != nil {
		t.files = r.Tail.Take() // the tail owns the files now
		t.nextPos = r.Position + 1
	}
	t.publish(0)
	return t
}

// publish makes the tail and position as they are now the view. queuing, if
// not 0, is the segment bound of a file about to be queued, which TailFrom
// counts already.
func (t *tail) publish(queuing pos.Seg) {
	v := &tailView{position: t.nextPos - 1}
	note := func(seg pos.Seg) {
		if seg != 0 && (v.from == 0 || seg < v.from) {
			v.from = seg
		}
	}
	note(queuing)
	for _, f := range t.files {
		note(f.Seg)
		v.bytes += f.Body.Size()
	}
	if len(t.files) > 0 {
		v.files, v.oldest = len(t.files), t.files[0].At
	}
	t.view.Store(v)
}

func (t *tail) position() pos.Position { return t.nextPos - 1 }

// add records a file of count commits, queued in the journal at segment seg
// (see Shard.queue), and moves the position past it.
func (t *tail) add(body *spool.File, count pos.Count, seg pos.Seg) {
	t.files = append(t.files, TailFile{Position: t.nextPos, Count: count, Body: body.Retain(), At: time.Now(), Seg: seg})
	t.nextPos += pos.Position(count)
	t.publish(0)
}

// discard gives up every file of the tail. The tail holds none afterwards.
func (t *tail) discard() {
	t.files.Close()
	t.files = nil
	t.publish(0)
}

// drop removes files that end at or before position and closes their references.
func (t *tail) drop(position pos.Position) {
	i := 0
	for i < len(t.files) && t.files[i].Last() <= position {
		i++
	}
	t.files[:i].Close()
	t.files = append(TailFiles(nil), t.files[i:]...)
	t.publish(0)
}
