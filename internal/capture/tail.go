package capture

import (
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// TailFile is one captured commit file not yet in a delta.
type TailFile struct {
	Position pos.Position // the shard's position of its first commit
	Count    pos.Count    // the number of commits in the file
	// Body is the LTX file: a range of one of the TailFiles' logs.
	Body tempfile.File
	At   time.Time
	// Seg is a lower bound on the number of the node journal segment holding the
	// file: the writer's next number when the file was queued. The node's lease
	// records the oldest one still needed.
	Seg pos.Seg
}

// Last is the shard's position of its last commit.
func (t TailFile) Last() pos.Position { return t.Position.Add(t.Count) }

// TailFiles is a shard's captured commit files not yet in a delta, in position
// order, and the logs that hold them: a log holds the files of a run of commits
// and a file is a range of one. It has one owner at a time, which closes it
// exactly once; Take hands the ownership to another value.
type TailFiles struct {
	Logs  []*tempfile.Log
	Files []TailFile
}

// Close closes the logs. The space of a log is freed once every descriptor of
// it is closed, the dups of it included.
func (fs TailFiles) Close() {
	for _, l := range fs.Logs {
		l.Close()
	}
}

// Retained returns a copy over a dup of each log, which its owner closes
// separately from the original's: it can be read after the original is dropped.
func (fs TailFiles) Retained() (TailFiles, error) {
	out := TailFiles{Logs: make([]*tempfile.Log, 0, len(fs.Logs)), Files: make([]TailFile, len(fs.Files))}
	dups := make(map[*tempfile.Log]*tempfile.Log, len(fs.Logs))
	for _, l := range fs.Logs {
		d, err := l.Dup()
		if err != nil {
			out.Close()
			return TailFiles{}, err
		}
		out.Logs = append(out.Logs, d)
		dups[l] = d
	}
	for i, f := range fs.Files {
		f.Body = f.Body.In(dups[f.Body.Log()])
		out.Files[i] = f
	}
	return out, nil
}

// Take returns the files and leaves none: the ownership has moved.
func (fs *TailFiles) Take() TailFiles {
	t := *fs
	*fs = TailFiles{}
	return t
}

// tail is the files captured since the shard's last delta, in position order,
// and the position after them. The shard's lock guards it. Others read view.
type tail struct {
	files   TailFiles     // the files restored from the journal first, then this activation's
	cur     *tempfile.Log // the log the next file is appended to, nil if the next one starts a log
	nextPos pos.Position  // the position the next commit will have

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
	for _, f := range t.files.Files {
		note(f.Seg)
		v.bytes += f.Body.Size()
	}
	if len(t.files.Files) > 0 {
		v.files, v.oldest = len(t.files.Files), t.files.Files[0].At
	}
	t.view.Store(v)
}

func (t *tail) position() pos.Position { return t.nextPos - 1 }

// log is the log the next file is appended to: the tail's current one, or a new
// one when the tail has none or its current one is broken by a failed write.
func (t *tail) log(dir *tempfile.Dir) (*tempfile.Log, error) {
	if t.cur != nil && t.cur.Broken() != nil {
		t.cur = nil
	}
	if t.cur == nil {
		l, err := dir.Create()
		if err != nil {
			return nil, err
		}
		t.files.Logs = append(t.files.Logs, l)
		t.cur = l
	}
	return t.cur, nil
}

// add records a file of count commits, queued in the journal at segment seg
// (see Shard.queue) at time at, and moves the position past it.
func (t *tail) add(body tempfile.File, count pos.Count, seg pos.Seg, at time.Time) {
	t.files.Files = append(t.files.Files, TailFile{Position: t.nextPos, Count: count, Body: body, At: at, Seg: seg})
	t.nextPos += pos.Position(count)
	t.publish(0)
}

// discard gives up every file of the tail. The tail holds none afterwards.
func (t *tail) discard() {
	t.files.Close()
	t.files, t.cur = TailFiles{}, nil
	t.publish(0)
}

// take hands the tail's files to the caller, who closes them. The tail holds none
// afterwards, and its next file starts a log of its own.
func (t *tail) take() TailFiles {
	fs := t.files.Take()
	t.cur = nil
	t.publish(0)
	return fs
}

// drop removes files that end at or before position and closes the logs that
// hold no other file. If files are left, the tail is cut: its next file starts
// a new log, so that the log they are in is closed once they are dropped too
// (a shard that commits all the time has a tail that is never empty).
func (t *tail) drop(position pos.Position) {
	i := 0
	for i < len(t.files.Files) && t.files.Files[i].Last() <= position {
		i++
	}
	if i == 0 {
		return
	}
	t.files.Files = append([]TailFile(nil), t.files.Files[i:]...)
	if len(t.files.Files) == 0 {
		t.files.Close()
		t.files.Logs, t.cur = nil, nil
	} else {
		t.cur = nil
		used := make(map[*tempfile.Log]bool, len(t.files.Logs))
		for _, f := range t.files.Files {
			used[f.Body.Log()] = true
		}
		kept := t.files.Logs[:0:0]
		for _, l := range t.files.Logs {
			if used[l] {
				kept = append(kept, l)
			} else {
				l.Close()
			}
		}
		t.files.Logs = kept
	}
	t.publish(0)
}
