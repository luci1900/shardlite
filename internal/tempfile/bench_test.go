package tempfile

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"
)

// BenchmarkAppend is one commit appended to a log page by page, as capture's
// sync does, and read back whole, as the segment upload and the delta do.
// append-ns/op is the append alone, the part that runs under the capture lock.
func BenchmarkAppend(b *testing.B) {
	page := bytes.Repeat([]byte{0xa5}, 4<<10)
	for _, size := range []int{4 << 10, 64 << 10, 1 << 20, 4 << 20, 64 << 20} {
		b.Run(fmt.Sprintf("%dKB", size>>10), func(b *testing.B) {
			dir := NewDir(b.TempDir())
			l, err := dir.Create()
			if err != nil {
				b.Fatal(err)
			}
			defer func() { l.Close() }()
			b.SetBytes(int64(size))
			var appending time.Duration
			n := 0
			for b.Loop() {
				if l.Size() > 256<<20 { // a tail is cut at its delta
					l.Close()
					if l, err = dir.Create(); err != nil {
						b.Fatal(err)
					}
				}
				start := time.Now()
				a, err := l.Append()
				if err != nil {
					b.Fatal(err)
				}
				for range size / len(page) {
					a.Write(page)
				}
				f, err := a.Finish()
				appending += time.Since(start)
				n++
				if err != nil {
					b.Fatal(err)
				}
				if _, err := io.Copy(io.Discard, f.Reader()); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(appending.Nanoseconds())/float64(n), "append-ns/op")
		})
	}
}
