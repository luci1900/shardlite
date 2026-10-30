package capture_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// nowhere is a writer that queues nothing, so a capture costs only its own
// work: the WAL read, the encode and holding the file.
type nowhere struct{}

func (nowhere) NextSeq() pos.Seg { return 1 }
func (nowhere) AppendFile(shard.Name, shard.Gen, pos.Position, tempfile.File) error {
	return nil
}
func (nowhere) Flush(context.Context) error { return nil }

// BenchmarkCapture is the cost of one sync under the shard's lock, at commits
// from one page to 64 MB: what the tail's storage is weighed against. The app's
// commit and the tail's drop are outside the timer, and the WAL is never
// restarted, so a restart's checkpoint is not counted.
func BenchmarkCapture(b *testing.B) {
	const rowBytes = 3900 // a row a page, at 4 KB pages
	for _, size := range []int{4 << 10, 64 << 10, 1 << 20, 4 << 20, 64 << 20} {
		b.Run(fmt.Sprintf("%dKB", size>>10), func(b *testing.B) {
			app := apptest.Start(b)
			path := filepath.Join(b.TempDir(), "bench.db")
			app.Must(b, "open", "a", path)
			sh, err := capture.New(capture.Config{Name: shardName("test.bench/1"), Path: path, Generation: "g1", Writer: nowhere{}, TempDir: tempfile.NewDir(b.TempDir()), RestartFrames: 1 << 30})
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			defer sh.Close(ctx)
			if err := sh.Capture(ctx); err != nil {
				b.Fatal(err)
			}
			rows := strconv.Itoa(size / 4096)
			b.SetBytes(int64(size))
			var encoded int64
			for b.Loop() {
				b.StopTimer()
				sh.DropTail(sh.Position())
				app.Must(b, "insert", "a", rows, strconv.Itoa(rowBytes))
				b.StartTimer()
				if err := sh.Capture(ctx); err != nil {
					b.Fatal(err)
				}
				encoded = sh.TailBytes()
			}
			b.ReportMetric(float64(encoded), "ltx-bytes")
		})
	}
}
