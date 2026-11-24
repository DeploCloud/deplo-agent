package replica

import (
	"os"

	"golang.org/x/sys/unix"
)

// gentleStep is how much a rebuilt file is written before the copy waits for the disk and lets the pages go: at
// page-cache speed a 2 GB file stalled its whole server for 3 s on eu (24 Nov 2026), and with it a lease renewal.
const gentleStep = 8 << 20

// flushRange waits until a written range is on disk and drops it from memory; a var so a test can count.
var flushRange = func(f *os.File, off, n int64) {
	fd := int(f.Fd())
	_ = unix.SyncFileRange(fd, off, n, unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
	_ = unix.Fadvise(fd, off, n, unix.FADV_DONTNEED)
}

// gentle writes to f, a step at a time.
type gentle struct {
	f            *os.File
	off, flushed int64
}

func (g *gentle) Write(p []byte) (int, error) {
	n, err := g.f.Write(p)
	if g.off += int64(n); g.off-g.flushed >= gentleStep {
		g.finish()
	}
	return n, err
}

// finish flushes what is still in memory.
func (g *gentle) finish() {
	if g.off > g.flushed {
		flushRange(g.f, g.flushed, g.off-g.flushed)
		g.flushed = g.off
	}
}
