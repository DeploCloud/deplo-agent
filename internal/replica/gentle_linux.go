package replica

import (
	"os"

	"github.com/DeploCloud/deplo-agent/internal/room"
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

// gentle writes to f, a step at a time, and stops once its disk is down to a tenth free (deplo ADR-0043).
type gentle struct {
	f            *os.File
	off, flushed int64
}

func (g *gentle) Write(p []byte) (int, error) {
	if g.off == 0 {
		if err := room.Left(g.f); err != nil {
			return 0, err
		}
	}
	n, err := g.f.Write(p)
	if g.off += int64(n); g.off-g.flushed >= gentleStep {
		g.finish()
		if err == nil {
			err = room.Left(g.f)
		}
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

// dropPages lets a range already read go from memory; a var so a test can count.
var dropPages = func(f *os.File, off, n int64) { _ = unix.Fadvise(int(f.Fd()), off, n, unix.FADV_DONTNEED) }

// behind reads f and lets what it read go from memory a step at a time: three copies reading a 2.2 GB file through
// the page cache of a 2 GB server stalled every task on reclaim, its lease renewals with them (eu, 24 Nov 2026).
type behind struct {
	f            *os.File
	off, dropped int64
}

func (b *behind) Read(p []byte) (int, error) {
	n, err := b.f.Read(p)
	if b.off += int64(n); b.off-b.dropped >= gentleStep || err != nil && b.off > b.dropped {
		dropPages(b.f, b.dropped, b.off-b.dropped)
		b.dropped = b.off
	}
	return n, err
}
