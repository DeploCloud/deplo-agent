// Package room keeps every copy the agent writes from filling a disk: a tenth of it always stays free (deplo ADR-0043).
package room

import (
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// Marker ends every refusal, so the panel tells a copy that does not fit from any other failure.
const Marker = "keep a tenth of its disk free"

// Step is how much a Writer writes between two looks at the disk.
const Step = 8 << 20

// ErrNoRoom is a copy refused or stopped for room; errors.Is finds it under the sentence.
var ErrNoRoom = errors.New(Marker)

type noRoom struct {
	who        string
	free, need int64
}

func (e noRoom) Error() string {
	who := e.who
	if who == "" {
		who = "this server"
	}
	if e.need > 0 {
		return fmt.Sprintf("%s has %s free, too little for this copy (about %s) and to %s", who, Human(e.free), Human(e.need), Marker)
	}
	return fmt.Sprintf("%s has %s free, too little to go on with this copy and %s", who, Human(e.free), Marker)
}

// Refuse is a copy of need bytes that another server, as who, has no room for.
func Refuse(who string, free, need int64) error { return noRoom{who: who, free: free, need: need} }

func (noRoom) Is(target error) bool { return target == ErrNoRoom }

// Fits says whether need more bytes leave a tenth of the disk free; an unknown disk always fits.
func Fits(free, total, need int64) bool { return total <= 0 || free-need >= total/10 }

// Stat is the free and total bytes of the filesystem a path, or an open file, is on; vars so tests can fill a disk.
var (
	Stat = func(path string) (free, total int64, ok bool) {
		var st unix.Statfs_t
		if unix.Statfs(path, &st) != nil {
			return 0, 0, false
		}
		return int64(st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize), true
	}
	StatFile = func(f *os.File) (free, total int64, ok bool) {
		var st unix.Statfs_t
		if unix.Fstatfs(int(f.Fd()), &st) != nil {
			return 0, 0, false
		}
		return int64(st.Bavail) * int64(st.Bsize), int64(st.Blocks) * int64(st.Bsize), true
	}
)

// Check refuses a copy of need bytes onto path's disk unless a tenth of it stays free; nil when the disk is unknown.
func Check(path string, need int64) error {
	free, total, ok := Stat(path)
	if !ok || Fits(free, total, need) {
		return nil
	}
	return noRoom{free: free, need: need}
}

// CheckDir is Check on whatever disk dir is on, for a copy whose size is unknown: refused once a tenth is left.
func CheckDir(dir string) error {
	free, total, ok := Stat(dir)
	if !ok || Fits(free, total, 0) {
		return nil
	}
	return noRoom{free: free}
}

// Left says why a copy writing onto f stops now, nil while a tenth of its disk is still free.
func Left(f *os.File) error {
	free, total, ok := StatFile(f)
	if !ok || Fits(free, total, 0) {
		return nil
	}
	return noRoom{free: free}
}

// Writer writes to F, and fails once F's disk is down to a tenth free; it looks every Step.
type Writer struct {
	F                *os.File
	written, checked int64
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.written == 0 || w.written-w.checked >= Step {
		w.checked = w.written
		if err := Left(w.F); err != nil {
			return 0, err
		}
	}
	n, err := w.F.Write(p)
	w.written += int64(n)
	return n, err
}

var _ io.Writer = (*Writer)(nil)

// Human is a size as the panel writes one.
func Human(b int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	i, f := 0, float64(b)
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%d B", b)
	}
	return fmt.Sprintf("%.1f %s", f, units[i])
}
