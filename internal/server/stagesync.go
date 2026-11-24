package server

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// StageSync is `deplo-agent stage-sync [--since <unix ns>] <from> <to>`, run inside a backup's copier: it makes <to>
// a copy of <from> and prints the files and bytes it copied.
func StageSync(args []string, stdout, stderr io.Writer) int {
	var since int64
	if len(args) == 4 && args[0] == "--since" {
		n, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			fmt.Fprintln(stderr, "stage-sync: --since takes unix nanoseconds")
			return 2
		}
		since, args = n, args[2:]
	}
	if len(args) != 2 {
		fmt.Fprintln(stderr, "usage: deplo-agent stage-sync [--since <unix ns>] <from> <to>")
		return 2
	}
	var from time.Time
	if since > 0 {
		from = time.Unix(0, since)
	} else {
		// The first copy runs beside the App and the agent: it yields the CPU and the disk to them.
		_ = syscall.Setpriority(syscall.PRIO_PROCESS, 0, 19)
		_, _, _ = unix.Syscall(unix.SYS_IOPRIO_SET, 1, 0, 3<<13)
	}
	files, bytes, err := syncTree(args[0], args[1], from)
	if err != nil {
		fmt.Fprintln(stderr, "stage-sync:", err)
		return 1
	}
	fmt.Fprintf(stdout, "%d %d\n", files, bytes)
	return 0
}

// syncTree makes dst a copy of src: an entry whose type, size, time, mode or owner differ is copied, as is one
// changed at or after since, and what src no longer has goes. A second pass copies only what changed meanwhile.
// ponytail: hard links are copied as separate files, track inodes if a volume ever relies on them.
func syncTree(src, dst string, since time.Time) (files int, bytes int64, err error) {
	s := &syncer{since: since, gentle: since.IsZero()}
	root, err := os.Stat(src)
	if err != nil {
		return 0, 0, err
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		return 0, 0, err
	}
	// The volume's own folder too: a restore gives it these again, and a database refuses a data folder others read.
	if err = s.dir(src, dst); err == nil {
		err = copyMeta(dst, root)
	}
	return s.files, s.bytes, err
}

type syncer struct {
	since time.Time
	// gentle writes in synced steps: a first copy of 2 GB at page-cache speed left a 1-core server unable to renew
	// its lease (eu, 24 Nov 2026), and its writeback stalled every server on the same disk for 20 s.
	gentle bool
	files  int
	bytes  int64
}

func (s *syncer) dir(src, dst string) error {
	ents, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	have := map[string]bool{}
	for _, e := range ents {
		have[e.Name()] = true
	}
	if old, err := os.ReadDir(dst); err == nil {
		for _, o := range old {
			if !have[o.Name()] {
				if err := os.RemoveAll(filepath.Join(dst, o.Name())); err != nil {
					return err
				}
			}
		}
	}
	for _, e := range ents {
		if err := s.entry(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (s *syncer) entry(sp, dp string) error {
	si, err := os.Lstat(sp)
	if errors.Is(err, fs.ErrNotExist) {
		// Gone while the App runs: the pass made while it is paused settles it.
		return nil
	} else if err != nil {
		return err
	}
	di, derr := os.Lstat(dp)
	if derr == nil && di.Mode().Type() != si.Mode().Type() {
		if err := os.RemoveAll(dp); err != nil {
			return err
		}
		derr = fs.ErrNotExist
	}
	switch {
	case si.IsDir():
		if derr != nil {
			if err := os.Mkdir(dp, 0o700); err != nil {
				return err
			}
		}
		if err := s.dir(sp, dp); err != nil {
			return err
		}
	case si.Mode().IsRegular():
		if derr == nil && s.same(si, di) {
			return nil
		}
		if err := copyRegular(sp, dp, s.gentle); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		s.files++
		s.bytes += si.Size()
	case si.Mode()&fs.ModeSymlink != 0:
		to, err := os.Readlink(sp)
		if err != nil {
			return err
		}
		if was, err := os.Readlink(dp); derr != nil || err != nil || was != to {
			_ = os.Remove(dp)
			if err := os.Symlink(to, dp); err != nil {
				return err
			}
		}
	default:
		if derr == nil && s.same(si, di) {
			return nil
		}
		_ = os.Remove(dp)
		st := si.Sys().(*syscall.Stat_t)
		// A socket is made by whoever listens on it again, so a copy has none.
		if si.Mode()&fs.ModeSocket != 0 {
			return nil
		}
		if err := syscall.Mknod(dp, st.Mode, int(st.Rdev)); err != nil {
			return err
		}
	}
	return copyMeta(dp, si)
}

// same says the copy still matches: what a quick look at both tells, and no change since the pass began.
func (s *syncer) same(si, di fs.FileInfo) bool {
	a, b := si.Sys().(*syscall.Stat_t), di.Sys().(*syscall.Stat_t)
	return si.Size() == di.Size() && si.ModTime().Equal(di.ModTime()) && si.Mode() == di.Mode() &&
		a.Uid == b.Uid && a.Gid == b.Gid && (s.since.IsZero() || time.Unix(a.Ctim.Sec, a.Ctim.Nsec).Before(s.since))
}

// gentleStep is how much a gentle copy writes before it waits for the disk and lets the pages go.
const gentleStep = 8 << 20

// flushRange waits until a written range is on disk and drops it from memory; a var so tests can count.
var flushRange = func(f *os.File, off, n int64) {
	fd := int(f.Fd())
	_ = unix.SyncFileRange(fd, off, n, unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER)
	_ = unix.Fadvise(fd, off, n, unix.FADV_DONTNEED)
}

func copyRegular(sp, dp string, gentle bool) error {
	in, err := os.Open(sp)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	if !gentle {
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	}
	buf := make([]byte, 1<<20)
	var off, flushed int64
	for {
		n, rerr := in.Read(buf)
		if n > 0 {
			if _, err := out.Write(buf[:n]); err != nil {
				out.Close()
				return err
			}
			if off += int64(n); off-flushed >= gentleStep {
				flushRange(out, flushed, off-flushed)
				flushed = off
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			out.Close()
			return rerr
		}
	}
	if off > flushed {
		flushRange(out, flushed, off-flushed)
	}
	return out.Close()
}

func copyMeta(dp string, si fs.FileInfo) error {
	st := si.Sys().(*syscall.Stat_t)
	if err := os.Lchown(dp, int(st.Uid), int(st.Gid)); err != nil {
		return err
	}
	if si.Mode()&fs.ModeSymlink != 0 {
		return nil
	}
	if err := os.Chmod(dp, si.Mode()&(fs.ModePerm|fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky)); err != nil {
		return err
	}
	return os.Chtimes(dp, si.ModTime(), si.ModTime())
}
