//go:build linux

package replica

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// A root is a directory opened once. Every path below it is walked by handle, one component at a time, never by
// name: a symlink an App planted in its own volume can neither be followed out of it nor swapped in mid-walk,
// though this agent runs as root.
type root struct {
	fd   int
	path string
}

func openRoot(p string) (*root, error) {
	fd, err := unix.Open(p, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("replica: open %s: %w", p, err)
	}
	return &root{fd: fd, path: p}, nil
}

func (r *root) close() { _ = unix.Close(r.fd) }

const dirFlags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_NOFOLLOW | unix.O_CLOEXEC

// dir opens rel itself as a directory ("" is the root), creating what is missing when create.
func (r *root) dir(rel string, create bool) (int, error) {
	// A fresh open, never a dup: a dup shares the directory's read position, and a second listing would be empty.
	fd, err := unix.Openat(r.fd, ".", dirFlags, 0)
	if err != nil {
		return -1, err
	}
	if rel == "" {
		return fd, nil
	}
	for _, c := range strings.Split(rel, "/") {
		next, err := unix.Openat(fd, c, dirFlags, 0)
		if errors.Is(err, unix.ENOENT) && create {
			if merr := unix.Mkdirat(fd, c, 0o755); merr != nil && !errors.Is(merr, unix.EEXIST) {
				unix.Close(fd)
				return -1, merr
			}
			next, err = unix.Openat(fd, c, dirFlags, 0)
		}
		unix.Close(fd)
		if err != nil {
			return -1, err
		}
		fd = next
	}
	return fd, nil
}

// parent opens the directory that holds rel and names rel's last component in it.
func (r *root) parent(rel string, create bool) (int, string, error) {
	dir, name := path.Split(rel)
	fd, err := r.dir(strings.TrimSuffix(dir, "/"), create)
	return fd, name, err
}

func entryOf(st *unix.Stat_t) (Entry, bool) {
	e := Entry{Mode: st.Mode & 0o7777, UID: st.Uid, GID: st.Gid, MTime: st.Mtim.Nano(), Size: st.Size}
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		e.Kind = kindFile
	case unix.S_IFDIR:
		e.Kind, e.Size = kindDir, 0
	case unix.S_IFLNK:
		e.Kind = kindLink
	default:
		// Sockets, fifos and devices are not data an App keeps: they are made again where it runs.
		return Entry{}, false
	}
	return e, true
}

func (r *root) statAt(dfd int, name string) (Entry, bool, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(dfd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return Entry{}, false, err
	}
	e, ok := entryOf(&st)
	if ok && e.Kind == kindLink {
		buf := make([]byte, maxPath)
		n, err := unix.Readlinkat(dfd, name, buf)
		if err != nil {
			return Entry{}, false, err
		}
		e.Target, e.Size = string(buf[:n]), 0
	}
	return e, ok, nil
}

// stat reads one path without following it; a path that is gone answers os.ErrNotExist.
func (r *root) stat(rel string) (Entry, bool, error) {
	dfd, name, err := r.parent(rel, false)
	if err != nil {
		return Entry{}, false, notExist(err)
	}
	defer unix.Close(dfd)
	e, ok, err := r.statAt(dfd, name)
	return e, ok, notExist(err)
}

func notExist(err error) error {
	if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP) {
		return os.ErrNotExist
	}
	return err
}

// list names what a directory holds; "" is the root.
func (r *root) list(rel string) ([]string, error) {
	fd, err := r.dir(rel, false)
	if err != nil {
		return nil, notExist(err)
	}
	f := os.NewFile(uintptr(fd), rel)
	defer f.Close()
	return f.Readdirnames(-1)
}

// open reads one regular file, by handle.
func (r *root) open(rel string) (*os.File, Entry, error) {
	dfd, name, err := r.parent(rel, false)
	if err != nil {
		return nil, Entry{}, notExist(err)
	}
	defer unix.Close(dfd)
	fd, err := unix.Openat(dfd, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, Entry{}, notExist(err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		unix.Close(fd)
		return nil, Entry{}, err
	}
	e, ok := entryOf(&st)
	if !ok || e.Kind != kindFile {
		unix.Close(fd)
		return nil, Entry{}, os.ErrNotExist
	}
	return os.NewFile(uintptr(fd), rel), e, nil
}

// chunksOf reads one file of a copy to say which chunks it holds, and how many bytes.
func (r *root) chunksOf(rel string, fast bool) ([]chunk, int64, error) {
	f, _, err := r.open(rel)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	var l listOf
	n, err := io.Copy(&l, &behind{f: f, fast: fast})
	if err != nil {
		return nil, 0, err
	}
	list := l.done()
	if list == nil {
		list = []chunk{}
	}
	return list, n, nil
}

var tmpSeq atomic.Uint64

// tempFile is a file being written beside its place, renamed in only once whole: a reader never sees half of one.
type tempFile struct {
	f    *os.File
	dfd  int
	tmp  string
	name string
	r    *root
}

func (r *root) temp(rel string) (*tempFile, error) {
	dfd, name, err := r.parent(rel, true)
	if err != nil {
		return nil, err
	}
	tmp := fmt.Sprintf(".deplo-replica-%d-%d", os.Getpid(), tmpSeq.Add(1))
	fd, err := unix.Openat(dfd, tmp, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		unix.Close(dfd)
		return nil, err
	}
	return &tempFile{f: os.NewFile(uintptr(fd), tmp), dfd: dfd, tmp: tmp, name: name, r: r}, nil
}

// commit gives the file its owner, mode and time and puts it in place of whatever held its name.
func (t *tempFile) commit(e Entry) error {
	err := settle(int(t.f.Fd()), e)
	if cerr := t.f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = t.r.clearForAt(t.dfd, t.name, kindFile)
	}
	if err == nil {
		err = unix.Renameat(t.dfd, t.tmp, t.dfd, t.name)
	}
	if err != nil {
		_ = unix.Unlinkat(t.dfd, t.tmp, 0)
	}
	unix.Close(t.dfd)
	return err
}

func (t *tempFile) abort() {
	t.f.Close()
	_ = unix.Unlinkat(t.dfd, t.tmp, 0)
	unix.Close(t.dfd)
}

// put writes a file whole from src beside its place and renames it in.
func (r *root) put(rel string, e Entry, src io.Reader) error {
	t, err := r.temp(rel)
	if err != nil {
		return err
	}
	if _, err := io.CopyN(t.f, src, e.Size); err != nil {
		t.abort()
		return err
	}
	return t.commit(e)
}

func settle(fd int, e Entry) error {
	if err := unix.Fchown(fd, int(e.UID), int(e.GID)); err != nil {
		return err
	}
	if err := unix.Fchmod(fd, e.Mode); err != nil {
		return err
	}
	ts := []unix.Timespec{unix.NsecToTimespec(e.MTime), unix.NsecToTimespec(e.MTime)}
	return unix.UtimesNanoAt(fd, "", ts, unix.AT_EMPTY_PATH)
}

// clearForAt removes what holds name when it is not of kind, so the new entry can take its place.
func (r *root) clearForAt(dfd int, name string, kind byte) error {
	cur, ok, err := r.statAt(dfd, name)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	if err != nil {
		return err
	}
	if ok && cur.Kind == kind && kind != kindLink {
		return nil
	}
	return removeAt(dfd, name)
}

func (r *root) mkdir(rel string, e Entry) error {
	dfd, name, err := r.parent(rel, true)
	if err != nil {
		return err
	}
	defer unix.Close(dfd)
	if err := r.clearForAt(dfd, name, kindDir); err != nil {
		return err
	}
	if err := unix.Mkdirat(dfd, name, 0o700); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}
	fd, err := unix.Openat(dfd, name, dirFlags, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Fchown(fd, int(e.UID), int(e.GID)); err != nil {
		return err
	}
	return unix.Fchmod(fd, e.Mode)
}

func (r *root) symlink(rel string, e Entry) error {
	dfd, name, err := r.parent(rel, true)
	if err != nil {
		return err
	}
	defer unix.Close(dfd)
	if err := r.clearForAt(dfd, name, kindLink); err != nil {
		return err
	}
	if err := unix.Symlinkat(e.Target, dfd, name); err != nil {
		return err
	}
	if err := unix.Fchownat(dfd, name, int(e.UID), int(e.GID), unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	ts := []unix.Timespec{unix.NsecToTimespec(e.MTime), unix.NsecToTimespec(e.MTime)}
	return unix.UtimesNanoAt(dfd, name, ts, unix.AT_SYMLINK_NOFOLLOW)
}

// remove takes a path away, a whole directory with what it holds; one already gone is no error.
func (r *root) remove(rel string) error {
	dfd, name, err := r.parent(rel, false)
	if err != nil {
		if errors.Is(notExist(err), os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer unix.Close(dfd)
	return removeAt(dfd, name)
}

func removeAt(dfd int, name string) error {
	err := unix.Unlinkat(dfd, name, 0)
	if err == nil || errors.Is(err, unix.ENOENT) {
		return nil
	}
	if !errors.Is(err, unix.EISDIR) && !errors.Is(err, unix.EPERM) {
		return err
	}
	fd, oerr := unix.Openat(dfd, name, dirFlags, 0)
	if oerr != nil {
		return oerr
	}
	f := os.NewFile(uintptr(fd), name)
	names, lerr := f.Readdirnames(-1)
	if lerr == nil {
		for _, n := range names {
			if lerr = removeAt(fd, n); lerr != nil {
				break
			}
		}
	}
	f.Close()
	if lerr != nil {
		return lerr
	}
	return unix.Unlinkat(dfd, name, unix.AT_REMOVEDIR)
}

// empty removes everything a root holds, the root itself staying.
func (r *root) empty() error {
	names, err := r.list("")
	if err != nil {
		return err
	}
	for _, n := range names {
		if err := removeAt(r.fd, n); err != nil {
			return err
		}
	}
	return nil
}

// walk visits every entry below the root, parents first.
func (r *root) walk(fn func(rel string, e Entry) error) error {
	return r.walkDir("", fn)
}

func (r *root) walkDir(rel string, fn func(rel string, e Entry) error) error {
	names, err := r.list(rel)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, n := range names {
		child := n
		if rel != "" {
			child = rel + "/" + n
		}
		if strings.HasPrefix(n, ".deplo-replica-") {
			continue
		}
		e, ok, err := r.stat(child)
		if errors.Is(err, os.ErrNotExist) || (err == nil && !ok) {
			continue
		}
		if err != nil {
			return err
		}
		if err := fn(child, e); err != nil {
			return err
		}
		if e.Kind == kindDir {
			if err := r.walkDir(child, fn); err != nil {
				return err
			}
		}
	}
	return nil
}

// moveTree puts what from holds into to, which is emptied first, by renaming it there when both are on one disk:
// at once, where a copy of 2 GB at a failover starved every server sharing that disk (eu, 24 Nov 2026). used is
// called before the first rename, as from is being used up.
func moveTree(from, to *root, used func()) error {
	var a, b unix.Stat_t
	if unix.Fstat(from.fd, &a) != nil || unix.Fstat(to.fd, &b) != nil || a.Dev != b.Dev {
		return copyTree(from, to)
	}
	if err := to.empty(); err != nil {
		return err
	}
	names, err := from.list("")
	if err != nil {
		return err
	}
	used()
	for _, n := range names {
		if strings.HasPrefix(n, ".deplo-replica-") {
			continue
		}
		if err := unix.Renameat(from.fd, n, to.fd, n); err != nil {
			return err
		}
	}
	return to.dropTemps("")
}

// dropTemps removes the files a write left half done below rel: a copy never holds them as data.
func (r *root) dropTemps(rel string) error {
	names, err := r.list(rel)
	if err != nil {
		return err
	}
	for _, n := range names {
		child := n
		if rel != "" {
			child = rel + "/" + n
		}
		if strings.HasPrefix(n, ".deplo-replica-") {
			if err := r.remove(child); err != nil {
				return err
			}
			continue
		}
		if e, ok, err := r.stat(child); err == nil && ok && e.Kind == kindDir {
			if err := r.dropTemps(child); err != nil {
				return err
			}
		}
	}
	return nil
}

// copyTree puts what from holds into to, which is emptied first: a promoted copy is the App's data, exactly.
func copyTree(from, to *root) error {
	if err := to.empty(); err != nil {
		return err
	}
	return from.walk(func(rel string, e Entry) error {
		switch e.Kind {
		case kindDir:
			return to.mkdir(rel, e)
		case kindLink:
			return to.symlink(rel, e)
		}
		f, cur, err := from.open(rel)
		if err != nil {
			return err
		}
		defer f.Close()
		e.Size = cur.Size
		return to.put(rel, e, f)
	})
}
