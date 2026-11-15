//go:build linux

package replica

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// watcher says which directories changed. It never says how: the sender reads the directory again and compares,
// so a missed or merged event costs nothing but a look.
type watcher struct {
	fd    int
	mu    sync.Mutex
	dirs  map[int32]Key
	byDir map[Key]int32
	dirty chan Key
	over  chan struct{}
	stop  chan struct{}
	once  sync.Once
	// full is set once the kernel refused another watch: from then on the sender rescans on a timer.
	full bool
}

const watchMask = unix.IN_CREATE | unix.IN_DELETE | unix.IN_MODIFY | unix.IN_CLOSE_WRITE | unix.IN_MOVED_FROM |
	unix.IN_MOVED_TO | unix.IN_ATTRIB | unix.IN_DELETE_SELF | unix.IN_MOVE_SELF | unix.IN_ONLYDIR | unix.IN_EXCL_UNLINK

func newWatcher() (*watcher, error) {
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return nil, fmt.Errorf("replica: inotify: %w", err)
	}
	w := &watcher{fd: fd, dirs: map[int32]Key{}, byDir: map[Key]int32{}, dirty: make(chan Key, 4096),
		over: make(chan struct{}, 1), stop: make(chan struct{})}
	go w.read()
	return w, nil
}

// add watches one directory, reached by handle so the path the kernel resolves is the one walked.
func (w *watcher) add(r *root, k Key) error {
	fd, err := r.dir(k.Path, false)
	if err != nil {
		return notExist(err)
	}
	defer unix.Close(fd)
	wd, err := unix.InotifyAddWatch(w.fd, fmt.Sprintf("/proc/self/fd/%d", fd), watchMask)
	if errors.Is(err, unix.ENOSPC) {
		w.mu.Lock()
		w.full = true
		w.mu.Unlock()
		return nil
	}
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.dirs[int32(wd)] = k
	w.byDir[k] = int32(wd)
	w.mu.Unlock()
	return nil
}

// drop stops watching a directory and what was below it.
func (w *watcher) drop(k Key) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for d, wd := range w.byDir {
		if d.Root == k.Root && (d.Path == k.Path || len(d.Path) > len(k.Path) && d.Path[:len(k.Path)+1] == k.Path+"/") {
			_, _ = unix.InotifyRmWatch(w.fd, uint32(wd))
			delete(w.byDir, d)
			delete(w.dirs, wd)
		}
	}
}

func (w *watcher) degraded() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.full
}

func (w *watcher) read() {
	defer unix.Close(w.fd)
	buf := make([]byte, 64<<10)
	fds := []unix.PollFd{{Fd: int32(w.fd), Events: unix.POLLIN}}
	for {
		select {
		case <-w.stop:
			return
		default:
		}
		// A blocked read is never woken by a close: wait a little at a time, and look for the stop between.
		if _, err := unix.Poll(fds, 250); err != nil && !errors.Is(err, unix.EINTR) {
			return
		}
		n, err := unix.Read(w.fd, buf)
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return
		}
		for off := 0; off+unix.SizeofInotifyEvent <= n; {
			ev := (*unix.InotifyEvent)(unsafe.Pointer(&buf[off]))
			off += unix.SizeofInotifyEvent + int(ev.Len)
			if ev.Mask&unix.IN_Q_OVERFLOW != 0 {
				select {
				case w.over <- struct{}{}:
				default:
				}
				continue
			}
			w.mu.Lock()
			k, ok := w.dirs[ev.Wd]
			if ev.Mask&unix.IN_IGNORED != 0 {
				delete(w.dirs, ev.Wd)
				delete(w.byDir, k)
			}
			w.mu.Unlock()
			if !ok {
				continue
			}
			select {
			case w.dirty <- k:
			default:
				// More directories changed than fit in the queue: look at everything again.
				select {
				case w.over <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (w *watcher) close() { w.once.Do(func() { close(w.stop) }) }
