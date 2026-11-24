// Package logpipe keeps the agent's log from ever holding it up: a busy disk once stalled journald, every line
// written then waited, and the renewals that wrote one with them (deplo ADR-0043).
package logpipe

import (
	"net"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// pipeBytes is what the log may fall behind by before a line is dropped rather than waited on.
const pipeBytes = 1 << 20

var (
	mu   sync.Mutex
	orig = map[int]int{}
)

// Unblock puts a pipe of its own in front of fd: a write lands in it at once, and is dropped when the reader of the
// real log falls more than pipeBytes behind. A goroutine carries what lands to where fd pointed before.
func Unblock(fd int) error {
	out, err := unix.Dup(fd)
	if err != nil {
		return err
	}
	var p [2]int
	if err := unix.Pipe2(p[:], unix.O_CLOEXEC); err != nil {
		unix.Close(out)
		return err
	}
	_, _ = unix.FcntlInt(uintptr(p[1]), unix.F_SETPIPE_SZ, pipeBytes)
	if err := unix.SetNonblock(p[1], true); err != nil {
		closeAll(out, p[0], p[1])
		return err
	}
	if err := unix.Dup3(p[1], fd, 0); err != nil {
		closeAll(out, p[0], p[1])
		return err
	}
	unix.Close(p[1])
	mu.Lock()
	orig[fd] = out
	mu.Unlock()
	go carry(os.NewFile(uintptr(p[0]), "log pipe"), os.NewFile(uintptr(out), "log"))
	return nil
}

// Restore points each fd back at the log it had, before an exec: the pipe's reader does not survive it, and the
// next run's first line killed it by SIGPIPE, a stop systemd takes as clean (eu, 24 Nov 2026, canary.47).
func Restore() {
	mu.Lock()
	defer mu.Unlock()
	for fd, out := range orig {
		_ = unix.Dup3(out, fd, 0)
	}
}

func carry(from, to *os.File) {
	buf := make([]byte, 64<<10)
	for {
		n, err := from.Read(buf)
		if n > 0 {
			_, _ = to.Write(buf[:n])
		}
		if err != nil {
			return
		}
	}
}

func closeAll(fds ...int) {
	for _, fd := range fds {
		unix.Close(fd)
	}
}

// journalStream is where systemd's journal takes a service's output; a var so a test can listen in its place.
var journalStream = "/run/systemd/journal/stdout"

// Revive gives fd a log again when it is a pipe nobody reads, as canary.47 left it across a self-update: a stream
// to the journal, else /dev/null, so a line never stops the agent by SIGPIPE. It says whether fd was dead.
func Revive(fd int, name string) bool {
	p := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLOUT}}
	if n, err := unix.Poll(p, 0); err != nil || n == 0 || p[0].Revents&unix.POLLERR == 0 {
		return false
	}
	if c, err := net.Dial("unix", journalStream); err == nil {
		// The journal's stream header: identifier, unit, priority, then no level prefix and no forwarding.
		if _, err := c.Write([]byte(name + "\n\n6\n0\n0\n0\n0\n")); err == nil {
			if f, err := c.(*net.UnixConn).File(); err == nil {
				_ = unix.Dup3(int(f.Fd()), fd, 0)
				f.Close()
				c.Close()
				return true
			}
		}
		c.Close()
	}
	if null, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0); err == nil {
		_ = unix.Dup3(int(null.Fd()), fd, 0)
		null.Close()
	}
	return true
}
