// Package logpipe keeps the agent's log from ever holding it up: a busy disk once stalled journald, every line
// written then waited, and the renewals that wrote one with them (deplo ADR-0043).
package logpipe

import (
	"os"

	"golang.org/x/sys/unix"
)

// pipeBytes is what the log may fall behind by before a line is dropped rather than waited on.
const pipeBytes = 1 << 20

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
	go carry(os.NewFile(uintptr(p[0]), "log pipe"), os.NewFile(uintptr(out), "log"))
	return nil
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
