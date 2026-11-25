package server

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Listeners a visitor reaches this agent on outlive the agent: systemd keeps them in the service's fd store across a
// crash, and a self-update hands them to the new binary, so a visitor arriving meanwhile waits instead of a reset.

// keptEnv carries the kept listeners across a self-update's exec, as name=fd pairs.
const keptEnv = "DEPLO_KEPT_LISTENERS"

var kept = struct {
	sync.Mutex
	once   sync.Once
	byName map[string]*os.File // inherited from the run before, not claimed yet
	mine   map[string]*os.File // claimed or made by this run
	live   map[string]net.Listener
}{}

// execDrain is what the visitors already taken get to finish before an exec: newer ones wait in the socket's queue.
// ponytail: a fixed wait, not a count of what is in flight; a kept-open connection still ends at the exec.
var execDrain = time.Second

// inheritKept reads, once, what systemd (LISTEN_FDS) or the binary before (keptEnv) handed this process.
func inheritKept() {
	kept.once.Do(func() {
		kept.byName, kept.mine, kept.live = map[string]*os.File{}, map[string]*os.File{}, map[string]net.Listener{}
		if pid, _ := strconv.Atoi(os.Getenv("LISTEN_PID")); pid == os.Getpid() {
			n, _ := strconv.Atoi(os.Getenv("LISTEN_FDS"))
			names := strings.Split(os.Getenv("LISTEN_FDNAMES"), ":")
			for i := 0; i < n && i < len(names); i++ {
				keepInherited(names[i], 3+i)
			}
		}
		for _, pair := range strings.Split(os.Getenv(keptEnv), ",") {
			name, fd, ok := strings.Cut(pair, "=")
			if n, err := strconv.Atoi(fd); ok && err == nil {
				keepInherited(name, n)
			}
		}
		for _, k := range []string{"LISTEN_PID", "LISTEN_FDS", "LISTEN_FDNAMES", keptEnv} {
			_ = os.Unsetenv(k)
		}
	})
}

func keepInherited(name string, fd int) {
	unix.CloseOnExec(fd)
	if name == "" || kept.byName[name] != nil {
		_ = unix.Close(fd)
		return
	}
	kept.byName[name] = os.NewFile(uintptr(fd), name)
}

// keptUnixListener is the listener at path, the one the run before kept under name when there is one; a new one is
// made otherwise and kept for the next run.
func keptUnixListener(name, path string) (net.Listener, error) {
	inheritKept()
	kept.Lock()
	defer kept.Unlock()
	if f := kept.byName[name]; f != nil {
		delete(kept.byName, name)
		if ln, err := net.FileListener(f); err == nil {
			kept.mine[name], kept.live[name] = f, ln
			return ln, nil
		}
		_ = f.Close()
		storeKept("FDSTOREREMOVE=1\nFDNAME="+name, -1)
	}
	if f := kept.mine[name]; f != nil {
		_ = f.Close()
		delete(kept.mine, name)
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	f, err := ln.(*net.UnixListener).File()
	if err != nil {
		return ln, nil
	}
	kept.mine[name], kept.live[name] = f, ln
	storeKept("FDSTORE=1\nFDNAME="+name, rawFd(f))
	return ln, nil
}

// rawFd is f's descriptor; f.Fd() would put the socket it shares with a live listener in blocking mode.
func rawFd(f *os.File) int {
	fd := -1
	if rc, err := f.SyscallConn(); err == nil {
		_ = rc.Control(func(d uintptr) { fd = int(d) })
	}
	return fd
}

// dropUnclaimedKept forgets what the run before kept under prefix and this run no longer serves.
func dropUnclaimedKept(prefix string) {
	inheritKept()
	kept.Lock()
	var gone []string
	for name := range kept.byName {
		if strings.HasPrefix(name, prefix) {
			gone = append(gone, name)
		}
	}
	kept.Unlock()
	for _, name := range gone {
		forgetKept(name)
	}
}

// forgetKept drops a listener this agent no longer serves, here and in systemd's store.
func forgetKept(name string) {
	inheritKept()
	kept.Lock()
	defer kept.Unlock()
	for _, m := range []map[string]*os.File{kept.byName, kept.mine} {
		if f := m[name]; f != nil {
			_ = f.Close()
			delete(m, name)
		}
	}
	delete(kept.live, name)
	storeKept("FDSTOREREMOVE=1\nFDNAME="+name, -1)
}

// restarting is this agent ending or exec'ing: it takes no visitor, and another member holds the ones it passes.
var restarting atomic.Bool

// DrainVisitors stops taking visitors before the agent exits: what arrives waits in the queue the kept copies hold
// for the next run, and what it carries gets execDrain to finish.
func DrainVisitors() {
	inheritKept()
	kept.Lock()
	defer kept.Unlock()
	drainLocked()
}

// drainLocked is DrainVisitors; the caller holds kept.
func drainLocked() {
	restarting.Store(true)
	runRestartHooks()
	for name, ln := range kept.live {
		_ = ln.Close()
		delete(kept.live, name)
	}
	for until := time.Now().Add(execDrain); time.Now().Before(until) && !carriedQuiet(); {
		time.Sleep(20 * time.Millisecond)
	}
}

// restoreKept hands systemd every listener this run serves again.
func restoreKept() {
	inheritKept()
	kept.Lock()
	defer kept.Unlock()
	for name, f := range kept.mine {
		storeKept("FDSTORE=1\nFDNAME="+name, rawFd(f))
	}
}

// keptForExec readies the kept listeners for a self-update's exec and says how the new binary finds them.
func keptForExec() string {
	inheritKept()
	kept.Lock()
	defer kept.Unlock()
	drainLocked()
	var pairs []string
	for name, f := range kept.mine {
		fd := rawFd(f)
		if _, _, errno := unix.Syscall(unix.SYS_FCNTL, uintptr(fd), unix.F_SETFD, 0); errno == 0 {
			pairs = append(pairs, fmt.Sprintf("%s=%d", name, fd))
		}
	}
	return keptEnv + "=" + strings.Join(pairs, ",")
}

// storeKept tells systemd about one listener, when this agent runs under it; fd -1 sends none.
var storeKept = func(state string, fd int) {
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return
	}
	if sock[0] == '@' {
		sock = "\x00" + sock[1:]
	}
	c, err := unix.Socket(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return
	}
	defer unix.Close(c)
	var oob []byte
	if fd >= 0 {
		oob = unix.UnixRights(fd)
	}
	if err := unix.Sendmsg(c, []byte(state), oob, &unix.SockaddrUnix{Name: sock}, 0); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not keep a listener across a restart: %v\n", err)
	}
}
