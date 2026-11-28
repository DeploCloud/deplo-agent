//go:build linux

package dockercli

import (
	"os"
	"os/exec"
	"sync/atomic"
	"syscall"
)

// cliGroup is the cgroup every child is started in, once UseCgroup took it; -1 is where the agent runs. cliDir
// holds its directory open: a file the collector finalizes closes its descriptor under every later child.
var (
	cliGroup atomic.Int64
	cliDir   atomic.Pointer[os.File]
)

func init() { cliGroup.Store(-1) }

// UseCgroup starts every later docker and git child in the cgroup at dir, so what they spend yields to the agent's
// leases (c39: a recreated proxy's docker commands took half of a one-core leader's CPU, renewals came 2.2 s apart).
// A cgroup a child cannot be started in is not used.
func UseCgroup(dir string) bool {
	f, err := os.Open(dir)
	if err != nil {
		return false
	}
	fd := int(f.Fd())
	probe := exec.Command("/bin/true")
	probe.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: fd}
	if probe.Run() != nil {
		f.Close()
		return false
	}
	// Kept open for the agent's life: every child is cloned into it.
	cliDir.Store(f)
	cliGroup.Store(int64(fd))
	return true
}

func intoCgroup(attr *syscall.SysProcAttr) {
	if fd := cliGroup.Load(); fd >= 0 {
		attr.UseCgroupFD, attr.CgroupFD = true, int(fd)
	}
}
