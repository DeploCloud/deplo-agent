package dnszone

import (
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
)

// lockDown keeps the process from ever gaining a privilege again, through a setuid binary or otherwise.
func lockDown() error {
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	return unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0)
}

// procAttr starts the process as nobody, in an empty root and a network of its own, when the agent is root.
func procAttr(root string, namespaces bool) *syscall.SysProcAttr {
	a := &syscall.SysProcAttr{Setpgid: true}
	if unix.Geteuid() != 0 {
		return a
	}
	a.Credential = &syscall.Credential{Uid: nobody, Gid: nobody, Groups: []uint32{}}
	a.Chroot = root
	if namespaces {
		a.Cloneflags = syscall.CLONE_NEWNET | syscall.CLONE_NEWIPC | syscall.CLONE_NEWUTS
	}
	return a
}

// namespaceRefused is a start the host refused only for the namespaces.
func namespaceRefused(err error) bool {
	return errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOSPC)
}
