// Package reap collects the child processes an earlier run of this process left behind.
package reap

import "syscall"

// Exited waits for every child process that has already ended and says how many there were. After a self-update
// the agent is still the parent of what its earlier run started; call it before this run starts any of its own.
func Exited() int {
	n := 0
	for {
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
		if pid <= 0 || err != nil {
			return n
		}
		n++
	}
}
