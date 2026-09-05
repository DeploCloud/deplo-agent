package server

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// The variable every process of a cron job carries. Docker has no "kill the
// exec'd process" API: when the exec client goes away the command lives on, so
// the job is found again by the marker its whole process tree inherited.
const jobMarkerEnv = "DEPLO_JOB_ID"

// killMarkedProcesses stops every process on the host whose environment carries
// the job's marker: TERM, a short grace, then KILL for whatever is left.
func killMarkedProcesses(jobID string, grace time.Duration) int {
	self := os.Getpid()
	marker := []byte(jobMarkerEnv + "=" + jobID + "\x00")
	find := func() []int {
		var pids []int
		matches, _ := filepath.Glob("/proc/[0-9]*/environ")
		for _, path := range matches {
			pid, err := strconv.Atoi(filepath.Base(filepath.Dir(path)))
			if err != nil || pid == self {
				continue
			}
			// Unreadable (gone, or a kernel thread) is "not ours".
			env, err := os.ReadFile(path)
			if err != nil || !bytes.Contains(env, marker) {
				continue
			}
			pids = append(pids, pid)
		}
		return pids
	}

	pids := find()
	for _, pid := range pids {
		_ = syscall.Kill(pid, syscall.SIGTERM)
	}
	deadline := time.Now().Add(grace)
	for len(pids) > 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		pids = find()
	}
	for _, pid := range pids {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
	return len(pids)
}
