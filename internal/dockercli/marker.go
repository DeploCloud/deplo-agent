package dockercli

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

// MarkerEnv tags a process started inside a container, so it can be found in /proc and stopped
// after the docker client that started it is gone.
const MarkerEnv = "DEPLO_JOB_ID"

// KillMarked stops every host process whose environment carries MarkerEnv=id - SIGTERM, then
// SIGKILL after grace - and returns how many were still alive at the end of the grace.
func KillMarked(id string, grace time.Duration) int {
	self := os.Getpid()
	marker := []byte(MarkerEnv + "=" + id + "\x00")
	find := func() []int {
		var pids []int
		matches, _ := filepath.Glob("/proc/[0-9]*/environ")
		for _, path := range matches {
			pid, err := strconv.Atoi(filepath.Base(filepath.Dir(path)))
			if err != nil || pid == self {
				continue
			}
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
