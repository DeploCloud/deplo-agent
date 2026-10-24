package server

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Where dockerd's BuildKit puts each RUN step's cgroup on cgroup v2 with the systemd driver
// (Docker 29): <dir>/system.slice:docker:<id>. Empty turns the cap off.
var buildStepCgroupDir = "/sys/fs/cgroup/system.slice"

const buildStepCgroupPrefix = "system.slice:docker:"

var hostMemTotal = func() int64 {
	var si syscall.Sysinfo_t
	if syscall.Sysinfo(&si) != nil {
		return 0
	}
	return int64(si.Totalram) * int64(si.Unit)
}

var buildMemCap struct {
	sync.Mutex
	builds int
	limit  int64
	stop   chan struct{}
	done   chan struct{}
}

var memCapUnavailable sync.Once

// capBuildSteps caps every BuildKit step cgroup that appears from now on at 75% of the host's
// memory, until release; builds running at once share one watcher. limit is 0 on a host
// whose layout it does not know (cgroup v1, another driver), which it logs once.
func capBuildSteps() (limit int64, release func()) {
	c := &buildMemCap
	c.Lock()
	defer c.Unlock()
	if c.builds == 0 {
		dir := buildStepCgroupDir
		limit, why := hostMemTotal()*3/4, buildCapLayout(dir)
		if why == "" && limit <= 0 {
			why = "its memory size is unknown"
		}
		if why != "" {
			if dir != "" {
				memCapUnavailable.Do(func() { log.Printf("deplo-agent: no build memory cap on this host (%s)", why) })
			}
			return 0, func() {}
		}
		c.limit, c.stop, c.done = limit, make(chan struct{}), make(chan struct{})
		go watchBuildSteps(dir, c.limit, stepCgroups(dir), c.stop, c.done)
	}
	c.builds++
	return c.limit, func() {
		c.Lock()
		defer c.Unlock()
		if c.builds--; c.builds == 0 {
			close(c.stop)
			<-c.done
		}
	}
}

func buildCapLayout(dir string) string {
	if dir == "" {
		return "turned off"
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "cgroup.controllers")); err != nil {
		return "not cgroup v2"
	}
	ctl, err := os.ReadFile(filepath.Join(dir, "cgroup.subtree_control"))
	if err != nil || !slices.Contains(strings.Fields(string(ctl)), "memory") {
		return "no memory controller under " + dir
	}
	return ""
}

// watchBuildSteps caps each step cgroup missing from seen, the ones already there being no build of this agent's.
func watchBuildSteps(dir string, limit int64, seen map[string]bool, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		now := stepCgroups(dir)
		for name := range now {
			if !seen[name] {
				path := filepath.Join(dir, name)
				_ = writeCgroupFile(filepath.Join(path, "memory.max"), strconv.FormatInt(limit, 10))
				_ = writeCgroupFile(filepath.Join(path, "memory.oom.group"), "1")
			}
		}
		seen = now
	}
}

func stepCgroups(dir string) map[string]bool {
	entries, _ := os.ReadDir(dir)
	out := map[string]bool{}
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), buildStepCgroupPrefix) {
			out[e.Name()] = true
		}
	}
	return out
}

func writeCgroupFile(path, value string) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, err = f.WriteString(value)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// memoryKilled names a build step the memory cap stopped; limit 0 means no cap was set.
func memoryKilled(lines []string, limit int64) string {
	if !strings.Contains(strings.Join(lines, "\n"), "exit code: 137") {
		return ""
	}
	if limit <= 0 {
		return "The build ran out of memory (exit code 137)."
	}
	return fmt.Sprintf("The build needed more than %.1f GB of memory, the most a build may use on this server.",
		float64(limit)/(1<<30))
}
