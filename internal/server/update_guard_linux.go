package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// A self-update never takes a server down (deplo ADR-0043): any exit starts the agent again, and a new binary that
// does not answer its first Hello within updateAnswerWithin is replaced by the one before it.
var updateAnswerWithin = 30 * time.Second

// agentUnit is the service install-agent.sh writes; restartDropIn makes every exit of it start it again.
const (
	agentUnit     = "deplo-agent.service"
	restartDropIn = "# Written by deplo-agent: any exit starts it again (deplo ADR-0043).\n" +
		"[Unit]\nStartLimitIntervalSec=0\n\n[Service]\nRestart=always\nRestartSec=1\n"
)

// unitDir is where systemd reads units from; a var so tests write elsewhere.
var unitDir = "/etc/systemd/system"

func restartDropInPath() string { return filepath.Join(unitDir, agentUnit+".d", "restart.conf") }

// EnsureRestart puts the drop-in in place when this agent runs as deplo-agent.service and it is missing or old.
func EnsureRestart(ctx context.Context) {
	raw, _ := os.ReadFile("/proc/self/cgroup")
	if ownUnit(string(raw)) != agentUnit {
		return
	}
	if err := ensureRestart(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "deplo-agent: could not make every exit of the agent start it again: %v\n", err)
	}
}

func ensureRestart(ctx context.Context) error {
	p := restartDropInPath()
	if b, err := os.ReadFile(p); err == nil && string(b) == restartDropIn {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	if err := writeFileAtomic(p, []byte(restartDropIn), 0o644); err != nil {
		return err
	}
	return systemctl(ctx, "daemon-reload")
}

// startGuard runs, outside this agent's service, the binary before the update: it puts itself back unless the new
// one answers in time. It says why when no guard runs.
var startGuard = func(prev, exe, version, answered string) error {
	raw, _ := os.ReadFile("/proc/self/cgroup")
	unit := ownUnit(string(raw))
	if unit != agentUnit {
		return fmt.Errorf("the agent does not run as %s", agentUnit)
	}
	out, err := exec.Command("systemd-run", "--quiet", "--collect",
		"--unit=deplo-agent-update-guard-"+strconv.FormatInt(time.Now().UnixNano(), 36),
		prev, "update-guard", exe, prev, version, answered, unit).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// restartUnit starts the agent's service again on the binary the guard put back; a var so tests see the call.
var restartUnit = func(unit string) error {
	return systemctl(context.Background(), "restart", "--no-block", unit)
}

// UpdateGuard is `deplo-agent update-guard <exe> <previous> <version> <answered> <unit>`, run by the binary before
// an update: once the new one says it answered, the previous one goes; past the wait it is put back and run.
func UpdateGuard(args []string) int {
	if len(args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: deplo-agent update-guard <exe> <previous> <version> <answered> <unit>")
		return 2
	}
	exe, prev, version, answered, unit := args[0], args[1], args[2], args[3], args[4]
	if guardUpdate(exe, prev, version, answered, time.Now().Add(updateAnswerWithin)) {
		return 0
	}
	if err := restartUnit(unit); err != nil {
		log.Printf("deplo-agent: could not start the agent again on its earlier binary: %v", err)
		return 1
	}
	return 0
}

// guardUpdate says whether v`version` answered before the deadline; when it did not, the previous binary is back at exe.
func guardUpdate(exe, prev, version, answered string, deadline time.Time) bool {
	for {
		if b, err := os.ReadFile(answered); err == nil && strings.TrimSpace(string(b)) == version {
			_ = os.Remove(prev)
			return true
		}
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err := os.Rename(prev, exe); err != nil {
		log.Printf("deplo-agent: v%s did not answer within %s, and the binary before it could not be put back: %v", version, updateAnswerWithin, err)
		return false
	}
	log.Printf("deplo-agent: v%s did not answer within %s: running the binary before it again", version, updateAnswerWithin)
	return false
}
