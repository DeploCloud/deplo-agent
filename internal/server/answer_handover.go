package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// What an agent that restarts knew of its visitors' way: the Apps it found ready and its proxy's process. The next run
// starts from it instead of asking docker first, which on one busy core held the visitors 4 s (c43).

const handoverFile = "answer-handover.json"

// handoverFor is how old a handover may be: a restart takes about a second.
const handoverFor = 30 * time.Second

type answerHandover struct {
	At       time.Time `json:"at"`
	Boot     string    `json:"boot"`
	ProxyPid int       `json:"proxyPid,omitempty"`
	Ready    []string  `json:"ready,omitempty"`
}

var beforeRestart struct {
	sync.Mutex
	hooks []func()
}

// onRestart runs hook as the agent stops taking visitors, before an exit or an exec.
func onRestart(hook func()) {
	beforeRestart.Lock()
	defer beforeRestart.Unlock()
	beforeRestart.hooks = append(beforeRestart.hooks, hook)
}

func runRestartHooks() {
	beforeRestart.Lock()
	hooks := beforeRestart.hooks
	beforeRestart.Unlock()
	for _, h := range hooks {
		h()
	}
}

// handoverEvery is how often a ready App has what this run knows written down: a crash writes nothing.
var handoverEvery = 2 * time.Second

// keepHandover writes the handover now and then, so a run after a crash starts from it too; the caller holds f.mu.
func (f *forwarders) keepHandover() {
	if time.Since(f.handoverAt) < handoverEvery {
		return
	}
	f.handoverAt = time.Now()
	go f.saveHandover()
}

// saveHandover writes what this run knows for the next one.
func (f *forwarders) saveHandover() {
	if f.s.agentDir == "" {
		return
	}
	h := answerHandover{At: bootClock(), Boot: bootID()}
	proxyPid.Lock()
	h.ProxyPid = proxyPid.pid
	proxyPid.Unlock()
	f.mu.Lock()
	now := time.Now()
	for app, rc := range f.readiness {
		// Every App it answers for that its last look found ready, visited lately or not (c50: "1 Apps" of 2).
		if rc.ok && (now.Before(rc.until) || slices.Contains(f.st.ServeAnswers, app)) {
			h.Ready = append(h.Ready, app)
		}
	}
	f.mu.Unlock()
	if raw, err := json.Marshal(h); err == nil {
		_ = writeFileAtomic(filepath.Join(f.s.agentDir, handoverFile), raw, 0o600)
	}
}

// takeHandover starts from what the run before wrote, when it is this boot's and fresh, and removes it.
func (f *forwarders) takeHandover() {
	if f.s.agentDir == "" {
		return
	}
	path := filepath.Join(f.s.agentDir, handoverFile)
	raw, err := os.ReadFile(path)
	_ = os.Remove(path)
	var h answerHandover
	if err != nil || json.Unmarshal(raw, &h) != nil || h.Boot == "" || h.Boot != bootID() || bootClock().Sub(h.At) > handoverFor {
		return
	}
	if h.ProxyPid > 0 && proxyProcess(h.ProxyPid) {
		proxyPid.Lock()
		if proxyPid.pid == 0 {
			proxyPid.pid = h.ProxyPid
		}
		proxyPid.Unlock()
	}
	stderrf("deplo-agent: cluster: started from what its last run knew of %d Apps, docker waits until %d ms after the start",
		len(h.Ready), startQuiet.Milliseconds())
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handedOver = true
	until := time.Now().Add(answerReadyFor)
	for _, app := range h.Ready {
		if f.readiness[app] == nil {
			f.readiness[app] = &answerReadyCheck{ok: true, until: until}
		}
	}
}

// proxyProcess says pid is still a running proxy.
var proxyProcess = func(pid int) bool {
	comm, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/comm")
	return err == nil && strings.TrimSpace(string(comm)) == "traefik"
}
