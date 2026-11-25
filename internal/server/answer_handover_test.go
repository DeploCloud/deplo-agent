package server

import (
	"context"
	"testing"
	"time"
)

// c43: back from a restart, visitors waited 4 s for docker to say again what the run before knew. The next run of
// the agent starts from it: the Apps found ready, the proxy's process; not after a reboot, nor from an old file.
func TestHandover_theNextRunStartsFromWhatTheLastOneKnew(t *testing.T) {
	dir := t.TempDir()
	oldProc := proxyProcess
	proxyProcess = func(int) bool { return true }
	proxyPid.Lock()
	oldPid := proxyPid.pid
	proxyPid.pid = 4242
	proxyPid.Unlock()
	t.Cleanup(func() {
		proxyProcess = oldProc
		proxyPid.Lock()
		proxyPid.pid = oldPid
		proxyPid.Unlock()
	})
	first := &Service{agentDir: dir}
	f := first.forwarders()
	f.mu.Lock()
	f.readiness["prj1"] = &answerReadyCheck{ok: true, until: time.Now().Add(time.Second)}
	f.readiness["prj2"] = &answerReadyCheck{ok: false, until: time.Now().Add(time.Second)}
	f.mu.Unlock()
	f.saveHandover()

	proxyPid.Lock()
	proxyPid.pid = 0
	proxyPid.Unlock()
	next := &Service{agentDir: dir}
	next.fwdOps = forwardOps{ensure: func(context.Context, forwardSpec) error { return nil },
		ready: func(context.Context, string) appReadiness { t.Fatal("docker was asked again"); return appReadiness{} }}
	if !next.forwarders().ready(context.Background(), "prj1") {
		t.Fatal("an App ready before the restart waits for docker")
	}
	proxyPid.Lock()
	got := proxyPid.pid
	proxyPid.Unlock()
	if got != 4242 {
		t.Fatalf("the proxy's process is %d", got)
	}
	if next.forwarders().readiness["prj2"] != nil {
		t.Fatal("an App not ready before is taken as known")
	}

	// A reboot, or a file from long ago, hands nothing over.
	for _, stale := range []func(){
		func() { b := bootID; bootID = func() string { return b() + "-next" }; t.Cleanup(func() { bootID = b }) },
	} {
		f.saveHandover()
		stale()
		later := &Service{agentDir: dir}
		if later.forwarders().readiness["prj1"] != nil {
			t.Fatal("a handover from another boot was taken")
		}
	}
}
