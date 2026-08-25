package server

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubSystemctl swaps runSystemctl for one that records the verbs it was asked
// for (or fails with `err`), and restores it on cleanup. Keeps the tests off the
// host's real service manager.
func stubSystemctl(t *testing.T, err error) func() []string {
	t.Helper()
	var mu sync.Mutex
	var calls []string
	orig := runSystemctl
	runSystemctl = func(_ context.Context, args ...string) error {
		mu.Lock()
		calls = append(calls, args[0])
		mu.Unlock()
		return err
	}
	t.Cleanup(func() { runSystemctl = orig })
	return func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), calls...) }
}

// captureExit swaps exitProcess for one that records the call instead of ending the
// test runner.
func captureExit(t *testing.T) func() bool {
	t.Helper()
	var mu sync.Mutex
	var fired bool
	orig := exitProcess
	exitProcess = func(int) { mu.Lock(); fired = true; mu.Unlock() }
	t.Cleanup(func() { exitProcess = orig })
	return func() bool { mu.Lock(); defer mu.Unlock(); return fired }
}

// waitExit blocks until the deferred exit fires, and fails the test if it never
// does - the agent promising "stopping" and then staying up would leave a live
// agent on a host the control plane has already forgotten.
func waitExit(t *testing.T, exited func() bool) {
	t.Helper()
	deadline := time.After(selfUninstallGrace + 5*time.Second)
	for !exited() {
		select {
		case <-deadline:
			t.Fatal("the agent never exited")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// unitAt points agentUnitPath at a temp file for the duration of one test.
func unitAt(t *testing.T, path string) {
	t.Helper()
	orig := agentUnitPath
	agentUnitPath = path
	t.Cleanup(func() { agentUnitPath = orig })
}

// The whole footprint goes: unit, state dir, binary, and the process then exits
// on its own rather than being stopped (which would kill it mid-removal).
func TestSelfUninstall_removesUnitStateAndBinary(t *testing.T) {
	ctx := context.Background()
	binDir, agentDir := t.TempDir(), t.TempDir()
	exe := filepath.Join(binDir, "deplo-agent")
	if err := os.WriteFile(exe, []byte("BINARY"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(agentDir, "agent.key"), []byte("KEY"), 0o600); err != nil {
		t.Fatal(err)
	}
	unit := filepath.Join(t.TempDir(), "deplo-agent.service")
	if err := os.WriteFile(unit, []byte("[Unit]"), 0o600); err != nil {
		t.Fatal(err)
	}
	unitAt(t, unit)
	verbs := stubSystemctl(t, nil)
	exited := captureExit(t)

	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(agentDir)

	resp, err := s.applyUninstall(ctx, exe)
	if err != nil {
		t.Fatalf("applyUninstall: %v", err)
	}
	if !resp.GetStopping() {
		t.Error("response does not say the agent is stopping")
	}
	if len(resp.GetRemoved()) != 3 {
		t.Errorf("removed = %v, want the unit, the state dir and the binary", resp.GetRemoved())
	}
	for _, p := range []string{unit, agentDir, exe} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s still exists after the uninstall", p)
		}
	}
	// `disable`, never `stop`/`disable --now`: the unit has no KillMode, so
	// stopping it would kill this process before anything was removed.
	got := verbs()
	if len(got) != 2 || got[0] != "disable" || got[1] != "daemon-reload" {
		t.Errorf("systemctl verbs = %v, want [disable daemon-reload]", got)
	}
	// The exit is deferred, not immediate: the response has to reach the wire.
	if exited() {
		t.Error("the process exited before the response could be flushed")
	}
	waitExit(t, exited)
}

// No unit file (an agent running in a container) is a skip, not a failure.
func TestSelfUninstall_missingUnitIsNotAFailure(t *testing.T) {
	binDir := t.TempDir()
	exe := filepath.Join(binDir, "deplo-agent")
	if err := os.WriteFile(exe, []byte("BINARY"), 0o755); err != nil {
		t.Fatal(err)
	}
	unitAt(t, filepath.Join(t.TempDir(), "absent.service"))
	stubSystemctl(t, errNoSystemd)
	exited := captureExit(t)

	s := New(t.TempDir(), t.TempDir(), "/", "")
	resp, err := s.applyUninstall(context.Background(), exe)
	if err != nil {
		t.Fatalf("applyUninstall: %v", err)
	}
	if len(resp.GetRemoved()) != 1 || resp.GetRemoved()[0] != exe {
		t.Errorf("removed = %v, want just the binary", resp.GetRemoved())
	}
	waitExit(t, exited)
}

// An install dir we cannot write to fails BEFORE anything is removed: finding
// that out after deleting the mTLS materials would leave an agent that can never
// be commanded again and a binary still on disk.
func TestSelfUninstall_readOnlyInstallDirRemovesNothing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores the directory write bit")
	}
	binDir, agentDir := t.TempDir(), t.TempDir()
	exe := filepath.Join(binDir, "deplo-agent")
	if err := os.WriteFile(exe, []byte("BINARY"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(agentDir, "agent.key"), []byte("KEY"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(binDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(binDir, 0o755) })
	stubSystemctl(t, nil)
	captureExit(t)

	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(agentDir)

	if _, err := s.applyUninstall(context.Background(), exe); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v, want FailedPrecondition", err)
	}
	for _, p := range []string{exe, filepath.Join(agentDir, "agent.key")} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s was removed despite the failure: %v", p, err)
		}
	}
}
