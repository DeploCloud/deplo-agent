package server

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// Any exit starts the agent again, on a server installed before the unit said so too (deplo ADR-0043).
func TestEnsureRestart_writesTheDropInOnce(t *testing.T) {
	was, wasDir := systemctl, unitDir
	unitDir = t.TempDir()
	var calls []string
	systemctl = func(_ context.Context, args ...string) error {
		calls = append(calls, strings.Join(args, " "))
		return nil
	}
	t.Cleanup(func() { systemctl, unitDir = was, wasDir })
	for i := 0; i < 2; i++ {
		if err := ensureRestart(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(unitDir, "deplo-agent.service.d", "restart.conf"))
	if err != nil || !strings.Contains(string(raw), "Restart=always") || !strings.Contains(string(raw), "StartLimitIntervalSec=0") ||
		!strings.Contains(string(raw), "FileDescriptorStoreMax=") || !strings.Contains(string(raw), "NotifyAccess=main") ||
		!strings.Contains(string(raw), "FileDescriptorStorePreserve=restart") {
		t.Fatalf("drop-in: %q, %v", raw, err)
	}
	if len(calls) != 1 || calls[0] != "daemon-reload" {
		t.Fatalf("systemctl calls = %v, want one daemon-reload", calls)
	}
}

func guardFiles(t *testing.T) (exe, prev, answered string) {
	t.Helper()
	dir := t.TempDir()
	exe, prev, answered = filepath.Join(dir, "deplo-agent"), filepath.Join(dir, "deplo-agent.previous"), filepath.Join(dir, "answered")
	if os.WriteFile(exe, []byte("NEW"), 0o755) != nil || os.WriteFile(prev, []byte("OLD"), 0o755) != nil ||
		os.WriteFile(answered, []byte("1.0.0\n"), 0o600) != nil {
		t.Fatal("write")
	}
	return exe, prev, answered
}

func TestGuardUpdate_keepsANewBinaryThatAnswers(t *testing.T) {
	exe, prev, answered := guardFiles(t)
	go func() { time.Sleep(300 * time.Millisecond); _ = os.WriteFile(answered, []byte("2.0.0\n"), 0o600) }()
	if !guardUpdate(exe, prev, "2.0.0", answered, time.Now().Add(5*time.Second)) {
		t.Fatal("a binary that answered was taken back")
	}
	if b, _ := os.ReadFile(exe); string(b) != "NEW" {
		t.Fatalf("exe holds %q", b)
	}
	if _, err := os.Stat(prev); !os.IsNotExist(err) {
		t.Fatal("the previous binary is still kept")
	}
}

// The run before the update answered too: only the new version's answer counts.
func TestGuardUpdate_putsThePreviousBackWhenTheNewOneIsSilent(t *testing.T) {
	exe, prev, answered := guardFiles(t)
	if guardUpdate(exe, prev, "2.0.0", answered, time.Now().Add(500*time.Millisecond)) {
		t.Fatal("a silent binary was kept")
	}
	if b, _ := os.ReadFile(exe); string(b) != "OLD" {
		t.Fatalf("exe holds %q, want the previous binary", b)
	}
}

func TestUpdateGuard_startsTheServiceAgainOnThePreviousBinary(t *testing.T) {
	exe, prev, answered := guardFiles(t)
	was, wasRestart := updateAnswerWithin, restartUnit
	updateAnswerWithin = 300 * time.Millisecond
	var restarted string
	restartUnit = func(unit string) error { restarted = unit; return nil }
	t.Cleanup(func() { updateAnswerWithin, restartUnit = was, wasRestart })
	if code := UpdateGuard([]string{exe, prev, "2.0.0", answered, "deplo-agent.service"}); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if restarted != "deplo-agent.service" {
		t.Fatalf("restarted %q", restarted)
	}
}

// The update keeps the running binary beside the new one and hands the guard what it needs.
func TestSelfUpdate_keepsThePreviousBinaryForItsGuard(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "deplo-agent")
	if err := os.WriteFile(exe, []byte("OLD-BINARY"), 0o755); err != nil {
		t.Fatal(err)
	}
	newBytes := []byte("NEW-BINARY")
	stubDownload(t, newBytes, nil)
	captureReexec(t)
	var got []string
	was := startGuard
	startGuard = func(prev, exe, version, answered string) error {
		got = []string{prev, exe, version, answered}
		return nil
	}
	t.Cleanup(func() { startGuard = was })
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(t.TempDir())
	if _, err := s.applyUpdate(context.Background(), exe, &pb.SelfUpdateRequest{Version: "9.9.9",
		Binaries: binariesFor("https://example/deplo-agent-linux-"+runtime.GOARCH, sha256Hex(newBytes))}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got[0] != exe+".previous" || got[1] != exe || got[2] != "9.9.9" || got[3] != s.answeredFile(exe) {
		t.Fatalf("guard started with %v", got)
	}
	if b, _ := os.ReadFile(exe + ".previous"); string(b) != "OLD-BINARY" {
		t.Fatalf("previous holds %q", b)
	}
}

func TestHello_saysItAnsweredForAnUpdatesGuard(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(t.TempDir())
	if _, err := s.Hello(context.Background(), &pb.HelloRequest{}); err != nil {
		t.Fatal(err)
	}
	exe, _ := os.Executable()
	if b, _ := os.ReadFile(s.answeredFile(exe)); strings.TrimSpace(string(b)) != AgentVersion {
		t.Fatalf("answered file holds %q, want %s", b, AgentVersion)
	}
}
