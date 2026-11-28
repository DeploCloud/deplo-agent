package server

import (
	"cmp"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The unit a server installed before deplo ADR-0043's restart rule still has: the agent fixes it itself.
const oldUnit = `[Unit]
Description=Deplo server agent

[Service]
Type=simple
ExecStart=/usr/local/bin/deplo-agent --insecure --addr 127.0.0.1:9443 --data-dir / --stack-dir /tmp/stacks --build-tmp /tmp/build
Restart=on-failure
RestartSec=5
`

// The real self-update path, inside a throwaway container whose PID 1 is systemd (the CI job of the same name):
// the exec, the new run's first log lines, a restart on SIGPIPE and on exit 0, and a new binary that exits at once.
func TestSystemd_aSelfUpdateRunsTheNewBinaryAndAnyExitStartsItAgain(t *testing.T) {
	bins := os.Getenv("DEPLO_AGENT_TEST_SYSTEMD")
	if bins == "" {
		t.Skip("needs systemd as PID 1: DEPLO_AGENT_TEST_SYSTEMD names the folder holding deplo-agent-<from> and -<to>")
	}
	run := func(name string, args ...string) string {
		t.Helper()
		out, err := exec.Command(name, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v: %s", name, args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	// The versions it updates from and to; the release check on a clone starts from the release before.
	from, to := cmp.Or(os.Getenv("DEPLO_AGENT_TEST_SYSTEMD_FROM"), "1.0.0"), cmp.Or(os.Getenv("DEPLO_AGENT_TEST_SYSTEMD_TO"), "2.0.0")
	const exe = "/usr/local/bin/deplo-agent"
	copyFile(t, filepath.Join(bins, "deplo-agent-"+from), exe)
	if err := os.WriteFile("/etc/systemd/system/deplo-agent.service", []byte(oldUnit), 0o644); err != nil {
		t.Fatal(err)
	}
	run("systemctl", "daemon-reload")
	run("systemctl", "start", "deplo-agent")
	t.Cleanup(func() { _ = exec.Command("journalctl", "-u", "deplo-agent", "--no-pager", "-o", "short-precise").Run() })
	mainPID := func() int {
		n, _ := strconv.Atoi(run("systemctl", "show", "-p", "MainPID", "--value", "deplo-agent"))
		return n
	}

	conn, err := grpc.NewClient("127.0.0.1:9443", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewAgentClient(conn)
	answers := func(version string, within time.Duration) {
		t.Helper()
		end := time.Now().Add(within)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			r, err := client.Hello(ctx, &pb.HelloRequest{})
			cancel()
			if err == nil && r.GetAgentVersion() == version {
				return
			}
			if time.Now().After(end) {
				t.Fatalf("v%s did not answer within %s (last: %v, %v)\n%s", version, within, r.GetAgentVersion(), err,
					run("journalctl", "-u", "deplo-agent", "--no-pager", "-n", "40"))
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	answers(from, 15*time.Second)

	srv := httptest.NewServer(http.FileServer(http.Dir(bins)))
	defer srv.Close()
	update := func(version, file string) {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(bins, file))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := client.SelfUpdate(ctx, &pb.SelfUpdateRequest{Version: version,
			Binaries: map[string]*pb.ArchBinary{runtime.GOARCH: {Url: srv.URL + "/" + file, Sha256: sha256Hex(raw)}}}); err != nil {
			t.Fatalf("SelfUpdate %s: %v", version, err)
		}
	}

	// The exec: same process, the new binary, its first lines in the journal.
	pid := mainPID()
	update(to, "deplo-agent-"+to)
	answers(to, 15*time.Second)
	if got := mainPID(); got != pid {
		t.Fatalf("the update replaced the process (%d, then %d): it should exec in place", pid, got)
	}
	if j := run("journalctl", "-u", "deplo-agent", "--no-pager"); !strings.Contains(j, "deplo-agent "+to+" listening on") {
		t.Fatalf("the new run's first lines are not in the journal:\n%s", j)
	}
	waitWithin(t, 10*time.Second, "every exit to restart the agent", func() bool {
		return run("systemctl", "show", "-p", "Restart", "--value", "deplo-agent") == "always"
	})
	waitWithin(t, 5*time.Second, "the previous binary to go once the new one answered", func() bool {
		_, err := os.Stat(exe + ".previous")
		return os.IsNotExist(err)
	})

	// Any exit starts it again: a death by a signal systemd takes as clean, as canary.47's SIGPIPE was (Go ignores
	// a SIGPIPE sent from outside, so SIGHUP stands in), and exit 0 after a drain.
	for _, sig := range []syscall.Signal{syscall.SIGHUP, syscall.SIGTERM} {
		pid := mainPID()
		if err := syscall.Kill(pid, sig); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		for p := mainPID(); p == 0 || p == pid; p = mainPID() {
			if time.Since(start) > 5*time.Second {
				t.Fatalf("the agent did not run again within 5 s of %s", sig)
			}
			time.Sleep(100 * time.Millisecond)
		}
		answers(to, 5*time.Second-time.Since(start))
	}

	// A new binary that exits at once: the one before it is back within the wait.
	if err := os.WriteFile(filepath.Join(bins, "deplo-agent-broken"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	pid = mainPID()
	update("99.0.0", "deplo-agent-broken")
	want, _ := os.ReadFile(filepath.Join(bins, "deplo-agent-"+to))
	waitWithin(t, updateAnswerWithin+15*time.Second, "v"+to+" to be back on disk", func() bool {
		got, _ := os.ReadFile(exe)
		return sha256Hex(got) == sha256Hex(want) && mainPID() != pid
	})
	answers(to, 10*time.Second)
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	raw, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, raw, 0o755); err != nil {
		t.Fatal(err)
	}
}

// keptHelperEnv makes TestKeptHelper a service that echoes on a kept listener; SIGUSR1 has it exec itself.
const keptHelperEnv = "DEPLO_KEPT_HELPER"

func TestKeptHelper(t *testing.T) {
	path := os.Getenv(keptHelperEnv)
	if path == "" {
		t.Skip("run by TestSystemd_aVisitorWaitsAcrossAKillAndAnExec as its service")
	}
	ln, err := keptUnixListener("fwd-helper", path)
	if err != nil {
		t.Fatal(err)
	}
	usr1 := make(chan os.Signal, 1)
	signal.Notify(usr1, syscall.SIGUSR1)
	go func() {
		<-usr1
		exe, _ := os.Executable()
		_ = reexec(exe, os.Args, os.Environ())
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			select {} // the exec closed it: the new binary takes over
		}
		go func() {
			defer c.Close()
			buf := make([]byte, 64)
			n, _ := c.Read(buf)
			_, _ = c.Write([]byte(strconv.Itoa(os.Getpid()) + ":" + string(buf[:n])))
		}()
	}
}

// c42: a kill -9 of the agent reset the visitors its forwarder handed it meanwhile. Under systemd, a visitor that
// arrives while the agent is dead, or while it execs a new binary, is answered once it is back.
func TestSystemd_aVisitorWaitsAcrossAKillAndAnExec(t *testing.T) {
	if os.Getenv("DEPLO_AGENT_TEST_SYSTEMD") == "" {
		t.Skip("needs systemd as PID 1")
	}
	self, _ := os.Executable()
	sock := "/run/deplo-kept-test.sock"
	out, err := exec.Command("systemd-run", "--unit=deplo-kept-test", "-p", "Restart=always", "-p", "RestartSec=1",
		"-p", "FileDescriptorStoreMax=4", "-p", "FileDescriptorStorePreserve=restart", "-p", "NotifyAccess=main",
		"--setenv="+keptHelperEnv+"="+sock,
		self, "-test.run", "^TestKeptHelper$").CombinedOutput()
	if err != nil {
		t.Fatalf("systemd-run: %v: %s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("systemctl", "stop", "deplo-kept-test").Run() })
	visit := func(what string) string {
		t.Helper()
		var c net.Conn
		var err error
		for i := 0; i < 50; i++ {
			if c, err = net.Dial("unix", sock); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("%s: refused: %v", what, err)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		_, _ = c.Write([]byte(what))
		buf := make([]byte, 64)
		n, err := c.Read(buf)
		if err != nil {
			t.Fatalf("%s: no answer: %v", what, err)
		}
		return string(buf[:n])
	}
	main := func() int {
		raw, _ := exec.Command("systemctl", "show", "-p", "MainPID", "--value", "deplo-kept-test").Output()
		n, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
		return n
	}
	first := visit("before")
	pid := main()
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("a visitor arriving while the agent is dead was refused: %v", err)
	}
	_, _ = c.Write([]byte("meanwhile"))
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	c.Close()
	if err != nil || !strings.HasSuffix(string(buf[:n]), ":meanwhile") || strings.HasPrefix(string(buf[:n]), strconv.Itoa(pid)+":") {
		t.Fatalf("after the kill: %q (%v), first %q", buf[:n], err, first)
	}
	// An explicit restart keeps the store too (c43: an install by hand reset 7 visitors).
	restarted := make(chan error, 1)
	go func() { restarted <- exec.Command("systemctl", "restart", "deplo-kept-test").Run() }()
	for done := false; !done; {
		select {
		case err := <-restarted:
			if err != nil {
				t.Fatal(err)
			}
			done = true
		default:
			c, err := net.Dial("unix", sock)
			if err != nil {
				t.Fatalf("a visitor during a restart was refused: %v", err)
			}
			c.Close()
			time.Sleep(10 * time.Millisecond)
		}
	}
	if got := visit("restart"); !strings.HasSuffix(got, ":restart") {
		t.Fatalf("after a restart: %q", got)
	}
	pid = main()
	if err := syscall.Kill(pid, syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if got := visit("exec" + strconv.Itoa(i)); !strings.HasSuffix(got, ":exec"+strconv.Itoa(i)) {
			t.Fatalf("across the exec: %q", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if main() != pid {
		t.Fatalf("the exec replaced the process")
	}
}
