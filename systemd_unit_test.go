package main

import (
	"bytes"
	"flag"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/link"
)

// asAgent makes the test binary run main with these newline-separated arguments, the way a unit starts the agent.
const asAgent = "DEPLO_AGENT_TEST_AS_AGENT"

func TestMain(m *testing.M) {
	if args, ok := os.LookupEnv(asAgent); ok {
		os.Args = append([]string{"deplo-agent"}, strings.Split(args, "\n")...)
		flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
		flag.CommandLine.Usage = func() { flag.Usage() } // as the real CommandLine does
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// syncBuffer is the agent's log, written by the process and read by the test.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// agentProc is the agent running as a process; err is how it ended, once exited is closed.
type agentProc struct {
	exited chan struct{}
	err    error
	log    *syncBuffer
}

// startUnderUnit starts the agent as a process with a systemd unit's arguments and environment, stopped like systemd stops it.
func startUnderUnit(t *testing.T, args, env []string) *agentProc {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe)
	log := &syncBuffer{}
	cmd.Stdout, cmd.Stderr = log, log
	cmd.Env = append(os.Environ(), append(env,
		asAgent+"="+strings.Join(args, "\n"),
		"TMPDIR="+t.TempDir(),
		"DEPLO_AGENT_STACK_DIR="+t.TempDir(),
		"DEPLO_AGENT_BUILD_TMP="+t.TempDir())...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &agentProc{exited: make(chan struct{}), log: log}
	go func() { p.err = cmd.Wait(); close(p.exited) }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.exited:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			t.Error("the agent did not stop on SIGTERM")
		}
		if t.Failed() {
			t.Logf("agent log:\n%s", p.log)
		}
	})
	return p
}

// freePort is a port nothing listens on, for an --addr the agent must leave closed.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// linkedOrExited is the agent's first link, or a failure as soon as it exits.
func linkedOrExited(t *testing.T, links <-chan linked, p *agentProc) linked {
	t.Helper()
	select {
	case l := <-links:
		return l
	case <-p.exited:
		t.Fatalf("the agent exited under an old unit: %v", p.err)
	case <-time.After(15 * time.Second):
		t.Fatal("the agent never linked")
	}
	return linked{}
}

// enrolledHost is a host provisioned before the agent link: its agent dir, the panel door it was
// installed from, and a port for --addr that must stay closed.
type enrolledHost struct {
	pki   *testPKI
	dir   string
	door  string
	links <-chan linked
	port  int
}

func newEnrolledHost(t *testing.T) *enrolledHost {
	t.Helper()
	noDocker(t)
	h := &enrolledHost{pki: newTestPKI(t), dir: t.TempDir(), port: freePort(t)}
	h.pki.provisionAgent(t, h.dir)
	h.door, h.links = panelDoor(t)
	return h
}

// oldUnitArgs is install-agent.sh's ExecStart from before the agent link, then any extra flags.
func (h *enrolledHost) oldUnitArgs(extra ...string) []string {
	return append([]string{"--addr", "0.0.0.0:" + strconv.Itoa(h.port), "--data-dir", "/", "--agent-dir", h.dir}, extra...)
}

// Self-update replaces only the binary: a unit written before the agent link still passes --addr.
func TestOldUnit_theAgentLinksAndListensNowhere(t *testing.T) {
	h := newEnrolledHost(t)
	writeInstallEnv(t, h.dir, h.door)
	if err := link.SaveAddresses(h.dir, link.Addresses{Primary: h.door}); err != nil {
		t.Fatal(err)
	}

	// With the EnvironmentFile install-agent.sh points it at.
	p := startUnderUnit(t, h.oldUnitArgs(),
		[]string{"DEPLO_BOOTSTRAP_URL=" + h.door, "DEPLO_BOOTSTRAP_TOKEN=spent", "DEPLO_BOOTSTRAP_FINGERPRINT=aa:bb"})

	l := linkedOrExited(t, h.links, p)
	if hello(t, overLink(t, l.conn, h.pki.panelTLS(t, nil))).GetListening() {
		t.Error("Hello reports a listener")
	}
	if c, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(h.port), time.Second); err == nil {
		c.Close()
		t.Errorf("the agent listens on --addr's port %d", h.port)
	}
}

// The first units (2026-06) named the panel on the command line, before bootstrap.env existed.
// An agent that never received a pair has no other way back to its panel.
func TestOldUnit_theFirstUnitsLinkToTheURLTheyName(t *testing.T) {
	h := newEnrolledHost(t)

	p := startUnderUnit(t, h.oldUnitArgs("--bootstrap-url", h.door, "--bootstrap-token", "spent", "--bootstrap-fingerprint", "aa:bb"), nil)

	l := linkedOrExited(t, h.links, p)
	hello(t, overLink(t, l.conn, h.pki.panelTLS(t, nil)))
	if saved, err := link.LoadAddresses(h.dir); err != nil || saved.Primary != h.door {
		t.Errorf("saved pair %+v (%v), want %s for the next start", saved, err, h.door)
	}
}

func TestUsage_leavesOutTheListenAddress(t *testing.T) {
	p := startUnderUnit(t, []string{"-h"}, nil)
	select {
	case <-p.exited:
	case <-time.After(15 * time.Second):
		t.Fatal("-h did not exit")
	}
	if p.err != nil {
		t.Fatalf("-h: %v", p.err)
	}
	if out := p.log.String(); !strings.Contains(out, "-agent-dir") || strings.Contains(out, "-addr") {
		t.Fatalf("usage text:\n%s", out)
	}
}
