package server

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func freshKept(t *testing.T) *[]string {
	t.Helper()
	kept.once, kept.byName, kept.mine = sync.Once{}, nil, nil
	was := storeKept
	var sent []string
	storeKept = func(state string, fd int) { sent = append(sent, strings.ReplaceAll(state, "\n", " ")) }
	t.Cleanup(func() { storeKept = was; kept.once, kept.byName, kept.mine = sync.Once{}, nil, nil })
	return &sent
}

// shortDir fits a socket path: t.TempDir() is past the 108 bytes one may have.
func shortDir(t *testing.T) string {
	d, err := os.MkdirTemp("", "kept")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

// c42: at an agent kill -9 the answer forwarder found no listener and reset two visitors of lab-c.
func TestForwarderSocket_aVisitorArrivingWhileTheAgentRestartsWaits(t *testing.T) {
	sent := freshKept(t)
	dir := filepath.Join(shortDir(t), "forwarders", "deplo_fwd_0123456789abcdef")
	first, err := listenForwarder(dir)
	if err != nil {
		t.Fatal(err)
	}
	handed := keptForExec()
	first.Close() // the old agent is gone; only what it kept holds the socket

	c, err := net.DialTimeout("unix", filepath.Join(dir, fwdSocket), time.Second)
	if err != nil {
		t.Fatalf("a visitor arriving while the agent restarts was refused: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("PORT 443\n")); err != nil {
		t.Fatal(err)
	}

	kept.once, kept.byName, kept.mine = sync.Once{}, nil, nil
	name, value, _ := strings.Cut(handed, "=")
	t.Setenv(name, value)
	second, err := listenForwarder(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	in, err := second.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	_ = in.SetReadDeadline(time.Now().Add(time.Second))
	if line, err := bufio.NewReader(in).ReadString('\n'); err != nil || line != "PORT 443\n" {
		t.Fatalf("the new agent read %q, %v", line, err)
	}
	if len(*sent) != 1 || !strings.HasPrefix((*sent)[0], "FDSTORE=1 FDNAME=fwd-deplo_fwd_0123456789abcdef") {
		t.Fatalf("systemd was told %q, want the one listener stored once", *sent)
	}
	if os.Getenv(keptEnv) != "" {
		t.Fatal("the handed listeners stay in the environment of what the agent starts")
	}
}

// A forwarder that is gone leaves nothing behind in systemd's store.
func TestForgetKept_removesTheListenerFromTheStore(t *testing.T) {
	sent := freshKept(t)
	dir := filepath.Join(shortDir(t), "forwarders", "deplo_fwd_00000000000000ff")
	ln, err := listenForwarder(dir)
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()
	forgetKept(keptForwarder(dir))
	if got := (*sent)[len(*sent)-1]; got != "FDSTOREREMOVE=1 FDNAME=fwd-deplo_fwd_00000000000000ff" {
		t.Fatalf("last message %q", got)
	}
	if keptForExec() != keptEnv+"=" {
		t.Fatal("a forgotten listener is still handed to the next binary")
	}
}
