package logpipe

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// journal stands in for journald: a blocking pipe whose reader the test controls, and fd the agent's log on it.
func journal(t *testing.T) (r *os.File, fd int) {
	t.Helper()
	var p [2]int
	if err := unix.Pipe2(p[:], unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	r = os.NewFile(uintptr(p[0]), "journal")
	t.Cleanup(func() { r.Close(); unix.Close(p[1]) })
	return r, p[1]
}

func writeLines(fd int, n int) {
	line := []byte(strings.Repeat("x", 199) + "\n")
	for i := 0; i < n; i++ {
		_, _ = unix.Write(fd, line)
	}
}

// eu, 24 Nov 2026: with the disk busy, journald took nothing for seconds and every renewal that wrote a line waited.
func TestUnblock_aLogNobodyReadsNeverHoldsAWriteUp(t *testing.T) {
	_, fd := journal(t)
	if err := Unblock(fd); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { writeLines(fd, 50_000); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writing 10 MB to a log nobody reads waited")
	}
}

func TestUnblock_whatIsWrittenReachesTheLog(t *testing.T) {
	r, fd := journal(t)
	if err := Unblock(fd); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(fd, []byte("one line\n")); err != nil {
		t.Fatal(err)
	}
	got := make(chan []byte, 1)
	go func() {
		b := make([]byte, 64)
		n, _ := io.ReadAtLeast(r, b, len("one line\n"))
		got <- b[:n]
	}()
	select {
	case b := <-got:
		if !bytes.Equal(b, []byte("one line\n")) {
			t.Fatalf("the log got %q", b)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the line never reached the log")
	}
}

// eu, 24 Nov 2026: a self-update re-execs the agent, the pipe's reader does not survive the exec, and canary.47's
// first log line killed it by SIGPIPE. Before an exec each fd is the log it was again, and a line reaches it.
func TestRestore_anExecFindsTheLogItHad(t *testing.T) {
	r, fd := journal(t)
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		t.Fatal(err)
	}
	if err := Unblock(fd); err != nil {
		t.Fatal(err)
	}
	Restore()
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		t.Fatal(err)
	}
	if after.Ino != before.Ino {
		t.Fatal("the fd still points at the agent's own pipe, which an exec leaves with no reader")
	}
	if _, err := unix.Write(fd, []byte("after\n")); err != nil {
		t.Fatal(err)
	}
	got := make(chan []byte, 1)
	go func() {
		b := make([]byte, 64)
		n, _ := io.ReadAtLeast(r, b, len("after\n"))
		got <- b[:n]
	}()
	select {
	case b := <-got:
		if !bytes.Equal(b, []byte("after\n")) {
			t.Fatalf("the log got %q", b)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the line never reached the log")
	}
}

// eu, 24 Nov 2026: canary.47 re-execed its update with stderr a pipe nobody read, and the next run's first line
// stopped it by SIGPIPE. Such a run finds its journal again before it logs.
func TestRevive_aDeadPipeBecomesTheJournalAgain(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "stdout")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	was := journalStream
	journalStream = sock
	t.Cleanup(func() { journalStream = was })
	var p [2]int
	if err := unix.Pipe2(p[:], unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	unix.Close(p[0])
	defer unix.Close(p[1])
	got := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		b, _ := io.ReadAll(io.LimitReader(c, 64))
		got <- string(b)
	}()
	if !Revive(p[1], "deplo-agent") {
		t.Fatal("a pipe nobody reads was taken for a live log")
	}
	if _, err := unix.Write(p[1], []byte("first line\n")); err != nil {
		t.Fatalf("the first line still fails: %v", err)
	}
	unix.Close(p[1])
	select {
	case b := <-got:
		if !strings.HasPrefix(b, "deplo-agent\n\n6\n0\n0\n0\n0\nfirst line\n") {
			t.Fatalf("the journal got %q", b)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing reached the journal")
	}
	r, fd := journal(t)
	_ = r
	if Revive(fd, "deplo-agent") {
		t.Fatal("a log someone reads was replaced")
	}
}

func TestMain(m *testing.M) {
	// As a child: the run a self-update starts, its stderr a pipe nobody reads.
	if os.Getenv("LOGPIPE_REVIVE_CHILD") == "1" {
		journalStream = filepath.Join(os.TempDir(), "no-journal-here")
		Revive(2, "deplo-agent")
		for i := 0; i < 100; i++ {
			fmt.Fprintln(os.Stderr, "a line of the new run")
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestRevive_aRunStartedWithADeadStderrLivesPastItsFirstLine(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), "LOGPIPE_REVIVE_CHILD=1")
	cmd.Stderr = w
	err = cmd.Run()
	w.Close()
	if err != nil {
		t.Fatalf("the new run died on its first line: %v", err)
	}
}
