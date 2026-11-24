package logpipe

import (
	"bytes"
	"io"
	"os"
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
