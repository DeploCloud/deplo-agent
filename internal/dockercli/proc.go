package dockercli

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// WaitDelay bounds how long a cancelled child's output is still read after it was killed.
const WaitDelay = 10 * time.Second

// Command is exec.CommandContext that kills the child's whole process group on cancel, so a
// plugin or helper the child started cannot keep its pipes open and Wait blocked.
func Command(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return cmd.Process.Kill()
		}
		return nil
	}
	return cmd
}

// RunLines runs cmd with every output the caller left unset merged into one pipe read line by line
// into onLine. Once ctx ends the pipe is abandoned after WaitDelay, so a process that escaped the
// group cannot hold the caller hostage.
func RunLines(ctx context.Context, cmd *exec.Cmd, onLine LineFn) error {
	r, w, err := os.Pipe()
	if err != nil {
		return err
	}
	if cmd.Stdout == nil {
		cmd.Stdout = w
	}
	if cmd.Stderr == nil {
		cmd.Stderr = w
	}
	err = cmd.Start()
	w.Close()
	if err != nil {
		r.Close()
		return err
	}
	read := make(chan struct{})
	go func() {
		defer close(read)
		drainLines(r, onLine)
	}()
	werr := cmd.Wait()
	select {
	case <-read:
	case <-ctx.Done():
		select {
		case <-read:
		case <-time.After(WaitDelay):
		}
	}
	r.Close()
	<-read
	return werr
}

// drainLines feeds r to onLine until EOF and keeps reading past any error, so the writer never stalls.
func drainLines(r io.Reader, onLine LineFn) {
	err := ScanLines(r, maxLineBytes, func(l string) { onLine(l) })
	if err != nil && !errors.Is(err, os.ErrClosed) {
		onLine("output read failed: " + err.Error())
		_, _ = io.Copy(io.Discard, r)
	}
}

const maxLineBytes = 8 * 1024 * 1024

const truncatedMark = " …[line truncated]"

// ScanLines calls fn for each line of r and cuts a line longer than max instead of stopping,
// so the writer never blocks on a pipe nobody reads any more.
func ScanLines(r io.Reader, max int, fn func(string)) error {
	br := bufio.NewReaderSize(r, 64*1024)
	var line []byte
	over := false
	for {
		chunk, err := br.ReadSlice('\n')
		if len(chunk) > 0 && !over {
			if room := max - len(line); len(chunk) > room {
				line, over = append(line, chunk[:room]...), true
			} else {
				line = append(line, chunk...)
			}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err == nil || len(line) > 0 || over {
			s := strings.TrimRight(string(line), "\r\n")
			if over {
				s += truncatedMark
			}
			fn(s)
			line, over = line[:0], false
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
