package reap

import (
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestExited_collectsAChildThatEndedAndLeavesARunningOne(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reads /proc")
	}
	done, err := exec.LookPath("true")
	if err != nil {
		t.Skip("no true")
	}
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no sleep")
	}
	ended, err := syscall.ForkExec(done, []string{done}, &syscall.ProcAttr{Files: []uintptr{0, 1, 2}})
	if err != nil {
		t.Skip(err)
	}
	running, err := syscall.ForkExec(sleep, []string{sleep, "300"}, &syscall.ProcAttr{Files: []uintptr{0, 1, 2}})
	if err != nil {
		t.Skip(err)
	}
	t.Cleanup(func() { syscall.Kill(running, syscall.SIGKILL); syscall.Wait4(running, nil, 0, nil) })
	// Nobody waits for the one that ends: it stays behind as a zombie until collected.
	for end := time.Now().Add(5 * time.Second); !zombie(ended); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("the child never ended")
		}
	}
	if n := Exited(); n != 1 {
		t.Fatalf("collected %d, want the one that ended", n)
	}
	if _, err := os.Stat("/proc/" + strconv.Itoa(ended)); err == nil {
		t.Fatal("the child that ended is still there")
	}
	if err := syscall.Kill(running, 0); err != nil {
		t.Fatalf("the running child was collected: %v", err)
	}
}

func zombie(pid int) bool {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	// The state follows the command, which is in parentheses.
	s := string(raw)
	i := strings.LastIndexByte(s, ')')
	return i > 0 && strings.HasPrefix(strings.TrimSpace(s[i+1:]), "Z")
}
