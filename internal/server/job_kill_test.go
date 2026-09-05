package server

import (
	"os/exec"
	"testing"
	"time"
)

func TestKillMarkedProcessesStopsTheWholeTree(t *testing.T) {
	// A shell that forks two sleepers - the shape of a job whose command spawns
	// helpers. Only the marker ties them together; there is no exec id to ask for.
	cmd := exec.Command("sh", "-c", "sleep 300 & sleep 300 & wait")
	cmd.Env = append(cmd.Environ(), jobMarkerEnv+"=testjob-"+t.Name())
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() })
	time.Sleep(200 * time.Millisecond)

	killMarkedProcesses("testjob-"+t.Name(), 2*time.Second)

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the shell is still alive after the kill")
	}
	if left := killMarkedProcesses("testjob-"+t.Name(), 0); left != 0 {
		t.Fatalf("%d marked processes survived", left)
	}
	if n := killMarkedProcesses("someone-else", 0); n != 0 {
		t.Fatalf("killed %d processes of another job", n)
	}
}
