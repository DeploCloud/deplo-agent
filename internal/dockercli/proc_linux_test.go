//go:build linux

package dockercli

import (
	"context"
	"runtime"
	"testing"
)

// A cgroup no child can be cloned into is never used: every docker command would fail to start in it.
func TestUseCgroup_aGroupNoChildStartsInIsNotUsed(t *testing.T) {
	t.Cleanup(func() { cliGroup.Store(-1) })
	if UseCgroup(t.TempDir()) {
		t.Fatal("a plain directory was taken as the cgroup for docker commands")
	}
	if cliGroup.Load() != -1 {
		t.Fatal("a child would be cloned into it")
	}
	if err := Command(context.Background(), "true").Run(); err != nil {
		t.Fatalf("a command no longer starts: %v", err)
	}
}

func TestCommand_aStreamStaysWithTheAgent(t *testing.T) {
	cliGroup.Store(99)
	t.Cleanup(func() { cliGroup.Store(-1) })
	if c := Command(context.Background(), "docker", "events"); c.SysProcAttr.UseCgroupFD {
		t.Fatal("docker events would outlive a stop of the agent")
	}
	if c := Command(context.Background(), "docker", "ps"); !c.SysProcAttr.UseCgroupFD || c.SysProcAttr.CgroupFD != 99 {
		t.Fatal("a one-shot docker command runs beside the agent")
	}
}

// c39: the directory's file was left to the collector, which closed its descriptor: every docker command then
// failed with "bad file descriptor".
func TestUseCgroup_theGroupStaysOpenForTheAgentsLife(t *testing.T) {
	t.Cleanup(func() { cliGroup.Store(-1); cliDir.Store(nil) })
	if !UseCgroup("/sys/fs/cgroup") {
		t.Skip("no cgroup this test may start a child in")
	}
	for range 3 {
		runtime.GC()
	}
	if err := Command(context.Background(), "true").Run(); err != nil {
		t.Fatalf("a command no longer starts: %v", err)
	}
}
