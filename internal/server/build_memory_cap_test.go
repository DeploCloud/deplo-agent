package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeCgroupTree is a cgroup v2 root with system.slice delegating memory, as Docker 29 lays it out.
func fakeCgroupTree(t *testing.T, controllers string) string {
	t.Helper()
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "cgroup.controllers"), "cpu memory pids\n")
	dir := filepath.Join(root, "system.slice")
	mustWrite(t, filepath.Join(dir, "cgroup.subtree_control"), controllers)
	prevDir, prevMem := buildStepCgroupDir, hostMemTotal
	buildStepCgroupDir, hostMemTotal = dir, func() int64 { return 8 << 30 }
	t.Cleanup(func() { buildStepCgroupDir, hostMemTotal = prevDir, prevMem })
	return dir
}

func fakeStepCgroup(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	mustWrite(t, filepath.Join(path, "memory.max"), "max\n")
	mustWrite(t, filepath.Join(path, "memory.oom.group"), "0\n")
	return path
}

func readTrim(t *testing.T, path string) string {
	t.Helper()
	b, _ := os.ReadFile(path)
	return strings.TrimSpace(string(b))
}

// Every step cgroup that appears while a build runs is capped at 75% of RAM and killed whole;
// one that was already there, an app container's, or one after the build are left alone.
func TestCapBuildStepsCapsOnlyNewStepsWhileABuildRuns(t *testing.T) {
	dir := fakeCgroupTree(t, "cpu io memory pids\n")
	before := fakeStepCgroup(t, dir, buildStepCgroupPrefix+"before")
	app := fakeStepCgroup(t, dir, "docker-0123abcd.scope")

	limit, release := capBuildSteps()
	if limit != 6<<30 {
		t.Fatalf("limit = %d, want 75%% of 8 GiB", limit)
	}
	_, second := capBuildSteps()
	step := fakeStepCgroup(t, dir, buildStepCgroupPrefix+"rlskm6fxfesv6mgdof1006sgb")
	deadline := time.Now().Add(5 * time.Second)
	for readTrim(t, filepath.Join(step, "memory.max")) != "6442450944" {
		if time.Now().After(deadline) {
			t.Fatal("the new step cgroup was never capped")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := readTrim(t, filepath.Join(step, "memory.oom.group")); got != "1" {
		t.Fatalf("memory.oom.group = %q, want the whole step killed together", got)
	}
	release()
	late := fakeStepCgroup(t, dir, buildStepCgroupPrefix+"while-the-second-build-runs")
	for readTrim(t, filepath.Join(late, "memory.max")) == "max" {
		if time.Now().After(deadline) {
			t.Fatal("one build finishing stopped the cap of the other")
		}
		time.Sleep(20 * time.Millisecond)
	}
	second()
	after := fakeStepCgroup(t, dir, buildStepCgroupPrefix+"after")
	time.Sleep(500 * time.Millisecond)
	for _, p := range []string{before, app, after} {
		if got := readTrim(t, filepath.Join(p, "memory.max")); got != "max" {
			t.Errorf("%s was capped (%s)", filepath.Base(p), got)
		}
	}
}

// cgroup v1 or a slice without the memory controller has nothing to cap: the build runs as before.
func TestCapBuildStepsSkipsAnUnknownLayout(t *testing.T) {
	dir := fakeCgroupTree(t, "cpu pids\n")
	if limit, release := capBuildSteps(); limit != 0 {
		release()
		t.Fatalf("capped without a memory controller: %d", limit)
	}
	_ = os.Remove(filepath.Join(filepath.Dir(dir), "cgroup.controllers"))
	mustWrite(t, filepath.Join(dir, "cgroup.subtree_control"), "memory\n")
	if limit, release := capBuildSteps(); limit != 0 {
		release()
		t.Fatalf("capped on cgroup v1: %d", limit)
	}
	buildStepCgroupDir = ""
	if limit, _ := capBuildSteps(); limit != 0 {
		t.Fatal("capped with the cap turned off")
	}
}

func TestMemoryKilled(t *testing.T) {
	lines := []string{`#5 ERROR: process "/bin/sh -c npm run build" did not complete successfully: exit code: 137`}
	if got := memoryKilled(lines, 6<<30); got != "The build needed more than 6.0 GB of memory, the most a build may use on this server." {
		t.Fatalf("got %q", got)
	}
	if got := memoryKilled(lines, 0); !strings.Contains(got, "ran out of memory") {
		t.Fatalf("no cap: %q", got)
	}
	if got := memoryKilled([]string{"exit code: 1"}, 6<<30); got != "" {
		t.Fatalf("not a memory kill: %q", got)
	}
}

func TestCapabilities_advertisesMemoryCap(t *testing.T) {
	if !containsString(Capabilities, "build.memory-cap") {
		t.Error(`Capabilities must advertise "build.memory-cap"`)
	}
}
