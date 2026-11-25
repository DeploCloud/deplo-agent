package dockercli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// c38: eu-2's agent ran the same docker ps and inspect several times a second, a quarter of its one core.
func TestRun_aReadAskedAgainIsAnsweredUntilSomethingChanges(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "argv.log")
	script := "#!/bin/sh\necho \"$*\" >> " + log + "\necho running\n"
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	CacheReads(time.Minute)
	t.Cleanup(func() { CacheReads(0); forgetReads() })
	calls := func() int {
		raw, _ := os.ReadFile(log)
		return strings.Count(string(raw), "\n")
	}
	ctx := context.Background()
	for range 3 {
		if res, err := Run(ctx, 5*time.Second, "ps", "-q", "--filter", "label=deplo.project=prj_1"); err != nil || res.Stdout != "running\n" {
			t.Fatalf("%q %v", res.Stdout, err)
		}
	}
	if calls() != 1 {
		t.Fatalf("docker ran %d times for one read asked three times", calls())
	}
	_, _ = Run(ctx, 5*time.Second, "stop", "deplo-web")
	_, _ = Run(ctx, 5*time.Second, "ps", "-q", "--filter", "label=deplo.project=prj_1")
	if calls() != 3 {
		t.Fatalf("a read after a stop was answered from before it (%d calls)", calls())
	}
	CacheReads(0)
	_, _ = Run(ctx, 5*time.Second, "ps", "-q", "--filter", "label=deplo.project=prj_1")
	if calls() != 4 {
		t.Fatal("with no cache a read still came from it")
	}
}
