package dockercli

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestEphemeralTagsOnlyRunRmAndExec(t *testing.T) {
	run, _ := ephemeral([]string{"run", "--rm", "img", "tar"})
	if run[0] != "run" || run[1] != "--name" || !strings.HasPrefix(run[2], "deplo-helper-") || run[3] != "--rm" {
		t.Fatalf("run --rm: %q", run)
	}
	exec, _ := ephemeral([]string{"exec", "-i", "db", "pg_dump", "-d", "app"})
	if exec[1] != "-e" || !strings.HasPrefix(exec[2], MarkerEnv+"=") || strings.Join(exec[3:], " ") != "-i db pg_dump -d app" {
		t.Fatalf("exec: %q", exec)
	}
	for _, args := range [][]string{{"run", "-d", "img"}, {"run", "--rm", "--name", "x", "img"}, {"compose", "up"}} {
		if got, _ := ephemeral(args); strings.Join(got, " ") != strings.Join(args, " ") {
			t.Fatalf("%q was rewritten to %q", args, got)
		}
	}
}

// A process docker exec started inside a container outlives the killed client; the marker finds it.
func TestCancelledExecStopsTheMarkedProcess(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "pid")
	script := "#!/bin/sh\n[ \"$1\" = rm ] && exit 0\n" +
		"setsid env \"$3\" sleep 100 >/dev/null 2>&1 </dev/null &\necho $! > " + pidFile + "\nwait\n"
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, err := Stream(ctx, time.Minute, func(string) {}, "", "exec", "db", "sleep"); err == nil {
		t.Fatal("expected the cancel to surface")
	}
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	deadline := time.Now().Add(5 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			t.Fatal("the exec'd process survived the cancel")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
