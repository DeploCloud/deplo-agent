package dockercli

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/pace"
)

func TestRunPipesSurvivesAHugeSingleStderrLine(t *testing.T) {
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, "sh", "-c", "head -c 9000000 /dev/zero | tr '\\0' x >&2; echo done")
	var out, errb bytes.Buffer
	start := time.Now()
	code, err := runPipes(ctx, time.Minute, cmd, &out, &errb, "test")
	if err != nil || code != 0 {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if time.Since(start) > 20*time.Second {
		t.Fatalf("took %s: the child was stalled on stderr", time.Since(start))
	}
	if out.String() != "done\n" || errb.Len() != 9000000 {
		t.Fatalf("stdout=%q stderr=%d bytes", out.String(), errb.Len())
	}
}

func TestRunPipesReportsTheExitCode(t *testing.T) {
	cmd := exec.Command("sh", "-c", "echo oops >&2; exit 7")
	var out, errb bytes.Buffer
	code, err := runPipes(context.Background(), time.Minute, cmd, &out, &errb, "test")
	if err != nil || code != 7 || errb.String() != "oops\n" {
		t.Fatalf("code=%d err=%v stderr=%q", code, err, errb.String())
	}
}

func TestAPipeIsPaced(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\nexec cat\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	before := pace.Looks.Load()
	var out bytes.Buffer
	if _, err := PipeThrough(context.Background(), time.Minute, bytes.NewReader(make([]byte, 3*pace.Step)), &out, nil, "x"); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 3*pace.Step || pace.Looks.Load()-before < 6 {
		t.Fatalf("copied %d bytes with %d looks", out.Len(), pace.Looks.Load()-before)
	}
}
