package dockercli

import (
	"bytes"
	"context"
	"os/exec"
	"testing"
	"time"
)

func TestRunPipesSurvivesAHugeSingleStderrLine(t *testing.T) {
	// 9 MB on stderr with no newline: a line scanner gives up at its buffer
	// and the child blocks on the full pipe until the timeout. A writer does not.
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
