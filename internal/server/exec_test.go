package server

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func TestSplitArgv(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", []string{}},
		{"ls", []string{"ls"}},
		{"ls -la /tmp", []string{"ls", "-la", "/tmp"}},
		{`echo "hello world"`, []string{"echo", "hello world"}},
		{`echo 'single quoted'`, []string{"echo", "single quoted"}},
		{`cat "a b" 'c d'`, []string{"cat", "a b", "c d"}},
		{"  spaced   out  ", []string{"spaced", "out"}},
		{"tab\tsep", []string{"tab", "sep"}},
		{`mix"ed"quotes`, []string{"mixedquotes"}},
	}
	for _, c := range cases {
		got := splitArgv(c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("splitArgv(%q) = %#v, want %#v", c.in, got, c.want)
		}
	}
}

func TestIsDockerLevelStderr(t *testing.T) {
	dockerLevel := []string{
		"Error response from daemon: No such container: deplo-foo",
		"OCI runtime exec failed: exec failed: unable to start container process: exec: \"sh\": executable file not found in $PATH",
		"Cannot connect to the Docker daemon at unix:///var/run/docker.sock.",
		"Error response from daemon: Container deplo-x is not running",
		"cannot exec in a stopped state",
	}
	for _, s := range dockerLevel {
		if !isDockerLevelStderr(s) {
			t.Errorf("isDockerLevelStderr(%q) = false, want true", s)
		}
	}

	guest := []string{
		"sh: gtrger: not found",
		"bash: command-not-here: command not found",
		"ls: cannot access '/nope': No such file or directory",
		"",
		"permission denied",
	}
	for _, s := range guest {
		if isDockerLevelStderr(s) {
			t.Errorf("isDockerLevelStderr(%q) = true, want false (guest output)", s)
		}
	}
}

// A console command that floods its output must not make the agent buffer all of it.
func TestExecKeepsOnlyTheTailOfAFlood(t *testing.T) {
	bin := t.TempDir()
	script := `#!/bin/sh
case "$1" in
inspect) echo prj_deplo-agent-test; exit 0 ;;
exec) case "$*" in *"-c :") exit 0 ;; esac
	head -c 3000000 /dev/zero | tr '\0' x; echo END
	head -c 3000000 /dev/zero | tr '\0' y >&2; echo ERREND >&2
	exit 3 ;;
esac
exit 0
`
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	res, err := (&Service{}).Exec(context.Background(), &pb.ExecRequest{
		Container: "deplo-agent-test-exec-flood", ProjectId: "prj_deplo-agent-test", Command: "yes",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetCode() != 3 {
		t.Fatalf("code = %d, want 3", res.GetCode())
	}
	for name, out := range map[string]string{"stdout": res.GetStdout(), "stderr": res.GetStderr()} {
		if len(out) > execOutputTailBytes+256 {
			t.Errorf("%s kept %d bytes, want at most the %d-byte tail", name, len(out), execOutputTailBytes)
		}
		if !strings.Contains(out, "earlier output trimmed") || !strings.HasSuffix(out, "END\n") {
			t.Errorf("%s must keep the end and say it was trimmed", name)
		}
	}
}
