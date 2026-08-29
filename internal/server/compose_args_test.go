package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The app's own extra `docker compose up` flags. They are APPENDED to the
// bring-up the agent assembles, never a replacement, so the project name, the
// stack file and the env-file stay the agent's, whatever arrives on the wire.

func TestComposeUpArgsAppendsExtraFlags(t *testing.T) {
	args := composeUpArgs("deplo-app", "/stacks/app.yml", "", "", false,
		[]string{"--pull", "always", "--wait"})
	want := "compose -p deplo-app -f /stacks/app.yml up -d --remove-orphans --pull always --wait"
	if strings.Join(args, " ") != want {
		t.Fatalf("argv = %v, want %q", args, want)
	}
}

func TestComposeUpArgsKeepsTheDefaultBringUpWhenEmpty(t *testing.T) {
	for _, extra := range [][]string{nil, {}} {
		args := composeUpArgs("deplo-app", "/stacks/app.yml", "", "", false, extra)
		want := "compose -p deplo-app -f /stacks/app.yml up -d --remove-orphans"
		if strings.Join(args, " ") != want {
			t.Fatalf("argv = %v, want the untouched default %q", args, want)
		}
	}
}

// One bad token drops the WHOLE set: dropping just `-p` would leave its value
// behind as a positional arg, which `compose up` reads as "only this service".
func TestSanitizeComposeArgsRejectsTheWholeSet(t *testing.T) {
	cases := map[string][]string{
		"repoints the project":   {"--force-recreate", "-p", "somethingelse"},
		"repoints the project =": {"--project-name=other"},
		"repoints the stack":     {"-f", "/tmp/evil.yml"},
		"repoints the env-file":  {"--env-file", "/etc/shadow"},
		"repoints the workdir":   {"--project-directory", "/"},
		"expects a shell":        {"--pull always; rm -rf /"},
		"carries a newline":      {"--pull\nalways"},
		"is not printable":       {"--pull\x00always"},
		"is absurdly long":       {"--" + strings.Repeat("x", 200)},
	}
	for name, extra := range cases {
		if got := sanitizeComposeArgs(extra); got != nil {
			t.Errorf("%s: sanitize(%v) = %v, want nothing at all", name, extra, got)
		}
	}

	long := make([]string, 25)
	for i := range long {
		long[i] = "--wait"
	}
	if got := sanitizeComposeArgs(long); got != nil {
		t.Errorf("25 flags: sanitize = %v, want nothing at all", got)
	}
}

func TestSanitizeComposeArgsKeepsOrdinaryFlags(t *testing.T) {
	extra := []string{"--force-recreate", "--pull", "always", "--scale", "web=3", "--timeout=60"}
	got := sanitizeComposeArgs(extra)
	if strings.Join(got, " ") != strings.Join(extra, " ") {
		t.Fatalf("sanitize = %v, want it untouched", got)
	}
}

// A compose stack runs from its OWN directory. A `.env` there would be ONE file for all
// of them.
func TestComposeUpArgsPassesProjectDirectory(t *testing.T) {
	args := composeUpArgs(
		"deplo-app",
		"/data/stacks/app.yml",
		"/data/stacks/files/app/.env",
		"/data/stacks/files/app",
		false,
		nil,
	)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--project-directory /data/stacks/files/app") {
		t.Fatalf("project directory missing: %v", args)
	}
	if !strings.Contains(joined, "--env-file /data/stacks/files/app/.env") {
		t.Fatalf("env file missing: %v", args)
	}
	// The single-image path passes neither.
	plain := composeUpArgs("deplo-app", "/data/stacks/app.yml", "", "", false, nil)
	if strings.Contains(strings.Join(plain, " "), "--project-directory") {
		t.Fatalf("single-image stack should not get a project directory: %v", plain)
	}
}

// The env-file is written inside the stack's own directory and the pre-move copy
// beside the stack file is deleted: it held decrypted secrets and nothing points
// at it any more.
func TestWriteComposeEnvIsPerStack(t *testing.T) {
	dir := t.TempDir()
	s := &Service{stackDir: dir}
	legacy := filepath.Join(dir, "app.env")
	if err := os.WriteFile(legacy, []byte("OLD=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	envFile, projectDir, err := s.writeComposeEnv("app", map[string]string{"A": "1"})
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(dir, "files", "app"); projectDir != want {
		t.Fatalf("project dir = %q, want %q", projectDir, want)
	}
	if want := filepath.Join(dir, "files", "app", ".env"); envFile != want {
		t.Fatalf("env file = %q, want %q", envFile, want)
	}
	body, err := os.ReadFile(envFile)
	if err != nil || !strings.Contains(string(body), `A="1"`) {
		t.Fatalf("env body = %q, err %v", body, err)
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Fatalf("the old env file was left behind: %v", err)
	}
	// Two stacks never share a file.
	other, _, err := s.writeComposeEnv("other", map[string]string{"B": "2"})
	if err != nil || other == envFile {
		t.Fatalf("second stack reused %q (err %v)", other, err)
	}
}
