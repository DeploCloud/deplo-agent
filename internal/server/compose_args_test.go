package server

import (
	"strings"
	"testing"
)

// The app's own extra `docker compose up` flags. They are APPENDED to the
// bring-up the agent assembles — never a replacement — so the project name, the
// stack file and the env-file stay the agent's, whatever arrives on the wire.

func TestComposeUpArgsAppendsExtraFlags(t *testing.T) {
	args := composeUpArgs("deplo-app", "/stacks/app.yml", "", false,
		[]string{"--pull", "always", "--wait"})
	want := "compose -p deplo-app -f /stacks/app.yml up -d --remove-orphans --pull always --wait"
	if strings.Join(args, " ") != want {
		t.Fatalf("argv = %v, want %q", args, want)
	}
}

func TestComposeUpArgsKeepsTheDefaultBringUpWhenEmpty(t *testing.T) {
	for _, extra := range [][]string{nil, {}} {
		args := composeUpArgs("deplo-app", "/stacks/app.yml", "", false, extra)
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
