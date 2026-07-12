package server

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// readPlanSecrets returns the plan's declared secrets and ok=true on a good read,
// and ok=false (so the caller falls back) only when the plan is missing/unparseable.
func TestReadPlanSecrets(t *testing.T) {
	dir := t.TempDir()

	good := filepath.Join(dir, "railpack-plan.json")
	if err := os.WriteFile(good, []byte(`{"secrets":["RAILPACK_NODE_VERSION","RAILPACK_BUILD_CMD"],"steps":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, ok := readPlanSecrets(good); !ok || !slices.Equal(got, []string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD"}) {
		t.Fatalf("good plan: got %v ok=%v", got, ok)
	}

	// A valid plan that declares no secrets reads OK with an empty set (the caller
	// then passes no --secret flags, matching railpack exactly).
	none := filepath.Join(dir, "none.json")
	if err := os.WriteFile(none, []byte(`{"steps":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, ok := readPlanSecrets(none); !ok || len(got) != 0 {
		t.Fatalf("no-secrets plan: got %v ok=%v; want empty,true", got, ok)
	}

	// Missing / unparseable ⇒ ok=false so the caller falls back to the known set.
	if got, ok := readPlanSecrets(filepath.Join(dir, "absent.json")); ok || got != nil {
		t.Fatalf("absent plan: got %v ok=%v; want nil,false", got, ok)
	}
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{not json`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readPlanSecrets(bad); ok {
		t.Fatalf("bad json: want ok=false")
	}
}

// sanitizeSecretNames keeps identifier-shaped names and drops anything an
// untrusted plan might use to smuggle buildctl CSV attributes.
func TestSanitizeSecretNames(t *testing.T) {
	in := []string{
		"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD", "_ok", "a1",
		"x; rm -rf / #",       // shell metachars
		"foo,src=/etc/passwd", // CSV smuggling
		"has space", "-flag", "", "WITH-DASH", "uni¢ode",
	}
	got := sanitizeSecretNames(in)
	want := []string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD", "_ok", "a1"}
	if !slices.Equal(got, want) {
		t.Fatalf("sanitizeSecretNames = %v; want %v", got, want)
	}
	// Must not mutate the caller's slice (fallback literal reuse).
	if in[4] != "x; rm -rf / #" {
		t.Fatalf("input slice was mutated: %v", in)
	}
}

// railpackBuildctlArgs must forward every plan secret as a `docker exec -e NAME`
// AND a `buildctl --secret id=NAME,env=NAME`, and — the security property — emit
// each secret NAME as a single discrete argv token so a hostile name from an
// untrusted plan can never be word-split or shell-interpreted.
func TestRailpackBuildctlArgs(t *testing.T) {
	names := []string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD"}
	args := railpackBuildctlArgs("bk-cwars", "ghcr.io/railwayapp/railpack-frontend:latest", "deplo/cwars:dpl_abc", names)

	// No shell is ever involved: there is no `sh`/`-c` token anywhere.
	if slices.Contains(args, "sh") || slices.Contains(args, "-c") {
		t.Fatalf("argv must not invoke a shell: %v", args)
	}
	// Core buildctl shape is present in order.
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"exec -e RAILPACK_NODE_VERSION -e RAILPACK_BUILD_CMD bk-cwars buildctl build",
		"--opt filename=railpack-plan.json",
		"--secret id=RAILPACK_NODE_VERSION,env=RAILPACK_NODE_VERSION",
		"--secret id=RAILPACK_BUILD_CMD,env=RAILPACK_BUILD_CMD",
		"--output type=docker,name=deplo/cwars:dpl_abc",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("argv missing %q in: %s", want, joined)
		}
	}

	// Injection safety: a crafted secret name lands as EXACTLY one argv token in the
	// `-e` slot and one in the `--secret` slot — never split, never a command.
	evil := "x; rm -rf / #"
	adv := railpackBuildctlArgs("bk", "front", "img", []string{evil})
	eIdx := slices.Index(adv, "-e")
	if eIdx < 0 || adv[eIdx+1] != evil {
		t.Fatalf("hostile name not a single -e token: %v", adv)
	}
	if !slices.Contains(adv, "--secret") ||
		!slices.Contains(adv, "id="+evil+",env="+evil) {
		t.Fatalf("hostile name not a single --secret token: %v", adv)
	}
}
