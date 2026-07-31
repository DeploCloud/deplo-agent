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

// railpackBuildArgs must select the railpack frontend via BUILDKIT_SYNTAX, feed
// the plan as the Dockerfile, forward every plan secret as `--secret
// id=NAME,env=NAME`, and carry the caller's image output — all as discrete argv
// tokens, so a hostile name from an untrusted plan can never be word-split or
// shell-interpreted.
func TestRailpackBuildArgs(t *testing.T) {
	names := []string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD"}
	args := railpackBuildArgs(
		"ghcr.io/railwayapp/railpack-frontend:v0.35.0",
		"/tmp/plan/railpack-plan.json", "/tmp/ctx", names,
		[]string{"-t", "deplo/cwars:dpl_abc"},
	)

	// No shell is ever involved: there is no `sh`/`-c` token anywhere.
	if slices.Contains(args, "sh") || slices.Contains(args, "-c") {
		t.Fatalf("argv must not invoke a shell: %v", args)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"build --build-arg BUILDKIT_SYNTAX=ghcr.io/railwayapp/railpack-frontend:v0.35.0",
		"-f /tmp/plan/railpack-plan.json",
		"--secret id=RAILPACK_NODE_VERSION,env=RAILPACK_NODE_VERSION",
		"--secret id=RAILPACK_BUILD_CMD,env=RAILPACK_BUILD_CMD",
		"-t deplo/cwars:dpl_abc",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("argv missing %q in: %s", want, joined)
		}
	}
	// Labels are the relabel pass's job — the frontend drops them, so emitting
	// them here would be dead argv that reads as if it worked.
	if slices.Contains(args, "--label") {
		t.Fatalf("labels must not ride the railpack frontend build: %v", args)
	}
	// The build context is the final positional argument — docker reads it there.
	if args[len(args)-1] != "/tmp/ctx" {
		t.Fatalf("context must be the last arg: %v", args)
	}

	// Injection safety: a crafted secret name lands as EXACTLY one argv token in
	// the `--secret` slot — never split, never a command.
	evil := "x; rm -rf / #"
	adv := railpackBuildArgs("front", "plan.json", "ctx", []string{evil}, nil)
	if !slices.Contains(adv, "--secret") || !slices.Contains(adv, "id="+evil+",env="+evil) {
		t.Fatalf("hostile name not a single --secret token: %v", adv)
	}
}
