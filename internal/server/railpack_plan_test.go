package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The shape railpack 0.43 plans for a Ruby app: one locked apt cache for two apt steps.
const rubyPlan = `{"caches":{"apt":{"directory":"/var/cache/apt","type":"locked"},` +
	`"apt-lists":{"directory":"/var/lib/apt/lists","type":"locked"},"npm":{"directory":"/root/.npm","type":"shared"}},` +
	`"steps":[` +
	`{"name":"packages:apt:build","caches":["apt","apt-lists"],"inputs":[{"image":"ghcr.io/railwayapp/railpack-builder:mise-2026.10.14"}],"size":12345678901},` +
	`{"name":"install","caches":["npm"],"inputs":[{"step":"packages:apt:build"}]},` +
	`{"name":"packages:apt:runtime","caches":["apt","apt-lists"],"inputs":[{"image":"ghcr.io/railwayapp/railpack-runtime:mise-2026.10.14"}]},` +
	`{"name":"build","caches":["npm"],"inputs":[{"image":"node:22-slim"}]}],` +
	`"deploy":{"base":{"image":"ghcr.io/railwayapp/railpack-runtime:mise-2026.10.14"}}}`

func tunedRubyPlan(t *testing.T) (string, map[string]any) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, []byte(rubyPlan), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := tuneRailpackPlan(path); err != nil {
		t.Fatal(err)
	}
	p, err := readPlan(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, p
}

func TestTuneRailpackPlanUnblocksTheRuntimeAptStep(t *testing.T) {
	path, p := tunedRubyPlan(t)
	steps := p["steps"].([]any)
	refs := func(i int) string {
		var out []string
		for _, r := range steps[i].(map[string]any)["caches"].([]any) {
			out = append(out, r.(string))
		}
		return strings.Join(out, ",")
	}
	if got := refs(0); got != "apt,apt-lists" {
		t.Fatalf("the build apt step lost its cache: %s", got)
	}
	if got := refs(2); got != "apt-packages-apt-runtime,apt-lists-packages-apt-runtime" {
		t.Fatalf("runtime apt step caches = %s", got)
	}
	own := p["caches"].(map[string]any)["apt-lists-packages-apt-runtime"].(map[string]any)
	if own["directory"] != "/var/lib/apt/lists" || own["type"] != "locked" {
		t.Fatalf("the runtime copy is %v", own)
	}
	if refs(1) != "npm" || refs(3) != "npm" {
		t.Fatal("a shared cache was split")
	}
	if b, _ := os.ReadFile(path); !strings.Contains(string(b), `"size":12345678901`) {
		t.Fatalf("the rewrite changed a number: %s", b)
	}
}

func TestTuneRailpackPlanPinsOnlyRailpacksOwnImages(t *testing.T) {
	path, p := tunedRubyPlan(t)
	b, _ := os.ReadFile(path)
	for _, want := range []string{
		`"ghcr.io/railwayapp/railpack-builder:mise-2026.10.14@sha256:03df9e9aa1e786c2e0dd12c5409acad3bbd5a0fa8a75c1117c7ddab5c0c3fde4"`,
		`"ghcr.io/railwayapp/railpack-runtime:mise-2026.10.14@sha256:02820533713338569ae4d4f0df986c3e7cc6e93457fd06fbb0d2cfa86fba9cdb"`,
		`"node:22-slim"`,
	} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("plan lacks %s: %s", want, b)
		}
	}
	base := p["deploy"].(map[string]any)["base"].(map[string]any)["image"].(string)
	if !strings.HasSuffix(base, "@"+railpackImageDigests["ghcr.io/railwayapp/railpack-runtime:mise-2026.10.14"]) {
		t.Fatalf("deploy base = %s", base)
	}

	// Its own output again: nothing left to change, and no digest is appended twice.
	before, _ := os.ReadFile(path)
	if err := tuneRailpackPlan(path); err != nil {
		t.Fatal(err)
	}
	if after, _ := os.ReadFile(path); string(after) != string(before) {
		t.Fatalf("a second pass changed the plan:\n%s\n%s", before, after)
	}
}

// The two mise steps railpack 0.43 plans for a Gradle app (spring-petclinic), assets as it writes them.
func javaPlan(runtimeJava string) string {
	settings := `\n[settings]\n  paranoid = true\n  [settings.node]\n    verify = false\n`
	return `{"steps":[` +
		`{"name":"packages:mise","inputs":[{"image":"ghcr.io/railwayapp/railpack-builder:mise-2026.10.14"}],` +
		`"assets":{"generated-mise-toml":"[tools]\n  gradle = \"9.8.1\"\n  java = \"21.0.12+101.0.LTS\"\n` + settings + `"},` +
		`"commands":[{"path":"/mise/shims"},{"cmd":"mise install","customName":"install mise packages: gradle, java"}]},` +
		`{"name":"build","inputs":[{"step":"packages:mise"},{"local":true,"include":["."]}]},` +
		`{"name":"packages:mise:runtime","inputs":[{"image":"ghcr.io/railwayapp/railpack-builder:mise-2026.10.14"}],` +
		`"assets":{"generated-mise-toml":"[tools]\n  java = \"` + runtimeJava + `\"\n` + settings + `"},` +
		`"commands":[{"path":"/mise/shims"},{"cmd":"mise install","customName":"install mise packages: java"}]}],` +
		`"deploy":{"inputs":[{"step":"packages:mise:runtime","include":["/mise/shims","/mise/installs"]}]}}`
}

func TestTuneRailpackPlanCopiesTheRuntimeToolchainFromTheBuild(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, []byte(javaPlan("21.0.12+101.0.LTS")), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := tuneRailpackPlan(path); err != nil {
		t.Fatal(err)
	}
	p, _ := readPlan(path)
	runtime := planStep(p["steps"].([]any), "packages:mise:runtime")
	inputs := runtime["inputs"].([]any)
	if len(inputs) != 2 {
		t.Fatalf("runtime inputs = %v", inputs)
	}
	if !strings.HasPrefix(inputs[0].(map[string]any)["image"].(string), "ghcr.io/railwayapp/railpack-builder:mise-2026.10.14@sha256:") {
		t.Fatalf("the runtime step's base changed: %v", inputs[0])
	}
	copied := inputs[1].(map[string]any)
	if copied["step"] != "packages:mise" || len(copied["include"].([]any)) != 1 || copied["include"].([]any)[0] != "/mise/installs/java" {
		t.Fatalf("copied = %v; want only the JDK, never gradle", copied)
	}
}

func TestTuneRailpackPlanKeepsARuntimeToolchainItCannotMatch(t *testing.T) {
	for name, plan := range map[string]string{
		"another version": javaPlan("17.0.15+6"),
		"a tool the build lacks": strings.Replace(javaPlan("21.0.12+101.0.LTS"),
			`[tools]\n  java = \"21.0.12+101.0.LTS\"`, `[tools]\n  java = \"21.0.12+101.0.LTS\"\n  node = \"22\"`, 1),
	} {
		path := filepath.Join(t.TempDir(), "plan.json")
		if err := os.WriteFile(path, []byte(plan), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := tuneRailpackPlan(path); err != nil {
			t.Fatal(err)
		}
		p, _ := readPlan(path)
		if n := len(planStep(p["steps"].([]any), "packages:mise:runtime")["inputs"].([]any)); n != 1 {
			t.Fatalf("%s: the runtime step got %d inputs, want its own install", name, n)
		}
	}
}
