package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func writeRepo(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		full := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// The ordinary single-package Node app: the install step may be restricted to
// the manifests, which is what keeps its dependency layer cached across a code
// change (and stops a 1.88 GB layer being re-exported for nothing).
func TestManifestOnlyInstallFiles_plainApp(t *testing.T) {
	dir := writeRepo(t, map[string]string{
		"package.json": `{"name":"app","scripts":{"build":"next build"},"dependencies":{"next":"15"}}`,
		"bun.lock":     "lock",
		".npmrc":       "registry=https://example.test",
		"src/index.ts": "console.log(1)",
	})
	files, ok := manifestOnlyInstallFiles(dir)
	if !ok {
		t.Fatal("a plain single-package app must qualify")
	}
	if files[0] != "package.json" {
		t.Fatalf("package.json must come first, got %v", files)
	}
	for _, want := range []string{"package.json", "bun.lock", ".npmrc"} {
		if !slices.Contains(files, want) {
			t.Errorf("missing %s in %v", want, files)
		}
	}
	// Only files that EXIST are listed - a COPY of an absent path fails the build.
	for _, absent := range []string{"yarn.lock", "pnpm-lock.yaml", "package-lock.json"} {
		if slices.Contains(files, absent) {
			t.Errorf("listed a file that does not exist: %s", absent)
		}
	}
	// Application source must NOT be in the list; including it would defeat the
	// whole point.
	if slices.Contains(files, "src/index.ts") {
		t.Errorf("source file leaked into the install scope: %v", files)
	}
}

// Every case where the install step legitimately needs more than the manifests
// must fall back to copying everything, because restricting it would break the
// build outright.
func TestManifestOnlyInstallFiles_refusesUnsafeRepos(t *testing.T) {
	cases := []struct {
		name  string
		files map[string]string
	}{
		{"workspaces array", map[string]string{
			"package.json": `{"name":"root","workspaces":["packages/*"]}`}},
		{"workspaces object", map[string]string{
			"package.json": `{"name":"root","workspaces":{"packages":["a"]}}`}},
		{"postinstall script", map[string]string{
			"package.json": `{"name":"a","scripts":{"postinstall":"node build-native.js"}}`}},
		{"preinstall script", map[string]string{
			"package.json": `{"name":"a","scripts":{"preinstall":"./check.sh"}}`}},
		{"prepare script", map[string]string{
			"package.json": `{"name":"a","scripts":{"prepare":"husky"}}`}},
		{"patch-package", map[string]string{
			"package.json":           `{"name":"a"}`,
			"patches/left-pad.patch": "diff",
		}},
		{"repo owns nixpacks.toml", map[string]string{
			"package.json":  `{"name":"a"}`,
			"nixpacks.toml": "[phases.install]",
		}},
		{"repo owns nixpacks.json", map[string]string{
			"package.json":  `{"name":"a"}`,
			"nixpacks.json": "{}",
		}},
		{"no package.json", map[string]string{"main.go": "package main"}},
		{"unparseable package.json", map[string]string{"package.json": "{not json"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, ok := manifestOnlyInstallFiles(writeRepo(t, c.files)); ok {
				t.Fatalf("%s must NOT be scoped - the install needs the whole repo", c.name)
			}
		})
	}
}

// The config nixpacks reads must land OUTSIDE the build context: a file written
// inside it would change the very context whose stability this exists to protect.
func TestWriteInstallScopeConfig(t *testing.T) {
	tmp := t.TempDir()
	path, err := writeNixpacksConfig(tmp, "blinkmypc", []string{"package.json", "bun.lock"}, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(path) != tmp {
		t.Fatalf("config landed at %s, want it under %s", path, tmp)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Phases map[string]struct {
			OnlyIncludeFiles []string `json:"onlyIncludeFiles"`
		} `json:"phases"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("config is not valid JSON for nixpacks: %v (%s)", err, body)
	}
	if !slices.Equal(got.Phases["install"].OnlyIncludeFiles, []string{"package.json", "bun.lock"}) {
		t.Fatalf("install scope = %v", got.Phases["install"].OnlyIncludeFiles)
	}
}

// A phase is emptied with `cmds: []`. Passing `-b ""` sets ONE empty command
// instead, which nixpacks happily runs - measured against nixpacks 1.41.0.
func TestNixpacksConfigEmptiesASkippedPhase(t *testing.T) {
	tmp := t.TempDir()
	path, err := writeNixpacksConfig(tmp, "app", nil, true, true)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var got struct {
		Phases map[string]struct {
			Cmds             *[]string `json:"cmds"`
			OnlyIncludeFiles []string  `json:"onlyIncludeFiles"`
		} `json:"phases"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, phase := range []string{"install", "build"} {
		p, ok := got.Phases[phase]
		if !ok {
			t.Fatalf("%s phase missing from %s", phase, body)
		}
		if p.Cmds == nil || len(*p.Cmds) != 0 {
			t.Fatalf("%s must carry an empty cmds array, got %s", phase, body)
		}
	}
}

// Nothing to say means no config at all, so the ordinary detection runs.
func TestNixpacksConfigIsOmittedWhenThereIsNothingToSay(t *testing.T) {
	path, err := writeNixpacksConfig(t.TempDir(), "app", nil, false, false)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if path != "" {
		t.Fatalf("expected no config file, got %q", path)
	}
}

// railpack ignores an empty RAILPACK_BUILD_CMD, so the skip rides a config file -
// under a Deplo name, because a repo may own railpack.json itself.
func TestRailpackSkipConfigDoesNotClobberTheRepoOwnConfig(t *testing.T) {
	dir := t.TempDir()
	mine := filepath.Join(dir, "railpack.json")
	if err := os.WriteFile(mine, []byte(`{"steps":{"build":{"commands":["keep me"]}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	name, err := writeRailpackSkipConfig(dir, false, true)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if name == "railpack.json" {
		t.Fatal("must not write over the repo's own railpack.json")
	}
	kept, err := os.ReadFile(mine)
	if err != nil || !strings.Contains(string(kept), "keep me") {
		t.Fatalf("the repo's own config was touched: %s (%v)", kept, err)
	}
	body, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"build":{"commands":[]}`) {
		t.Fatalf("build step not emptied: %s", body)
	}
	if strings.Contains(string(body), `"install"`) {
		t.Fatalf("install must be untouched when only the build is skipped: %s", body)
	}
}
