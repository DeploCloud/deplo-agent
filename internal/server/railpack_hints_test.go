package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func infoWithErrors(msgs ...string) railpackInfo {
	var info railpackInfo
	for _, m := range msgs {
		info.Logs = append(info.Logs, struct {
			Level string
			Msg   string
		}{"error", m})
	}
	return info
}

// A failed prepare reads as railpack's first sentence plus the change that fixes it.
func TestPrepareFailure(t *testing.T) {
	cases := []struct {
		name  string
		code  int
		info  railpackInfo
		token bool
		want  []string
	}{
		{"no start", 1, infoWithErrors("No start command detected. Specify a start command: https://railpack.com/config/file#deploy\n\nhelp"), false,
			[]string{"No start command detected. Set a Start command in Settings -> Build & output."}},
		{"unknown folder", 1, infoWithErrors("Railpack could not determine how to build the app.\n\nThe following languages are supported:\n- Php"), false,
			[]string{"Railpack could not determine how to build the app. Railpack does not recognise this folder: set the Root directory, or add a Dockerfile."}},
		{"python version", 1, infoWithErrors("failed to resolve version 3.99 of python"), false,
			[]string{"python 3.99 is not available: set RAILPACK_PYTHON_VERSION"}},
		{"php version", 1, infoWithErrors("no version available for php 7.1"), false,
			[]string{"PHP 7.1 is not available", `"php": "^8.3"`}},
		{"node version", 1, infoWithErrors("failed to get latest version for package `node` after trying versions: 99, 99.0: failed to run mise command"), false,
			[]string{"Node.js 99 is not available: pin a released Node.js version in Settings -> Build & output."}},
		{"github without a token", 1, infoWithErrors("failed to run mise: HTTP status client error (403 Forbidden) for url (https://api.github.com/repos/x)"), false,
			[]string{"rate limit", "GitHub App"}},
		{"github with a token", 1, infoWithErrors("API rate limit exceeded for https://api.github.com"), true,
			[]string{"deploy again in a few minutes"}},
		{"transient", railpackTransientExit, infoWithErrors("failed to ensure mise is installed: download failed"), false,
			[]string{"failed to ensure mise is installed: download failed.", "passing network problem"}},
		{"no info file", 2, railpackInfo{}, false, []string{"railpack prepare failed (exit 2)"}},
	}
	for _, c := range cases {
		got := prepareFailure(c.code, c.info, c.token, false)
		for _, w := range c.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: %q lacks %q", c.name, got, w)
			}
		}
	}
	if got := prepareFailure(1, infoWithErrors("No start command detected. Specify"), false, false); strings.Contains(got, "https://") {
		t.Errorf("railpack's own link must not crowd the hint: %q", got)
	}
}

// The info file railpack writes even on failure: its logs and the versions it resolved.
func TestReadRailpackInfo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "railpack-info.json")
	mustWrite(t, path, `{"railpackVersion":"0.43.0","resolvedPackages":{"node":{"name":"node","requestedVersion":"lts","resolvedVersion":"24.11.1","source":"railpack default"}},`+
		`"logs":[{"Level":"info","Msg":"Detected Node","DocsPath":""},{"Level":"error","Msg":"boom","DocsPath":""}],"success":false}`)
	info := readRailpackInfo(path)
	if info.major("node") != "24" || info.major("python") != "" {
		t.Fatalf("node major = %q", info.major("node"))
	}
	if errs := info.errors(); len(errs) != 1 || errs[0] != "boom" {
		t.Fatalf("errors = %v", errs)
	}
	if got := readRailpackInfo(filepath.Join(t.TempDir(), "absent.json")); len(got.Logs) != 0 {
		t.Fatalf("absent info = %+v", got)
	}
	_ = os.Remove(path)
}

func TestBuildHint(t *testing.T) {
	gyp := []string{"npm error gyp ERR! build error", "npm error gyp ERR! stack Error: `make` failed"}
	if h := buildHint(gyp, false, "24"); !strings.Contains(h, "pin Node 22") {
		t.Errorf("node-gyp under 24: %q", h)
	}
	if h := buildHint(gyp, false, "22"); h != "" {
		t.Errorf("node-gyp under 22 is not the Node 24 problem: %q", h)
	}
	if h := buildHint([]string{"mise ERROR no precompiled python found for core:python@3.6.0"}, false, ""); !strings.Contains(h, "RAILPACK_PYTHON_VERSION") {
		t.Errorf("precompiled python: %q", h)
	}
	if h := buildHint([]string{"error: 403 Forbidden from https://github.com/oven-sh/bun/releases"}, false, ""); !strings.Contains(h, "rate limit") {
		t.Errorf("github 403: %q", h)
	}
	if h := buildHint([]string{"npm ERR! missing script: build"}, false, "24"); h != "" {
		t.Errorf("an unknown failure must not get a guess: %q", h)
	}
}

func TestLineTailKeepsTheLast(t *testing.T) {
	tail := &lineTail{max: 3}
	for _, l := range []string{"a", "b", "c", "d", "e"} {
		tail.add(l)
	}
	if strings.Join(tail.lines, "") != "cde" {
		t.Fatalf("tail = %v", tail.lines)
	}
}

func TestPrepareFailureNamesADockerignoreThatHidesEverything(t *testing.T) {
	info := infoWithErrors("Railpack could not determine how to build the app.")
	if got := prepareFailure(1, info, false, true); !strings.Contains(got, ".dockerignore leaves out every file") {
		t.Fatalf("got %q", got)
	}
	if got := prepareFailure(1, info, false, false); !strings.Contains(got, "set the Root directory") {
		t.Fatalf("got %q", got)
	}
	dir := t.TempDir()
	if dockerignoreHidesAll(dir) {
		t.Fatal("no .dockerignore hides nothing")
	}
	_ = os.WriteFile(filepath.Join(dir, ".dockerignore"), []byte("*\n!target/*-runner\n"), 0o644)
	if !dockerignoreHidesAll(dir) {
		t.Fatal("a leading * hides every file")
	}
	_ = os.WriteFile(filepath.Join(dir, ".dockerignore"), []byte("node_modules\n*.log\n"), 0o644)
	if dockerignoreHidesAll(dir) {
		t.Fatal("*.log hides only logs")
	}
}

func TestBuildHintNamesACopiedFileTheRepositoryLacks(t *testing.T) {
	lines := []string{`ERROR: failed to build: failed to solve: failed to compute cache key: failed to calculate checksum of ref smfbcj0::q3v7: "/dist/shiori_linux_amd64/shiori": not found`}
	got := buildHint(lines, false, "")
	if !strings.Contains(got, "copies dist/shiori_linux_amd64/shiori, which is not in the repository") {
		t.Fatalf("got %q", got)
	}
}
