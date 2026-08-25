package server

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Why a Nixpacks build re-installs every dependency on every deploy, and when it is
// safe not to. The Dockerfile Nixpacks generates copies the WHOLE repository before the
// install step: COPY . /app/.

// installManifestCandidates are the files an install step legitimately reads: the
// manifest, the lockfiles of every supported package manager, and the registry/runtime
// config that changes how install resolves.
var installManifestCandidates = []string{
	"package.json",
	"package-lock.json",
	"npm-shrinkwrap.json",
	"yarn.lock",
	"pnpm-lock.yaml",
	"pnpm-workspace.yaml",
	"bun.lock",
	"bun.lockb",
	".npmrc",
	".yarnrc",
	".yarnrc.yml",
	".bunfig.toml",
	".nvmrc",
	".node-version",
	".tool-versions",
}

// installLifecycleScripts run as part of `install` itself and may touch any file
// in the repo, so their presence disqualifies a manifest-only copy.
var installLifecycleScripts = []string{"preinstall", "install", "postinstall", "prepare"}

// manifestOnlyInstallFiles returns the files the install phase may be restricted
// to, and whether restricting it is safe for this repository at all.
func manifestOnlyInstallFiles(dir string) ([]string, bool) {
	// A repo that configures Nixpacks itself is never second-guessed.
	for _, own := range []string{"nixpacks.toml", "nixpacks.json"} {
		if _, err := os.Stat(filepath.Join(dir, own)); err == nil {
			return nil, false
		}
	}
	// patch-package and friends rewrite node_modules from files in the repo
	// during install.
	if fi, err := os.Stat(filepath.Join(dir, "patches")); err == nil && fi.IsDir() {
		return nil, false
	}

	raw, err := os.ReadFile(filepath.Join(dir, "package.json"))
	if err != nil {
		return nil, false // not a Node app, or unreadable - leave it alone
	}
	var pkg struct {
		Workspaces json.RawMessage   `json:"workspaces"`
		Scripts    map[string]string `json:"scripts"`
	}
	if err := json.Unmarshal(raw, &pkg); err != nil {
		return nil, false
	}
	// A workspace root's install reads every member's package.json, none of which
	// the manifest list can enumerate.
	if len(pkg.Workspaces) > 0 {
		return nil, false
	}
	for _, s := range installLifecycleScripts {
		if pkg.Scripts[s] != "" {
			return nil, false
		}
	}

	files := make([]string, 0, len(installManifestCandidates))
	for _, name := range installManifestCandidates {
		if fi, err := os.Stat(filepath.Join(dir, name)); err == nil && !fi.IsDir() {
			files = append(files, name)
		}
	}
	// package.json is always first when present; without it there is nothing to
	// install from and no restriction to make.
	if len(files) == 0 || files[0] != "package.json" {
		return nil, false
	}
	return files, true
}

// writeInstallScopeConfig writes a Nixpacks config restricting the install phase
// to `files`, OUTSIDE the build context (a file inside it would change the very
// context whose stability this exists to protect), and returns its path.
func writeInstallScopeConfig(tmpDir, slug string, files []string) (string, error) {
	cfg := struct {
		Phases map[string]struct {
			OnlyIncludeFiles []string `json:"onlyIncludeFiles"`
		} `json:"phases"`
	}{Phases: map[string]struct {
		OnlyIncludeFiles []string `json:"onlyIncludeFiles"`
	}{"install": {OnlyIncludeFiles: files}}}

	body, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	path := filepath.Join(tmpDir, "deplo-nixpacks-"+slug+".json")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return "", err
	}
	return path, nil
}
