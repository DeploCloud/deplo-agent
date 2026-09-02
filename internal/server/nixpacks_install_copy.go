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

// nixpacksPhase is the slice of a Nixpacks config Deplo writes. `cmds: []` is
// how a phase is emptied - passing `-b ""` sets ONE empty command instead, which
// is not the same thing.
type nixpacksPhase struct {
	OnlyIncludeFiles []string  `json:"onlyIncludeFiles,omitempty"`
	Cmds             *[]string `json:"cmds,omitempty"`
}

// writeNixpacksConfig writes the Nixpacks config OUTSIDE the build context (a
// file inside it would change the very context whose stability the install scope
// exists to protect) and returns its path. Returns "" when there is nothing to
// say. `files` restricts the install phase; the skips empty a phase outright.
func writeNixpacksConfig(tmpDir, slug string, files []string, skipInstall, skipBuild bool) (string, error) {
	phases := map[string]nixpacksPhase{}
	if len(files) > 0 {
		phases["install"] = nixpacksPhase{OnlyIncludeFiles: files}
	}
	if skipInstall {
		empty := []string{}
		phases["install"] = nixpacksPhase{Cmds: &empty}
	}
	if skipBuild {
		empty := []string{}
		phases["build"] = nixpacksPhase{Cmds: &empty}
	}
	if len(phases) == 0 {
		return "", nil
	}

	body, err := json.Marshal(struct {
		Phases map[string]nixpacksPhase `json:"phases"`
	}{Phases: phases})
	if err != nil {
		return "", err
	}
	path := filepath.Join(tmpDir, "deplo-nixpacks-"+slug+".json")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// railpackSkipConfigName is written into the build context because railpack's
// --config-file takes a path RELATIVE to it. The Deplo name keeps a repo's own
// railpack.json intact, and the caller removes the file after the prepare.
const railpackSkipConfigName = "deplo-railpack.json"

// writeRailpackSkipConfig empties a railpack step. `commands: []` is what railpack
// reads as "nothing to run"; an empty RAILPACK_BUILD_CMD is IGNORED (measured
// against railpack 0.35.0), which is why this file exists at all.
func writeRailpackSkipConfig(buildDir string, skipInstall, skipBuild bool) (string, error) {
	type step struct {
		Commands []string `json:"commands"`
	}
	steps := map[string]step{}
	if skipInstall {
		steps["install"] = step{Commands: []string{}}
	}
	if skipBuild {
		steps["build"] = step{Commands: []string{}}
	}
	body, err := json.Marshal(struct {
		Steps map[string]step `json:"steps"`
	}{Steps: steps})
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(buildDir, railpackSkipConfigName), body, 0o644); err != nil {
		return "", err
	}
	return railpackSkipConfigName, nil
}
