package server

import (
	"encoding/json"
	"os"
	"path/filepath"
)

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

var installLifecycleScripts = []string{"preinstall", "install", "postinstall", "prepare"}

func manifestOnlyInstallFiles(dir string) ([]string, bool) {
	for _, own := range []string{"nixpacks.toml", "nixpacks.json"} {
		if _, err := os.Stat(filepath.Join(dir, own)); err == nil {
			return nil, false
		}
	}
	if fi, err := os.Stat(filepath.Join(dir, "patches")); err == nil && fi.IsDir() {
		return nil, false
	}

	raw, err := os.ReadFile(filepath.Join(dir, "package.json"))
	if err != nil {
		return nil, false
	}
	var pkg struct {
		Workspaces json.RawMessage   `json:"workspaces"`
		Scripts    map[string]string `json:"scripts"`
	}
	if err := json.Unmarshal(raw, &pkg); err != nil {
		return nil, false
	}
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
	if len(files) == 0 || files[0] != "package.json" {
		return nil, false
	}
	return files, true
}

type nixpacksPhase struct {
	OnlyIncludeFiles []string  `json:"onlyIncludeFiles,omitempty"`
	Cmds             *[]string `json:"cmds,omitempty"`
}

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

const railpackSkipConfigName = "deplo-railpack.json"

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
