package server

import (
	"encoding/json"
	"path/filepath"
)

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
	if err := writeBuildFile(filepath.Join(buildDir, railpackSkipConfigName), body); err != nil {
		return "", err
	}
	return railpackSkipConfigName, nil
}
