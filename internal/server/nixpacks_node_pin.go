package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

func repoPinsNodeVersion(dir string) bool {
	for _, name := range []string{".nvmrc", ".node-version"} {
		if fi, err := os.Stat(filepath.Join(dir, name)); err == nil && !fi.IsDir() {
			return true
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "package.json"))
	if err != nil {
		return false
	}
	var pkg struct {
		Engines map[string]string `json:"engines"`
	}
	if err := json.Unmarshal(raw, &pkg); err != nil {
		return false
	}
	return strings.TrimSpace(pkg.Engines["node"]) != ""
}

func writeNodeVersionPin(dir, version string) (bool, error) {
	version = strings.TrimSpace(version)
	if version == "" || repoPinsNodeVersion(dir) {
		return false, nil
	}
	return true, os.WriteFile(filepath.Join(dir, ".nvmrc"), []byte(version+"\n"), 0o644)
}
