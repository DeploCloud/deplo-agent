package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

// Nixpacks picks the nodejs PACKAGE from its environment but the nixpkgs ARCHIVE
// from the repository alone, so NIXPACKS_NODE_VERSION on its own asks an archive
// that has nodejs_18 and nodejs_20 for a nodejs_24 that is not in it:
// "undefined variable 'nodejs_24'", on every Node app with no version of its own.

// repoPinsNodeVersion reports whether the repository already says which Node it
// wants - the three places nixpacks reads, in its own order of precedence.
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

// writeNodeVersionPin puts the pinned major where nixpacks reads it from the repo,
// so the package and the archive are chosen from the same answer. Returns false
// when the repository pins its own version, which nixpacks must be left to follow.
func writeNodeVersionPin(dir, version string) (bool, error) {
	version = strings.TrimSpace(version)
	if version == "" || repoPinsNodeVersion(dir) {
		return false, nil
	}
	return true, os.WriteFile(filepath.Join(dir, ".nvmrc"), []byte(version+"\n"), 0o644)
}
