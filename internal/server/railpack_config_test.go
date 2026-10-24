package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// railpack ignores an empty RAILPACK_BUILD_CMD, so the skip rides a config file - under a Deplo name, because a repo may own railpack.json itself.
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
