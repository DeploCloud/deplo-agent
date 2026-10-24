package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// railpack ignores an empty RAILPACK_BUILD_CMD, so the skip rides a config file - under a Deplo
// name, holding the repo's own railpack.json with only the skipped step emptied.
func TestRailpackSkipConfigMergesTheRepoOwnConfig(t *testing.T) {
	dir := t.TempDir()
	mine := filepath.Join(dir, "railpack.json")
	own := `{
  // the repo's start command must survive the skip
  "deploy": {"startCommand": "node server.js",},
  "steps": {"build": {"commands": ["npm run build"], "caches": ["npm"]}, "install": {"commands": ["npm ci"]}},
  "packages": {"note": "a // in a string is not a comment"},
}`
	mustWrite(t, mine, own)
	name, err := writeRailpackSkipConfig(dir, "", false, true)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if name == "railpack.json" {
		t.Fatal("must not write over the repo's own railpack.json")
	}
	if kept, _ := os.ReadFile(mine); string(kept) != own {
		t.Fatalf("the repo's own config was touched: %s", kept)
	}
	var got struct {
		Deploy struct {
			StartCommand string `json:"startCommand"`
		} `json:"deploy"`
		Steps map[string]struct {
			Commands []string `json:"commands"`
			Caches   []string `json:"caches"`
		} `json:"steps"`
		Packages map[string]string `json:"packages"`
	}
	body, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("merged config is not JSON: %v\n%s", err, body)
	}
	if got.Deploy.StartCommand != "node server.js" {
		t.Fatalf("the repo's start command was dropped: %s", body)
	}
	if b := got.Steps["build"]; b.Commands == nil || len(b.Commands) != 0 || len(b.Caches) != 1 {
		t.Fatalf("build step not emptied, or its other keys lost: %s", body)
	}
	if i := got.Steps["install"]; len(i.Commands) != 1 {
		t.Fatalf("install must be untouched when only the build is skipped: %s", body)
	}
	if got.Packages["note"] != "a // in a string is not a comment" {
		t.Fatalf("a string lost its slashes: %s", body)
	}
}

// RAILPACK_CONFIG_FILE names the repo's config instead of railpack.json; it must exist and stay inside.
func TestRailpackSkipConfigReadsTheNamedConfig(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "deploy", "rp.json"), `{"deploy":{"startCommand":"./run"}}`)
	name, err := writeRailpackSkipConfig(dir, "deploy/rp.json", true, false)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := os.ReadFile(filepath.Join(dir, name))
	if !strings.Contains(string(body), `"startCommand":"./run"`) || !strings.Contains(string(body), `"install":{"commands":[]}`) {
		t.Fatalf("named config not merged: %s", body)
	}
	for _, bad := range []string{"missing.json", "../escape.json"} {
		if _, err := writeRailpackSkipConfig(dir, bad, true, false); err == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
	mustWrite(t, filepath.Join(dir, "railpack.json"), `{"steps": nope}`)
	if _, err := writeRailpackSkipConfig(dir, "", true, false); err == nil {
		t.Fatal("an unreadable railpack.json was silently dropped")
	}
	if _, err := writeRailpackSkipConfig(t.TempDir(), "", true, true); err != nil {
		t.Fatalf("no railpack.json at all must still write the skip: %v", err)
	}
}

func TestJsoncToJSON(t *testing.T) {
	for in, want := range map[string]string{
		`{"a":1,}`:                    `{"a":1}`,
		"[1,2, // two\n]":             "[1,2]",
		`{"u":"http://x/*y*/"}`:       `{"u":"http://x/*y*/"}`,
		`{"q":"say \"hi\" // ok"} //`: `{"q":"say \"hi\" // ok"} `,
		"/* head */{}":                "{}",
	} {
		if got := string(jsoncToJSON([]byte(in))); got != want {
			t.Errorf("jsoncToJSON(%q) = %q, want %q", in, got, want)
		}
	}
}

// A single-page app gets a Staticfile with index fallback, but a repo's own Staticfile wins.
func TestWriteStaticfile(t *testing.T) {
	dir := t.TempDir()
	wrote, err := writeStaticfile(dir, "dist")
	if err != nil || !wrote {
		t.Fatalf("wrote=%v err=%v", wrote, err)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "Staticfile")); string(b) != "root: \"dist\"\nindex_fallback: true\n" {
		t.Fatalf("Staticfile = %q", b)
	}
	mustWrite(t, filepath.Join(dir, "Staticfile"), "root: public\n")
	if wrote, _ := writeStaticfile(dir, "dist"); wrote {
		t.Fatal("the repo's own Staticfile was replaced")
	}
	bare := t.TempDir()
	if _, err := writeStaticfile(bare, ""); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(bare, "Staticfile")); string(b) != "index_fallback: true\n" {
		t.Fatalf("no output folder must leave railpack to find the root: %q", b)
	}
}
