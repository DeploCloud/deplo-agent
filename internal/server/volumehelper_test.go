package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVolumeHelperRunSilencesTheContainerLog(t *testing.T) {
	argv := volumeHelperRun(t.Context(), "-v", "vol:/v:ro", volumeHelperImage, "tar", "-C", "/v", "-cf", "-", ".")
	joined := strings.Join(argv, " ")
	if !strings.Contains(joined, "--log-driver=none") {
		t.Fatalf("no --log-driver=none in %q", joined)
	}
	if argv[0] != "run" || argv[1] != "--rm" {
		t.Fatalf("argv does not start with `run --rm`: %q", joined)
	}
	if !strings.HasSuffix(joined, "tar -C /v -cf - .") {
		t.Fatalf("the caller's own args were not kept: %q", joined)
	}
}

// A helper container started by hand writes a full second copy of the archive
// into its json log - 88 GB of it, once, on a production host. Every call site
// goes through volumeHelperRun, and this is what says so.
func TestNoHandWrittenVolumeHelperRun(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(body), "\n")
		for i, line := range lines {
			if !strings.Contains(line, "volumeHelperImage") {
				continue
			}
			for j := max(0, i-3); j <= i; j++ {
				if strings.Contains(lines[j], `"run", "--rm"`) {
					t.Errorf("%s:%d starts a volume helper by hand - use volumeHelperRun", name, j+1)
				}
			}
		}
	}
}
