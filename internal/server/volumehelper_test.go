package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVolumeHelperRunSilencesTheContainerLog(t *testing.T) {
	was := missingVolumes
	missingVolumes = func(context.Context, []string) []string { return nil }
	t.Cleanup(func() { missingVolumes = was })
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

// A helper container started by hand writes a full second copy of the archive into its json log - 88 GB of it, once, on a production host.
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

// c39: a helper that mounted a deleted App's volume read-only made it again, empty, 12 s after the delete.
func TestVolumeHelperRunNeverMakesAVolumeItOnlyReads(t *testing.T) {
	was := missingVolumes
	var asked []string
	missingVolumes = func(_ context.Context, names []string) []string {
		asked = names
		return []string{"deplo-lab-a_data"}
	}
	t.Cleanup(func() { missingVolumes = was })
	argv := volumeHelperRun(t.Context(), "-d", "-v", "deplo-lab-a_data:/v/0:ro", "-v", "/tmp/stage:/stage", "-v", "out:/to",
		volumeHelperImage, "sleep", "60")
	if strings.Join(asked, " ") != "deplo-lab-a_data" {
		t.Fatalf("asked about %q: only a named volume mounted read-only is looked up", asked)
	}
	if strings.Join(argv, " ") != "volume inspect deplo-lab-a_data" {
		t.Fatalf("a helper would mount a missing volume read-only, and docker would make it: %q", argv)
	}
}
