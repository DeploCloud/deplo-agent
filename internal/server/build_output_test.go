package server

import (
	"slices"
	"strings"
	"testing"
)

// On a daemon that cannot take image-exporter options, the argv must stay the
// plain `-t <ref>` every Docker version understands - the flag is not merely an
// optimisation there, it is rejected outright.
func TestImageOutputArgsFallsBackToTag(t *testing.T) {
	got := imageOutputArgsFor("deplo/hub:dpl_abc", false)
	if !slices.Equal(got, []string{"-t", "deplo/hub:dpl_abc"}) {
		t.Fatalf("imageOutputArgsFor(fast=false) = %v; want -t form", got)
	}
}

// With the containerd image store the argv switches to the image exporter with
// zstd compression - the change that took a 900 MB layer's export from 25 s to
// 8 s.
func TestImageOutputArgsUsesFastExport(t *testing.T) {
	got := imageOutputArgsFor("deplo/hub:dpl_abc", true)
	want := []string{"--output", "type=image,name=deplo/hub:dpl_abc,compression=zstd,compression-level=1"}
	if !slices.Equal(got, want) {
		t.Fatalf("imageOutputArgsFor(fast=true) = %v; want %v", got, want)
	}
}

// `--output` takes a CSV, so a ref carrying a comma (or space, or quote) could smuggle
// a second attribute - `push=true` being the one that would ship a private image to a
// registry.
func TestImageOutputArgsRefusesCSVSmuggling(t *testing.T) {
	for _, ref := range []string{
		"deplo/x:tag,push=true",
		"deplo/x:tag,name=evil/other:latest",
		"deplo/x:tag with space",
		`deplo/x:"quoted"`,
		"deplo/x:tag\nname=evil",
		"  deplo/x:tag",
		"",
	} {
		got := imageOutputArgsFor(ref, true)
		if len(got) == 0 || got[0] != "-t" {
			t.Fatalf("ref %q reached the --output CSV: %v", ref, got)
		}
	}
}

// The refs the platform actually mints must take the fast path, or the whole
// change is a no-op in production.
func TestImageOutputArgsAcceptsMintedRefs(t *testing.T) {
	for _, ref := range []string{
		"deplo/hub:dpl_1de86a50",
		"deplo/my-app-2:dpl_c47ef96e",
		"deplo-nixpacks-staging:dpl_abc123",
	} {
		got := imageOutputArgsFor(ref, true)
		if len(got) != 2 || got[0] != "--output" || !strings.Contains(got[1], "name="+ref+",") {
			t.Fatalf("minted ref %q did not take the fast path: %v", ref, got)
		}
	}
}
