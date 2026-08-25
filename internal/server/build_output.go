package server

import (
	"context"
	"strings"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// How a finished build lands in the local image store.
const buildCompressionOpts = "compression=zstd,compression-level=1"

// imageOutputArgs returns the `docker build` flags that name and export the image being
// built: the fast `--output type=image,…` form where the daemon supports it, plain `-t`
// everywhere else.
func imageOutputArgs(ctx context.Context, imageRef string) []string {
	return imageOutputArgsFor(imageRef, dockercli.ImageExportOptsSupported(ctx))
}

// imageOutputArgsFor is the pure decision behind imageOutputArgs, split out so
// both branches are testable without a Docker daemon.
func imageOutputArgsFor(imageRef string, fastExport bool) []string {
	// `--output` takes a CSV of key=value attributes, so a ref carrying a comma (or
	// whitespace, or a quote) could smuggle an extra attribute — `push=true` being the
	// alarming one.
	if !fastExport || !safeImageRef(imageRef) {
		return []string{"-t", imageRef}
	}
	return []string{"--output", "type=image,name=" + imageRef + "," + buildCompressionOpts}
}

// safeImageRef reports whether a ref can be embedded in an `--output` CSV value
// without changing its meaning.
func safeImageRef(ref string) bool {
	if strings.TrimSpace(ref) == "" || ref != strings.TrimSpace(ref) {
		return false
	}
	return !strings.ContainsAny(ref, ",=\"' \t\r\n")
}
