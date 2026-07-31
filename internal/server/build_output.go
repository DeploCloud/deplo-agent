package server

import (
	"context"
	"strings"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// How a finished build lands in the local image store.
//
// The obvious `docker build -t <ref>` hides an expensive default on a
// containerd-image-store host: BuildKit exports every new layer through GZIP on
// its way into the content store, then unpacks it again into the snapshotter.
// Measured on this platform's own Nixpacks image (a 927 MB node_modules layer),
// that export cost 25 s of the deploy — more than `npm install` and the app's
// own build step put together — to produce an image that never leaves the host.
// zstd at level 1 does the same job in 8 s and lands the same size on disk
// (1.28 GB vs 1.29 GB), so it is a straight win with no trade to make.
//
// Only the `image` exporter takes compression options, and only a containerd
// image store exposes it — hence the dockercli probe. On a classic overlay2
// daemon `-t` already skips compression entirely (the `moby` exporter stores raw
// diffs), so the fallback is not a slow path, it is the same fast path by
// another route.
const buildCompressionOpts = "compression=zstd,compression-level=1"

// imageOutputArgs returns the `docker build` flags that name and export the
// image being built: the fast `--output type=image,…` form where the daemon
// supports it, plain `-t` everywhere else. Callers splice the result where they
// used to write `"-t", ref`.
func imageOutputArgs(ctx context.Context, imageRef string) []string {
	return imageOutputArgsFor(imageRef, dockercli.ImageExportOptsSupported(ctx))
}

// imageOutputArgsFor is the pure decision behind imageOutputArgs, split out so
// both branches are testable without a Docker daemon.
func imageOutputArgsFor(imageRef string, fastExport bool) []string {
	// `--output` takes a CSV of key=value attributes, so a ref carrying a comma
	// (or whitespace, or a quote) could smuggle an extra attribute — `push=true`
	// being the alarming one. Every ref we mint is `deplo/<validated slug>:<id>`,
	// so this only ever fires on a malformed request; `-t` then rejects it as the
	// bad ref it is, which is the honest failure.
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
