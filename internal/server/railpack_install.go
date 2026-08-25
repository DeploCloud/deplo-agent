package server

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// railpackVersion is the railpack release the agent installs when a project does not
// pin one. Pinned (not "latest") for the same reason nixpacksVersion is: a build must
// not change output because upstream cut a release overnight.
const railpackVersion = "0.35.0"

// ensureRailpack returns the path to a railpack binary at the requested version,
// installing it lazily on first use — the same "lazy: fetch on first use" policy as
// ensureNixpacks, and the reason the railpack path no longer needs a throwaway Debian
// container.
func (s *Service) ensureRailpack(ctx context.Context, version string, e *emitter) (string, error) {
	if p, err := exec.LookPath("railpack"); err == nil && railpackBinaryVersion(ctx, p) == version {
		return p, nil
	}

	toolsDir := filepath.Join(s.dataBase, "tools")
	dest := filepath.Join(toolsDir, "railpack-"+version)
	if usableBinary(dest) {
		return dest, nil
	}

	e.log("info", fmt.Sprintf("Installing railpack %s (first use)…", version))
	url, err := railpackDownloadURL(version)
	if err != nil {
		return "", err
	}
	if err := installTarBinary(ctx, url, "railpack", dest); err != nil {
		return "", err
	}
	e.log("info", "railpack installed")
	return dest, nil
}

// railpackBinaryVersion returns the bare version a railpack binary reports
// (`railpack version 0.35.0` → "0.35.0"), or "" if it cannot be asked.
func railpackBinaryVersion(ctx context.Context, path string) string {
	out, err := exec.CommandContext(ctx, path, "--version").Output()
	if err != nil {
		return ""
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return ""
	}
	return strings.TrimPrefix(fields[len(fields)-1], "v")
}

// railpackDownloadURL builds the GitHub release asset URL for this host's
// OS/arch. railpack publishes per-target gzipped tarballs holding a single
// `railpack` executable (e.g. railpack-v0.35.0-x86_64-unknown-linux-musl.tar.gz).
func railpackDownloadURL(version string) (string, error) {
	if runtime.GOOS != "linux" {
		return "", fmt.Errorf("railpack auto-install supports linux only (host is %s)", runtime.GOOS)
	}
	var target string
	switch runtime.GOARCH {
	case "amd64":
		target = "x86_64-unknown-linux-musl"
	case "arm64":
		target = "aarch64-unknown-linux-musl"
	default:
		return "", fmt.Errorf("railpack auto-install: unsupported arch %s", runtime.GOARCH)
	}
	return fmt.Sprintf(
		"https://github.com/railwayapp/railpack/releases/download/v%s/railpack-v%s-%s.tar.gz",
		version, version, target), nil
}
