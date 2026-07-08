package server

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

// nixpacksVersion is the nixpacks release the agent installs on first use. Pinned
// (not "latest") so a build is reproducible and a surprise upstream release can't
// change build output under a running agent. Bump deliberately.
//
// Held at >=1.41.0: nixpacks 1.21.0 silently ignored EVERY Node-version signal
// (NIXPACKS_NODE_VERSION, .nvmrc, package.json engines) and always built on
// nodejs_18 — so pinning a Node version did nothing. 1.41.0 honours all three.
const nixpacksVersion = "1.41.0"

// ensureNixpacks returns the path to a usable nixpacks binary, installing it
// lazily on first use (the "lazy: fetch on first use" tooling policy). Resolution
// order: a nixpacks already on PATH (operator-provided) → a previously-installed
// copy under <dataBase>/tools → a fresh download of the pinned release for this
// host's arch. The binary is cached so only the first heavy nixpacks build pays
// the download. Mirrors the old builders.ts assumption that `nixpacks` is present,
// but provisions it itself rather than erroring when it is absent.
func (s *Service) ensureNixpacks(ctx context.Context, e *emitter) (string, error) {
	// 1. An operator-installed nixpacks on PATH wins (lets a host pin its own).
	if p, err := exec.LookPath("nixpacks"); err == nil {
		return p, nil
	}

	toolsDir := filepath.Join(s.dataBase, "tools")
	// Version-scope the cached binary so bumping nixpacksVersion re-downloads the
	// new release instead of silently reusing a stale cached copy (an unversioned
	// path would pin every server to whatever it first downloaded — the exact trap
	// that kept broken 1.21.0 around after a bump).
	dest := filepath.Join(toolsDir, "nixpacks-"+nixpacksVersion)

	// 2. A previously-installed copy of THIS version under <dataBase>/tools.
	if fi, err := os.Stat(dest); err == nil && !fi.IsDir() && fi.Mode()&0o111 != 0 {
		return dest, nil
	}

	// 3. Download the pinned release for this arch and cache it.
	e.log("info", fmt.Sprintf("Installing nixpacks %s (first use)…", nixpacksVersion))
	if err := os.MkdirAll(toolsDir, 0o755); err != nil {
		return "", fmt.Errorf("create tools dir: %w", err)
	}
	url, err := nixpacksDownloadURL()
	if err != nil {
		return "", err
	}
	if err := downloadNixpacks(ctx, url, dest); err != nil {
		return "", err
	}
	e.log("info", "nixpacks installed")
	return dest, nil
}

// nixpacksDownloadURL builds the GitHub release asset URL for this host's OS/arch.
// nixpacks publishes per-target gzipped tarballs (e.g.
// nixpacks-v1.21.0-x86_64-unknown-linux-musl.tar.gz). Only Linux is supported (the
// agent runs on Linux servers).
func nixpacksDownloadURL() (string, error) {
	if runtime.GOOS != "linux" {
		return "", fmt.Errorf("nixpacks auto-install supports linux only (host is %s)", runtime.GOOS)
	}
	var target string
	switch runtime.GOARCH {
	case "amd64":
		target = "x86_64-unknown-linux-musl"
	case "arm64":
		target = "aarch64-unknown-linux-musl"
	default:
		return "", fmt.Errorf("nixpacks auto-install: unsupported arch %s", runtime.GOARCH)
	}
	return fmt.Sprintf(
		"https://github.com/railwayapp/nixpacks/releases/download/v%s/nixpacks-v%s-%s.tar.gz",
		nixpacksVersion, nixpacksVersion, target), nil
}

// downloadNixpacks fetches the gzipped tarball at url and extracts the `nixpacks`
// binary to dest (0755), atomically (temp file + rename). The archive holds a
// single top-level `nixpacks` executable.
func downloadNixpacks(ctx context.Context, url, dest string) error {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download nixpacks: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download nixpacks: HTTP %d from %s", resp.StatusCode, url)
	}

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("gunzip nixpacks: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return fmt.Errorf("nixpacks binary not found in archive")
		}
		if err != nil {
			return fmt.Errorf("read nixpacks archive: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg || filepath.Base(hdr.Name) != "nixpacks" {
			continue
		}
		tmp := dest + ".tmp"
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
		if err != nil {
			return err
		}
		// Bound the copy to defend against a decompression bomb (256 MiB ≫ the
		// real ~30 MiB binary, but finite).
		if _, err := io.Copy(f, io.LimitReader(tr, 256<<20)); err != nil {
			f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("extract nixpacks: %w", err)
		}
		f.Close()
		if err := os.Rename(tmp, dest); err != nil {
			_ = os.Remove(tmp)
			return err
		}
		return nil
	}
}
