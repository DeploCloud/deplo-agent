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
	"strings"
	"time"
)

// The railpack CLI and frontend every build uses: the version Deplo tests, never an app's choice.
const railpackVersion = "0.43.0"

const railpackFrontend = "ghcr.io/railwayapp/railpack-frontend:v" + railpackVersion

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

func railpackDownloadURL(version string) (string, error) {
	return railpackAssetURL(version, runtime.GOOS, runtime.GOARCH)
}

func railpackAssetURL(version, goos, goarch string) (string, error) {
	if goos != "linux" {
		return "", fmt.Errorf("railpack auto-install supports linux only (host is %s)", goos)
	}
	var target string
	switch goarch {
	case "amd64":
		target = "x86_64-unknown-linux-musl"
	case "arm64":
		target = "arm64-unknown-linux-musl"
	default:
		return "", fmt.Errorf("railpack auto-install: unsupported arch %s", goarch)
	}
	return fmt.Sprintf(
		"https://github.com/railwayapp/railpack/releases/download/v%s/railpack-v%s-%s.tar.gz",
		version, version, target), nil
}

func usableBinary(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir() && fi.Mode()&0o111 != 0
}

func installTarBinary(ctx context.Context, url, binary, dest string) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("create tools dir: %w", err)
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", binary, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: HTTP %d from %s", binary, resp.StatusCode, url)
	}

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("gunzip %s: %w", binary, err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return fmt.Errorf("%s binary not found in archive", binary)
		}
		if err != nil {
			return fmt.Errorf("read %s archive: %w", binary, err)
		}
		if hdr.Typeflag != tar.TypeReg || filepath.Base(hdr.Name) != binary {
			continue
		}
		f, err := os.CreateTemp(filepath.Dir(dest), "."+binary+"-*.part")
		if err != nil {
			return err
		}
		tmp := f.Name()
		if _, err := io.Copy(f, io.LimitReader(tr, 256<<20)); err != nil {
			f.Close()
			_ = os.Remove(tmp)
			return fmt.Errorf("extract %s: %w", binary, err)
		}
		f.Close()
		if err := os.Chmod(tmp, 0o755); err != nil {
			_ = os.Remove(tmp)
			return err
		}
		if err := os.Rename(tmp, dest); err != nil {
			_ = os.Remove(tmp)
			return err
		}
		return nil
	}
}
