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

const nixpacksVersion = "1.41.0"

func (s *Service) ensureNixpacks(ctx context.Context, e *emitter) (string, error) {
	if p, err := exec.LookPath("nixpacks"); err == nil {
		return p, nil
	}

	toolsDir := filepath.Join(s.dataBase, "tools")
	dest := filepath.Join(toolsDir, "nixpacks-"+nixpacksVersion)

	if usableBinary(dest) {
		return dest, nil
	}

	e.log("info", fmt.Sprintf("Installing nixpacks %s (first use)…", nixpacksVersion))
	url, err := nixpacksDownloadURL()
	if err != nil {
		return "", err
	}
	if err := installTarBinary(ctx, url, "nixpacks", dest); err != nil {
		return "", err
	}
	e.log("info", "nixpacks installed")
	return dest, nil
}

func usableBinary(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && !fi.IsDir() && fi.Mode()&0o111 != 0
}

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
