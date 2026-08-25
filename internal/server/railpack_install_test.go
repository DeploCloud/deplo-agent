package server

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The download URL must name the per-arch musl asset railpack actually
// publishes - a wrong shape 404s and the build method is dead on arrival.
func TestRailpackDownloadURL(t *testing.T) {
	url, err := railpackDownloadURL("0.35.0")
	if err != nil {
		// Non-linux/arch build hosts legitimately refuse; nothing to assert there.
		t.Skipf("unsupported host for railpack auto-install: %v", err)
	}
	if !strings.HasPrefix(url, "https://github.com/railwayapp/railpack/releases/download/v0.35.0/railpack-v0.35.0-") {
		t.Fatalf("unexpected release URL: %s", url)
	}
	if !strings.HasSuffix(url, "-unknown-linux-musl.tar.gz") {
		t.Fatalf("expected a musl linux asset: %s", url)
	}
}

// railpackBinaryVersion parses the bare version out of `railpack --version`
// ("railpack version 0.35.0"), which is what decides whether an operator's own
// binary on PATH may be used instead of the pinned one.
func TestRailpackBinaryVersion(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "railpack")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\necho 'railpack version 0.35.0'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := railpackBinaryVersion(context.Background(), fake); got != "0.35.0" {
		t.Fatalf("railpackBinaryVersion = %q; want 0.35.0", got)
	}

	// A `v` prefix is normalised away so it compares equal to the pinned string.
	vpfx := filepath.Join(dir, "railpack-v")
	if err := os.WriteFile(vpfx, []byte("#!/bin/sh\necho 'railpack version v0.35.0'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := railpackBinaryVersion(context.Background(), vpfx); got != "0.35.0" {
		t.Fatalf("v-prefixed version = %q; want 0.35.0", got)
	}

	// A binary that cannot be asked reports "", never a version we'd trust.
	if got := railpackBinaryVersion(context.Background(), filepath.Join(dir, "absent")); got != "" {
		t.Fatalf("absent binary = %q; want empty", got)
	}
}

// installTarBinary extracts the single named executable from a release tarball,
// creates the tools dir on the way, and leaves it executable.
func TestInstallTarBinary(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		gz := gzip.NewWriter(w)
		tw := tar.NewWriter(gz)
		// A real release tarball ships LICENSE/README alongside the binary; only
		// the named one may be extracted.
		for _, f := range []struct{ name, body string }{
			{"LICENSE", "MIT"},
			{"railpack", "#!/bin/sh\ntrue\n"},
		} {
			_ = tw.WriteHeader(&tar.Header{Name: f.name, Mode: 0o644, Size: int64(len(f.body)), Typeflag: tar.TypeReg})
			_, _ = tw.Write([]byte(f.body))
		}
		_ = tw.Close()
		_ = gz.Close()
	}))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "tools", "railpack-0.35.0")
	if err := installTarBinary(context.Background(), srv.URL, "railpack", dest); err != nil {
		t.Fatalf("installTarBinary: %v", err)
	}
	if !usableBinary(dest) {
		t.Fatalf("%s is not a usable executable after install", dest)
	}
	body, err := os.ReadFile(dest)
	if err != nil || !strings.Contains(string(body), "true") {
		t.Fatalf("wrong file extracted: %q (%v)", body, err)
	}

	// No `.part` temp file may survive - a leftover would be mistaken for a tool.
	entries, _ := os.ReadDir(filepath.Dir(dest))
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".part") {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
}

// A tarball without the requested binary must fail loudly rather than leave a
// half-installed tool behind.
func TestInstallTarBinaryMissingEntry(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		gz := gzip.NewWriter(w)
		tw := tar.NewWriter(gz)
		_ = tw.WriteHeader(&tar.Header{Name: "README.md", Mode: 0o644, Size: 2, Typeflag: tar.TypeReg})
		_, _ = tw.Write([]byte("hi"))
		_ = tw.Close()
		_ = gz.Close()
	}))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "tools", "railpack-0.35.0")
	if err := installTarBinary(context.Background(), srv.URL, "railpack", dest); err == nil {
		t.Fatal("want an error when the archive holds no railpack binary")
	}
	if _, err := os.Stat(dest); err == nil {
		t.Fatal("a failed install must not leave the destination behind")
	}
}
