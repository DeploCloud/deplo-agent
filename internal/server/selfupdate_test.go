package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func binariesFor(url, sha string) map[string]*pb.ArchBinary {
	return map[string]*pb.ArchBinary{runtime.GOARCH: {Url: url, Sha256: sha}}
}

func stubDownload(t *testing.T, body []byte, err error) {
	t.Helper()
	orig := downloadFile
	downloadFile = func(context.Context, string) (io.ReadCloser, error) {
		if err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	t.Cleanup(func() { downloadFile = orig })
}

func captureReexec(t *testing.T) (fired func() bool, gotPath func() string) {
	t.Helper()
	var mu sync.Mutex
	var didFire bool
	var path string
	orig := reexec
	reexec = func(p string, _ []string, _ []string) error {
		mu.Lock()
		didFire, path = true, p
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { reexec = orig })
	return func() bool { mu.Lock(); defer mu.Unlock(); return didFire },
		func() string { mu.Lock(); defer mu.Unlock(); return path }
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// A successful update replaces the on-disk binary with the downloaded bytes and schedules a re-exec of that same path.
func TestSelfUpdate_swapsBinaryAndReexecs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	exe := filepath.Join(dir, "deplo-agent")
	if err := os.WriteFile(exe, []byte("OLD-BINARY"), 0o755); err != nil {
		t.Fatal(err)
	}

	newBytes := []byte("NEW-BINARY-v1.2.0")
	stubDownload(t, newBytes, nil)
	fired, gotPath := captureReexec(t)

	s := New(t.TempDir(), t.TempDir(), "/", "")
	resp, err := s.applyUpdate(ctx, exe, &pb.SelfUpdateRequest{
		Version:  "1.2.0",
		Binaries: binariesFor("https://example/deplo-agent-linux-"+runtime.GOARCH, sha256Hex(newBytes)),
	})
	if err != nil {
		t.Fatalf("applyUpdate: %v", err)
	}
	if !resp.GetRestarting() || resp.GetVersion() != "1.2.0" {
		t.Fatalf("unexpected response: restarting=%v version=%q", resp.GetRestarting(), resp.GetVersion())
	}

	got, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(newBytes) {
		t.Errorf("binary not swapped: got %q want %q", got, newBytes)
	}
	if fi, _ := os.Stat(exe); fi.Mode().Perm()&0o111 == 0 {
		t.Errorf("swapped binary is not executable: mode %v", fi.Mode())
	}

	deadline := time.After(selfUpdateGrace + 2*time.Second)
	for !fired() {
		select {
		case <-deadline:
			t.Fatal("re-exec never fired")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if gotPath() != exe {
		t.Errorf("re-exec targeted %q, want %q", gotPath(), exe)
	}
}

// A checksum mismatch must REFUSE the update and leave the running binary byte- for-byte untouched - the agent never installs an unverified binary (P2 parity).
func TestSelfUpdate_checksumMismatchLeavesBinaryUntouched(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	exe := filepath.Join(dir, "deplo-agent")
	original := []byte("OLD-BINARY-STAYS")
	if err := os.WriteFile(exe, original, 0o755); err != nil {
		t.Fatal(err)
	}

	stubDownload(t, []byte("TAMPERED-BYTES"), nil)
	fired, _ := captureReexec(t)

	s := New(t.TempDir(), t.TempDir(), "/", "")
	_, err := s.applyUpdate(ctx, exe, &pb.SelfUpdateRequest{
		Version:  "1.2.0",
		Binaries: binariesFor("https://example/deplo-agent-linux-"+runtime.GOARCH, sha256Hex([]byte("the-bytes-we-expected-but-did-not-get"))),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition on checksum mismatch, got %v", err)
	}

	got, _ := os.ReadFile(exe)
	if string(got) != string(original) {
		t.Errorf("binary was modified despite checksum mismatch: got %q want %q", got, original)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "deplo-agent" {
			t.Errorf("leftover file in install dir after failed update: %s", e.Name())
		}
	}
	time.Sleep(selfUpdateGrace + 200*time.Millisecond)
	if fired() {
		t.Error("re-exec fired after a refused update - must not restart into an unverified binary")
	}
}

// A download failure surfaces as Unavailable and leaves the binary untouched.
func TestSelfUpdate_downloadFailureIsUnavailable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	exe := filepath.Join(dir, "deplo-agent")
	if err := os.WriteFile(exe, []byte("OLD"), 0o755); err != nil {
		t.Fatal(err)
	}
	stubDownload(t, nil, context.DeadlineExceeded)

	s := New(t.TempDir(), t.TempDir(), "/", "")
	_, err := s.applyUpdate(ctx, exe, &pb.SelfUpdateRequest{
		Version: "1.2.0", Binaries: binariesFor("https://example/x", sha256Hex([]byte("x"))),
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable on download failure, got %v", err)
	}
	if got, _ := os.ReadFile(exe); string(got) != "OLD" {
		t.Errorf("binary changed on a failed download: %q", got)
	}
}

// No binary for this host's arch (empty map, or an entry with blank url/sha) is a FailedPrecondition before anything is downloaded - the agent tells the operator to re-run the installer rather than installing a wrong/absent binary.
func TestSelfUpdate_noBinaryForThisArch(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	cases := []*pb.SelfUpdateRequest{
		{Version: "1.2.0", Binaries: map[string]*pb.ArchBinary{}},
		{Version: "1.2.0", Binaries: map[string]*pb.ArchBinary{"sparc64": {Url: "https://x", Sha256: "abc"}}},
		{Version: "1.2.0", Binaries: binariesFor("", "")},
	}
	for _, req := range cases {
		if _, err := s.applyUpdate(context.Background(), filepath.Join(t.TempDir(), "x"), req); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("expected FailedPrecondition for %+v, got %v", req, err)
		}
	}
}

func TestVersionNewerMatchesTheControlPlane(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"0.8.0", "0.7.9", true},
		{"0.7.9", "0.8.0", false},
		{"0.8.0", "0.8.0", false},
		{"0.8.0", "0.8.0-canary.3", true},
		{"0.8.0-canary.10", "0.8.0-canary.9", true},
		{"0.8.0-canary.1", "0.7.5", true},
		{"0.8.0-3-gb39f8a7", "0.8.0", false},
		{"0.8.0-3-gb39f8a7-dirty", "0.7.0", true},
		{"v1.0.0", "0.9.0", true},
		{"dev", "0.1.0", false},
		{"0.1.0", "dev", false},
	}
	for _, c := range cases {
		if got := versionNewer(c.a, c.b); got != c.want {
			t.Errorf("versionNewer(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestSelfUpdate_refusesADowngrade(t *testing.T) {
	dir := t.TempDir()
	exe := filepath.Join(dir, "deplo-agent")
	if err := os.WriteFile(exe, []byte("CURRENT"), 0o755); err != nil {
		t.Fatal(err)
	}
	orig := AgentVersion
	AgentVersion = "0.9.0"
	t.Cleanup(func() { AgentVersion = orig })
	old := []byte("OLDER-BINARY")
	stubDownload(t, old, nil)
	fired, _ := captureReexec(t)

	_, err := New(t.TempDir(), t.TempDir(), "/", "").applyUpdate(context.Background(), exe, &pb.SelfUpdateRequest{
		Version:  "0.8.4",
		Binaries: binariesFor("https://example/deplo-agent", sha256Hex(old)),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a downgrade must be refused, got %v", err)
	}
	if got, _ := os.ReadFile(exe); string(got) != "CURRENT" || fired() {
		t.Fatal("a refused downgrade must leave the running binary alone")
	}
}

func TestDownloadFileIsHTTPSOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "http://example.com/deplo-agent", http.StatusFound)
			return
		}
		w.Write([]byte("BIN"))
	}))
	defer srv.Close()
	ctx := context.Background()

	if body, err := downloadFile(ctx, srv.URL+"/bin"); err != nil {
		t.Fatalf("a loopback test server must be reachable: %v", err)
	} else {
		body.Close()
	}
	for _, u := range []string{"http://example.com/deplo-agent", "ftp://example.com/x", srv.URL + "/redirect"} {
		if body, err := downloadFile(ctx, u); err == nil {
			body.Close()
			t.Errorf("%s must be refused", u)
		}
	}
}
