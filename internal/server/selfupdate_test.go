package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// binariesFor builds a SelfUpdateRequest.binaries map carrying a single entry for
// THIS host's arch (runtime.GOARCH), which is what applyUpdate selects.
func binariesFor(url, sha string) map[string]*pb.ArchBinary {
	return map[string]*pb.ArchBinary{runtime.GOARCH: {Url: url, Sha256: sha}}
}

// stubDownload swaps the package-level downloadFile for one returning `body` (or
// `err`), and restores it on cleanup. Keeps tests off the network.
func stubDownload(t *testing.T, body []byte, err error) {
	t.Helper()
	orig := downloadFile
	downloadFile = func(context.Context, string) ([]byte, error) { return body, err }
	t.Cleanup(func() { downloadFile = orig })
}

// captureReexec swaps reexec for one that records its call instead of replacing
// the process (a real syscall.Exec would nuke the test runner). Returns a getter
// for whether it fired and with what path. Restored on cleanup.
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
		return nil // pretend the exec succeeded (never returns in prod)
	}
	t.Cleanup(func() { reexec = orig })
	return func() bool { mu.Lock(); defer mu.Unlock(); return didFire },
		func() string { mu.Lock(); defer mu.Unlock(); return path }
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// A successful update replaces the on-disk binary with the downloaded bytes and
// schedules a re-exec of that same path. The mTLS materials are out of scope — we
// assert nothing about them precisely because the handler never reads or writes
// them (that IS the cert-preserving guarantee).
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

	// The binary on disk is now the new bytes.
	got, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(newBytes) {
		t.Errorf("binary not swapped: got %q want %q", got, newBytes)
	}
	// And it stayed executable.
	if fi, _ := os.Stat(exe); fi.Mode().Perm()&0o111 == 0 {
		t.Errorf("swapped binary is not executable: mode %v", fi.Mode())
	}

	// The deferred re-exec fires (after the grace sleep) and targets the swapped path.
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

// A checksum mismatch must REFUSE the update and leave the running binary byte-
// for-byte untouched — the agent never installs an unverified binary (P2 parity).
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
	// No staged temp file left behind in the install dir.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() != "deplo-agent" {
			t.Errorf("leftover file in install dir after failed update: %s", e.Name())
		}
	}
	// Give any (incorrectly scheduled) re-exec a chance to fire, then assert none did.
	time.Sleep(selfUpdateGrace + 200*time.Millisecond)
	if fired() {
		t.Error("re-exec fired after a refused update — must not restart into an unverified binary")
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

// No binary for this host's arch (empty map, or an entry with blank url/sha) is a
// FailedPrecondition before anything is downloaded — the agent tells the operator
// to re-run the installer rather than installing a wrong/absent binary.
func TestSelfUpdate_noBinaryForThisArch(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	cases := []*pb.SelfUpdateRequest{
		// Empty map: no arch published at all.
		{Version: "1.2.0", Binaries: map[string]*pb.ArchBinary{}},
		// Only a DIFFERENT arch is present (never this host's).
		{Version: "1.2.0", Binaries: map[string]*pb.ArchBinary{"sparc64": {Url: "https://x", Sha256: "abc"}}},
		// This host's arch present but blank fields.
		{Version: "1.2.0", Binaries: binariesFor("", "")},
	}
	for _, req := range cases {
		if _, err := s.applyUpdate(context.Background(), filepath.Join(t.TempDir(), "x"), req); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("expected FailedPrecondition for %+v, got %v", req, err)
		}
	}
}
