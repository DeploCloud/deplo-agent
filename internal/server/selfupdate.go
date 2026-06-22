package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// selfUpdateGrace is how long the handler waits after replying before re-execing,
// so the SelfUpdateResponse is flushed to the control plane (and the gRPC stream
// torn down) before this process is replaced. Short — just enough to not race the
// reply onto the wire.
const selfUpdateGrace = 750 * time.Millisecond

// reexec is the function that replaces the running process with the freshly
// swapped binary. Overridable in tests (a real syscall.Exec never returns, which
// would kill the test runner). Production value re-execs via syscall.Exec so the
// new binary inherits the SAME argv (including --agent-dir and the listen addr),
// finds the existing mTLS materials, skips bootstrap, and serves — all under the
// same PID, so systemd's Restart policy is irrelevant.
var reexec = func(path string, argv []string, env []string) error {
	return syscall.Exec(path, argv, env)
}

// downloadFile is the HTTP fetch, overridable in tests so they can serve bytes
// without a network. Returns the raw body or an error.
var downloadFile = func(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download %s: HTTP %d", url, resp.StatusCode)
	}
	// Bound the read so a hostile/broken URL can't exhaust memory. The agent
	// binary is ~20-30 MiB; 256 MiB is a generous ceiling that still fails closed.
	return io.ReadAll(io.LimitReader(resp.Body, 256*1024*1024))
}

// SelfUpdate replaces the agent's own binary in place with a newer release and
// restarts to run it, WITHOUT touching the mTLS materials — so the server keeps
// its identity and pinned fingerprint across the upgrade (see the RPC's contract
// in proto/agent.proto). The control plane has already resolved the per-arch
// asset; we download it, verify the sha256 (refusing a mismatch — never exec an
// unverified binary), atomically swap our own binary, reply, then re-exec.
//
// Ordering matters: the swap + verification happen synchronously so any failure
// is returned to the caller (the running binary untouched); the actual re-exec is
// deferred to a goroutine AFTER we return the response, so the control plane gets
// a clean "restarting=true" rather than a dropped connection it must guess about.
func (s *Service) SelfUpdate(ctx context.Context, req *pb.SelfUpdateRequest) (*pb.SelfUpdateResponse, error) {
	// Where we live. Resolve symlinks so we replace the REAL file (install-agent.sh
	// installs to /usr/local/bin/deplo-agent directly, but a packaged setup may
	// symlink it; swapping the link target is what actually upgrades the binary).
	exe, err := os.Executable()
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot locate the agent binary to update: %v (re-run the installer to upgrade)", err)
	}
	if resolved, rerr := filepath.EvalSymlinks(exe); rerr == nil {
		exe = resolved
	}
	return s.applyUpdate(ctx, exe, req)
}

// applyUpdate performs the verified swap of the binary AT exePath and schedules
// the re-exec. Split out from SelfUpdate (which only resolves os.Executable())
// so tests drive the whole swap against a throwaway temp file instead of the
// running test binary. On success the binary at exePath now holds the new bytes
// and a deferred re-exec is in flight; on any failure the file is untouched and a
// gRPC status error is returned.
func (s *Service) applyUpdate(ctx context.Context, exePath string, req *pb.SelfUpdateRequest) (*pb.SelfUpdateResponse, error) {
	// Pick the asset for THIS host's architecture — the agent is the authority on
	// its own arch (runtime.GOARCH), exactly as install-agent.sh selects by
	// `uname -m`. An absent arch means the release didn't publish a binary for this
	// host; fail cleanly rather than install the wrong one.
	bin := req.GetBinaries()[runtime.GOARCH]
	if bin == nil || bin.GetUrl() == "" || bin.GetSha256() == "" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"the agent release has no binary for this host's architecture (%s); re-run the installer once a release includes it", runtime.GOARCH)
	}

	// Stage the new binary as a sibling temp file so the final rename is atomic and
	// stays on the SAME filesystem (rename across mounts fails). A failure here
	// leaves the running binary in place.
	staged, err := s.stageVerifiedBinary(ctx, exePath, bin.GetUrl(), bin.GetSha256())
	if err != nil {
		return nil, err
	}

	// Atomic swap: on Linux you can rename over the file of a running process —
	// the open text segment keeps the old inode until exit, the path now points at
	// the new binary, and the next exec of this path runs the new code.
	if err := os.Rename(staged, exePath); err != nil {
		os.Remove(staged) // best-effort: don't leave the staged file behind
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot replace the agent binary at %s: %v (is the install dir writable? re-run the installer to upgrade)", exePath, err)
	}
	log.Printf("deplo-agent: self-update staged v%s at %s; restarting to apply", req.GetVersion(), exePath)

	// Re-exec AFTER the response is flushed. We capture argv/env now (on the
	// request goroutine) and hand them to the re-exec; the new process inherits the
	// same flags, finds the existing materials under --agent-dir, and serves.
	argv := append([]string{exePath}, os.Args[1:]...)
	env := os.Environ()
	go func() {
		time.Sleep(selfUpdateGrace)
		log.Printf("deplo-agent: re-execing %s to complete self-update to v%s", exePath, req.GetVersion())
		if err := reexec(exePath, argv, env); err != nil {
			// syscall.Exec only returns on failure. If it does, the old process is
			// still running the old code — log loudly; the operator can restart the
			// service (or systemd will on the next failure) to pick up the new binary
			// that is already on disk.
			log.Printf("deplo-agent: re-exec failed: %v (new binary is on disk; restart the service to apply)", err)
		}
	}()

	return &pb.SelfUpdateResponse{Version: req.GetVersion(), Restarting: true}, nil
}

// stageVerifiedBinary downloads url, verifies its bytes against wantSha256
// (lowercase hex), writes them to a 0755 temp file beside `exe`, and returns that
// temp file's path. The caller renames it over `exe`. Any failure removes the temp
// file and returns a gRPC status error; the running binary is never touched until
// the bytes are proven to match — the same "refuse an unverified binary" guarantee
// install-agent.sh enforces before it ever runs the downloaded agent.
func (s *Service) stageVerifiedBinary(ctx context.Context, exe, url, wantSha256 string) (string, error) {
	body, err := downloadFile(ctx, url)
	if err != nil {
		return "", status.Errorf(codes.Unavailable, "download new agent binary: %v", err)
	}

	sum := sha256.Sum256(body)
	got := hex.EncodeToString(sum[:])
	if got != wantSha256 {
		return "", status.Errorf(codes.FailedPrecondition,
			"agent binary checksum mismatch: expected %s, got %s (refusing to install an unverified binary)", wantSha256, got)
	}

	dir := filepath.Dir(exe)
	tmp, err := os.CreateTemp(dir, ".deplo-agent-update-*")
	if err != nil {
		return "", status.Errorf(codes.FailedPrecondition,
			"cannot stage the update in %s: %v (re-run the installer to upgrade)", dir, err)
	}
	tmpPath := tmp.Name()
	cleanup := func(e error) (string, error) {
		tmp.Close()
		os.Remove(tmpPath)
		return "", e
	}
	if _, err := tmp.Write(body); err != nil {
		return cleanup(status.Errorf(codes.Internal, "write staged binary: %v", err))
	}
	if err := tmp.Chmod(0o755); err != nil {
		return cleanup(status.Errorf(codes.Internal, "chmod staged binary: %v", err))
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return "", status.Errorf(codes.Internal, "close staged binary: %v", err)
	}
	return tmpPath, nil
}
