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

const selfUpdateGrace = 750 * time.Millisecond

var reexec = func(path string, argv []string, env []string) error {
	return syscall.Exec(path, argv, env)
}

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
	return io.ReadAll(io.LimitReader(resp.Body, 256*1024*1024))
}

// SelfUpdate replaces the agent's own binary in place with a newer release and restarts to run it, WITHOUT touching the mTLS materials, so the server keeps its identity and pinned fingerprint across the upgrade (see the RPC's contract in proto/agent.proto).
func (s *Service) SelfUpdate(ctx context.Context, req *pb.SelfUpdateRequest) (*pb.SelfUpdateResponse, error) {
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

func (s *Service) applyUpdate(ctx context.Context, exePath string, req *pb.SelfUpdateRequest) (*pb.SelfUpdateResponse, error) {
	bin := req.GetBinaries()[runtime.GOARCH]
	if bin == nil || bin.GetUrl() == "" || bin.GetSha256() == "" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"the agent release has no binary for this host's architecture (%s); re-run the installer once a release includes it", runtime.GOARCH)
	}

	staged, err := s.stageVerifiedBinary(ctx, exePath, bin.GetUrl(), bin.GetSha256())
	if err != nil {
		return nil, err
	}

	if err := os.Rename(staged, exePath); err != nil {
		os.Remove(staged)
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot replace the agent binary at %s: %v (is the install dir writable? re-run the installer to upgrade)", exePath, err)
	}
	log.Printf("deplo-agent: self-update staged v%s at %s; restarting to apply", req.GetVersion(), exePath)

	argv := append([]string{exePath}, os.Args[1:]...)
	env := os.Environ()
	go func() {
		time.Sleep(selfUpdateGrace)
		log.Printf("deplo-agent: re-execing %s to complete self-update to v%s", exePath, req.GetVersion())
		if err := reexec(exePath, argv, env); err != nil {
			log.Printf("deplo-agent: re-exec failed: %v (new binary is on disk; restart the service to apply)", err)
		}
	}()

	return &pb.SelfUpdateResponse{Version: req.GetVersion(), Restarting: true}, nil
}

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
