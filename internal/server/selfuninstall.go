package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const selfUninstallGrace = 750 * time.Millisecond

var agentUnitPath = "/etc/systemd/system/deplo-agent.service"

var runSystemctl = func(ctx context.Context, args ...string) error {
	path, err := exec.LookPath("systemctl")
	if err != nil {
		return errNoSystemd
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	out, err := exec.CommandContext(cctx, path, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemctl %v: %w (%s)", args, err, string(out))
	}
	return nil
}

var errNoSystemd = errors.New("systemctl not found")

var exitProcess = func(code int) { os.Exit(code) }

// SelfUninstall removes the agent's own footprint from this host and stops.
func (s *Service) SelfUninstall(ctx context.Context, _ *pb.SelfUninstallRequest) (*pb.SelfUninstallResponse, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot locate the agent binary to remove: %v", err)
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return s.applyUninstall(ctx, exe)
}

func (s *Service) applyUninstall(ctx context.Context, exe string) (*pb.SelfUninstallResponse, error) {
	if err := probeWritableDir(filepath.Dir(exe)); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot remove the agent binary at %s: %v (run the uninstall command on the host instead)", exe, err)
	}

	removed := make([]string, 0, 3)

	if err := runSystemctl(ctx, "disable", "deplo-agent"); err != nil && !errors.Is(err, errNoSystemd) {
		log.Printf("deplo-agent: self-uninstall: %v (continuing)", err)
	}

	if err := os.Remove(agentUnitPath); err == nil {
		removed = append(removed, agentUnitPath)
	} else if !os.IsNotExist(err) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot remove the systemd unit at %s: %v", agentUnitPath, err)
	}

	if s.agentDir != "" {
		if err := os.RemoveAll(s.agentDir); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition,
				"cannot remove the agent state dir at %s: %v", s.agentDir, err)
		}
		removed = append(removed, s.agentDir)
	}

	if err := os.Remove(exe); err == nil {
		removed = append(removed, exe)
	} else if !os.IsNotExist(err) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot remove the agent binary at %s: %v", exe, err)
	}

	if err := runSystemctl(ctx, "daemon-reload"); err != nil && !errors.Is(err, errNoSystemd) {
		log.Printf("deplo-agent: self-uninstall: %v (continuing)", err)
	}

	log.Printf("deplo-agent: self-uninstall removed %v; exiting", removed)

	go func() {
		time.Sleep(selfUninstallGrace)
		log.Print("deplo-agent: self-uninstall complete, stopping")
		exitProcess(0)
	}()

	return &pb.SelfUninstallResponse{Removed: removed, Stopping: true}, nil
}

func probeWritableDir(dir string) error {
	f, err := os.CreateTemp(dir, ".deplo-agent-uninstall-*")
	if err != nil {
		return err
	}
	name := f.Name()
	f.Close()
	return os.Remove(name)
}
