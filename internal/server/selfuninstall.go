package server

// https://deplo.build/docs/operations/remove-a-server-or-uninstall

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

// selfUninstallGrace is how long the handler waits after replying before exiting, so
// the SelfUninstallResponse is flushed to the control plane (and the gRPC stream torn
// down) before this process is gone.
const selfUninstallGrace = 750 * time.Millisecond

// agentUnitPath is the systemd unit install-agent.sh writes. FIXED, never taken from
// the request: every path this RPC deletes is one the agent already knows, because
// handing a remote peer an `rm -rf` argument is the whole class of bug this avoids.
var agentUnitPath = "/etc/systemd/system/deplo-agent.service"

// runSystemctl runs one systemctl verb. Overridable in tests. A host with no
// systemd at all (the agent in a container) is not an error: there is no unit to
// disable, so the uninstall simply skips that step.
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

// errNoSystemd means the host has no systemctl — a skip, never a failure.
var errNoSystemd = errors.New("systemctl not found")

// exitProcess ends this process. Overridable in tests (a real os.Exit would take
// the test runner with it).
var exitProcess = func(code int) { os.Exit(code) }

// SelfUninstall removes the agent's own footprint from this host and stops. The unit
// file has no KillMode, so the default is control-group: stopping it kills THIS process
// (and any child we forked) before a single file is removed.
func (s *Service) SelfUninstall(ctx context.Context, _ *pb.SelfUninstallRequest) (*pb.SelfUninstallResponse, error) {
	// Where we live. Resolve symlinks so we delete the REAL file, exactly as
	// SelfUpdate resolves it before swapping it.
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

// applyUninstall performs the removals for the binary AT exePath and schedules
// the exit. Split from the RPC for the same reason applyUpdate is: the tests
// drive a temp-dir binary instead of the running test runner's own.
func (s *Service) applyUninstall(ctx context.Context, exe string) (*pb.SelfUninstallResponse, error) {
	// Pre-flight the one failure that is both likely and awkward: an install dir we cannot
	// write to.
	if err := probeWritableDir(filepath.Dir(exe)); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot remove the agent binary at %s: %v (run the uninstall command on the host instead)", exe, err)
	}

	removed := make([]string, 0, 3)

	// 1. Stop systemd from starting us again. Best effort: a unit that was never
	// enabled, or a host with no systemd, must not abort the uninstall.
	if err := runSystemctl(ctx, "disable", "deplo-agent"); err != nil && !errors.Is(err, errNoSystemd) {
		log.Printf("deplo-agent: self-uninstall: %v (continuing)", err)
	}

	// 2. The unit file. Absent is a skip — that is what an agent run in a
	// container looks like, and it is not a failure.
	if err := os.Remove(agentUnitPath); err == nil {
		removed = append(removed, agentUnitPath)
	} else if !os.IsNotExist(err) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot remove the systemd unit at %s: %v", agentUnitPath, err)
	}

	// 3. The state dir: the mTLS materials, and whatever else the installer put
	// under --agent-dir. This is the one removal that must not be skipped
	// silently — leaving the keys behind is leaving the trust behind.
	if s.agentDir != "" {
		if err := os.RemoveAll(s.agentDir); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition,
				"cannot remove the agent state dir at %s: %v", s.agentDir, err)
		}
		removed = append(removed, s.agentDir)
	}

	// 4. The binary. Deleting the file of a running process is fine on Linux: the
	// open text segment keeps the inode alive until we exit.
	if err := os.Remove(exe); err == nil {
		removed = append(removed, exe)
	} else if !os.IsNotExist(err) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot remove the agent binary at %s: %v", exe, err)
	}

	// 5. Let systemd forget the unit we just deleted. Best effort, and last: it
	// changes nothing about whether the uninstall succeeded.
	if err := runSystemctl(ctx, "daemon-reload"); err != nil && !errors.Is(err, errNoSystemd) {
		log.Printf("deplo-agent: self-uninstall: %v (continuing)", err)
	}

	log.Printf("deplo-agent: self-uninstall removed %v; exiting", removed)

	// Exit AFTER the response is flushed, and cleanly: systemd's
	// Restart=on-failure ignores a zero exit, and the unit is gone anyway.
	go func() {
		time.Sleep(selfUninstallGrace)
		log.Print("deplo-agent: self-uninstall complete, stopping")
		exitProcess(0)
	}()

	return &pb.SelfUninstallResponse{Removed: removed, Stopping: true}, nil
}

// probeWritableDir answers "can this process create and delete a file here?" the
// only way that is not a lie on a root-squashed or read-only mount: by doing it.
func probeWritableDir(dir string) error {
	f, err := os.CreateTemp(dir, ".deplo-agent-uninstall-*")
	if err != nil {
		return err
	}
	name := f.Name()
	f.Close()
	return os.Remove(name)
}
