package server

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// tunnel.go ports the VS Code Remote Tunnel half of lib/deploy/dev.ts to the
// agent (PLAN Part D). `code tunnel` runs INSIDE the dev container and dials OUT
// to Microsoft's relay over HTTPS — no inbound port, no gateway change — so these
// RPCs are thin `docker exec` wrappers. The variable bit (the launch script: CLI
// download URL + tunnel name) is rendered by the control plane and sent on
// StartTunnel; the in-container tunnel paths below are a fixed convention (the
// same constants the control plane's tunnelLaunchScript writes to), the Go twin
// of the gateway container name being a constant. The LOG PARSING (device-login
// link / connected URL) stays pure in the control plane (parseTunnelLog).

const (
	// In-container tunnel state paths — mirror lib/deploy/dev.ts TUNNEL_* (under
	// /workspace/.deplo so they persist across stop/start on the workspace bind).
	tunnelDir = "/workspace/.deplo"
	tunnelLog = tunnelDir + "/tunnel.log"
	tunnelPid = tunnelDir + "/tunnel.pid"
	codeCLI   = tunnelDir + "/code"
	cliData   = tunnelDir + "/cli-data"
)

// readTunnelStatus reads the tunnel log + running flag from the container. The
// running marker mirrors getVscodeTunnel: a sentinel printed when the pid file's
// process is alive. Best-effort: an absent/never-tunnelled container reads empty.
func readTunnelStatus(ctx context.Context, slug string) *pb.TunnelStatus {
	name := devProjectName(slug)
	script := "cat " + tunnelLog + " 2>/dev/null; " +
		"if [ -f " + tunnelPid + " ] && kill -0 \"$(cat " + tunnelPid + " 2>/dev/null)\" 2>/dev/null; then echo __DEPLO_RUNNING__; fi"
	res, err := dockercli.Run(ctx, 15*time.Second, "exec", name, "/bin/sh", "-c", script)
	if err != nil {
		return &pb.TunnelStatus{Running: false, Log: ""}
	}
	out := res.Stdout
	running := strings.Contains(out, "__DEPLO_RUNNING__")
	log := strings.TrimSpace(strings.ReplaceAll(out, "__DEPLO_RUNNING__", ""))
	return &pb.TunnelStatus{Running: running, Log: log}
}

// StartTunnel launches the tunnel (idempotent) using the control-plane-rendered
// launch script, then returns the current status. Mirrors startVscodeTunnel's
// launch step; the control plane does its own brief poll loop via GetTunnel.
// Dev mode (VS Code tunnel) was removed from the control plane, so this handler
// is dormant. Refuse instead of executing privileged `docker exec` as UID 1000 —
// no live caller should reach it, and a refusal removes the dead attack surface.
func (s *Service) StartTunnel(ctx context.Context, req *pb.TunnelRequest) (*pb.TunnelStatus, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) GetTunnel(ctx context.Context, req *pb.TunnelRequest) (*pb.TunnelStatus, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) StopTunnel(ctx context.Context, req *pb.TunnelRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}
