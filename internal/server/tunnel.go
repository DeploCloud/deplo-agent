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

// tunnel.go ports the VS Code Remote Tunnel half of lib/deploy/dev.ts to the agent
// (PLAN Part D).

const (
	// In-container tunnel state paths - mirror lib/deploy/dev.ts TUNNEL_* (under
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

// StartTunnel launches the tunnel (idempotent) using the control-plane-rendered launch
// script, then returns the current status.
func (s *Service) StartTunnel(ctx context.Context, req *pb.TunnelRequest) (*pb.TunnelStatus, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) GetTunnel(ctx context.Context, req *pb.TunnelRequest) (*pb.TunnelStatus, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) StopTunnel(ctx context.Context, req *pb.TunnelRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}
