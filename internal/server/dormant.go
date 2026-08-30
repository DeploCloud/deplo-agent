package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// Dev mode and its SSH gateway were removed from the control plane (#33/#34) and
// nothing here is reachable. These methods exist only so the generated Agent
// interface stays satisfied; every one refuses before any Docker/ssh/fs work, and
// the bodies that used to sit behind them are gone. Never revive them here: a
// return goes through the agent's own RPCs, not a second host-lifecycle path.

func (s *Service) StartDev(req *pb.StartDevRequest, stream pb.Agent_StartDevServer) error {
	return status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) ResetDevWorkspace(req *pb.StartDevRequest, stream pb.Agent_ResetDevWorkspaceServer) error {
	return status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) StopDev(ctx context.Context, req *pb.StopDevRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) TeardownDev(ctx context.Context, req *pb.TeardownDevRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) EnsureGateway(ctx context.Context, req *pb.EnsureGatewayRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) ProvisionSshUser(ctx context.Context, req *pb.ProvisionSshUserRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) DeprovisionSshUser(ctx context.Context, req *pb.DeprovisionSshUserRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) StartTunnel(ctx context.Context, req *pb.TunnelRequest) (*pb.TunnelStatus, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) GetTunnel(ctx context.Context, req *pb.TunnelRequest) (*pb.TunnelStatus, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

func (s *Service) StopTunnel(ctx context.Context, req *pb.TunnelRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}
