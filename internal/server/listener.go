package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/link"
)

// SetListener names the agent's own port, so Hello reports it and StopListening can close it.
func (s *Service) SetListener(d *link.Direct) { s.listener = d }

// StopListening closes the agent's own port for good once its link works (ADR-0038).
// Asked over that port instead, the agent cannot tell its link works, so it keeps the port.
func (s *Service) StopListening(ctx context.Context, _ *pb.StopListeningRequest) (*pb.StopListeningResponse, error) {
	if p, ok := peer.FromContext(ctx); !ok || !link.Over(p.Addr) {
		return nil, status.Error(codes.FailedPrecondition, "ask over the agent link: closing this port would leave the agent no way in")
	}
	if s.listener.Listening() {
		if err := s.listener.Stop(); err != nil {
			return nil, status.Errorf(codes.Internal, "stop listening: %v", err)
		}
	}
	return &pb.StopListeningResponse{}, nil
}
