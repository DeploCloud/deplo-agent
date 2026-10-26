package server

import (
	"context"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// StopListening succeeds and changes nothing: this agent has no port of its own (ADR-0040).
// The RPC stays in the contract for panels that still ask.
func (s *Service) StopListening(context.Context, *pb.StopListeningRequest) (*pb.StopListeningResponse, error) {
	return &pb.StopListeningResponse{}, nil
}
