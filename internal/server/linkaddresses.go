package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/link"
)

// EnableAgentLink lets Hello report the saved panel addresses and SetLinkAddresses replace them.
func (s *Service) EnableAgentLink(st *link.Store) { s.linkAddresses = st }

// SetLinkAddresses replaces the pair the agent links to; the control plane names both (ADR-0040).
func (s *Service) SetLinkAddresses(_ context.Context, req *pb.LinkAddresses) (*pb.LinkAddresses, error) {
	if s.linkAddresses == nil {
		return nil, status.Error(codes.FailedPrecondition, "this agent does not link to its panel")
	}
	kept, err := s.linkAddresses.Replace(link.Addresses{Primary: req.GetPrimary(), Fallback: req.GetFallback()})
	if errors.Is(err, link.ErrInvalidAddresses) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "save the panel addresses: %v", err)
	}
	return &pb.LinkAddresses{Primary: kept.Primary, Fallback: kept.Fallback}, nil
}

func (s *Service) savedLinkAddresses() *pb.LinkAddresses {
	if s.linkAddresses == nil {
		return nil
	}
	a := s.linkAddresses.Current()
	return &pb.LinkAddresses{Primary: a.Primary, Fallback: a.Fallback}
}
