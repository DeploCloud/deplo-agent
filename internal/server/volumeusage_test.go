package server

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The control plane only asks for a volume's size once this is advertised; without
// it an older agent would answer UNIMPLEMENTED and the card would read as broken.
func TestCapabilities_advertisesVolumeUsage(t *testing.T) {
	if !containsString(Capabilities, "volume-usage") {
		t.Error("Capabilities must advertise \"volume-usage\"")
	}
}

func TestVolumeUsage_rejectsUnsafeNames(t *testing.T) {
	s := &Service{}
	for _, name := range []string{"/etc", "../secrets", "a/b", ".hidden"} {
		_, err := s.VolumeUsage(context.Background(), &pb.VolumeUsageRequest{
			VolumeNames: []string{name},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("VolumeUsage(%q) = %v, want InvalidArgument", name, err)
		}
	}
}

func TestVolumeUsage_requiresAName(t *testing.T) {
	s := &Service{}
	_, err := s.VolumeUsage(context.Background(), &pb.VolumeUsageRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("VolumeUsage(nil) = %v, want InvalidArgument", err)
	}
}

// A container that has never run carries docker's zero time, whose epoch is
// negative - rendered as uptime it reads as decades.
func TestStartedAtUnix_neverStartedIsZero(t *testing.T) {
	for _, ts := range []string{"", "0001-01-01T00:00:00Z", "nonsense"} {
		if got := startedAtUnix(ts); got != 0 {
			t.Errorf("startedAtUnix(%q) = %d, want 0", ts, got)
		}
	}
	if got := startedAtUnix("2026-08-27T10:00:00.123456789Z"); got != 1787824800 {
		t.Errorf("startedAtUnix(real) = %d, want 1787824800", got)
	}
}
