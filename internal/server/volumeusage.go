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

const volumeUsageTimeout = 60 * time.Second

// VolumeUsage reports the disk each named volume occupies.
func (s *Service) VolumeUsage(
	ctx context.Context,
	req *pb.VolumeUsageRequest,
) (*pb.VolumeUsageResponse, error) {
	names := req.GetVolumeNames()
	if len(names) == 0 {
		return nil, status.Error(codes.InvalidArgument, "volume_names is required")
	}
	for _, n := range names {
		if err := validateVolumeName(n); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	cctx, cancel := context.WithTimeout(ctx, volumeUsageTimeout)
	defer cancel()

	args := append([]string{"volume", "inspect", "--format", "{{.Name}}\t{{.Mountpoint}}"}, names...)
	res, err := dockercli.Run(cctx, volumeUsageTimeout, args...)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "docker is not reachable on this host: %v", err)
	}

	out := &pb.VolumeUsageResponse{}
	for _, line := range strings.Split(res.Stdout, "\n") {
		name, mount, ok := strings.Cut(strings.TrimSpace(line), "\t")
		if !ok || name == "" || mount == "" {
			continue
		}
		out.Volumes = append(out.Volumes, &pb.VolumeUsage{
			VolumeName: name,
			Bytes:      dirSize(mount),
		})
	}
	return out, nil
}
