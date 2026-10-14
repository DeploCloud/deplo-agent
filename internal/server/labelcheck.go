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

// assertDatabaseOwned checks a backup/restore container's label; an older control plane sends no project_id.
func assertDatabaseOwned(ctx context.Context, d *pb.DatabaseDescriptor) error {
	if d.GetProjectId() == "" {
		return nil
	}
	return assertOwned(ctx, d.GetContainer(), d.GetProjectId())
}

func assertOwned(ctx context.Context, container, projectID string) error {
	if container == "" {
		return status.Error(codes.InvalidArgument, "container is required")
	}
	if projectID == "" {
		return status.Error(codes.InvalidArgument, "project_id is required")
	}
	res, err := dockercli.Run(ctx, 5*time.Second,
		"inspect", "--type", "container", "-f", `{{index .Config.Labels "deplo.project"}}`, container)
	if err != nil {
		return status.Errorf(codes.Unavailable, "inspect %s: %v", container, err)
	}
	if res.Code != 0 {
		return status.Errorf(codes.NotFound, "no such container %q", container)
	}
	got := strings.TrimSpace(res.Stdout)
	if got != projectID {
		return status.Errorf(codes.PermissionDenied,
			"container %q does not belong to project %q", container, projectID)
	}
	return nil
}
