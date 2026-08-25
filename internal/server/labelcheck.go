package server

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// assertOwned confirms that `container` carries the label deplo.project=<projectID>
// before any console RPC (FollowLogs/Attach/Exec/ShellLabel) acts on it. An empty
// projectID is rejected: a console RPC must always identify its project.
func assertOwned(ctx context.Context, container, projectID string) error {
	if container == "" {
		return status.Error(codes.InvalidArgument, "container is required")
	}
	if projectID == "" {
		return status.Error(codes.InvalidArgument, "project_id is required")
	}
	res, err := dockercli.Run(ctx, 5*time.Second,
		"inspect", "-f", `{{index .Config.Labels "deplo.project"}}`, container)
	if err != nil {
		// Docker could not run / timed out: treat as unreachable, not a denial.
		return status.Errorf(codes.Unavailable, "inspect %s: %v", container, err)
	}
	if res.Code != 0 {
		// No such container (or inspect failed): nothing to act on.
		return status.Errorf(codes.NotFound, "no such container %q", container)
	}
	got := strings.TrimSpace(res.Stdout)
	if got != projectID {
		return status.Errorf(codes.PermissionDenied,
			"container %q does not belong to project %q", container, projectID)
	}
	return nil
}
