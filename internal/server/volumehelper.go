package server

import (
	"context"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const (
	volumeHelperPullTimeout = 5 * time.Minute
	volumeHelperPullTries   = 3
)

func volumeHelperRun(ctx context.Context, args ...string) []string {
	ensureVolumeHelperImage(ctx)
	return append([]string{"run", "--rm", "--log-driver=none"}, args...)
}

func ensureVolumeHelperImage(ctx context.Context) {
	if res, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", volumeHelperImage); err == nil && res.Code == 0 {
		return
	}
	for i := 0; i < volumeHelperPullTries; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(i) * 2 * time.Second):
			}
		}
		if res, err := dockercli.Run(ctx, volumeHelperPullTimeout, "pull", volumeHelperImage); err == nil && res.Code == 0 {
			return
		}
	}
}
