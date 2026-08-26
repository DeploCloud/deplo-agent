package server

import (
	"context"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// How long one pull of the helper image may take, and how many tries it gets.
const (
	volumeHelperPullTimeout = 5 * time.Minute
	volumeHelperPullTries   = 3
)

// volumeHelperRun builds the argv for a throwaway volume-helper container.
//
// --log-driver=none is load-bearing: the archive goes to the container's stdout,
// and json-file would write a second, uncompressed copy of it to the host disk.
func volumeHelperRun(ctx context.Context, args ...string) []string {
	ensureVolumeHelperImage(ctx)
	return append([]string{"run", "--rm", "--log-driver=none"}, args...)
}

// ensureVolumeHelperImage puts the helper image on this host before the copy.
// `docker run` would otherwise pull it mid-copy, and a registry hiccup then ends
// the copy with exit 127 and nothing retries it. Best effort: a pull that will
// not work leaves the run to fail with its own message.
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
