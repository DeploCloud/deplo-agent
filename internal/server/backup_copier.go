package server

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// The proxy drops a paused App's routes whenever it reads containers, and reads them when one starts or stops.
// So a backup's copier starts before the pause and stops after the App runs again: nothing starts in between.
const copierLabel = "deplo.copier=1"

// copierSettle is how long the App runs on after the copier starts: the proxy's read of that start must end first.
var copierSettle = 3 * time.Second

type volumeCopier struct {
	id   string
	dirs map[string]string
}

// startCopier runs one helper that mounts every volume read-only and waits; nil when it cannot, and the copy
// then runs a helper per volume as before.
func startCopier(ctx context.Context, vols []string) *volumeCopier {
	c := &volumeCopier{dirs: map[string]string{}}
	args := []string{"-d", "--network", "none", "--label", copierLabel}
	for i, v := range vols {
		if v == "" || validateVolumeName(v) != nil || c.dirs[v] != "" {
			continue
		}
		c.dirs[v] = fmt.Sprintf("/v/%d", i)
		args = append(args, "-v", v+":"+c.dirs[v]+":ro")
	}
	if len(c.dirs) == 0 {
		return nil
	}
	args = append(args, volumeHelperImage, "sleep", strconv.Itoa(int(backupStepTimeout.Seconds())))
	res, err := dockercli.Run(ctx, holdStepTimeout, volumeHelperRun(ctx, args...)...)
	if err != nil || res.Code != 0 {
		return nil
	}
	if c.id = strings.TrimSpace(res.Stdout); c.id == "" {
		return nil
	}
	return c
}

// tarArgs is the command that writes vol as a tar stream to stdout.
func (c *volumeCopier) tarArgs(ctx context.Context, vol string) []string {
	if c != nil && c.dirs[vol] != "" {
		return []string{"exec", c.id, "tar", "-C", c.dirs[vol], "-cf", "-", "."}
	}
	return volumeHelperRun(ctx, "-v", vol+":/v:ro", volumeHelperImage, "tar", "-C", "/v", "-cf", "-", ".")
}

func (c *volumeCopier) remove() {
	if c == nil || c.id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), holdStepTimeout)
	defer cancel()
	_, _ = dockercli.Run(ctx, holdStepTimeout, "rm", "-f", c.id)
	c.id = ""
}

// removeCopiers removes what an earlier run of this agent left; its stop makes the proxy read containers again.
func removeCopiers() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res, err := dockercli.Run(ctx, holdStepTimeout, "ps", "-aq", "--filter", "label="+copierLabel)
	if err != nil {
		return
	}
	for _, id := range strings.Fields(res.Stdout) {
		_, _ = dockercli.Run(ctx, holdStepTimeout, "rm", "-f", id)
	}
}
