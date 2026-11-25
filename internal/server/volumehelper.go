package server

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const (
	volumeHelperPullTimeout = 5 * time.Minute
	volumeHelperPullTries   = 3
)

// volumeHelperRun is a helper container's docker args. A read-only mount never makes a volume: a missing one turns the
// run into `volume inspect` of it, which fails (c39: a deleted App's volume came back empty 12 s after the delete).
func volumeHelperRun(ctx context.Context, args ...string) []string {
	if missing := missingVolumes(ctx, readVolumes(args)); len(missing) > 0 {
		return append([]string{"volume", "inspect"}, missing...)
	}
	ensureVolumeHelperImage(ctx)
	return append(append([]string{"run", "--rm", "--log-driver=none"}, helperPriority()...), args...)
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

// readVolumes is every named volume the args mount read-only.
func readVolumes(args []string) []string {
	var out []string
	for i := 0; i+1 < len(args); i++ {
		src, rest, ok := strings.Cut(args[i+1], ":")
		if args[i] == "-v" && ok && strings.HasSuffix(rest, ":ro") && src != "" && !strings.HasPrefix(src, "/") {
			out = append(out, src)
		}
	}
	return out
}

// missingVolumes is which of names docker says it does not have; none when it says nothing. A var so a test answers.
var missingVolumes = func(ctx context.Context, names []string) []string {
	if len(names) == 0 {
		return nil
	}
	res, err := dockercli.Run(ctx, 15*time.Second, append([]string{"volume", "inspect", "--format", "{{.Name}}"}, names...)...)
	if err != nil {
		return nil
	}
	var out []string
	for _, line := range strings.Split(strings.ToLower(res.Stderr), "\n") {
		if !strings.Contains(line, "no such volume") {
			continue
		}
		for _, n := range names {
			if strings.Contains(line, strings.ToLower(n)) && !slices.Contains(out, n) {
				out = append(out, n)
			}
		}
	}
	return out
}
