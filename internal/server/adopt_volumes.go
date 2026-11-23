package server

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const adoptTimeout = 30 * time.Minute

type dockerRun func(ctx context.Context, timeout time.Duration, args ...string) (dockercli.Result, error)

// adoptVolumes carries each old unnamed volume into the named one Deplo now gives its place, before
// compose up replaces the container (https://deplo.build/docs/guides/data/persistent-storage). false: the result is sent.
func (s *Service) adoptVolumes(ctx context.Context, req *pb.DeployRequest, e *emitter) bool {
	run := func(ctx context.Context, timeout time.Duration, args ...string) (dockercli.Result, error) {
		if len(args) > 0 && args[0] == "run" {
			args = volumeHelperRun(ctx, args[1:]...)
		}
		return dockercli.Run(ctx, timeout, args...)
	}
	for _, a := range req.GetAdoptVolumes() {
		carried, err := adoptVolume(ctx, run, "deplo-"+req.GetSlug(), a)
		if err != nil {
			e.result(false, err.Error(), "")
			return false
		}
		if carried {
			e.log("info", fmt.Sprintf("Kept the data of %s at %s: it is now in volume %s", a.GetService(), a.GetTarget(), a.GetVolume()))
		}
	}
	return true
}

// adoptVolume copies the unnamed volume a container of the service mounts at target into the named
// volume, which must not exist yet. The old volume is left as it was. true: something was carried.
func adoptVolume(ctx context.Context, run dockerRun, project string, a *pb.VolumeAdoption) (bool, error) {
	target := strings.TrimRight(a.GetTarget(), "/")
	if err := validateVolumeName(a.GetVolume()); err != nil {
		return false, err
	}
	if !composeServiceName.MatchString(a.GetService()) || !path.IsAbs(target) || path.Clean(target) != target {
		return false, fmt.Errorf("adopt volume: unsafe service %q or path %q", a.GetService(), a.GetTarget())
	}
	if res, err := run(ctx, volumeExistsTimeout, "volume", "inspect", a.GetVolume()); err == nil && res.Code == 0 {
		return false, nil
	}
	id, from, running, err := unnamedMount(ctx, run, project, a.GetService(), target)
	if err != nil || from == "" {
		return false, err
	}
	fail := func(what string, err error, res dockercli.Result) (bool, error) {
		_, _ = run(ctx, time.Minute, "volume", "rm", a.GetVolume())
		if running {
			_, _ = run(ctx, 2*time.Minute, "start", id)
		}
		detail := strings.TrimSpace(res.Stderr)
		if err != nil {
			detail = err.Error()
		}
		return false, fmt.Errorf("could not keep the data of %s at %s (%s: %s); nothing was changed", a.GetService(), target, what, detail)
	}
	if running {
		if res, err := run(ctx, 2*time.Minute, "stop", "-t", "30", id); err != nil || res.Code != 0 {
			return fail("stop", err, res)
		}
	}
	if res, err := run(ctx, time.Minute, "volume", "create", a.GetVolume()); err != nil || res.Code != 0 {
		return fail("create", err, res)
	}
	res, err := run(ctx, adoptTimeout, "run", "--network", "none", "-v", from+":/from:ro", "-v", a.GetVolume()+":/to",
		volumeHelperImage, "cp", "-a", "/from/.", "/to/")
	if err != nil || res.Code != 0 {
		return fail("copy", err, res)
	}
	return true, nil
}

// unnamedMount finds the newest container of the service with an unnamed volume at target.
func unnamedMount(ctx context.Context, run dockerRun, project, service, target string) (id, volume string, running bool, err error) {
	res, err := run(ctx, 30*time.Second, "ps", "-a", "--no-trunc", "--format", "{{.ID}}",
		"--filter", "label=com.docker.compose.project="+project, "--filter", "label=com.docker.compose.service="+service)
	if err != nil || res.Code != 0 {
		return "", "", false, fmt.Errorf("adopt volume: list containers: %v %s", err, strings.TrimSpace(res.Stderr))
	}
	for _, c := range uniqueLines(res.Stdout) {
		ins, err := run(ctx, 30*time.Second, "inspect", "--format",
			`{{.State.Running}}{{range .Mounts}}{{if eq .Type "volume"}} {{.Name}}={{.Destination}}{{end}}{{end}}`, c)
		if err != nil || ins.Code != 0 {
			continue
		}
		fields := strings.Fields(ins.Stdout)
		if len(fields) < 2 {
			continue
		}
		for _, m := range fields[1:] {
			name, dest, _ := strings.Cut(m, "=")
			if strings.TrimRight(dest, "/") == target && anonymousVolume.MatchString(name) {
				return c, name, fields[0] == "true", nil
			}
		}
	}
	return "", "", false, nil
}
