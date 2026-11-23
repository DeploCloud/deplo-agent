package server

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A returning member never deletes what its copy may not have sent: each volume of a moved stack is copied aside under
// a dated name first, and stays until an admin removes it from the App's page.
// https://deplo.build/docs/operations/clusters/failover#set-aside-copies

const (
	asideOfLabel = "deplo.aside.of"
	asideAtLabel = "deplo.aside.at"
)

var asideSuffix = regexp.MustCompile(`-aside-\d{8}T\d{6}Z$`)

// asideName is a volume's dated copy.
func asideName(vol string, at time.Time) string {
	return vol + "-aside-" + at.UTC().Format("20060102T150405Z")
}

// helperDocker runs docker, and a "run" as the volume helper does: removed after, no logs kept.
func helperDocker(ctx context.Context, timeout time.Duration, args ...string) (dockercli.Result, error) {
	if len(args) > 0 && args[0] == "run" {
		args = volumeHelperRun(ctx, args[1:]...)
	}
	return dockercli.Run(ctx, timeout, args...)
}

// stackVolumes is every named volume the stack's containers mount, and every one compose made for it.
func stackVolumes(ctx context.Context, run dockerRun, slug string) ([]string, error) {
	project := "label=com.docker.compose.project=deplo-" + slug
	var out []string
	add := func(v string) {
		if v != "" && !anonymousVolume.MatchString(v) && validateVolumeName(v) == nil && !slices.Contains(out, v) {
			out = append(out, v)
		}
	}
	ps, err := run(ctx, 30*time.Second, "ps", "-a", "-q", "--no-trunc", "--filter", project)
	if err != nil || ps.Code != 0 {
		return nil, fmt.Errorf("list the containers of %s: %v %s", slug, err, strings.TrimSpace(ps.Stderr))
	}
	for _, id := range uniqueLines(ps.Stdout) {
		ins, err := run(ctx, 30*time.Second, "inspect", "--format", `{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}} {{end}}{{end}}`, id)
		if err == nil && ins.Code == 0 {
			for _, v := range strings.Fields(ins.Stdout) {
				add(v)
			}
		}
	}
	if ls, err := run(ctx, 30*time.Second, "volume", "ls", "-q", "--filter", project); err == nil && ls.Code == 0 {
		for _, v := range uniqueLines(ls.Stdout) {
			add(v)
		}
	}
	slices.Sort(out)
	return out, nil
}

// setAside copies each volume to its dated copy; one already copied for that time is kept as it is.
func setAside(ctx context.Context, run dockerRun, slug string, vols []string, at time.Time) ([]lease.Aside, error) {
	var out []lease.Aside
	stamp := at.UTC().Format(time.RFC3339)
	for _, v := range vols {
		name := asideName(v, at)
		if res, err := run(ctx, volumeExistsTimeout, "volume", "inspect", name); err != nil || res.Code != 0 {
			if res, err := run(ctx, time.Minute, "volume", "create", "--label", asideOfLabel+"="+v, "--label", asideAtLabel+"="+stamp, name); err != nil || res.Code != 0 {
				return out, fmt.Errorf("could not set aside %s: %v %s", v, err, strings.TrimSpace(res.Stderr))
			}
			res, err := run(ctx, adoptTimeout, "run", "--network", "none", "-v", v+":/from:ro", "-v", name+":/to", volumeHelperImage, "cp", "-a", "/from/.", "/to/")
			if err != nil || res.Code != 0 {
				_, _ = run(ctx, time.Minute, "volume", "rm", name)
				return out, fmt.Errorf("could not set aside %s: %v %s", v, err, strings.TrimSpace(res.Stderr))
			}
		}
		out = append(out, lease.Aside{Slug: slug, Volume: name, Of: v, At: stamp, SizeBytes: volumeBytes(ctx, run, name)})
	}
	return out, nil
}

func volumeBytes(ctx context.Context, run dockerRun, name string) int64 {
	res, err := run(ctx, volumeExistsTimeout, "volume", "inspect", "--format", "{{.Mountpoint}}", name)
	if err != nil || res.Code != 0 {
		return 0
	}
	return dirSize(strings.TrimSpace(res.Stdout))
}

// asideStack stops a moved stack and copies its volumes aside, dated by its failover.
func (s *Service) asideStack(ctx context.Context, e lease.Entry, at time.Time) ([]lease.Aside, error) {
	vols, err := stackVolumes(ctx, helperDocker, e.Slug)
	if err != nil || len(vols) == 0 {
		return nil, err
	}
	if _, err := s.StopStack(stoppedBecause(ctx, "its data is set aside before its copy here goes"), &pb.StackRef{Slug: e.Slug}); err != nil {
		return nil, err
	}
	defer s.lockStack(e.Slug)()
	return setAside(ctx, helperDocker, e.Slug, vols, at)
}

// dropAside takes a stack down with the volumes it set aside: their copies stay.
func (s *Service) dropAside(ctx context.Context, e lease.Entry, asides []lease.Aside, still func() bool) error {
	defer s.lockStack(e.Slug)()
	if !still() {
		return errRunsAgain
	}
	var vols []string
	for _, a := range asides {
		vols = append(vols, a.Of)
	}
	return stackErr(s.destroyStack(ctx, &pb.StackRef{Slug: e.Slug, RemoveVolumes: true, ReclaimVolumes: vols}))
}

// asideCopies is every set-aside copy of the given volumes on this host.
func asideCopies(ctx context.Context, run dockerRun, of []string) []*pb.VolumeUsage {
	if len(of) == 0 {
		return nil
	}
	res, err := run(ctx, volumeUsageTimeout, "volume", "ls", "--filter", "label="+asideOfLabel, "--format",
		`{{.Name}}	{{.Label "`+asideOfLabel+`"}}	{{.Label "`+asideAtLabel+`"}}`)
	if err != nil || res.Code != 0 {
		return nil
	}
	var out []*pb.VolumeUsage
	for _, line := range uniqueLines(res.Stdout) {
		f := strings.Split(line, "\t")
		if len(f) != 3 || !slices.Contains(of, f[1]) || !asideSuffix.MatchString(f[0]) {
			continue
		}
		at, _ := time.Parse(time.RFC3339, f[2])
		out = append(out, &pb.VolumeUsage{VolumeName: f[0], Bytes: volumeBytes(ctx, run, f[0]), SetAsideOf: f[1], SetAsideAtMs: at.UnixMilli()})
	}
	return out
}

// RemoveSetAside removes one set-aside copy, and nothing that is not one.
func (s *Service) RemoveSetAside(ctx context.Context, req *pb.RemoveSetAsideRequest) (*pb.StackResult, error) {
	name := req.GetVolumeName()
	if validateVolumeName(name) != nil || !asideSuffix.MatchString(name) {
		return nil, status.Errorf(codes.InvalidArgument, "%q is not a set-aside copy", name)
	}
	res, err := dockercli.Run(ctx, volumeExistsTimeout, "volume", "inspect", "--format", `{{index .Labels "`+asideOfLabel+`"}}`, name)
	if err != nil || res.Code != 0 || strings.TrimSpace(res.Stdout) == "" {
		return nil, status.Errorf(codes.NotFound, "%s is not a set-aside copy on this server", name)
	}
	rm, err := dockercli.Run(ctx, time.Minute, "volume", "rm", name)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "docker is not reachable on this host: %v", err)
	}
	if rm.Code != 0 {
		return &pb.StackResult{Ok: false, Error: strings.TrimSpace(rm.Stderr)}, nil
	}
	return &pb.StackResult{Ok: true}, nil
}

// SetAside copies each named volume of a stack aside under a dated name, skipping one that does not exist.
func (s *Service) SetAside(ctx context.Context, req *pb.SetAsideRequest) (*pb.SetAsideResponse, error) {
	if err := validateSlug(req.GetSlug()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	var vols []string
	for _, v := range req.GetVolumeNames() {
		if validateVolumeName(v) != nil || asideSuffix.MatchString(v) {
			return nil, status.Errorf(codes.InvalidArgument, "%q cannot be set aside", v)
		}
		if res, err := setAsideRun(ctx, volumeExistsTimeout, "volume", "inspect", v); err == nil && res.Code == 0 {
			vols = append(vols, v)
		}
	}
	defer s.lockStack(req.GetSlug())()
	copies, err := setAside(ctx, setAsideRun, req.GetSlug(), vols, time.Now())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	out := &pb.SetAsideResponse{}
	for _, a := range copies {
		at, _ := time.Parse(time.RFC3339, a.At)
		out.Copies = append(out.Copies, &pb.VolumeUsage{VolumeName: a.Volume, Bytes: a.SizeBytes, SetAsideOf: a.Of, SetAsideAtMs: at.UnixMilli()})
	}
	return out, nil
}

// setAsideRun is a var so a test answers without Docker.
var setAsideRun dockerRun = helperDocker

// unionAside adds what a pass set aside to what earlier passes did; a copy named twice is kept once.
func unionAside(have, more []lease.Aside) []lease.Aside {
	for _, a := range more {
		if !slices.ContainsFunc(have, func(b lease.Aside) bool { return b.Volume == a.Volume }) {
			have = append(have, a)
		}
	}
	return have
}
