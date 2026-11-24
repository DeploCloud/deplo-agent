package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

// filesAsideOf is what a stack's Files folder is set aside as: a volume name no compose project or Deplo Volume takes.
func filesAsideOf(slug string) string {
	return "deplo-" + slug + ".files"
}

// setAside copies each volume, and the stack's Files folder when it holds anything, to its dated copy; one already
// copied for that time is kept as it is. move renames what they hold into the copy instead, for a stack that goes:
// at once, where a copy of 2 GB on a disk the cluster shares costs every server on it its lease (eu, 24 Nov 2026).
func setAside(ctx context.Context, run dockerRun, slug string, vols []string, files string, at time.Time, move ...bool) ([]lease.Aside, error) {
	var out []lease.Aside
	stamp := at.UTC().Format(time.RFC3339)
	copyAside := func(from, of string) error {
		name := asideName(of, at)
		if res, err := run(ctx, volumeExistsTimeout, "volume", "inspect", name); err != nil || res.Code != 0 {
			if res, err := run(ctx, time.Minute, "volume", "create", "--label", asideOfLabel+"="+of, "--label", asideAtLabel+"="+stamp, name); err != nil || res.Code != 0 {
				return fmt.Errorf("could not set aside %s: %v %s", of, err, strings.TrimSpace(res.Stderr))
			}
			if len(move) > 0 && move[0] && moveInto(ctx, run, from, name) {
				out = append(out, lease.Aside{Slug: slug, Volume: name, Of: of, At: stamp, SizeBytes: volumeBytes(ctx, run, name)})
				return nil
			}
			// What a move left, or all of it: copied, which merges with what was moved.
			res, err := run(ctx, adoptTimeout, "run", "--network", "none", "-v", from+":/from:ro", "-v", name+":/to", volumeHelperImage, "cp", "-a", "/from/.", "/to/")
			if err != nil || res.Code != 0 {
				_, _ = run(ctx, time.Minute, "volume", "rm", name)
				return fmt.Errorf("could not set aside %s: %v %s", of, err, strings.TrimSpace(res.Stderr))
			}
		}
		out = append(out, lease.Aside{Slug: slug, Volume: name, Of: of, At: stamp, SizeBytes: volumeBytes(ctx, run, name)})
		return nil
	}
	for _, v := range vols {
		if err := copyAside(v, v); err != nil {
			return out, err
		}
	}
	if entries, err := os.ReadDir(files); files != "" && err == nil && len(entries) > 0 {
		if err := copyAside(files, filesAsideOf(slug)); err != nil {
			return out, err
		}
	}
	return out, nil
}

// moveInto renames everything from holds, a volume or a folder, into the volume to: true when all of it moved.
func moveInto(ctx context.Context, run dockerRun, from, to string) bool {
	src := from
	if !filepath.IsAbs(from) {
		src = volumePath(ctx, run, from)
	}
	dst := volumePath(ctx, run, to)
	entries, err := os.ReadDir(src)
	if src == "" || dst == "" || err != nil {
		return false
	}
	for _, e := range entries {
		if os.Rename(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())) != nil {
			return false
		}
	}
	return true
}

func volumePath(ctx context.Context, run dockerRun, name string) string {
	res, err := run(ctx, volumeExistsTimeout, "volume", "inspect", "--format", "{{.Mountpoint}}", name)
	if err != nil || res.Code != 0 {
		return ""
	}
	return strings.TrimSpace(res.Stdout)
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
	if err != nil {
		return nil, err
	}
	if entries, _ := os.ReadDir(s.filesRoot(e.Slug)); len(vols) == 0 && len(entries) == 0 {
		return nil, nil
	}
	if _, err := s.StopStack(stoppedBecause(ctx, "its data is set aside before its copy here goes"), &pb.StackRef{Slug: e.Slug}); err != nil {
		return nil, err
	}
	defer s.lockStack(e.Slug)()
	t0 := time.Now()
	out, err := setAside(ctx, helperDocker, e.Slug, vols, s.filesRoot(e.Slug), at, true)
	var n int64
	for _, a := range out {
		n += a.SizeBytes
	}
	fmt.Fprintf(os.Stderr, "deplo-agent: cluster: set aside %d bytes of %s here in %v\n", n, e.Slug, time.Since(t0).Round(time.Millisecond))
	return out, err
}

// dropAside takes a stack down with the volumes it set aside: their copies stay.
func (s *Service) dropAside(ctx context.Context, e lease.Entry, asides []lease.Aside, still func() bool) error {
	defer s.lockStack(e.Slug)()
	if !still() {
		return errRunsAgain
	}
	var vols []string
	for _, a := range asides {
		// The Files folder goes with the stack's files, not as a volume.
		if a.Of != filesAsideOf(e.Slug) {
			vols = append(vols, a.Of)
		}
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

// dropSetAside removes every set-aside copy of the given volumes and of the stack's Files: the App is deleted. It
// says what stayed.
func dropSetAside(ctx context.Context, run dockerRun, slug string, vols []string) string {
	var left []string
	for _, c := range asideCopies(ctx, run, append(slices.Clone(vols), filesAsideOf(slug))) {
		if res, err := run(ctx, time.Minute, "volume", "rm", c.GetVolumeName()); err != nil || res.Code != 0 {
			left = append(left, c.GetVolumeName())
		}
	}
	if len(left) == 0 {
		return ""
	}
	return "could not remove the set-aside " + strings.Join(left, ", ")
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

// SetAside copies each named volume of a stack aside under a dated name, skipping one that does not exist, and its
// Files folder with them.
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
	copies, err := setAside(ctx, setAsideRun, req.GetSlug(), vols, s.filesRoot(req.GetSlug()), time.Now())
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

// createAsideTarget makes the dated copy another server's copy of `of` is written into: a new volume, labelled as
// one this server set aside, so the App's page lists it and RemoveSetAside takes it.
func createAsideTarget(ctx context.Context, run dockerRun, name, of string) error {
	stamp := strings.TrimPrefix(name, of+"-aside-")
	at, err := time.Parse("20060102T150405Z", stamp)
	if validateVolumeName(of) != nil || asideSuffix.MatchString(of) || stamp == name || err != nil {
		return fmt.Errorf("%q is not a set-aside copy of %q", name, of)
	}
	if res, err := run(ctx, volumeExistsTimeout, "volume", "inspect", name); err == nil && res.Code == 0 {
		return fmt.Errorf("%s exists already", name)
	}
	res, err := run(ctx, time.Minute, "volume", "create", "--label", asideOfLabel+"="+of, "--label", asideAtLabel+"="+at.UTC().Format(time.RFC3339), name)
	if err != nil || res.Code != 0 {
		return fmt.Errorf("could not create %s: %v %s", name, err, strings.TrimSpace(res.Stderr))
	}
	return nil
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
