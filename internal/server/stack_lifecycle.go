package server

// A stack's life on this server: stopped, started, destroyed, rerouted and read back.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// StopStack stops a compose-managed stack, or just the named services (falls back to
// the bare container only when stopping the whole stack).
func (s *Service) StopStack(ctx context.Context, ref *pb.StackRef) (*pb.StackResult, error) {
	slug := ref.GetSlug()
	if err := validateSlug(slug); err != nil {
		return nil, err
	}
	services := ref.GetServices()
	for _, name := range services {
		if !servicePattern.MatchString(name) {
			return nil, status.Errorf(codes.InvalidArgument, "invalid service %q", name)
		}
	}
	defer s.lockStack(slug)()
	res, err := dockercli.Run(ctx, time.Minute, s.composeCtl(slug, append([]string{"stop"}, services...)...)...)
	if err == nil && res.Code == 0 {
		s.writerStopped(ctx, slug, services)
		return &pb.StackResult{Ok: true}, nil
	}
	// A single-image app has no compose service to name, so its fallback is the stack's own container.
	if len(services) > 0 {
		return &pb.StackResult{Ok: false, Error: stackFailure(res, err, dockercli.Result{}, nil)}, nil
	}
	r2, err2 := dockercli.Run(ctx, 30*time.Second, "stop", "deplo-"+slug)
	if err2 == nil && r2.Code == 0 {
		s.writerStopped(ctx, slug, nil)
		return &pb.StackResult{Ok: true}, nil
	}
	return &pb.StackResult{Ok: false, Error: stackFailure(res, err, r2, err2)}, nil
}

// StartStack starts a previously stopped stack.
func (s *Service) StartStack(ctx context.Context, ref *pb.StackRef) (*pb.StackResult, error) {
	slug := ref.GetSlug()
	if err := validateSlug(slug); err != nil {
		return nil, err
	}
	done, ok := s.lockToStart(slug)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, awaitingLeader)
	}
	res, err := s.startLocked(ctx, slug)
	if done() {
		return nil, status.Error(codes.FailedPrecondition, awaitingLeader)
	}
	if err == nil && res.GetOk() {
		s.supervision().cleared(slug)
	}
	return res, err
}

// startLocked is StartStack for a caller that holds the stack's lock.
func (s *Service) startLocked(ctx context.Context, slug string) (*pb.StackResult, error) {
	s.restartWatch().resume(slug, "restart")
	if err := s.stripSourceLinks(slug); err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}
	var res dockercli.Result
	var err error
	stack, _ := os.ReadFile(s.stackPath(slug))
	if late := lateServices(string(stack), nil); len(late) > 0 {
		res, err = s.startHolding(ctx, slug, late)
	} else {
		res, err = dockercli.Run(ctx, time.Minute, s.composeCtl(slug, "start")...)
	}
	if err == nil && res.Code == 0 {
		return &pb.StackResult{Ok: true}, nil
	}
	r2, err2 := dockercli.Run(ctx, 30*time.Second, "start", "deplo-"+slug)
	if err2 == nil && r2.Code == 0 {
		return &pb.StackResult{Ok: true}, nil
	}
	return &pb.StackResult{Ok: false, Error: stackFailure(res, err, r2, err2)}, nil
}

// DestroyStack stops and removes a stack (compose down, falling back to rm -f).
func (s *Service) DestroyStack(ctx context.Context, ref *pb.StackRef) (*pb.StackResult, error) {
	slug := ref.GetSlug()
	if err := validateSlug(slug); err != nil {
		return nil, err
	}
	if ref.GetRemoveVolumes() || ref.GetDropSetAside() || len(ref.GetReclaimVolumes()) > 0 {
		s.endLeftSave(slug)
	}
	defer s.lockStack(slug)()
	res, err := s.destroyStack(ctx, ref)
	if err == nil && ref.GetDropSetAside() {
		if left := dropSetAside(ctx, setAsideRun, slug, ref.GetReclaimVolumes()); left != "" {
			res = &pb.StackResult{Ok: false, Error: strings.TrimPrefix(res.GetError()+"; "+left, "; ")}
		}
	}
	if err == nil && res.GetOk() {
		s.supervision().forget(slug)
	}
	return res, err
}

// destroyStack is DestroyStack for a caller that already holds the stack's lock.
func (s *Service) destroyStack(ctx context.Context, ref *pb.StackRef) (*pb.StackResult, error) {
	slug := ref.GetSlug()
	previewNets := stackPreviewNetworks(ctx, slug)
	if !isFile(s.stackPath(slug)) {
		r2, err := removeStackContainers(ctx, slug)
		if err != nil {
			return &pb.StackResult{Ok: false, Error: err.Error()}, nil
		}
		reclaimed := s.reclaimVolumes(ctx, ref.GetReclaimVolumes())
		if r2.Code == 0 {
			removePreviewNetworks(ctx, previewNets)
			s.removeWorkingCopy(slug)
			return &pb.StackResult{Ok: true, Error: reclaimed}, nil
		}
		return &pb.StackResult{Ok: false, Error: strings.TrimSpace(r2.Stderr)}, nil
	}
	downArgs := s.composeCtl(slug, "down", "--remove-orphans")
	if ref.GetRemoveVolumes() {
		downArgs = append(downArgs, "-v")
	}
	res, err := dockercli.Run(ctx, 90*time.Second, downArgs...)
	reclaimed := s.reclaimVolumes(ctx, ref.GetReclaimVolumes())
	if err == nil && res.Code == 0 {
		s.removeStackFiles(slug)
		removePreviewNetworks(ctx, previewNets)
		return &pb.StackResult{Ok: true, Error: reclaimed}, nil
	}
	r2, err := removeStackContainers(ctx, slug)
	if err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}
	if r2.Code == 0 {
		removePreviewNetworks(ctx, previewNets)
	}
	if ref.GetRemoveVolumes() {
		msg := r2.Stderr
		if msg == "" {
			msg = "down -v failed; container force-removed but the named volume was not reclaimed (stack file kept for retry)"
		}
		if reclaimed != "" {
			msg += "; " + reclaimed
		}
		return &pb.StackResult{Ok: false, Error: msg}, nil
	}
	return &pb.StackResult{Ok: r2.Code == 0, Error: r2.Stderr}, nil
}

func removeStackContainers(ctx context.Context, slug string) (dockercli.Result, error) {
	args := []string{"rm", "-f", "deplo-" + slug}
	ls, err := dockercli.Run(ctx, 30*time.Second, "ps", "-aq",
		"--filter", "label=com.docker.compose.project=deplo-"+slug)
	if err != nil {
		return ls, err
	}
	if ls.Code == 0 {
		args = append(args, splitLines(ls.Stdout)...)
	}
	return dockercli.Run(ctx, 60*time.Second, args...)
}

func (s *Service) removeStackFiles(slug string) {
	s.restartWatch().forget(slug)
	_ = os.Remove(s.stackPath(slug))
	_ = os.Remove(s.legacyEnvPath(slug))
	_ = os.Remove(filepath.Join(s.stackDir, "files", slug, ".env"))
	_ = os.RemoveAll(s.filesRoot(slug))
	s.removeWorkingCopy(slug)
}

func (s *Service) reclaimVolumes(ctx context.Context, names []string) string {
	var failed []string
	for _, name := range names {
		if !strings.HasPrefix(name, "deplo-") || strings.ContainsAny(name, " \t/") {
			continue
		}
		vctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		res, err := dockercli.Run(vctx, 20*time.Second, "volume", "rm", "-f", name)
		cancel()
		if err != nil {
			failed = append(failed, name+": "+err.Error())
			continue
		}
		if res.Code != 0 {
			failed = append(failed, name+": "+strings.TrimSpace(res.Stderr))
		}
	}
	if len(failed) == 0 {
		return ""
	}
	return "could not reclaim " + strings.Join(failed, "; ")
}

// Reroute re-renders a running stack in place: the control plane changed the stack's domain/label set (or rotated env) and ships the freshly rendered compose, env and mount files so the agent rewrites them and runs `up -d` to pick up the new config WITHOUT a rebuild.
func (s *Service) Reroute(ctx context.Context, req *pb.RerouteRequest) (*pb.StackResult, error) {
	slug := req.GetSlug()
	if err := validateSlug(slug); err != nil {
		return nil, err
	}
	leave, _, gerr := s.gate.enter(ctx, slug, false, appGateWait, nil)
	if gerr != nil {
		return &pb.StackResult{Ok: false, Error: fmt.Sprintf("this app was still paused by a backup after %s: %v", spoken(appGateWait), gerr)}, nil
	}
	defer leave()
	done, ok := s.lockToStart(slug)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, awaitingLeader)
	}
	res, err := s.rerouteLocked(ctx, req)
	if done() {
		return nil, status.Error(codes.FailedPrecondition, awaitingLeader)
	}
	s.writerRedeployed(ctx, slug)
	return res, err
}

// composeServiceName is what compose accepts as a service's name.
var composeServiceName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)

// rerouteLocked is Reroute for a caller that holds the stack's lock.
func (s *Service) rerouteLocked(ctx context.Context, req *pb.RerouteRequest) (*pb.StackResult, error) {
	slug := req.GetSlug()
	name := "deplo-" + slug

	if req.GetComposeYaml() == "" {
		return &pb.StackResult{Ok: false, Error: "reroute request missing rendered compose"}, nil
	}
	if err := s.checkFilesBinds(slug, req.GetFilesBindSources()); err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}

	if err := os.MkdirAll(s.stackDir, 0o755); err != nil {
		return &pb.StackResult{Ok: false, Error: "create stack dir: " + err.Error()}, nil
	}
	if err := ensureTenantNetwork(ctx, req.GetNetwork()); err != nil {
		return &pb.StackResult{Ok: false, Error: "ensure network: " + err.Error()}, nil
	}

	stackFile := s.stackPath(slug)
	if err := os.Chmod(stackFile, 0o600); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.WriteFile(stackFile, []byte(req.GetComposeYaml()), 0o600); err != nil {
		return &pb.StackResult{Ok: false, Error: "write stack file: " + err.Error()}, nil
	}

	if len(req.GetMounts()) > 0 {
		discard := &emitter{send: func(*pb.DeployEvent) error { return nil }}
		if err := s.writeMountFiles(slug, req.GetMounts(), discard); err != nil {
			return &pb.StackResult{Ok: false, Error: "write mount files: " + err.Error()}, nil
		}
	}

	envFile := ""
	projectDir := ""
	if len(req.GetEnv()) > 0 {
		var err error
		if envFile, projectDir, err = s.writeComposeEnv(slug, req.GetEnv()); err != nil {
			return &pb.StackResult{Ok: false, Error: "write env file: " + err.Error()}, nil
		}
	}
	if err := admitStackImages(ctx, composeBaseArgs(name, stackFile, envFile, projectDir), req.GetPullImages()); err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}
	if err := s.stripSourceLinks(slug); err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}
	// Its visitors here wait for the new containers rather than meet the old ones going (c40).
	defer s.forwarders().recreating(s.leaseGuard().appOf(slug))()
	if only := req.GetServices(); len(only) > 0 {
		for _, svc := range only {
			if !composeServiceName.MatchString(svc) {
				return &pb.StackResult{Ok: false, Error: fmt.Sprintf("%q is not a compose service", svc)}, nil
			}
		}
		args := append(composeBaseArgs(name, stackFile, envFile, projectDir), "up", "-d", "--no-deps")
		args = append(append(args, sanitizeComposeArgs(req.GetComposeUpArgs())...), only...)
		res, err := dockercli.Run(ctx, 120*time.Second, args...)
		if err != nil {
			return &pb.StackResult{Ok: false, Error: err.Error()}, nil
		}
		return &pb.StackResult{Ok: res.Code == 0, Error: res.Stderr}, nil
	}
	held := heldBack(ctx, name, lateServices(req.GetComposeYaml(), nil))
	composeArgs := withoutLate(composeUpArgs(name, stackFile, envFile, projectDir, false, req.GetComposeUpArgs()), held)

	res, err := dockercli.Run(ctx, 120*time.Second, composeArgs...)
	if err == nil && res.Code == 0 && len(held) > 0 {
		// ponytail: a reroute carries no ready check, so "ready" here is the rest of the stack being up.
		res, err = dockercli.Run(ctx, 120*time.Second, lateUp(composeBaseArgs(name, stackFile, envFile, projectDir), held)...)
	}
	if err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}
	return &pb.StackResult{Ok: res.Code == 0, Error: res.Stderr}, nil
}

// ReadStack returns the rendered stack YAML on disk for a slug so the control plane can preview/diff it before a reroute.
func (s *Service) ReadStack(ctx context.Context, ref *pb.StackRef) (*pb.ReadStackResponse, error) {
	if err := validateSlug(ref.GetSlug()); err != nil {
		return &pb.ReadStackResponse{Exists: false, Yaml: ""}, nil
	}
	out := &pb.ReadStackResponse{}
	if contents, err := os.ReadFile(s.stackPath(ref.GetSlug())); err == nil {
		out.Exists, out.Yaml = true, string(contents)
	}
	// Which copy of a moved stack's data is newer is the one whose containers ran last.
	if cs, err := listStackContainers(ctx, ref.GetSlug()); err == nil {
		out.LastRanMs, out.Running = lastRan(cs, time.Now())
	}
	return out, nil
}

// listStackContainers is a var so a test answers without Docker.
var listStackContainers = listWriterContainers

// lastRan is when a container of the stack last ran: now while one runs, else the latest exit; 0 with none.
func lastRan(cs []writerContainer, now time.Time) (int64, bool) {
	var at time.Time
	for _, c := range cs {
		if c.State == "running" || c.State == "restarting" || c.State == "paused" {
			return now.UnixMilli(), true
		}
		if c.FinishedAt.After(at) {
			at = c.FinishedAt
		}
	}
	if at.IsZero() {
		return 0, false
	}
	return at.UnixMilli(), false
}
