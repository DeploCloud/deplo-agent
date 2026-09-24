package server

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/hostmetrics"
)

// Capabilities this agent advertises in Hello.
var Capabilities = []string{
	"deploy.dockerfile",
	"deploy.image",
	"deploy.compose.single",
	"deploy.compose.multi",
	"deploy.static",
	"deploy.nixpacks",
	"deploy.buildpacks",
	"deploy.railpack",
	"deploy.buildenv",
	"deploy.nixpacks-runtime-pin",
	"deploy.nocache",
	"deploy.force-recreate",
	"deploy.compose-args",
	"deploy.compose.projectdir",
	"build.skip_steps",
	"build.env-not-baked",
	"logs.timerange",
	"metrics",
	"container-stats",
	"metrics-stream",
	"metrics.netns",
	"dev",
	"ssh-gateway",
	"tunnel",
	"self-update",
	"self-uninstall",
	"backup",
	"checkport",
	"http-probe",
	"cron",
	"cron.plain-shell",
	"cron.kill-tree",
	"cron.output-hardened",
	"volume-copy",
	"volume-copy-hardened",
	"host-path-copy.file",
	"volume-copy.drop-report",
	"files-copy",
	"volume-usage",
	"backup-store",
	"backup-encrypt-s3",
	"backup-s3-args",
	"backup-s3-read",
	"backup-untrusted-config",
	"docker-cleanup",
	"cleanup.keep-per-slug",
	"cleanup.leftover-files",
	"cleanup.leftover-networks.reclaims",
	"deploy.network.build-only-skips",
	"deploy.network.tenant-only",
	"deploy.network.audit2",
	"restore.network-retarget",
	"deploy.network.headroom",
	"cleanup.leftover-networks",
	"cleanup.orphan-volumes",
	"cleanup.pulled-images",
	"deploy.network",
	"cert-renewal",
	"hostops",
	"deploy.build-only",
	"build.nixpacks-vars",
	"build.railpack-output-dir",
	"image-copy",
	"deploy.registry-auth",
	"cleanup.images.by-repository",
	"volume-copy-6h",
	"copy.gzip-bestspeed",
	"copy.drain-eof",
	"teardown.preview-network",
	"teardown.missing-file-ok",
	"host-path-copy.stack-files",
	"control-plane.update",
	"control-plane.update.canary",
	"stack.stop-services",
	"deploy.context_stream",
}

// AgentVersion is the version this agent reports over Hello.
var AgentVersion = "dev"

const retainFinished = 10 * time.Minute

// Service is the gRPC Agent implementation.
type Service struct {
	pb.UnimplementedAgentServer

	stackDir      string
	buildTmpDir   string
	dataDir       string
	dataBase      string
	cacheSaltOnce sync.Once
	cacheSalt     []byte
	agentDir      string
	traefikApply  func(ctx context.Context, path string, restartOnly bool) error

	mu      sync.Mutex
	deploys map[string]*inflight
	jobs    map[string]*job

	certMgr    *CertManager
	pendingMu  sync.Mutex
	pendingKey ed25519.PrivateKey

	stackLocksMu sync.Mutex
	stackLocks   map[string]*stackLock
}

type stackLock struct {
	sync.Mutex
	refs int
}

// lockStack serializes operations on one stack; a slug's entry lives only while someone holds or waits on it.
func (s *Service) lockStack(slug string) func() {
	s.stackLocksMu.Lock()
	if s.stackLocks == nil {
		s.stackLocks = map[string]*stackLock{}
	}
	l := s.stackLocks[slug]
	if l == nil {
		l = &stackLock{}
		s.stackLocks[slug] = l
	}
	l.refs++
	s.stackLocksMu.Unlock()

	l.Lock()
	return func() {
		l.Unlock()
		s.stackLocksMu.Lock()
		if l.refs--; l.refs == 0 {
			delete(s.stackLocks, slug)
		}
		s.stackLocksMu.Unlock()
	}
}

// New builds the service.
func New(stackDir, buildTmpDir, dataDir, dataBase string) *Service {
	if dataBase == "" {
		dataBase = filepath.Dir(stackDir)
	}
	sweepDockerConfigs()
	return &Service{
		stackDir:    stackDir,
		buildTmpDir: buildTmpDir,
		dataDir:     dataDir,
		dataBase:    dataBase,
		deploys:     map[string]*inflight{},
		jobs:        map[string]*job{},
	}
}

// Hello is the health + identity handshake and the mandatory deploy pre-flight (PLAN P5).
func (s *Service) Hello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloResponse, error) {
	version, available := dockercli.Server(ctx)
	return &pb.HelloResponse{
		ContractVersion: pb.ContractVersion_CONTRACT_VERSION_V1,
		AgentVersion:    AgentVersion,
		DockerAvailable: available,
		DockerVersion:   version,
		Capabilities:    Capabilities,
		TraefikRunning:  available && dockercli.TraefikRunning(ctx),
		HostArch:        runtime.GOARCH,
	}, nil
}

// Metrics returns a host snapshot (replaces lib/infra/host.ts per server).
func (s *Service) Metrics(ctx context.Context, req *pb.MetricsRequest) (*pb.HostMetrics, error) {
	dataDir := req.GetDataDir()
	if dataDir == "" {
		dataDir = s.dataDir
	}
	m := hostmetrics.Collect(dataDir)
	return hostMetricsPB(m, dockercli.RunningContainers(ctx)), nil
}

func hostMetricsPB(m hostmetrics.Metrics, runningContainers int) *pb.HostMetrics {
	return &pb.HostMetrics{
		Cpu:               m.CPU,
		CpuCores:          int32(m.CPUCores),
		MemUsed:           m.MemUsed,
		MemTotal:          m.MemTotal,
		MemPct:            m.MemPct,
		DiskUsed:          m.DiskUsed,
		DiskTotal:         m.DiskTotal,
		DiskPct:           m.DiskPct,
		NetRx:             m.NetRx,
		NetTx:             m.NetTx,
		Load1:             m.Load1,
		Load5:             m.Load5,
		Load15:            m.Load15,
		UptimeSec:         m.UptimeSec,
		RunningContainers: int32(runningContainers),
		MemFree:           m.MemFree,
		MemCache:          m.MemCache,
	}
}

// Deploy runs a deployment and streams its events.
func (s *Service) Deploy(req *pb.DeployRequest, stream pb.Agent_DeployServer) error {
	id := req.GetDeployId()
	if id == "" {
		return status.Error(codes.InvalidArgument, "deploy_id is required")
	}

	s.mu.Lock()
	existing := s.deploys[id]
	if existing != nil {
		s.mu.Unlock()
		return existing.subscribe(stream.Context(), 0, stream.Send)
	}
	deployCtx, cancel := context.WithCancel(context.Background())
	f := newInflight(cancel)
	s.deploys[id] = f
	s.mu.Unlock()

	go s.driveDeploy(deployCtx, id, req, "", f)

	return f.subscribe(stream.Context(), 0, stream.Send)
}

// maxContextBytes caps a streamed build context on disk.
const maxContextBytes = 8 << 30

// DeployStream is Deploy with the upload's build context sent in chunks and spooled to a file,
// so a large archive never sits whole in memory.
func (s *Service) DeployStream(stream pb.Agent_DeployStreamServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	req := first.GetRequest()
	if req == nil {
		return status.Error(codes.InvalidArgument, "the first DeployStream frame must be the request")
	}
	id := req.GetDeployId()
	if id == "" {
		return status.Error(codes.InvalidArgument, "deploy_id is required")
	}
	s.mu.Lock()
	existing := s.deploys[id]
	s.mu.Unlock()
	if existing != nil {
		return existing.subscribe(stream.Context(), 0, stream.Send)
	}

	spool, err := s.spoolContext(stream)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if existing := s.deploys[id]; existing != nil {
		s.mu.Unlock()
		_ = os.Remove(spool)
		return existing.subscribe(stream.Context(), 0, stream.Send)
	}
	deployCtx, cancel := context.WithCancel(context.Background())
	f := newInflight(cancel)
	s.deploys[id] = f
	s.mu.Unlock()

	go s.driveDeploy(deployCtx, id, req, spool, f)

	return f.subscribe(stream.Context(), 0, stream.Send)
}

func (s *Service) spoolContext(stream pb.Agent_DeployStreamServer) (string, error) {
	f, err := os.CreateTemp(s.buildTmpDir, "deplo-context-*.tar")
	if err != nil {
		return "", status.Errorf(codes.Internal, "spool build context: %v", err)
	}
	fail := func(err error) (string, error) {
		f.Close()
		_ = os.Remove(f.Name())
		return "", err
	}
	var n int64
	for {
		in, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fail(err)
		}
		chunk := in.GetContextChunk()
		if n += int64(len(chunk)); n > maxContextBytes {
			return fail(status.Errorf(codes.ResourceExhausted, "build context is larger than %d bytes", int64(maxContextBytes)))
		}
		if _, err := f.Write(chunk); err != nil {
			return fail(status.Errorf(codes.Internal, "spool build context: %v", err))
		}
	}
	if err := f.Close(); err != nil {
		return fail(status.Errorf(codes.Internal, "spool build context: %v", err))
	}
	return f.Name(), nil
}

// ReattachDeploy reconnects to an in-flight or recently-finished deploy and replays events past from_seq, then follows it live to completion (D5).
func (s *Service) ReattachDeploy(req *pb.ReattachRequest, stream pb.Agent_ReattachDeployServer) error {
	id := req.GetDeployId()
	s.mu.Lock()
	f := s.deploys[id]
	s.mu.Unlock()
	if f == nil {
		return status.Errorf(codes.NotFound, "no record of deploy %q", id)
	}
	return f.subscribe(stream.Context(), req.GetFromSeq(), stream.Send)
}

func (s *Service) driveDeploy(ctx context.Context, id string, req *pb.DeployRequest, contextFile string, f *inflight) {
	defer f.cancel()
	if contextFile != "" {
		defer os.Remove(contextFile)
	}
	e := &emitter{send: func(ev *pb.DeployEvent) error {
		f.append(ev)
		return nil
	}}
	func() {
		defer func() {
			if r := recover(); r != nil {
				e.result(false, fmt.Sprintf("deploy panicked: %v", r), "")
			}
		}()
		s.runDeployFrom(ctx, req, contextFile, e)
	}()
	s.trimFinished()
	time.AfterFunc(retainFinished, func() {
		s.mu.Lock()
		if s.deploys[id] == f {
			delete(s.deploys, id)
		}
		s.mu.Unlock()
	})
}

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
		return &pb.StackResult{Ok: true}, nil
	}
	// A single-image app has no compose service to name, so its fallback is the stack's own container.
	if len(services) > 0 {
		return &pb.StackResult{Ok: false, Error: stackFailure(res, err, dockercli.Result{}, nil)}, nil
	}
	r2, err2 := dockercli.Run(ctx, 30*time.Second, "stop", "deplo-"+slug)
	if err2 == nil && r2.Code == 0 {
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
	defer s.lockStack(slug)()
	res, err := dockercli.Run(ctx, time.Minute, s.composeCtl(slug, "start")...)
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
	defer s.lockStack(slug)()
	previewNets := stackPreviewNetworks(ctx, slug)
	if !isFile(s.stackPath(slug)) {
		r2, err := removeStackContainers(ctx, slug)
		if err != nil {
			return &pb.StackResult{Ok: false, Error: err.Error()}, nil
		}
		reclaimed := s.reclaimVolumes(ctx, ref.GetReclaimVolumes())
		if r2.Code == 0 {
			removePreviewNetworks(ctx, previewNets)
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
	_ = os.Remove(s.stackPath(slug))
	_ = os.Remove(s.legacyEnvPath(slug))
	_ = os.Remove(filepath.Join(s.stackDir, "files", slug, ".env"))
	_ = os.RemoveAll(s.filesRoot(slug))
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
	defer s.lockStack(slug)()
	name := "deplo-" + slug

	if req.GetComposeYaml() == "" {
		return &pb.StackResult{Ok: false, Error: "reroute request missing rendered compose"}, nil
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
	composeArgs := composeUpArgs(name, stackFile, envFile, projectDir, false, req.GetComposeUpArgs())

	res, err := dockercli.Run(ctx, 120*time.Second, composeArgs...)
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
	contents, err := os.ReadFile(s.stackPath(ref.GetSlug()))
	if err != nil {
		return &pb.ReadStackResponse{Exists: false, Yaml: ""}, nil
	}
	return &pb.ReadStackResponse{Exists: true, Yaml: string(contents)}, nil
}

// Inspect reports a container's existence + running state for live status.
func (s *Service) Inspect(ctx context.Context, req *pb.InspectRequest) (*pb.InspectResponse, error) {
	if err := validateSlug(req.GetSlug()); err != nil {
		return nil, err
	}
	name := "deplo-" + req.GetSlug()
	exists, state := dockercli.State(ctx, name)
	return &pb.InspectResponse{
		Exists:  exists,
		Running: state == "running",
		State:   state,
	}, nil
}

// CheckPort reports whether a host TCP port is free to publish.
func (s *Service) CheckPort(ctx context.Context, req *pb.CheckPortRequest) (*pb.CheckPortResponse, error) {
	port := req.GetPort()
	if port < 1 || port > 65535 {
		return &pb.CheckPortResponse{
			Available: false,
			Reason:    fmt.Sprintf("port %d is out of range (1-65535)", port),
		}, nil
	}
	addr := net.JoinHostPort("0.0.0.0", strconv.Itoa(int(port)))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return &pb.CheckPortResponse{
			Available: false,
			Reason:    fmt.Sprintf("port %d is already in use on the host", port),
		}, nil
	}
	_ = ln.Close()
	return &pb.CheckPortResponse{Available: true}, nil
}

func (s *Service) stackPath(slug string) string {
	return filepath.Join(s.stackDir, slug+".yml")
}

func (s *Service) legacyEnvPath(slug string) string {
	return filepath.Join(s.stackDir, slug+".env")
}

func (s *Service) composeCtl(slug string, verb ...string) []string {
	args := []string{"compose", "-p", "deplo-" + slug, "-f", s.stackPath(slug)}
	if dir := s.filesRoot(slug); isFile(filepath.Join(dir, ".env")) {
		args = append(args, "--project-directory", dir, "--env-file", filepath.Join(dir, ".env"))
	} else if legacy := s.legacyEnvPath(slug); isFile(legacy) {
		args = append(args, "--env-file", legacy)
	}
	return append(args, verb...)
}

func isFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

func stackFailure(res dockercli.Result, err error, fb dockercli.Result, fbErr error) string {
	for _, msg := range []string{errText(err), strings.TrimSpace(res.Stderr), errText(fbErr), strings.TrimSpace(fb.Stderr)} {
		if msg != "" {
			return msg
		}
	}
	return "the stack could not be stopped or started and docker said nothing"
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
