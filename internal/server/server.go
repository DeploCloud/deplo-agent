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
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dnszone"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/hostinfo"
	"github.com/DeploCloud/deplo-agent/internal/hostmetrics"
	"github.com/DeploCloud/deplo-agent/internal/link"
)

// Capabilities this agent advertises in Hello.
var Capabilities = []string{
	"deploy.dockerfile",
	"deploy.image",
	"deploy.compose.single",
	"deploy.compose.multi",
	"deploy.railpack",
	"deploy.buildenv",
	"deploy.nocache",
	"deploy.force-recreate",
	"deploy.start-after-ready",
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
	"volumes.adopt",
	"host-path-copy.file",
	"volume-copy.drop-report",
	"files-copy",
	"volume-usage",
	"backup-store",
	"backup-encrypt-s3",
	"backup-s3-args",
	"backup-s3-read",
	"backup-untrusted-config",
	"backup.volume-hold",
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
	"cleanup.safe-prune",
	"deploy.network",
	"cert-renewal",
	"hostops",
	"deploy.build-only",
	"build.railpack-output-dir",
	"build.github-token",
	"build.memory-cap",
	"deploy.ready-check",
	"deploy.compose.source",
	"build.auto-method",
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
	"host.addresses",
	"git.commit",
	"git.submodules",
	"deploy.files-bind-check",
	"backup.db-owner-check",
	"backup.redis-aof",
	"image-inventory",
	"image-inventory.candidate",
	"image-inventory.deps",
	"image-inventory.direct",
	"deploy.pull-base",
	"deploy.reuse-image",
	"deploy.overlap",
	"deploy.overlap.stack",
	"deploy.replicas",
	"deploy.spread",
	"reroute.pull",
	"reroute.services",
	"address.prove",
	"agent-link",
	"screenshot",
	"health.restart",
	"cluster.host",
	"cluster",
	"cluster.images",
	"cluster.failover",
	"cluster.failover.restore",
	"cluster.forwarders",
	"backup.postgres-archive",
	"cluster.standby",
	"cluster.replicas",
	"cluster.network",
	// SetClusterRotations: a peer that rotated its key is still reached, and still known, by its old one (ADR-0050).
	"cluster.rotation",
	"cluster.forwarders.services",
	"cluster.forwarders.answers",
	"cluster.supervise",
	// The supervisor that starts a writer whose restart policy it cannot read: a Deplo renders restart "no" only past this.
	"cluster.supervise.failsafe",
	// A member that stops its copy of a writer a current lease places elsewhere, and tells a probing leader what runs.
	"cluster.relinquish",
	// A failover start pulls only an image the server lacks, and a copied registry image prunes no other tag.
	"cluster.images.registry",
	// ReadProxyCertificates, and a failover's certificates installed as files then moved into the proxy's store.
	"cluster.answers.certificates",
	// SetClusterZone: the cluster's DNS zone, answered by an unprivileged process on port 53 (ADR-0049).
	"dns.zone",
	// SetGeoTable and ClusterZoneServer's place: the name server answers a visitor with the servers nearest it.
	"dns.geo",
	// Certificate checks over DNS answered by the cluster's own zone, and the zone's own certificate (ADR-0049 phase 3).
	"dns.challenge",
	// A name's own certificate over DNS renewed by one server and held by every other as a file (#228).
	"dns.certificates",
	// ClusterZone.homes: a name whose App runs on one server is answered with that server first (ADR-0049).
	"dns.home",
	// The health check is this binary, mounted from the stack directory's .deplo-probe: no shell in the image (deplo#281).
	"deploy.probe",
	// ClaimServer: what another Deplo left on this machine is set aside, never loaded (ADR-0050).
	"panel.claim",
	// SetClusterNetwork's retire_public_key: a caught copy's network key is replaced with its identity (ADR-0050).
	"network.rekey",
	// A failover start refuses while this server holds no valid certificate a domain of the App has (ADR-0049).
	"failover.certificates",
	// A line of succession: pre-votes, one leader per epoch, and a member next in line asks the leader beside it to lead.
	"cluster.succession",
	// A returning member sets each moved volume aside under a dated name; VolumeUsage lists them, RemoveSetAside removes one.
	"volumes.set-aside",
	// ReadStack.last_ran_ms and SetAside: a moved stack's newer copy wins, and the other is set aside first.
	"volumes.newest-wins",
	// DeployResult.image_volumes: what a built image declares as volumes, read on the server that built it.
	"deploy.image-volumes",
	// A stack's Files folder is set aside with its volumes, as `deplo-<slug>.files-aside-<UTC time>`.
	"volumes.set-aside-files",
	// WriteStoreFile's sealed stream: an artifact is kept only when a trailer names exactly what landed.
	"backup-store.sealed",
	// DestroyStack.drop_set_aside: a deleted App's set-aside copies go with it.
	"teardown.set-aside",
	// ExpectClusterBackup and BackupRequest.member_fingerprint: a backup goes member to member, not through Deplo.
	"backup-store.member",
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
	linkAddresses *link.Store
	port          *link.Port

	mu      sync.Mutex
	deploys map[string]*inflight
	jobs    map[string]*job

	certMgr    *CertManager
	pendingMu  sync.Mutex
	pendingKey ed25519.PrivateKey

	stackLocksMu sync.Mutex
	stackLocks   map[string]*stackLock

	inventoryMu sync.Mutex

	clusterOnce sync.Once
	cluster     *clusterHost

	// answered is the first Hello of this run, which an update's guard waits for.
	answered sync.Once

	memberOnce sync.Once
	member     *clusterMember
	// imageOps is the docker side of cluster image copies; tests replace it.
	imageOps clusterImageOps
	// expected is the images Deplo told this member to take from another one, by ref.
	expectedMu sync.Mutex
	expected   map[string]expectedImage
	// expectedBackups is the backups Deplo told this member to keep from another, by object key.
	expectedBackups map[string]expectedBackup

	leaseOnce sync.Once
	lease     *leaseGuard

	// Name forwarders (#200); fwdOps is their docker side, which tests replace.
	fwdOnce sync.Once
	fwd     *forwarders
	fwdOps  forwardOps

	heldMu sync.Mutex
	// unpausedAtStart: this run unpaused what an earlier one held, so the proxy may lack those routes.
	unpausedAtStart bool
	held            map[string]bool
	// gate keeps a deploy and a backup's pause of the same App apart; see app_gate.go.
	gate appGate

	restartOnce sync.Once
	rw          *restartWatcher

	// The cluster's single-writer stacks, started under their lease; see cluster_supervise.go.
	supOnce sync.Once
	sup     *supervisor

	// Replication of App data; see cluster_replicas.go.
	replOnce sync.Once
	repl     *replicas

	// The cluster's private network; see cluster_mesh.go.
	netOnce sync.Once
	cnet    *clusterNetwork

	// The key rotations Deplo announced for the cluster's servers; see cluster_rotations.go.
	rotOnce sync.Once
	rot     *clusterRotations

	// The cluster's DNS zone; see cluster_zone.go. Tests replace what the network heard and how the process starts.
	zoneOnce   sync.Once
	zone       *clusterZones
	zoneHeard  func() (map[string]time.Time, time.Time)
	zoneConfig func(*dnszone.Config)
	// zoneView is what the proxy's store and routes file need of the zones' certificates; see cluster_zone_cert.go.
	zoneView atomic.Pointer[zoneCertView]
	// The certificate checks' endpoint for this server's proxy; tests replace where it listens and the proxy's log.
	chalOnce   sync.Once
	chal       *challengeEndpoint
	chalListen func() (net.Listener, uint64, error)
	proxyLogs  func(ctx context.Context, since time.Time) (string, error)
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
	sweepTempSecrets()
	sweepInventoryDirs(buildTmpDir)
	sweepInventoryDirs(filepath.Join(dataBase, "inventory"))
	unpaused := sweepBackupHolds(filepath.Join(dataBase, "backup-holds"))
	return &Service{
		unpausedAtStart: unpaused,
		stackDir:        stackDir,
		buildTmpDir:     buildTmpDir,
		dataDir:         dataDir,
		dataBase:        dataBase,
		deploys:         map[string]*inflight{},
		jobs:            map[string]*job{},
	}
}

// Hello is the health + identity handshake and the mandatory deploy pre-flight (PLAN P5).
func (s *Service) Hello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloResponse, error) {
	s.noteAnswered()
	version, available := dockercli.Server(ctx)
	return &pb.HelloResponse{
		ContractVersion: pb.ContractVersion_CONTRACT_VERSION_V1,
		AgentVersion:    AgentVersion,
		DockerAvailable: available,
		DockerVersion:   version,
		Capabilities:    Capabilities,
		TraefikRunning:  available && dockercli.TraefikRunning(ctx),
		HostArch:        runtime.GOARCH,
		PublicAddresses: hostinfo.PublicAddresses(),
		LinkAddresses:   s.savedLinkAddresses(),
		Listening:       s.port.Listening(),
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
	f.slug = req.GetSlug()
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
	f.slug = req.GetSlug()
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

// cancelDeploysOf ends every deploy in flight of those stacks; one that already finished takes no harm.
func (s *Service) cancelDeploysOf(slugs map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.deploys {
		if slugs[f.slug] {
			f.cancel()
		}
	}
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
		if !req.GetBuildOnly() && s.leaseGuard().Holds(req.GetSlug()) {
			e.result(false, awaitingLeader, "")
			return
		}
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
