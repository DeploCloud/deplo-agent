// Package server implements the Agent gRPC service - the server side of the second
// system boundary (ADR-0006).
package server

import (
	"context"
	"crypto/ed25519"
	"fmt"
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

// Capabilities this agent advertises in Hello. The control plane routes only
// what the agent supports here through the agent path, keeping a local fallback
// for everything else (Part A: the Dockerfile build + single-image compose-up).
var Capabilities = []string{
	"deploy.dockerfile",     // builds the Dockerfile method
	"deploy.image",          // runs a prebuilt image as-is
	"deploy.compose.single", // single-image compose-up
	"deploy.compose.multi",  // multi-service compose stack (env-file + label-wait)
	// The heavy builders, ported from builders.ts (build_methods.go).
	"deploy.static",     // nginx static site
	"deploy.nixpacks",   // nixpacks (binary lazily installed on first use)
	"deploy.buildpacks", // Cloud Native Buildpacks (heroku + paketo) via pack
	"deploy.railpack",   // railpack via buildkitd/buildctl
	"deploy.buildenv",   // req.env reaches BUILDS too (build args / plan secrets), not just the runtime stack
	// A pinned runtime version reaches nixpacks as a file in the build dir, not only
	// as NIXPACKS_NODE_VERSION - which picks the nodejs package but not the nixpkgs
	// archive, so every unpinned Node app failed on "undefined variable 'nodejs_24'".
	"deploy.nixpacks-runtime-pin",
	// The two "give me a genuinely fresh one" switches on DeployRequest.
	"deploy.nocache",
	"deploy.force-recreate",
	// The app's own extra `docker compose up` flags ride DeployRequest / RerouteRequest
	// and are appended to the bring-up the agent assembles.
	"deploy.compose-args",
	// A compose stack is brought up with `--project-directory` pointing at its OWN
	// directory, and its env-file is `.env` inside it. Gated because a stack imported from
	// a platform that writes its own `.env` simply will not start on an agent without it.
	"deploy.compose.projectdir",
	// BuildSpec.skip_install / skip_build are honoured: a step is emptied rather
	// than detected. Without this the control plane must not offer the choice -
	// the command strings alone cannot carry it, and detecting is the safe read.
	"build.skip_steps",
	// A build var is declared ARG-only in a generated Dockerfile: the arg already
	// reaches every RUN, and the paired ENV persisted the value in the image config
	// where `docker inspect` handed it back in plaintext.
	"build.env-not-baked",
	// FollowLogsRequest carries a time window and a timestamp prefix
	// (`--since`/`--until`/`--timestamps`).
	"logs.timerange",
	"metrics",
	"container-stats", // per-container `docker stats` snapshot (ContainerStats) - the per-app/per-database Monitoring tab
	// ONE long-lived host+container telemetry stream (StreamMetrics), sampled on the
	// agent's own ticker.
	"metrics-stream",
	// ContainerStat carries net_ns_id / net_ns_host, so the control plane can count a
	// shared network namespace once and recognise `network_mode: host`. Also marks the
	// build whose host counters skip bridges and veths - the rollout's check.
	"metrics.netns",
	"dev",         // dev container lifecycle (StartDev/StopDev/Reset/Teardown) - Part D
	"ssh-gateway", // the per-host SSH gateway singleton (Ensure/Provision/Deprovision)
	"tunnel",      // the VS Code remote tunnel (Start/Get/Stop)
	"self-update", // in-place agent binary update over mTLS (SelfUpdate), certs kept
	// The agent removes ITSELF from the host (SelfUninstall): unit, binary, state dir.
	// Docker is never touched; uninstall-agent.sh stays the answer for a host that is
	// unreachable or already de-trusted.
	"self-uninstall",
	"backup",    // dump/restore a DB or project to/from S3 (Backup/Restore/S3Check/S3Delete)
	"checkport", // host TCP port availability probe (CheckPort) - gates DB "expose publicly"
	// One bounded HTTP GET to a container of an app's own stack (ProbeHttp).
	"http-probe",
	// Scheduled `docker exec` the agent owns for its whole lifetime
	// (StartJob/PollJob/KillJob) - the Cron jobs feature.
	"cron",
	"volume-copy", // cross-host named-volume copy for a server move (ExportVolume/ImportVolume)
	// The import half of a copy is hardened: the incoming tar is sanitised (setuid
	// bits, device nodes and escaping links dropped), a host path is symlink-resolved
	// before the deny-list judges it, and the result always carries its sha256.
	"volume-copy-hardened",
	// ExportHostPath/ImportHostPath carry a single FILE, not only a directory: a
	// stack that binds `/srv/site/nginx.conf` moves the file instead of failing the
	// copy and coming back up on an empty directory of that name.
	"host-path-copy.file",
	// ImportVolume / ImportHostPath report what the sanitising pump dropped
	// (StackResult.dropped_*). Absent means the agent does not report it, never that
	// nothing was dropped.
	"volume-copy.drop-report",
	"files-copy", // cross-host files-dir copy for a service move (ExportFiles/ImportFiles)
	// On-disk size of a named volume (VolumeUsage) - what a database's Data card
	// reports, measured rather than stored.
	"volume-usage",
	// Backup artifacts held on THIS host's filesystem instead of an S3 bucket: the
	// StoreTarget arms of Backup/Restore/S3Check/S3Delete, plus the relay primitives
	// ReadStoreFile / WriteStoreFile / RestoreFrom.
	"backup-store",
	// Two things that changed about a BUCKET artifact, shipped as one flag because they
	// land together and a control plane that finds it absent must fall back on both at
	// once: - `age_recipient` is honoured for an S3 destination, so a bucket artifact is
	// encrypted like a store one.
	"backup-encrypt-s3",
	// `S3Target.extra_args` is read: a destination's advanced quirk flags
	// (`--s3-sign-accept-encoding=false`, `--s3-force-path-style=true`, …) reach the minio
	// client instead of being ignored.
	"backup-s3-args",
	// ReadStoreFile accepts an S3Target, so a BUCKET artifact can be streamed out
	// decrypted the same way a store one already is.
	"backup-s3-read",
	// RestoreFrom honours `untrusted_config`: an artifact that came from outside the fleet
	// contributes DATA only, never the compose/env/mounts the stack comes back up with.
	"backup-untrusted-config",
	// Allow-listed Docker disk reclaim (DockerCleanup): build cache, dangling
	// images, orphaned buildkit volumes, unused app images. NEVER a bare prune.
	"docker-cleanup",
	// DockerCleanupRequest.keep_per_slug is read: app-image retention is per APP instead
	// of one number for the whole host, which is what lets each app carry its own rollback
	// depth.
	"cleanup.keep-per-slug",
	// CLEANUP_SCOPE_LEFTOVER_APP_FILES is implemented: the files/<slug> directories of
	// deleted stacks are reclaimed, judged against the live-slug list the request carries.
	"cleanup.leftover-files",
	// CLEANUP_SCOPE_LEFTOVER_NETWORKS is implemented AND actually reclaims: the proxy
	// is not counted as a live attachment and is disconnected before the removal, so an
	// emptied Environment's network is a candidate instead of being pinned forever.
	// Its own string because the first cut of the scope advertised the name and
	// reclaimed nothing - and a moved tag makes the version no proof of which is running.
	"cleanup.leftover-networks.reclaims",
	// A BUILD-ONLY deploy no longer creates the app's network on a machine that
	// runs nothing of it, and an HTTP probe reads the address the app is ROUTED on
	// rather than whichever network name sorted first. Its own string because the
	// tag is moved rather than bumped, so the version proves nothing about which
	// binary a host is running.
	"deploy.network.build-only-skips",
	// The network a Deploy/Reroute names is REFUSED unless it is a tenant's
	// (`deplo-env-` / `deplo-team-` / `deplo-preview-`), so a control-plane bug that
	// sent the platform's own name cannot put a stack beside the panel. Its own
	// string for the usual reason: the tag moves, so the version proves nothing.
	"deploy.network.tenant-only",
	// Nothing new in the agent for the control plane's second network audit - the
	// fixes were all control-plane side. Its own string so a fleet rolled onto THAT
	// release can be told apart from one still on the previous binary.
	"deploy.network.audit2",
	// A restore re-points the archived stack file at the network the request names,
	// instead of bringing it up on the one the app had on the day of the backup -
	// which by then may have been reclaimed, leaving the data restored and the
	// stack down.
	"restore.network-retarget",
	// Two things this binary does that the previous one did not: it warns while
	// there is still address space left instead of failing a deploy on an exhausted
	// pool, and it puts Traefik back on EVERY tenant network at each deploy, healing
	// a proxy recreated outside the config-apply path.
	"deploy.network.headroom",
	"cleanup.leftover-networks",
	// DeployRequest.network / RerouteRequest.network are honoured: a stack joins the
	// network its Environment owns instead of one shared network, and the agent puts
	// Traefik on it. There is no shared-network fallback - this agent cannot serve a
	// control plane that does not send it.
	"deploy.network",
	// mTLS leaf renewal over the existing pinned channel (RenewalCSR +
	// InstallRenewedCert): the control plane re-signs a fresh CSR before the ~365d cert
	// expires and the agent hot-reloads it WITHOUT a restart.
	"cert-renewal",
	// The host-level verbs the Servers page needs (HostInfo, SetTimezone, TraefikConfig,
	// RestartControlPlane): what this hardware IS, what time it thinks it is, restarting
	// Traefik, restarting the panel.
	"hostops",
	// DeployRequest.build_only is honoured: this agent can build an image and stop, for a
	// BUILD SERVER that compiles for hosts it does not run on.
	"deploy.build-only",
	// ExportImage/ImportImage: a built image streams host-to-host through the control
	// plane, the third sibling of the volume and files-dir relays.
	"image-copy",
	// DeployRequest.registry_auth is honoured: every pull this deploy makes reads the
	// team's registry credentials, so a private image works without a host `docker login`.
	"deploy.registry-auth",
	// Two things this binary does that the previous one did not: it ranks an app's
	// images within their REPOSITORY (a pulled image labelled with someone else's slug
	// can no longer evict their rollback), and it reads the daemon's address pools from
	// `docker info` and measures them instead of looking for a key in daemon.json. Its
	// own string because the tag moves, so the version proves nothing.
	"cleanup.images.by-repository",
	// A volume or host-path copy gets a 6h wall clock instead of 30 minutes, so a large
	// volume over a slow link is no longer killed mid-transfer. Its own string because
	// the tag moves, so the version proves nothing.
	"volume-copy-6h",
}

// AgentVersion is the version this agent reports over Hello. "dev" for a build that
// skipped the stamp (e.g. a bare `go build`), which the control plane treats as "can't
// compare", never "outdated".
var AgentVersion = "dev"

// retainFinished is how long a finished deploy's event buffer is kept so a
// control plane that dropped just before the terminal result can still reattach
// and fetch it (PLAN D5). After this it is evicted to bound memory.
const retainFinished = 10 * time.Minute

// Service is the gRPC Agent implementation.
type Service struct {
	pb.UnimplementedAgentServer

	// stackDir is where rendered stack files are written (mirrors the control
	// plane's /data/stacks). buildTmpDir is where upload contexts are extracted.
	stackDir    string
	buildTmpDir string
	dataDir     string
	// dataBase is the host data root (the control plane's DEPLO_DATA_DIR, e.g. /data),
	// under which dev workspaces (<dataBase>/dev) and the SSH gateway
	// (<dataBase>/ssh-gateway) live - the Part D per-host singletons.
	dataBase string
	// agentDir is the agent's OWN data root (--agent-dir, the installer's
	// /var/lib/deplo-agent): mTLS materials, and the Traefik stack the installer puts
	// under traefik/.
	agentDir string
	// traefikApply overrides how the Traefik stack is brought up. nil in
	// production (bringUpTraefik runs docker); set by tests so exercising the
	// rollback path cannot start a container on the machine running them.
	traefikApply func(ctx context.Context, path string, restartOnly bool) error

	mu      sync.Mutex
	deploys map[string]*inflight
	// Cron jobs this agent is running or recently finished (job.go). Same mutex as
	// `deploys` and the same ownership rule: the process lives on a job-scoped context, so
	// a control-plane disconnect never kills it.
	jobs map[string]*job

	// mTLS leaf renewal (nil for --insecure / tests). certMgr hot-swaps the live
	// server cert; pendingKey is the freshly-generated key from a RenewalCSR,
	// held until the matching signed cert arrives via InstallRenewedCert.
	certMgr    *CertManager
	pendingMu  sync.Mutex
	pendingKey ed25519.PrivateKey

	// One compose project is one lock. Two `docker compose` runs on the same `-p`
	// interleave their own create/remove steps, and a move racing a start is exactly
	// that. Deploy is deliberately out: the control plane single-flights it, and
	// holding this across a ten-minute build would block the stop that cancels it.
	stackLocks sync.Map // slug -> *sync.Mutex
}

// lockStack serializes the compose operations on one stack. The returned func
// unlocks; call it with defer.
func (s *Service) lockStack(slug string) func() {
	v, _ := s.stackLocks.LoadOrStore(slug, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// New builds the service. stackDir/buildTmpDir are created lazily by the deploy
// path; dataDir is the filesystem measured for disk metrics; dataBase is the host
// data root for the Part D dev/gateway singletons (empty => parent of stackDir).
func New(stackDir, buildTmpDir, dataDir, dataBase string) *Service {
	if dataBase == "" {
		dataBase = filepath.Dir(stackDir)
	}
	// Credentials an agent that died mid-deploy left behind.
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

// Hello is the health + identity handshake and the mandatory deploy pre-flight (PLAN
// P5).
func (s *Service) Hello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloResponse, error) {
	available := dockercli.Available(ctx)
	version := ""
	if available {
		version = dockercli.ServerVersion(ctx)
	}
	return &pb.HelloResponse{
		ContractVersion: pb.ContractVersion_CONTRACT_VERSION_V1,
		AgentVersion:    AgentVersion,
		DockerAvailable: available,
		DockerVersion:   version,
		Capabilities:    Capabilities,
		// Read live so the control plane can set the server's traefikEnabled from
		// each Hello rather than a stored value that goes stale.
		TraefikRunning: available && dockercli.TraefikRunning(ctx),
		// This binary's own architecture, which is the host's: the release publishes
		// linux/amd64 and linux/arm64 and the installer picks by `uname -m`.
		HostArch: runtime.GOARCH,
	}, nil
}

// Metrics returns a host snapshot (replaces lib/infra/host.ts per server).
func (s *Service) Metrics(ctx context.Context, req *pb.MetricsRequest) (*pb.HostMetrics, error) {
	dataDir := req.GetDataDir()
	if dataDir == "" {
		dataDir = s.dataDir
	}
	m := hostmetrics.Collect(dataDir)
	// `docker ps -q` per call is affordable here (one unary RPC, on demand) but
	// NOT on the stream's ticker - StreamMetrics takes the count from its roster,
	// which is rebuilt on container churn rather than every 5s. See roster.go.
	return hostMetricsPB(m, dockercli.RunningContainers(ctx)), nil
}

// hostMetricsPB maps a hostmetrics.Metrics onto the wire type.
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
		// Already running (or finished + retained): attach instead of re-running.
		s.mu.Unlock()
		return existing.subscribe(stream.Context(), 0, stream.Send)
	}
	// Start a fresh deploy on a background, deploy-scoped context so a stream
	// disconnect does not abort the build.
	deployCtx, cancel := context.WithCancel(context.Background())
	f := newInflight(cancel)
	s.deploys[id] = f
	s.mu.Unlock()

	go s.driveDeploy(deployCtx, id, req, f)

	// The caller's stream subscribes from the start; its context cancelling just
	// detaches this reader (the build continues for a reattacher).
	return f.subscribe(stream.Context(), 0, stream.Send)
}

// ReattachDeploy reconnects to an in-flight or recently-finished deploy and replays
// events past from_seq, then follows it live to completion (D5).
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

// driveDeploy runs the deploy body, appending every emitted event to the inflight
// buffer (which fans out to all subscribers), then schedules the record's eviction
// after the retention window so a late reattacher can still fetch the terminal result.
func (s *Service) driveDeploy(ctx context.Context, id string, req *pb.DeployRequest, f *inflight) {
	defer f.cancel() // release the deploy context when the body returns
	e := &emitter{send: func(ev *pb.DeployEvent) error {
		f.append(ev)
		return nil
	}}
	// A panic inside a builder (a nil-deref on a partial proto, an index error on
	// malformed build input) must degrade to ONE failed deploy, not crash the whole
	// agent, which would take down every tenant's streams/metrics/management on this
	// shared host.
	func() {
		defer func() {
			if r := recover(); r != nil {
				e.result(false, fmt.Sprintf("deploy panicked: %v", r), "")
			}
		}()
		s.runDeploy(ctx, req, e)
	}()
	// Retain briefly for reconnection, then evict.
	time.AfterFunc(retainFinished, func() {
		s.mu.Lock()
		if s.deploys[id] == f {
			delete(s.deploys, id)
		}
		s.mu.Unlock()
	})
}

// StopStack stops a compose-managed stack (falls back to the bare container).
func (s *Service) StopStack(ctx context.Context, ref *pb.StackRef) (*pb.StackResult, error) {
	slug := ref.GetSlug()
	if err := validateSlug(slug); err != nil {
		return nil, err
	}
	defer s.lockStack(slug)()
	res, err := dockercli.Run(ctx, time.Minute, s.composeCtl(slug, "stop")...)
	if err == nil && res.Code == 0 {
		return &pb.StackResult{Ok: true}, nil
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

// DestroyStack stops and removes a stack (compose down, falling back to rm -f). An
// older control plane that never sends the field leaves it false too, so the default is
// the original volume-preserving behaviour.
func (s *Service) DestroyStack(ctx context.Context, ref *pb.StackRef) (*pb.StackResult, error) {
	slug := ref.GetSlug()
	if err := validateSlug(slug); err != nil {
		return nil, err
	}
	defer s.lockStack(slug)()
	downArgs := s.composeCtl(slug, "down", "--remove-orphans")
	if ref.GetRemoveVolumes() {
		downArgs = append(downArgs, "-v")
	}
	res, err := dockercli.Run(ctx, 90*time.Second, downArgs...)
	// Named volumes the control plane asked for BY NAME, reclaimed whatever the `down`
	// did. Best-effort by design: a volume that is gone, or still in use by something
	// else, must not turn a good destroy bad.
	reclaimed := s.reclaimVolumes(ctx, ref.GetReclaimVolumes())
	if err == nil && res.Code == 0 {
		// The stack files go on ANY successful `down`, not just a `down -v`. On a long-lived
		// host that is dozens of files nothing will ever read again, each holding the env the
		// app was running with.
		s.removeStackFiles(slug)
		return &pb.StackResult{Ok: true, Error: reclaimed}, nil
	}
	// `rm -f` is idempotent for a missing container (exit 0), so the common already-gone
	// case still reports Ok.
	r2, err := dockercli.Run(ctx, 30*time.Second, "rm", "-f", "deplo-"+slug)
	if err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}
	// A removeVolumes destroy that fell through to `rm -f` did NOT run a successful `down
	// -v`, and `rm -f` only removes a container - it can never reclaim a named volume.
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

// removeStackFiles deletes everything a destroyed stack leaves on disk: the compose
// file, its env sidecar, and the app's own files directory.
func (s *Service) removeStackFiles(slug string) {
	_ = os.Remove(s.stackPath(slug))
	_ = os.Remove(fmt.Sprintf("%s/%s.env", s.stackDir, slug))
	// The env-file moved into the stack's own directory (writeComposeEnv); the
	// whole directory goes with the stack, but remove the file explicitly first
	// so a failure to remove the tree never leaves decrypted secrets behind.
	_ = os.Remove(filepath.Join(s.stackDir, "files", slug, ".env"))
	// Safe as a recursive remove: every caller has already run validateSlug, whose
	// pattern cannot express a dot, a slash or a leading dash.
	_ = os.RemoveAll(s.filesRoot(slug))
}

// reclaimVolumes removes named volumes the control plane listed on a destroy.
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

// Reroute re-renders a running stack in place: the control plane changed the stack's
// domain/label set (or rotated env) and ships the freshly rendered compose, env and
// mount files so the agent rewrites them and runs `up -d` to pick up the new config
// WITHOUT a rebuild.
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
	// Same opener as Deploy, and for the same reason: the stack's network is declared
	// `external: true` in the rendered compose, so `compose up` fails outright if it does
	// not exist yet - and Traefik has to be on it for the re-rendered routers to resolve.
	if err := ensureTenantNetwork(ctx, req.GetNetwork()); err != nil {
		return &pb.StackResult{Ok: false, Error: "ensure network: " + err.Error()}, nil
	}

	stackFile := s.stackPath(slug)
	// 0600 + Chmod, same as the deploy path writes it: this YAML carries a
	// single-image app's whole environment, and Reroute is what a RESTORE runs -
	// so without this a restore would quietly hand the file back its old 0644.
	if err := os.Chmod(stackFile, 0o600); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.WriteFile(stackFile, []byte(req.GetComposeYaml()), 0o600); err != nil {
		return &pb.StackResult{Ok: false, Error: "write stack file: " + err.Error()}, nil
	}

	// (Re)materialise the compose mount files. There is no deploy stream here, so
	// any unsafe-path warnings are discarded (the control plane already validated
	// the rendered set; the in-agent guard stays as defence in depth).
	if len(req.GetMounts()) > 0 {
		discard := &emitter{send: func(*pb.DeployEvent) error { return nil }}
		if err := s.writeMountFiles(slug, req.GetMounts(), discard); err != nil {
			return &pb.StackResult{Ok: false, Error: "write mount files: " + err.Error()}, nil
		}
	}

	// Single-image stacks bake env into the YAML and send empty env+mounts;
	// compose stacks need a 0600 env-file for ${VAR} interpolation. Mirror Deploy:
	// write+pass the env-file only when there is env to interpolate.
	envFile := ""
	projectDir := ""
	if len(req.GetEnv()) > 0 {
		var err error
		if envFile, projectDir, err = s.writeComposeEnv(slug, req.GetEnv()); err != nil {
			return &pb.StackResult{Ok: false, Error: "write env file: " + err.Error()}, nil
		}
	}
	// Through the SAME assembler as a deploy: a reroute brings the stack up too,
	// so the operator's extra flags (and their vetting) must apply identically -
	// two hand-rolled argvs is how one of them silently stops matching the other.
	composeArgs := composeUpArgs(name, stackFile, envFile, projectDir, false, req.GetComposeUpArgs())

	res, err := dockercli.Run(ctx, 120*time.Second, composeArgs...)
	if err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}
	return &pb.StackResult{Ok: res.Code == 0, Error: res.Stderr}, nil
}

// ReadStack returns the rendered stack YAML on disk for a slug so the control plane can
// preview/diff it before a reroute.
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

// CheckPort reports whether a host TCP port is free to publish. We bind the IPv4
// wildcard, which is what a Docker `ports: "<p>:<p>"` publish contends for on this
// host; a bind failure (EADDRINUSE) => not available.
func (s *Service) CheckPort(ctx context.Context, req *pb.CheckPortRequest) (*pb.CheckPortResponse, error) {
	port := req.GetPort()
	if port < 1 || port > 65535 {
		return &pb.CheckPortResponse{
			Available: false,
			Reason:    fmt.Sprintf("port %d is out of range (1-65535)", port),
		}, nil
	}
	addr := net.JoinHostPort("0.0.0.0", strconv.Itoa(int(port)))
	// SO_REUSEADDR is NOT set (Go's default), so this bind contends for the port
	// exactly as a fresh docker-proxy publish would - a TIME_WAIT or an active
	// listener both make it fail, which is the answer we want.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return &pb.CheckPortResponse{
			Available: false,
			Reason:    fmt.Sprintf("port %d is already in use on the host", port),
		}, nil
	}
	// Release immediately - this was only a probe. Close errors are irrelevant
	// (the OS reclaims the socket regardless); the port is confirmed bindable.
	_ = ln.Close()
	return &pb.CheckPortResponse{Available: true}, nil
}

func (s *Service) stackPath(slug string) string {
	return fmt.Sprintf("%s/%s.yml", s.stackDir, slug)
}

// composeCtl builds the argv for a lifecycle verb (`stop`, `start`, `down`) on a stack
// that is already on disk.
func (s *Service) composeCtl(slug string, verb ...string) []string {
	args := []string{"compose", "-p", "deplo-" + slug, "-f", s.stackPath(slug)}
	if dir := s.filesRoot(slug); isFile(filepath.Join(dir, ".env")) {
		args = append(args, "--project-directory", dir, "--env-file", filepath.Join(dir, ".env"))
	} else if legacy := fmt.Sprintf("%s/%s.env", s.stackDir, slug); isFile(legacy) {
		args = append(args, "--env-file", legacy)
	}
	return append(args, verb...)
}

func isFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// stackFailure picks what to report when BOTH the compose verb and the bare-container
// fallback failed.
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
