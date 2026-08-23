// Package server implements the Agent gRPC service — the server side of the
// second system boundary (ADR-0006). It owns the host-coupled half of the
// platform on the machine the agent runs on: Docker exec, the Dockerfile build,
// stack lifecycle, host metrics. The control plane stays the source of truth
// (it renders the compose and decrypts env); the agent stays dumb about Deplo's
// store and policy.
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
	// The heavy builders, ported from builders.ts (build_methods.go). The control
	// plane routes a project's build method to the agent only when the matching
	// capability is present; an older agent without it gets an "update the agent"
	// error rather than a request it can't handle.
	"deploy.static",     // nginx static site
	"deploy.nixpacks",   // nixpacks (binary lazily installed on first use)
	"deploy.buildpacks", // Cloud Native Buildpacks (heroku + paketo) via pack
	"deploy.railpack",   // railpack via buildkitd/buildctl
	"deploy.buildenv",   // req.env reaches BUILDS too (build args / plan secrets), not just the runtime stack
	// The two "give me a genuinely fresh one" switches on DeployRequest. Split
	// because they answer different questions and the control plane warns about
	// them separately: no_build_cache is "don't reuse layers or nixpacks cache
	// mounts", force_recreate is "replace the container even if nothing changed".
	"deploy.nocache",
	"deploy.force-recreate",
	// The app's own extra `docker compose up` flags ride DeployRequest /
	// RerouteRequest and are appended to the bring-up the agent assembles. Gated
	// because an agent without it ignores the field entirely, and a silently
	// unapplied flag is exactly the kind of lie the control plane warns about.
	"deploy.compose-args",
	"metrics",
	"container-stats", // per-container `docker stats` snapshot (ContainerStats) — the per-app/per-database Monitoring tab
	// ONE long-lived host+container telemetry stream (StreamMetrics), sampled on
	// the agent's own ticker. Supersedes polling `metrics` + `container-stats`
	// per viewer per resource: with it the control plane holds a single stream
	// per host, so telemetry cost stops scaling with container and viewer count.
	// Both unary RPCs stay for agents (and control planes) without it.
	"metrics-stream",
	"dev",         // dev container lifecycle (StartDev/StopDev/Reset/Teardown) — Part D
	"ssh-gateway", // the per-host SSH gateway singleton (Ensure/Provision/Deprovision)
	"tunnel",      // the VS Code remote tunnel (Start/Get/Stop)
	"self-update", // in-place agent binary update over mTLS (SelfUpdate), certs kept
	// The agent removes ITSELF from the host (SelfUninstall): unit, binary, state
	// dir. Its reason to exist is the MIGRATION SOURCE - a host of another platform
	// we install onto only to read its volumes - where telling someone to open a
	// shell to undo an install we performed is the thing the product refuses to do.
	// Docker is never touched; uninstall-agent.sh stays the answer for a host that
	// is unreachable or already de-trusted.
	"self-uninstall",
	"backup",      // dump/restore a DB or project to/from S3 (Backup/Restore/S3Check/S3Delete)
	"checkport",   // host TCP port availability probe (CheckPort) — gates DB "expose publicly"
	// One bounded HTTP GET to a container of an app's own stack (ProbeHttp).
	// A compose app runs prebuilt images, so its favicon exists only inside the
	// image and is only ever served — asking the running app is the only way to
	// read it. An agent without this simply yields no icon for such an app.
	"http-probe",
	// Scheduled `docker exec` the agent owns for its whole lifetime
	// (StartJob/PollJob/KillJob) - the Cron jobs feature. Separate from `Exec`,
	// which is the console's synchronous 30s-capped call: a cron job runs for
	// hours and must survive a control-plane restart, so the process lives here
	// on a job-scoped context and the control plane polls for it.
	"cron",
	"volume-copy", // cross-host named-volume copy for a server move (ExportVolume/ImportVolume)
	"files-copy",  // cross-host files-dir copy for a service move (ExportFiles/ImportFiles)
	// Backup artifacts held on THIS host's filesystem instead of an S3 bucket:
	// the StoreTarget arms of Backup/Restore/S3Check/S3Delete, plus the relay
	// primitives ReadStoreFile / WriteStoreFile / RestoreFrom. Split from
	// "backup" rather than folded into it because the two shipped in different
	// releases, and an agent that can dump to S3 but cannot hold artifacts must
	// fail the SECOND thing with a clear "update the agent" instead of silently
	// accepting a store request it would ignore.
	"backup-store",
	// Two things that changed about a BUCKET artifact, shipped as one flag because
	// they land together and a control plane that finds it absent must fall back on
	// both at once:
	//   - `age_recipient` is honoured for an S3 destination, so a bucket artifact
	//     is encrypted like a store one. Without this the field is ignored and the
	//     archive - which carries the app's whole decrypted env - lands in the
	//     bucket in the clear, so the control plane REFUSES to run rather than let
	//     an encrypted destination silently downgrade.
	//   - the upload reports a sha256, which is what lets the control plane prove
	//     an artifact is the one it wrote before a restore executes its compose.
	// `allow_private_endpoint` rides the same flag: a self-hosted bucket on an
	// RFC1918 address is unreachable without it.
	"backup-encrypt-s3",
	// `S3Target.extra_args` is read: a destination's advanced quirk flags
	// (`--s3-sign-accept-encoding=false`, `--s3-force-path-style=true`, …) reach
	// the minio client instead of being ignored. Unlike the encryption flag above
	// this is a SOFT gate - the control plane sends the flags either way and warns
	// when the agent is too old, because a dropped workaround is a slower or
	// noisier backup, not a backup that silently lost its encryption.
	"backup-s3-args",
	// ReadStoreFile accepts an S3Target, so a BUCKET artifact can be streamed out
	// decrypted the same way a store one already is. Without it the control plane
	// has no way to hand a user a backup that lives in their bucket, and the
	// Download button can only say "fetch the object with your own credentials
	// and decrypt it yourself" - which is the answer this platform exists not to
	// give. Its own flag rather than folded into "backup-store": that one is
	// about artifacts held on this host's disk, and this is the opposite case.
	"backup-s3-read",
	// RestoreFrom honours `untrusted_config`: an artifact that came from outside
	// the fleet contributes DATA only, never the compose/env/mounts the stack
	// comes back up with. An agent without this ignores the field, and ignoring
	// it means restoring exactly what the flag exists to prevent - so the control
	// plane refuses the upload rather than sending it to an agent that would.
	"backup-untrusted-config",
	// Allow-listed Docker disk reclaim (DockerCleanup): build cache, dangling
	// images, orphaned buildkit volumes, unused app images. NEVER a bare prune.
	"docker-cleanup",
	// DockerCleanupRequest.keep_per_slug is read: app-image retention is per APP
	// instead of one number for the whole host, which is what lets each app carry
	// its own rollback depth. A SOFT gate - an agent without it ignores the map, so
	// the control plane compensates by raising the scalar to the map's maximum.
	// That over-keeps here rather than under-keeping, because the failure this
	// exists to prevent is deleting the image a rollback was about to run.
	"cleanup.keep-per-slug",
	// mTLS leaf renewal over the existing pinned channel (RenewalCSR +
	// InstallRenewedCert): the control plane re-signs a fresh CSR before the
	// ~365d cert expires and the agent hot-reloads it WITHOUT a restart. An agent
	// without this capability keeps its old behavior (the operator re-bootstraps).
	"cert-renewal",
	// The host-level verbs the Servers page needs (HostInfo, SetTimezone,
	// TraefikConfig, RestartControlPlane): what this hardware IS, what time it
	// thinks it is, restarting Traefik, restarting the panel. ONE flag for the
	// four because they land in the same release, and a control plane that finds
	// it absent tells the operator to update the agent rather than rendering a
	// panel whose every button fails.
	"hostops",
	// DeployRequest.build_only is honoured: this agent can build an image and stop,
	// for a BUILD SERVER that compiles for hosts it does not run on. A HARD gate,
	// and the reason is the failure mode of ignoring it - an older agent would read
	// the unknown field as absent and DEPLOY the app on the build server, quietly
	// running production on the wrong machine.
	"deploy.build-only",
	// ExportImage/ImportImage: a built image streams host-to-host through the
	// control plane, the third sibling of the volume and files-dir relays. Both
	// halves under one flag because a copy needs both ends and there is no useful
	// state where a host can send but not receive.
	"image-copy",
}

// AgentVersion is the version this agent reports over Hello. It is stamped at
// build time via -ldflags from the GIT TAG — see the Makefile's `git describe`
// (and release.yml, which stamps from the pushed tag directly). That tag is also
// what the control plane resolves as "latest" from this repo's GitHub releases
// (lib/agent/release.ts in IdraDev/deplo), so the binary's version and the
// control plane's notion of latest cannot drift. "dev" for a build that skipped
// the stamp (e.g. a bare `go build`), which the control plane treats as "can't
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
	// dataBase is the host data root (the control plane's DEPLO_DATA_DIR, e.g.
	// /data), under which dev workspaces (<dataBase>/dev) and the SSH gateway
	// (<dataBase>/ssh-gateway) live — the Part D per-host singletons. Defaults to
	// the parent of stackDir, since the control plane's STACK_DIR is <dataBase>/
	// stacks; the bind paths inside the rendered dev/gateway compose line up
	// because the agent uses the SAME layout the control plane assumed.
	dataBase string
	// agentDir is the agent's OWN data root (--agent-dir, the installer's
	// /var/lib/deplo-agent): mTLS materials, and the Traefik stack the installer
	// puts under traefik/. Set via SetAgentDir so New's signature — and every
	// call site that constructs a Service — is unaffected. Empty means this agent
	// manages no Traefik stack.
	agentDir string
	// traefikApply overrides how the Traefik stack is brought up. nil in
	// production (bringUpTraefik runs docker); set by tests so exercising the
	// rollback path cannot start a container on the machine running them.
	traefikApply func(ctx context.Context, path string, restartOnly bool) error

	mu      sync.Mutex
	deploys map[string]*inflight
	// Cron jobs this agent is running or recently finished (job.go). Same mutex
	// as `deploys` and the same ownership rule: the process lives on a
	// job-scoped context, so a control-plane disconnect never kills it. The map
	// is agent-process state on purpose - an agent restart loses it, and PollJob
	// answers `found: false` rather than inventing an outcome.
	jobs map[string]*job

	// mTLS leaf renewal (nil for --insecure / tests). certMgr hot-swaps the live
	// server cert; pendingKey is the freshly-generated key from a RenewalCSR,
	// held until the matching signed cert arrives via InstallRenewedCert.
	certMgr    *CertManager
	pendingMu  sync.Mutex
	pendingKey ed25519.PrivateKey
}

// New builds the service. stackDir/buildTmpDir are created lazily by the deploy
// path; dataDir is the filesystem measured for disk metrics; dataBase is the host
// data root for the Part D dev/gateway singletons (empty => parent of stackDir).
func New(stackDir, buildTmpDir, dataDir, dataBase string) *Service {
	if dataBase == "" {
		dataBase = filepath.Dir(stackDir)
	}
	return &Service{
		stackDir:    stackDir,
		buildTmpDir: buildTmpDir,
		dataDir:     dataDir,
		dataBase:    dataBase,
		deploys:     map[string]*inflight{},
		jobs:        map[string]*job{},
	}
}

// Hello is the health + identity handshake and the mandatory deploy pre-flight
// (PLAN P5). It never fails: an unreachable Docker daemon is reported as
// docker_available=false, a clear "this server can't deploy" signal, rather than
// an RPC error.
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
		// linux/amd64 and linux/arm64 and the installer picks by `uname -m`. The
		// control plane compares it across two hosts before letting one BUILD for the
		// other - an amd64 image loaded on an arm64 box dies with `exec format error`
		// at run time, long after the deploy called itself a success.
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
	// NOT on the stream's ticker — StreamMetrics takes the count from its roster,
	// which is rebuilt on container churn rather than every 5s. See roster.go.
	return hostMetricsPB(m, dockercli.RunningContainers(ctx)), nil
}

// hostMetricsPB maps a hostmetrics.Metrics onto the wire type. Extracted so the
// unary Metrics RPC and the StreamMetrics ticker cannot drift into reporting the
// same host two different ways — the two paths differ ONLY in how they obtain the
// running-container count and whether the sample slept for its window.
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
	}
}

// Deploy runs a deployment and streams its events. The stream is the live build
// log + phase transitions + a terminal result; the control plane writes these
// into the Deployment row and republishes over its existing SSE subscriptions.
//
// PART B (D5): the deploy itself runs in a BACKGROUND goroutine on a
// deploy-scoped context, NOT the stream's — so if the control plane disconnects
// mid-build, the build KEEPS GOING and the control plane can reattach
// (ReattachDeploy) to replay what it missed and follow it to completion. Every
// event is buffered (seq-stamped) so a reconnect loses nothing. A repeat Deploy
// for an already-running id attaches to it instead of starting a second build.
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

// ReattachDeploy reconnects to an in-flight or recently-finished deploy and
// replays events past from_seq, then follows it live to completion (D5).
// Returns NOT_FOUND if the agent has no record of the deploy (it never ran here,
// or its retention window elapsed) — the control plane then reconciles it.
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

// driveDeploy runs the deploy body, appending every emitted event to the
// inflight buffer (which fans out to all subscribers), then schedules the
// record's eviction after the retention window so a late reattacher can still
// fetch the terminal result.
func (s *Service) driveDeploy(ctx context.Context, id string, req *pb.DeployRequest, f *inflight) {
	defer f.cancel() // release the deploy context when the body returns
	e := &emitter{send: func(ev *pb.DeployEvent) error {
		f.append(ev)
		return nil
	}}
	// A panic inside a builder (a nil-deref on a partial proto, an index error on
	// malformed build input) must degrade to ONE failed deploy — not crash the
	// whole agent, which would take down every tenant's streams/metrics/management
	// on this shared host. Recover here and emit a failure result; the eviction
	// below still runs.
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
	res, err := dockercli.Run(ctx, time.Minute, "compose", "-p", "deplo-"+slug, "-f", s.stackPath(slug), "stop")
	if err == nil && res.Code == 0 {
		return &pb.StackResult{Ok: true}, nil
	}
	r2, err2 := dockercli.Run(ctx, 30*time.Second, "stop", "deplo-"+slug)
	if err2 != nil {
		return &pb.StackResult{Ok: false, Error: err2.Error()}, nil
	}
	return &pb.StackResult{Ok: r2.Code == 0, Error: r2.Stderr}, nil
}

// StartStack starts a previously stopped stack.
func (s *Service) StartStack(ctx context.Context, ref *pb.StackRef) (*pb.StackResult, error) {
	slug := ref.GetSlug()
	if err := validateSlug(slug); err != nil {
		return nil, err
	}
	res, err := dockercli.Run(ctx, time.Minute, "compose", "-p", "deplo-"+slug, "-f", s.stackPath(slug), "start")
	if err == nil && res.Code == 0 {
		return &pb.StackResult{Ok: true}, nil
	}
	r2, err2 := dockercli.Run(ctx, 30*time.Second, "start", "deplo-"+slug)
	if err2 != nil {
		return &pb.StackResult{Ok: false, Error: err2.Error()}, nil
	}
	return &pb.StackResult{Ok: r2.Code == 0, Error: r2.Stderr}, nil
}

// DestroyStack stops and removes a stack (compose down, falling back to rm -f).
//
// When ref.RemoveVolumes is set the `down` also drops the stack's named volumes
// (`-v`) and, on a clean teardown, the on-disk compose + env files are removed.
// Database deletion sets it so a DB's data volume is reclaimed rather than
// orphaned; app teardown leaves it false (volumes survive, file stays). An older
// control plane that never sends the field leaves it false too, so the default is
// the original volume-preserving behaviour.
//
// The volume sweep only happens on a successful `down -v` (the rm -f fallback can
// only remove a container, never a named volume). So a removeVolumes destroy that
// fails the compose-down and falls through to rm -f reports Ok:false and keeps the
// stack file — the volume was NOT reclaimed and the control plane must not believe
// otherwise.
func (s *Service) DestroyStack(ctx context.Context, ref *pb.StackRef) (*pb.StackResult, error) {
	slug := ref.GetSlug()
	if err := validateSlug(slug); err != nil {
		return nil, err
	}
	downArgs := []string{"compose", "-p", "deplo-" + slug, "-f", s.stackPath(slug), "down", "--remove-orphans"}
	if ref.GetRemoveVolumes() {
		downArgs = append(downArgs, "-v")
	}
	res, err := dockercli.Run(ctx, 90*time.Second, downArgs...)
	// Named volumes the control plane asked for BY NAME, reclaimed whatever the
	// `down` did. `down -v` can only reclaim what the on-disk compose file
	// declares, so a stack that was never deployed (no file, no containers — the
	// state a migrated app sits in until somebody deploys it) kept its data
	// forever, unnamed and unreachable. Best-effort by design: a volume that is
	// gone, or still in use by something else, must not turn a good destroy bad.
	reclaimed := s.reclaimVolumes(ctx, ref.GetReclaimVolumes())
	if err == nil && res.Code == 0 {
		// The stack files go on ANY successful `down`, not just a `down -v`.
		//
		// They used to be swept only when volumes were removed too, and deleting an
		// App deliberately keeps its volumes - so every app ever deleted left its
		// rendered compose and its env file behind, permanently. On a long-lived
		// host that is dozens of files nothing will ever read again, each holding
		// the env the app was running with. They were also the largest group of
		// world-readable secrets on disk before the mode fix.
		//
		// Nothing needs them once the stack is down: the compose is re-rendered by
		// the control plane on every deploy, and a surviving named volume is found
		// by its own docker name, not by this file. The FAILED paths below still
		// keep them, which is where the "a retry needs it" reasoning actually
		// applies.
		s.removeStackFiles(slug)
		return &pb.StackResult{Ok: true, Error: reclaimed}, nil
	}
	// `rm -f` is idempotent for a missing container (exit 0), so the common
	// already-gone case still reports Ok. Gate on the exit code — like
	// StopStack/StartStack — so a genuine removal failure is NOT reported as a
	// successful destroy (which would have the control plane mark a still-running
	// container destroyed).
	r2, err := dockercli.Run(ctx, 30*time.Second, "rm", "-f", "deplo-"+slug)
	if err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}
	// A removeVolumes destroy that fell through to `rm -f` did NOT run a successful
	// `down -v`, and `rm -f` only removes a container — it can never reclaim a named
	// volume. So the data volume survives here. Do NOT sweep the stack file (it is
	// the only on-disk record of the volume's compose name a retry needs) and do NOT
	// report a clean destroy: surface Ok:false so the control plane can warn / retry
	// rather than believe the volume was reclaimed when it wasn't.
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

// removeStackFiles deletes everything a destroyed stack leaves on disk: the
// compose file, its env sidecar, and the app's own files directory. Best-effort:
// a missing path is fine (already gone), and a remove failure must not flip an
// otherwise-successful destroy to failed — the container/volumes are already
// down, a stray file is cosmetic.
//
// The FILES DIRECTORY is here for the same reason the env sidecar is, and it was
// missed when that one was added: it holds the app's config files — its
// nginx.conf, its htpasswd, whatever a compose stack bind-mounts out of
// `./<name>` — and deleting the App left every one of them on a shared host,
// permanently, readable by whoever comes next. Nothing needs them afterwards: the
// control plane owns those rows and writes them again on the next deploy, so the
// only thing keeping them buys is a copy of a deleted app's configuration.
func (s *Service) removeStackFiles(slug string) {
	_ = os.Remove(s.stackPath(slug))
	_ = os.Remove(fmt.Sprintf("%s/%s.env", s.stackDir, slug))
	// Safe as a recursive remove: every caller has already run validateSlug, whose
	// pattern cannot express a dot, a slash or a leading dash.
	_ = os.RemoveAll(s.filesRoot(slug))
}

// reclaimVolumes removes named volumes the control plane listed on a destroy.
//
// Only names Deplo itself creates are accepted (`deplo-` prefix) — the list
// arrives over an authenticated channel, but a teardown that can name ANY volume
// on the host is a bigger verb than it needs to be, and the prefix costs nothing.
// Returns a summary of what could not be removed, for the caller to report
// alongside an otherwise-successful destroy; a volume that is simply not there
// is not a failure, which is the ordinary case once `down -v` did the work.
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

// Reroute re-renders a running stack in place: the control plane changed the
// stack's domain/label set (or rotated env) and ships the freshly rendered
// compose, env and mount files so the agent rewrites them and runs `up -d` to
// pick up the new config WITHOUT a rebuild. Unlike Deploy there is no event
// stream — this is a synchronous lifecycle verb like StopStack/StartStack — so
// writeMountFiles' warn logs go to a discarding emitter.
func (s *Service) Reroute(ctx context.Context, req *pb.RerouteRequest) (*pb.StackResult, error) {
	slug := req.GetSlug()
	if err := validateSlug(slug); err != nil {
		return nil, err
	}
	name := "deplo-" + slug

	if req.GetComposeYaml() == "" {
		return &pb.StackResult{Ok: false, Error: "reroute request missing rendered compose"}, nil
	}

	if err := os.MkdirAll(s.stackDir, 0o755); err != nil {
		return &pb.StackResult{Ok: false, Error: "create stack dir: " + err.Error()}, nil
	}
	// Same opener as Deploy, and for the same reason: every stack joins the shared
	// `deplo` network, declared `external: true` in the rendered compose, so
	// `compose up` fails outright if it does not exist yet. Deploy had this and
	// Reroute did not, which is invisible until a host's FIRST stack arrives
	// through Reroute - a managed database is provisioned that way, so a database
	// was the one thing that could not be created on a brand-new server whose
	// installer skipped Traefik (any host that already runs a reverse proxy).
	if err := dockercli.EnsureNetwork(ctx, "deplo"); err != nil {
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
	if len(req.GetEnv()) > 0 {
		envFile = fmt.Sprintf("%s/%s.env", s.stackDir, slug)
		if err := os.WriteFile(envFile, []byte(renderEnvFile(req.GetEnv())), 0o600); err != nil {
			return &pb.StackResult{Ok: false, Error: "write env file: " + err.Error()}, nil
		}
	}
	// Through the SAME assembler as a deploy: a reroute brings the stack up too,
	// so the operator's extra flags (and their vetting) must apply identically —
	// two hand-rolled argvs is how one of them silently stops matching the other.
	composeArgs := composeUpArgs(name, stackFile, envFile, false, req.GetComposeUpArgs())

	res, err := dockercli.Run(ctx, 120*time.Second, composeArgs...)
	if err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}
	return &pb.StackResult{Ok: res.Code == 0, Error: res.Stderr}, nil
}

// ReadStack returns the rendered stack YAML on disk for a slug so the control
// plane can preview/diff it before a reroute. A missing file is not an error —
// it just means "nothing deployed yet" (Exists:false). Any OTHER read failure is
// also reported as Exists:false (rather than an RPC error) so the preview shows
// "nothing yet" instead of surfacing a transient FS error to the operator.
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

// CheckPort reports whether a host TCP port is free to publish. The definitive
// test is to BIND it: a successful bind (immediately released) proves nothing
// else holds it, and — unlike parsing `docker ps` or `ss` output — it sees BOTH
// Docker-published ports and raw host listeners (a system Postgres on 5432, the
// control plane's own DB, an unrelated daemon). We bind the IPv4 wildcard, which
// is what a Docker `ports: "<p>:<p>"` publish contends for on this host; a bind
// failure (EADDRINUSE) => not available. An out-of-range port is reported
// unavailable with a reason rather than attempted.
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
	// exactly as a fresh docker-proxy publish would — a TIME_WAIT or an active
	// listener both make it fail, which is the answer we want.
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return &pb.CheckPortResponse{
			Available: false,
			Reason:    fmt.Sprintf("port %d is already in use on the host", port),
		}, nil
	}
	// Release immediately — this was only a probe. Close errors are irrelevant
	// (the OS reclaims the socket regardless); the port is confirmed bindable.
	_ = ln.Close()
	return &pb.CheckPortResponse{Available: true}, nil
}

func (s *Service) stackPath(slug string) string {
	return fmt.Sprintf("%s/%s.yml", s.stackDir, slug)
}
