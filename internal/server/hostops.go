package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/hostinfo"
)

// The host-level verbs behind the "hostops" capability. The agent's whole security
// value is that the control plane can only ask for what the proto enumerates; a
// RunCommand RPC would throw that away for convenience.

// traefikContainer is the name install-agent.sh gives the Traefik it installs.
const traefikContainer = "deplo-traefik"

// ensureTenantNetwork creates the stack's own network and puts Traefik on it. Every
// deploy re-asserts both: the network may be new, and Traefik may have been recreated
// (which drops every attachment not in its compose file).
func ensureTenantNetwork(ctx context.Context, network string) error {
	if network == "" {
		return fmt.Errorf("no network was sent for this stack")
	}
	// Only ever a name Deplo mints for a tenant. The control plane is trusted, but
	// this is what a stack joins: a `deplo` arriving here would put a tenant beside
	// the panel, and nothing else on this side would say so.
	if !dockercli.IsTenantNetwork(network) {
		return fmt.Errorf("%q is not a tenant network", network)
	}
	if err := dockercli.EnsureNetwork(ctx, network); err != nil {
		return err
	}
	// Traefik is absent on a host that runs no proxy; that is not a deploy failure.
	if exists, _ := dockercli.State(ctx, traefikContainer); !exists {
		return nil
	}
	return dockercli.ConnectNetwork(ctx, network, traefikContainer)
}

// ensureTenantNetworkWarned is ensureTenantNetwork plus the one thing the operator
// cannot find out for themselves: that this host is nearly out of docker networks.
// Said BEFORE the deploy that would fail, which is the whole point.
func ensureTenantNetworkWarned(ctx context.Context, network string) (string, error) {
	if err := ensureTenantNetwork(ctx, network); err != nil {
		return "", err
	}
	// One listing feeds both the reconnect and the headroom check.
	names, ok := dockercli.ListNetworks(ctx)
	if !ok {
		return "", nil
	}
	// Traefik is put back on EVERY tenant network here, not just this deploy's.
	// `applyTraefik` reconnects them after its own `--force-recreate`, but a proxy
	// recreated any other way - the installer, a hand-run `compose up` in the
	// traefik dir - comes back attached to nothing, and every site on the host 404s
	// until each app happens to be deployed again. A deploy is the natural moment
	// to heal that.
	reconnectTraefikToTenantNetworks(ctx, dockercli.TenantNetworksOf(names))
	return dockercli.NetworkHeadroomFor(ctx, len(names)), nil
}

// reconnectTraefikToTenantNetworks puts the proxy back on every tenant network it
// is missing. Best-effort and quiet: a failure here must never fail a deploy. One
// inspect says which it is already on, so the common deploy connects to nothing.
func reconnectTraefikToTenantNetworks(ctx context.Context, tenant []string) {
	on, exists := dockercli.ContainerNetworks(ctx, traefikContainer)
	if !exists {
		return
	}
	for _, n := range missingNetworks(on, tenant) {
		_ = dockercli.ConnectNetwork(ctx, n, traefikContainer)
	}
}

// missingNetworks is the subset of `want` a container is not on yet.
func missingNetworks(on map[string]bool, want []string) []string {
	var out []string
	for _, n := range want {
		if !on[n] {
			out = append(out, n)
		}
	}
	return out
}

// SetAgentDir tells the service where the agent's own data lives (the installer's
// $AGENT_DATA, i.e. --agent-dir) - the parent of the Traefik stack this manages.
func (s *Service) SetAgentDir(dir string) { s.agentDir = dir }

// traefikDir is where install-agent.sh puts the Traefik it installs:
// $AGENT_DATA/traefik, holding docker-compose.yml and acme/.
func (s *Service) traefikDir() string {
	if s.agentDir == "" {
		return ""
	}
	return filepath.Join(s.agentDir, "traefik")
}

func (s *Service) traefikCompose() string {
	dir := s.traefikDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "docker-compose.yml")
}

// HostInfo answers what this host IS - see the proto. Like Hello it never fails:
// every field is best-effort, because an operator opening the hardware panel on
// a half-broken box should still learn what they can.
func (s *Service) HostInfo(ctx context.Context, req *pb.HostInfoRequest) (*pb.HostInfoResponse, error) {
	return s.hostInfo(ctx, req.GetDataDir(), req.GetControlPlaneHint()), nil
}

func (s *Service) hostInfo(ctx context.Context, dataDir, cpHint string) *pb.HostInfoResponse {
	if dataDir == "" {
		dataDir = s.dataDir
	}
	info := hostinfo.Collect(dataDir)

	dockerVersion, dockerRoot := "", ""
	if v, available := dockercli.Server(ctx); available {
		dockerVersion = v
		dockerRoot = dockerRootDir(ctx)
	}

	// The Traefik stack file, when Deplo installed it here. Empty is the signal
	// the control plane needs: it means "there is no stack of ours to rewrite",
	// which is exactly when the dashboard toggle must not be offered.
	traefikYaml := ""
	if path := s.traefikCompose(); path != "" {
		if b, err := os.ReadFile(path); err == nil {
			traefikYaml = string(b)
		}
	}

	return &pb.HostInfoResponse{
		CpuModel:              info.CPUModel,
		CpuCores:              int32(info.CPUCores),
		CpuThreads:            int32(info.CPUThreads),
		MemTotalBytes:         info.MemTotalBytes,
		DiskTotalBytes:        info.DiskTotalBytes,
		DiskUsedBytes:         info.DiskUsedBytes,
		OsPretty:              info.OSPretty,
		Kernel:                info.Kernel,
		Arch:                  info.Arch,
		DockerVersion:         dockerVersion,
		DockerRootDir:         dockerRoot,
		UptimeSec:             info.UptimeSec,
		Timezone:              info.Timezone,
		TimeUnixMs:            info.TimeUnixMs,
		UtcOffsetMinutes:      info.UTCOffsetMinutes,
		TraefikComposeYaml:    traefikYaml,
		ControlPlaneContainer: resolveContainer(ctx, cpHint),
	}
}

// dockerRootDir is where images and volumes actually live, which on a host with
// a mounted data disk is not the root filesystem the operator is looking at.
func dockerRootDir(ctx context.Context) string {
	res, err := dockercli.Run(ctx, 10*time.Second, "info", "-f", "{{.DockerRootDir}}")
	if err != nil || res.Code != 0 {
		return ""
	}
	return strings.TrimSpace(res.Stdout)
}

// resolveContainer turns the caller's self-identifying hint into a container id, or ""
// if it names nothing running.
func resolveContainer(ctx context.Context, hint string) string {
	hint = strings.TrimSpace(hint)
	if hint == "" {
		return ""
	}
	res, err := dockercli.Run(ctx, 10*time.Second, "inspect", "-f", "{{.Id}}\t{{.State.Running}}", hint)
	if err != nil || res.Code != 0 {
		return ""
	}
	id, running, ok := strings.Cut(strings.TrimSpace(res.Stdout), "\t")
	if !ok || strings.TrimSpace(running) != "true" {
		return ""
	}
	return id
}

// SetTimezone moves the host clock.
func (s *Service) SetTimezone(ctx context.Context, req *pb.SetTimezoneRequest) (*pb.HostInfoResponse, error) {
	tz := strings.TrimSpace(req.GetTimezone())
	if !hostinfo.KnownTimezone(tz) {
		return nil, status.Errorf(codes.InvalidArgument,
			"%q is not a timezone this host knows about", tz)
	}
	if err := hostinfo.SetTimezone(ctx, tz); err != nil {
		return nil, status.Errorf(codes.Internal, "could not set the timezone: %v", err)
	}
	// A fresh read, not an echo of the request: the point of the answer is to
	// show the clock that MOVED, and a host where the write silently did not
	// take should say so rather than parrot the name back.
	return s.hostInfo(ctx, req.GetDataDir(), req.GetControlPlaneHint()), nil
}

// TraefikConfig restarts, or rewrites and restarts, this host's deplo-traefik
// stack. The YAML is rendered control-plane-side (ADR-0006) and applied here
// verbatim; the agent's job is the file and the bring-up, never the labels.
func (s *Service) TraefikConfig(ctx context.Context, req *pb.TraefikConfigRequest) (*pb.TraefikConfigResponse, error) {
	path := s.traefikCompose()
	if path == "" {
		return &pb.TraefikConfigResponse{
			Ok:    false,
			Error: "this agent has no data directory configured, so it does not manage a Traefik stack",
		}, nil
	}
	if _, err := os.Stat(path); err != nil {
		// Either Traefik was never installed here, or the operator runs their own
		// proxy - install-agent.sh skips its Traefik when one is already up. Both
		// mean the same thing: there is nothing of OURS to reconfigure.
		return &pb.TraefikConfigResponse{
			Ok: false,
			Error: "Deplo did not install Traefik on this host, so it cannot manage it here. " +
				"This server is either behind your own reverse proxy or has no proxy at all.",
		}, nil
	}

	if !req.GetRestartOnly() {
		yaml := req.GetComposeYaml()
		if strings.TrimSpace(yaml) == "" {
			return &pb.TraefikConfigResponse{Ok: false, Error: "no Traefik configuration was sent"}, nil
		}
		// Keep the outgoing file. A Traefik config change can take :80/:443 down
		// for every app on the host, and the operator's way back must not depend
		// on the control plane still being able to reach this agent.
		if err := os.Rename(path, path+".bak"); err != nil {
			return &pb.TraefikConfigResponse{
				Ok:    false,
				Error: fmt.Sprintf("could not back up the current Traefik config: %v", err),
			}, nil
		}
		// 0600, not 0644: this file can carry the private key of a TLS certificate the
		// operator installed (an inline compose config), and acme.json beside it is 0600 for
		// exactly the same reason.
		if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
			_ = os.Rename(path+".bak", path) // put it back; nothing has restarted yet
			return &pb.TraefikConfigResponse{
				Ok:    false,
				Error: fmt.Sprintf("could not write the Traefik config: %v", err),
			}, nil
		}
		_ = os.Chmod(path, 0o600)
		_ = os.Chmod(path+".bak", 0o600)
	}

	if err := s.applyTraefik(ctx, path, req.GetRestartOnly()); err != nil {
		if !req.GetRestartOnly() {
			// The new config did not come up. Restore the old file AND bring it
			// back up, so the host is left routing rather than merely holding a
			// good file it is not running.
			if rerr := os.Rename(path+".bak", path); rerr == nil {
				_ = s.applyTraefik(ctx, path, false)
			}
			return &pb.TraefikConfigResponse{
				Ok:          false,
				Error:       fmt.Sprintf("%v - the previous Traefik config was restored", err),
				ComposeYaml: readFileOrEmpty(path),
			}, nil
		}
		return &pb.TraefikConfigResponse{Ok: false, Error: err.Error(), ComposeYaml: readFileOrEmpty(path)}, nil
	}

	return &pb.TraefikConfigResponse{Ok: true, ComposeYaml: readFileOrEmpty(path)}, nil
}

// applyTraefik is the seam over bringUpTraefik.
func (s *Service) applyTraefik(ctx context.Context, path string, restartOnly bool) error {
	if s.traefikApply != nil {
		return s.traefikApply(ctx, path, restartOnly)
	}
	return s.bringUpTraefik(ctx, path, restartOnly)
}

// bringUpTraefik applies the stack file.
func (s *Service) bringUpTraefik(ctx context.Context, path string, restartOnly bool) error {
	if restartOnly {
		res, err := dockercli.Run(ctx, 90*time.Second, "restart", traefikContainer)
		if err != nil {
			return fmt.Errorf("could not restart Traefik: %v", err)
		}
		if res.Code != 0 {
			return fmt.Errorf("could not restart Traefik: %s", firstLine(res.Stderr))
		}
		return nil
	}
	// --force-recreate because a static-config change lives in `command:`, and
	// compose considers a container with an unchanged image+config current.
	res, err := dockercli.Run(ctx, 180*time.Second,
		"compose", "-f", path, "up", "-d", "--force-recreate", "--remove-orphans")
	if err != nil {
		return fmt.Errorf("could not apply the Traefik configuration: %v", err)
	}
	if res.Code != 0 {
		return fmt.Errorf("could not apply the Traefik configuration: %s", firstLine(res.Stderr))
	}
	// The recreated container only has the networks its compose file names, so every
	// tenant network a deploy attached it to is gone. Put them back before returning,
	// or every site on this host 404s until its app is deployed again.
	for _, n := range dockercli.DeploNetworks(ctx) {
		if err := dockercli.ConnectNetwork(ctx, n, traefikContainer); err != nil {
			return fmt.Errorf("could not reconnect Traefik to %s: %v", n, err)
		}
	}
	return nil
}

// RestartControlPlane bounces the container the Deplo panel runs in on this host.
func (s *Service) RestartControlPlane(ctx context.Context, req *pb.RestartControlPlaneRequest) (*pb.RestartControlPlaneResponse, error) {
	id := resolveContainer(ctx, req.GetControlPlaneHint())
	if id == "" {
		return &pb.RestartControlPlaneResponse{
			Ok: false,
			Error: "Deplo is not running as a container on this host that the agent can restart, " +
				"so it has to be restarted the way it was started.",
		}, nil
	}
	// Deliberately NOT CommandContext: the command must outlive this RPC (and the
	// process it restarts), so it must not be cancelled when the call returns.
	cmd := exec.Command("sh", "-c", fmt.Sprintf("sleep 2; docker restart %s", id))
	if err := cmd.Start(); err != nil {
		return &pb.RestartControlPlaneResponse{
			Ok:    false,
			Error: fmt.Sprintf("could not schedule the restart: %v", err),
		}, nil
	}
	// Reap it in the background so the agent does not accumulate a zombie for
	// every restart. The agent survives the container it just bounced.
	go func() { _ = cmd.Wait() }()

	return &pb.RestartControlPlaneResponse{Ok: true, Container: id}, nil
}

func readFileOrEmpty(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(b)
}

// firstLine keeps an error message to the one line the operator needs; docker's
// stderr on a failed compose up can run to dozens.
func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// stackPreviewNetworks lists the `deplo-preview-*` networks a stack's containers are
// on. A preview's network holds that one stack and nothing else, so once the stack is
// gone the network is litter - and address space, which is the scarce thing. An
// Environment's network is shared and is never in this list.
func stackPreviewNetworks(ctx context.Context, slug string) []string {
	// `docker ps` prints each container's networks itself, comma-joined: one call
	// for the whole stack instead of an inspect per container.
	res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-a",
		"--filter", "label=com.docker.compose.project=deplo-"+slug,
		"--format", "{{.Networks}}")
	if err != nil || res.Code != 0 {
		return nil
	}
	return previewNetworksIn(res.Stdout)
}

// previewNetworksIn picks the distinct `deplo-preview-*` names out of a `docker ps
// --format {{.Networks}}` listing (one comma-joined line per container).
func previewNetworksIn(psOut string) []string {
	seen := map[string]bool{}
	var out []string
	for _, n := range strings.FieldsFunc(psOut, func(r rune) bool { return r == ',' || r == '\n' || r == ' ' }) {
		if strings.HasPrefix(n, "deplo-preview-") && !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

// removePreviewNetworks takes Traefik off each network and removes it. Best-effort:
// a network that still holds a container is refused by docker and left for the
// leftover-networks cleanup scope, never a reason to fail a destroy that succeeded.
func removePreviewNetworks(ctx context.Context, names []string) {
	for _, n := range names {
		if !strings.HasPrefix(n, "deplo-preview-") {
			continue
		}
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "disconnect", "-f", n, traefikContainer)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", n)
	}
}

// The installer, which is also the updater: re-running it on a host that already
// has Deplo pulls the new image, dumps the database first and puts the old image
// back if the new one does not come up.
var installerURL = "https://raw.githubusercontent.com/DeploCloud/deplo/main/install.sh"

// Where the run is transcribed. Named in the reply because an update that did not
// take leaves the operator on the old version with nothing to read.
const controlPlaneUpdateLog = "/var/log/deplo-update.log"

// The transient systemd unit the updater runs in. It must NOT be a child of the
// agent: the agent's unit kills its whole cgroup on restart, which would abandon
// an update halfway through.
const controlPlaneUpdateUnit = "deplo-control-plane-update"

// MAJOR.MINOR.PATCH and nothing else - the value reaches a script that runs as root.
var controlPlaneVersion = regexp.MustCompile(`^[0-9]{1,5}\.[0-9]{1,5}\.[0-9]{1,5}$`)

// UpdateControlPlane re-runs the Deplo installer on this host. The version is the
// only thing the caller decides; the script itself is the agent's own constant.
func (s *Service) UpdateControlPlane(ctx context.Context, req *pb.UpdateControlPlaneRequest) (*pb.UpdateControlPlaneResponse, error) {
	version := strings.TrimSpace(req.GetVersion())
	if version != "" && !controlPlaneVersion.MatchString(version) {
		return nil, status.Errorf(codes.InvalidArgument, "%q is not a version", version)
	}
	id := resolveContainer(ctx, req.GetControlPlaneHint())
	if id == "" {
		return &pb.UpdateControlPlaneResponse{
			Ok: false,
			Error: "Deplo is not running as a container on this host that the agent can see, " +
				"so it has to be updated the way it was installed.",
		}, nil
	}
	dir := controlPlaneDir(ctx, id)
	if dir == "" {
		return &pb.UpdateControlPlaneResponse{
			Ok:    false,
			Error: "The agent cannot find the directory Deplo was installed in on this host.",
		}, nil
	}
	script, err := installerScript(ctx, dir)
	if err != nil {
		return &pb.UpdateControlPlaneResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := startInstaller(script, dir, version); err != nil {
		return &pb.UpdateControlPlaneResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.UpdateControlPlaneResponse{Ok: true, LogPath: controlPlaneUpdateLog}, nil
}

// controlPlaneDir is the directory the panel's compose project was brought up in
// (/opt/deplo on an ordinary install), read off the container compose labelled it.
func controlPlaneDir(ctx context.Context, id string) string {
	res, err := dockercli.Run(ctx, 10*time.Second, "inspect", "-f",
		`{{index .Config.Labels "com.docker.compose.project.working_dir"}}`, id)
	if err == nil && res.Code == 0 {
		if dir := strings.TrimSpace(res.Stdout); dir != "" && dir != "<no value>" {
			if _, err := os.Stat(filepath.Join(dir, ".env")); err == nil {
				return dir
			}
		}
	}
	// A panel started outside compose still lives where the installer puts it.
	if _, err := os.Stat("/opt/deplo/.env"); err == nil {
		return "/opt/deplo"
	}
	return ""
}

// installerScript downloads the current installer next to the instance it updates,
// falling back to the copy on disk when this host cannot reach GitHub.
func installerScript(ctx context.Context, dir string) (string, error) {
	path := filepath.Join(dir, ".deplo-update.sh")
	body, err := fetchInstaller(ctx)
	if err != nil {
		local := filepath.Join(dir, "install.sh")
		if _, statErr := os.Stat(local); statErr == nil {
			return local, nil
		}
		return "", fmt.Errorf("could not download the Deplo installer: %v", err)
	}
	if err := os.WriteFile(path, body, 0o700); err != nil {
		return "", fmt.Errorf("could not write the installer to %s: %v", path, err)
	}
	return path, nil
}

// fetchInstaller reads the installer over HTTPS. A body that is not a script is
// refused rather than run: a captive portal or an error page would otherwise be
// executed as root.
func fetchInstaller(ctx context.Context) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, installerURL, nil)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s answered %d", installerURL, res.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(body, []byte("#!")) {
		return nil, fmt.Errorf("%s did not answer with a script", installerURL)
	}
	return body, nil
}

// startInstaller runs it detached, so it survives both the panel it restarts and
// the agent that started it. systemd-run puts it in its own cgroup; without
// systemd the process is merely re-parented, which is the best this can do.
func startInstaller(script, dir, version string) error {
	log, err := os.OpenFile(controlPlaneUpdateLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("could not open %s: %v", controlPlaneUpdateLog, err)
	}
	defer log.Close()
	args := []string{script, "--yes", "--plain", "--quiet"}
	env := append(os.Environ(), "DEPLO_LOG_FILE="+controlPlaneUpdateLog)
	if version != "" {
		env = append(env, "DEPLO_VERSION="+version)
	}
	if systemd, lookErr := exec.LookPath("systemd-run"); lookErr == nil {
		run := []string{
			"--collect", "--unit=" + controlPlaneUpdateUnit,
			"--property=StandardOutput=append:" + controlPlaneUpdateLog,
			"--property=StandardError=append:" + controlPlaneUpdateLog,
			"--setenv=DEPLO_LOG_FILE=" + controlPlaneUpdateLog,
		}
		if version != "" {
			run = append(run, "--setenv=DEPLO_VERSION="+version)
		}
		run = append(run, "--working-directory="+dir, "/bin/bash")
		cmd := exec.Command(systemd, append(run, args...)...)
		out, runErr := cmd.CombinedOutput()
		if runErr == nil {
			return nil
		}
		// A unit of this name still running IS the answer: two installers at once
		// on one host is the failure this refusal exists for.
		if strings.Contains(string(out), "already exists") {
			return fmt.Errorf("an update is already running on this host")
		}
		fmt.Fprintf(log, "[deplo] systemd-run failed (%v): %s\n", runErr, strings.TrimSpace(string(out)))
	}
	cmd := exec.Command("/bin/bash", args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("could not start the installer: %v", err)
	}
	// Reaped in the background: the agent outlives the panel it just replaced.
	go func() { _ = cmd.Wait() }()
	return nil
}
