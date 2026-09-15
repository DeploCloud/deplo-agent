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

const traefikContainer = "deplo-traefik"

func ensureTenantNetwork(ctx context.Context, network string) error {
	if network == "" {
		return fmt.Errorf("no network was sent for this stack")
	}
	if !dockercli.IsTenantNetwork(network) {
		return fmt.Errorf("%q is not a tenant network", network)
	}
	if err := dockercli.EnsureNetwork(ctx, network); err != nil {
		return err
	}
	if exists, _ := dockercli.State(ctx, traefikContainer); !exists {
		return nil
	}
	return dockercli.ConnectNetwork(ctx, network, traefikContainer)
}

func ensureTenantNetworkWarned(ctx context.Context, network string) (string, error) {
	if err := ensureTenantNetwork(ctx, network); err != nil {
		return "", err
	}
	names, ok := dockercli.ListNetworks(ctx)
	if !ok {
		return "", nil
	}
	reconnectTraefikToTenantNetworks(ctx, dockercli.TenantNetworksOf(names))
	return dockercli.NetworkHeadroomFor(ctx, len(names)), nil
}

func reconnectTraefikToTenantNetworks(ctx context.Context, tenant []string) {
	on, exists := dockercli.ContainerNetworks(ctx, traefikContainer)
	if !exists {
		return
	}
	for _, n := range missingNetworks(on, tenant) {
		_ = dockercli.ConnectNetwork(ctx, n, traefikContainer)
	}
}

func missingNetworks(on map[string]bool, want []string) []string {
	var out []string
	for _, n := range want {
		if !on[n] {
			out = append(out, n)
		}
	}
	return out
}

// SetAgentDir tells the service where the agent's own data lives (the installer's $AGENT_DATA, i.e.
func (s *Service) SetAgentDir(dir string) { s.agentDir = dir }

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

// HostInfo answers what this host IS - see the proto.
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

func dockerRootDir(ctx context.Context) string {
	res, err := dockercli.Run(ctx, 10*time.Second, "info", "-f", "{{.DockerRootDir}}")
	if err != nil || res.Code != 0 {
		return ""
	}
	return strings.TrimSpace(res.Stdout)
}

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
	return s.hostInfo(ctx, req.GetDataDir(), req.GetControlPlaneHint()), nil
}

// TraefikConfig restarts, or rewrites and restarts, this host's deplo-traefik stack.
func (s *Service) TraefikConfig(ctx context.Context, req *pb.TraefikConfigRequest) (*pb.TraefikConfigResponse, error) {
	path := s.traefikCompose()
	if path == "" {
		return &pb.TraefikConfigResponse{
			Ok:    false,
			Error: "this agent has no data directory configured, so it does not manage a Traefik stack",
		}, nil
	}
	if _, err := os.Stat(path); err != nil {
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
		if err := os.Rename(path, path+".bak"); err != nil {
			return &pb.TraefikConfigResponse{
				Ok:    false,
				Error: fmt.Sprintf("could not back up the current Traefik config: %v", err),
			}, nil
		}
		if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
			_ = os.Rename(path+".bak", path)
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

func (s *Service) applyTraefik(ctx context.Context, path string, restartOnly bool) error {
	if s.traefikApply != nil {
		return s.traefikApply(ctx, path, restartOnly)
	}
	return s.bringUpTraefik(ctx, path, restartOnly)
}

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
	res, err := dockercli.Run(ctx, 180*time.Second,
		"compose", "-f", path, "up", "-d", "--force-recreate", "--remove-orphans")
	if err != nil {
		return fmt.Errorf("could not apply the Traefik configuration: %v", err)
	}
	if res.Code != 0 {
		return fmt.Errorf("could not apply the Traefik configuration: %s", firstLine(res.Stderr))
	}
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
	cmd := exec.Command("sh", "-c", fmt.Sprintf("sleep 2; docker restart %s", id))
	if err := cmd.Start(); err != nil {
		return &pb.RestartControlPlaneResponse{
			Ok:    false,
			Error: fmt.Sprintf("could not schedule the restart: %v", err),
		}, nil
	}
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

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func stackPreviewNetworks(ctx context.Context, slug string) []string {
	res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-a",
		"--filter", "label=com.docker.compose.project=deplo-"+slug,
		"--format", "{{.Networks}}")
	if err != nil || res.Code != 0 {
		return nil
	}
	return previewNetworksIn(res.Stdout)
}

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

func removePreviewNetworks(ctx context.Context, names []string) {
	for _, n := range names {
		if !strings.HasPrefix(n, "deplo-preview-") {
			continue
		}
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "disconnect", "-f", n, traefikContainer)
		_, _ = dockercli.Run(ctx, 20*time.Second, "network", "rm", n)
	}
}

var installerURL = "https://raw.githubusercontent.com/DeploCloud/deplo/main/install.sh"

const controlPlaneUpdateLog = "/var/log/deplo-update.log"

const controlPlaneUpdateUnit = "deplo-control-plane-update"

var controlPlaneVersion = regexp.MustCompile(`^[0-9]{1,5}\.[0-9]{1,5}\.[0-9]{1,5}$`)

// UpdateControlPlane re-runs the Deplo installer on this host.
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
	if _, err := os.Stat("/opt/deplo/.env"); err == nil {
		return "/opt/deplo"
	}
	return ""
}

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
	go func() { _ = cmd.Wait() }()
	return nil
}
