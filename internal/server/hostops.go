package server

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/hostinfo"
)

// The four host-level verbs behind the "hostops" capability. The agent's whole security
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
	if dockercli.Available(ctx) {
		dockerVersion = dockercli.ServerVersion(ctx)
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
