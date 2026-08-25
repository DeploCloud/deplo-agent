package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// sshgateway.go ports lib/infra/ssh-gateway.ts to the agent (PLAN Part D): the per-host
// SSH gateway singleton (ADR-0002). The agent never re-implements the security-critical
// wrapper / sshd_config / allowlist.

const (
	gatewayProject   = "deplo-ssh-gateway"
	gatewayContainer = "deplo-ssh-gateway"
	// GatewayHostDirSentinel is substituted in the rendered compose's bind path with the
	// agent's OWN ssh-gateway dir. The bind path is the only host-specific token in the
	// otherwise-opaque YAML, so this keeps the renderer single-source (D2).
	gatewayHostDirSentinel = "__DEPLO_GW_HOST_DIR__"
)

// gwDir is the host dir holding all gateway-managed files (host keys, sshd_config,
// wrapper, maps) — mirrors lib/infra/ssh-gateway.ts GW_DIR (<DATA_DIR>/ssh-gateway).
func (s *Service) gwDir() string { return filepath.Join(s.dataBase, "ssh-gateway") }

func (s *Service) gwStackFile() string { return filepath.Join(s.gwDir(), "docker-compose.yml") }

// EnsureGateway is DORMANT — dev mode (and its SSH gateway) was removed from the
// control plane (#33/#34). Kept only to satisfy the generated Agent interface; it
// refuses before touching Docker/ssh/fs. Never revive the body.
func (s *Service) EnsureGateway(ctx context.Context, req *pb.EnsureGatewayRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

// ProvisionSshUser is DORMANT — the SSH gateway was removed with dev mode
// (#33/#34). Kept only to satisfy the generated Agent interface; refuses before
// running any useradd/ssh/Docker work. Never revive the body.
func (s *Service) ProvisionSshUser(ctx context.Context, req *pb.ProvisionSshUserRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

// DeprovisionSshUser is DORMANT — the SSH gateway was removed with dev mode (#33/#34).
// Kept only to satisfy the generated Agent interface; refuses before running any
// deluser/ssh/Docker work.
func (s *Service) DeprovisionSshUser(ctx context.Context, req *pb.DeprovisionSshUserRequest) (*pb.StackResult, error) {
	return nil, status.Error(codes.Unimplemented, "dev mode has been removed")
}

// ensureGateway writes the rendered config files, brings the stack up, and waits
// for sshd. Mirrors lib/infra/ssh-gateway.ts ensureGateway (the file-write +
// compose-up half; user reconcile is separate so callers control which users).
func (s *Service) ensureGateway(ctx context.Context, cfg *pb.GatewayConfig) error {
	if cfg == nil {
		return status.Error(codes.InvalidArgument, "gateway config is required")
	}
	if err := dockercli.EnsureNetwork(ctx, "deplo"); err != nil {
		return fmt.Errorf("ensure network: %w", err)
	}
	if err := s.writeGatewayFiles(cfg); err != nil {
		return fmt.Errorf("write gateway files: %w", err)
	}
	res, err := dockercli.Run(ctx, 180*time.Second,
		"compose", "-p", gatewayProject, "-f", s.gwStackFile(), "up", "-d", "--remove-orphans")
	if err != nil {
		return err
	}
	if res.Code != 0 {
		return fmt.Errorf("gateway compose up failed: %s", res.Stderr)
	}
	s.waitGatewayReady(ctx, 60*time.Second)
	return nil
}

// writeGatewayFiles lands the rendered config on the bind mount. The compose's
// bind path sentinel is substituted with the agent's own gateway dir (see
// gatewayHostDirSentinel). Mirrors lib/infra/ssh-gateway.ts writeGatewayFiles.
func (s *Service) writeGatewayFiles(cfg *pb.GatewayConfig) error {
	dir := s.gwDir()
	if err := os.MkdirAll(filepath.Join(dir, "keys"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dir, "map"), 0o755); err != nil {
		return err
	}
	compose := strings.ReplaceAll(cfg.GetComposeYaml(), gatewayHostDirSentinel, dir)
	writes := []struct {
		name string
		body string
		mode os.FileMode
	}{
		{"docker-compose.yml", compose, 0o644},
		{"socket-filter.cfg", cfg.GetSocketFilterCfg(), 0o644},
		{"sshd_config", cfg.GetSshdConfig(), 0o644},
		{"deplo-dev-shell", cfg.GetWrapperScript(), 0o755},
		{"gateway-entrypoint", cfg.GetEntrypointScript(), 0o755},
	}
	for _, w := range writes {
		if w.body == "" {
			return fmt.Errorf("gateway config %q is empty", w.name)
		}
		p := filepath.Join(dir, w.name)
		if err := os.WriteFile(p, []byte(w.body), w.mode); err != nil {
			return err
		}
		if err := os.Chmod(p, w.mode); err != nil {
			return err
		}
	}
	return nil
}

// gatewayRunning reports whether the gateway container is up.
func (s *Service) gatewayRunning(ctx context.Context) bool {
	return dockercli.IsRunning(ctx, gatewayContainer)
}

// waitGatewayReady polls until the gateway is ready to PROVISION users — i.e. both sshd
// is installed AND the `devusers` group exists. Gating on the group closes that race.
func (s *Service) waitGatewayReady(ctx context.Context, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		res, err := dockercli.Run(ctx, 10*time.Second,
			"exec", gatewayContainer, "sh", "-c",
			"command -v sshd >/dev/null && getent group devusers >/dev/null && echo ok")
		if err == nil && strings.TrimSpace(res.Stdout) == "ok" {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(1500 * time.Millisecond):
		}
	}
}

// reconcileUsers runs every user's provision step-list inside the gateway. The
// control plane sends the FULL set (the store's truth), so a fresh gateway is
// rebuilt completely. Provisioning is idempotent. Mirrors reconcileGateway.
func (s *Service) reconcileUsers(ctx context.Context, users []*pb.UserSteps) {
	if !s.gatewayRunning(ctx) {
		return
	}
	for _, u := range users {
		for _, step := range u.GetSteps() {
			s.runGatewayStep(ctx, step)
		}
	}
}

// runGatewayStep runs one control-plane-computed step inside the gateway: `docker exec
// -i <gateway> <argv...>`, piping the step's `input` to stdin (so a password reaches
// chpasswd over stdin, never argv/env).
func (s *Service) runGatewayStep(ctx context.Context, step *pb.GatewayStep) {
	argv := step.GetArgv()
	if len(argv) == 0 {
		return
	}
	args := append([]string{"exec", "-i", gatewayContainer}, argv...)
	_, _ = dockercli.Stream(ctx, 30*time.Second, func(string) {}, step.GetInput(), args...)
}
