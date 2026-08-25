package server

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The one thing TraefikConfig must never do is rewrite a proxy Deplo did not install.
// install-agent.sh explicitly refuses to fight for :80/:443 when an operator already
// runs their own Traefik — and a remote rewrite of that config would be a far worse
// version of the same mistake.

func TestTraefikConfigRefusesWhenDeploDidNotInstallTraefik(t *testing.T) {
	ctx := context.Background()

	t.Run("no agent dir configured", func(t *testing.T) {
		svc := New(t.TempDir(), t.TempDir(), "/", "")
		res, err := svc.TraefikConfig(ctx, &pb.TraefikConfigRequest{RestartOnly: true})
		if err != nil {
			t.Fatalf("the refusal is a result, not an RPC error: %v", err)
		}
		if res.GetOk() {
			t.Fatal("must not claim success with no Traefik stack to manage")
		}
		if res.GetError() == "" {
			t.Error("a refusal must say why")
		}
	})

	t.Run("agent dir with no traefik stack", func(t *testing.T) {
		svc := New(t.TempDir(), t.TempDir(), "/", "")
		svc.SetAgentDir(t.TempDir()) // exists, but holds no traefik/ dir
		res, err := svc.TraefikConfig(ctx, &pb.TraefikConfigRequest{
			ComposeYaml: "services:\n  traefik:\n    image: traefik:v3.7\n",
		})
		if err != nil {
			t.Fatalf("unexpected RPC error: %v", err)
		}
		if res.GetOk() {
			t.Fatal("must refuse to install a Traefik stack where there was none")
		}
		if !strings.Contains(res.GetError(), "did not install Traefik") {
			t.Errorf("the message must name the reason, got %q", res.GetError())
		}
	})
}

func TestTraefikConfigRejectsAnEmptyConfig(t *testing.T) {
	svc, path := serviceWithTraefik(t, "services:\n  traefik:\n    image: traefik:v3.7\n")
	original := readFileOrEmpty(path)

	res, err := svc.TraefikConfig(context.Background(), &pb.TraefikConfigRequest{ComposeYaml: "  \n"})
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}
	if res.GetOk() {
		t.Fatal("an empty config must be refused, not written")
	}
	// The refusal must land before the file is touched: an empty compose file
	// would take every app on the host off the internet at the next bring-up.
	if got := readFileOrEmpty(path); got != original {
		t.Errorf("the stack file was modified by a refused request: %q", got)
	}
	if _, err := os.Stat(path + ".bak"); err == nil {
		t.Error("a refused request must not leave a backup behind either")
	}
}

// A rewrite that fails to come up must leave the host running its OLD config,
// not merely holding a good file it is not running.
func TestTraefikConfigRestoresThePreviousConfigWhenBringUpFails(t *testing.T) {
	const original = "services:\n  traefik:\n    image: traefik:v3.7\n    container_name: deplo-traefik\n"
	svc, path := serviceWithTraefik(t, original)

	// What the bring-up saw, in order. The rollback must not merely restore the
	// file — it must bring the restored file back up.
	var appliedContent []string
	svc.traefikApply = func(_ context.Context, p string, _ bool) error {
		appliedContent = append(appliedContent, readFileOrEmpty(p))
		if len(appliedContent) == 1 {
			return errors.New("traefik exited immediately")
		}
		return nil
	}

	res, err := svc.TraefikConfig(context.Background(), &pb.TraefikConfigRequest{
		ComposeYaml: "services:\n  traefik:\n    image: traefik:v3.7\n    command: [--api.dashboard=true]\n",
	})
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}
	if res.GetOk() {
		t.Fatal("a failed bring-up must be reported as a failure")
	}
	if got := readFileOrEmpty(path); got != original {
		t.Errorf("the previous config must be restored on failure, got:\n%s", got)
	}
	if len(appliedContent) != 2 {
		t.Fatalf("expected a failed apply then a rollback apply, got %d", len(appliedContent))
	}
	if appliedContent[1] != original {
		t.Error("the rollback must bring the RESTORED file up, not leave the host down")
	}
	if !strings.Contains(res.GetError(), "restored") {
		t.Errorf("the operator must be told the rollback happened, got %q", res.GetError())
	}
	// The returned YAML is read back off disk, so the control plane sees what is
	// actually running rather than what it asked for.
	if res.GetComposeYaml() != original {
		t.Error("the response must carry the config the host ended up with")
	}
}

// The happy path: the file is replaced, a backup of the outgoing config is kept,
// and the answer carries what landed on disk.
func TestTraefikConfigWritesAndKeepsABackup(t *testing.T) {
	const original = "services:\n  traefik:\n    image: traefik:v3.7\n"
	const updated = "services:\n  traefik:\n    image: traefik:v3.7\n    command: [--api.dashboard=true]\n"
	svc, path := serviceWithTraefik(t, original)
	svc.traefikApply = func(context.Context, string, bool) error { return nil }

	res, err := svc.TraefikConfig(context.Background(), &pb.TraefikConfigRequest{ComposeYaml: updated})
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetOk() {
		t.Fatalf("expected success, got %q", res.GetError())
	}
	if got := readFileOrEmpty(path); got != updated {
		t.Errorf("the new config must be on disk, got:\n%s", got)
	}
	if res.GetComposeYaml() != updated {
		t.Error("the response must echo what is on disk")
	}
	// A Traefik change can take :80/:443 down for every app on the host; the way
	// back must not depend on the control plane still being reachable.
	if got := readFileOrEmpty(path + ".bak"); got != original {
		t.Errorf("the outgoing config must be kept as .bak, got:\n%s", got)
	}
}

// The config can carry the private key of a TLS certificate the operator pasted in, and
// the .bak is a copy of the same secret.
func TestTraefikConfigIsNotWorldReadable(t *testing.T) {
	svc, path := serviceWithTraefik(t, "services:\n  traefik:\n    image: traefik:v3.7\n")
	svc.traefikApply = func(context.Context, string, bool) error { return nil }

	res, err := svc.TraefikConfig(context.Background(), &pb.TraefikConfigRequest{
		ComposeYaml: "services:\n  traefik:\n    image: traefik:v3.7\n    command: [--api.dashboard=true]\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetOk() {
		t.Fatalf("expected success, got %q", res.GetError())
	}
	for _, p := range []string{path, path + ".bak"} {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if mode := st.Mode().Perm(); mode != 0o600 {
			t.Errorf("%s must be 0600, got %#o", filepath.Base(p), mode)
		}
	}
}

// restart_only must never look at compose_yaml — the plain "restart Traefik"
// button must not become a silent config change.
func TestTraefikConfigRestartOnlyLeavesTheFileAlone(t *testing.T) {
	const original = "services:\n  traefik:\n    image: traefik:v3.7\n"
	svc, path := serviceWithTraefik(t, original)
	var sawRestartOnly bool
	svc.traefikApply = func(_ context.Context, _ string, restartOnly bool) error {
		sawRestartOnly = restartOnly
		return nil
	}

	res, err := svc.TraefikConfig(context.Background(), &pb.TraefikConfigRequest{
		RestartOnly: true,
		ComposeYaml: "services:\n  evil:\n    image: nope\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetOk() {
		t.Fatalf("expected success, got %q", res.GetError())
	}
	if !sawRestartOnly {
		t.Error("a restart must be applied as a restart, not a recreate")
	}
	if got := readFileOrEmpty(path); got != original {
		t.Errorf("restart_only must ignore compose_yaml entirely, got:\n%s", got)
	}
}

func TestRestartControlPlaneRefusesAnUnresolvableHint(t *testing.T) {
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	for _, hint := range []string{"", "definitely-not-a-container-abc123"} {
		res, err := svc.RestartControlPlane(context.Background(),
			&pb.RestartControlPlaneRequest{ControlPlaneHint: hint})
		if err != nil {
			t.Fatalf("unexpected RPC error: %v", err)
		}
		if res.GetOk() {
			t.Fatalf("hint %q must not schedule a restart", hint)
		}
		// "Did my panel restart?" is not answerable by looking, so a no-op that
		// reported success would be worse than an error.
		if res.GetError() == "" {
			t.Errorf("hint %q must explain the refusal", hint)
		}
	}
}

func TestHostInfoReportsTheHostAndIsNeverAnError(t *testing.T) {
	svc := New(t.TempDir(), t.TempDir(), "/", "")
	res, err := svc.HostInfo(context.Background(), &pb.HostInfoRequest{})
	if err != nil {
		t.Fatalf("HostInfo must not fail on a half-broken host: %v", err)
	}
	if res.GetTimeUnixMs() <= 0 {
		t.Error("the clock is always readable")
	}
	if res.GetDiskTotalBytes() <= 0 {
		t.Error("statfs on the data dir must report a total")
	}
	// No agent dir => no stack of ours => the control plane must see the empty
	// string, which is what tells it not to offer the dashboard toggle.
	if res.GetTraefikComposeYaml() != "" {
		t.Error("an agent with no data dir must report no Traefik stack")
	}
}

func TestHostInfoReportsTheTraefikStackWhenDeploInstalledIt(t *testing.T) {
	const yaml = "services:\n  traefik:\n    image: traefik:v3.7\n"
	svc, _ := serviceWithTraefik(t, yaml)

	res, err := svc.HostInfo(context.Background(), &pb.HostInfoRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetTraefikComposeYaml() != yaml {
		t.Errorf("the stack file must come back verbatim, got %q", res.GetTraefikComposeYaml())
	}
}

// serviceWithTraefik builds a Service whose agent dir holds a deplo-traefik
// stack, as install-agent.sh leaves it.
func serviceWithTraefik(t *testing.T, yaml string) (*Service, string) {
	t.Helper()
	agentDir := t.TempDir()
	dir := filepath.Join(agentDir, "traefik")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "docker-compose.yml")
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")
	svc.SetAgentDir(agentDir)
	return svc, path
}
