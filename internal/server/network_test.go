package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// An empty network is a control-plane bug, not a reason to fall back to a shared one:
// there is no shared network any more, so guessing would put the stack somewhere no
// Environment owns.
func TestEnsureTenantNetwork_emptyNameIsRefused(t *testing.T) {
	err := ensureTenantNetwork(context.Background(), "")
	if err == nil {
		t.Fatal("expected an error for an empty network name")
	}
	if !strings.Contains(err.Error(), "no network") {
		t.Errorf("error should name the missing network, got %q", err)
	}
}

// Two deploys of one Environment start together, both see no network and both
// create it. The loser must not fail the deploy: what it wanted now exists.
func TestEnsureNetwork_existingIsNotAFailure(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	const n = "deplo-env-agent-ensuretest"
	t.Cleanup(func() {
		_, _ = dockercli.Run(context.Background(), 20*time.Second, "network", "rm", n)
	})
	if err := dockercli.EnsureNetwork(ctx, n); err != nil {
		t.Fatalf("first EnsureNetwork: %v", err)
	}
	// The second call is the loser of the race: the network is already there.
	if err := dockercli.EnsureNetwork(ctx, n); err != nil {
		t.Fatalf("EnsureNetwork on an existing network must succeed, got: %v", err)
	}
	// And so is a create that races past the inspect, which is what actually happens.
	res, err := dockercli.Run(ctx, 15*time.Second, "network", "create", n)
	if err != nil || res.Code == 0 {
		t.Fatalf("expected docker to refuse the duplicate create, got code=%d err=%v", res.Code, err)
	}
	if !strings.Contains(res.Stderr, "already exists") {
		t.Errorf("the refusal EnsureNetwork tolerates changed shape: %q", res.Stderr)
	}
}

// Only Deplo's tenant namespace is ever a cleanup candidate or a Traefik reconnect
// target. The platform's own networks are declared in Traefik's compose file, and
// removing one would take the panel or the socket proxy off the air.
func TestIsTenantNetwork(t *testing.T) {
	for _, n := range []string{
		"deplo-env-environ_abc123",
		"deplo-team-team_abc123",
		"deplo-preview-shop__pr-42",
	} {
		if !dockercli.IsTenantNetwork(n) {
			t.Errorf("%q should be a tenant network", n)
		}
	}
	for _, n := range []string{
		"deplo", "deplo-internal", "deplo-socket", "bridge", "host", "",
		"deplo-traefik_default", "deplo-envious",
	} {
		if dockercli.IsTenantNetwork(n) {
			t.Errorf("%q must NOT be a tenant network", n)
		}
	}
}

// An emptied Environment leaves its network with ONLY Traefik on it, because a deploy
// attached the proxy and nothing detaches it. Counting the proxy made every tenant
// network look busy forever, so the scope reclaimed nothing at all.
func TestAttachedExcludingProxy(t *testing.T) {
	cases := []struct {
		names string
		want  int
	}{
		{"", 0},
		{"deplo-traefik", 0},
		{"deplo-traefik deplo-shop-web-1", 1},
		{"deplo-shop-web-1 deplo-traefik deplo-shop-redis-1", 2},
		{"deplo-traefik-lookalike", 1},
	}
	for _, c := range cases {
		if got := attachedExcludingProxy(c.names); got != c.want {
			t.Errorf("attachedExcludingProxy(%q) = %d, want %d", c.names, got, c.want)
		}
	}
}

// A restore ends in an internal Reroute, and that Reroute is refused without a
// network - which would leave the data restored and the stack down, the one moment
// that must not fail. The network comes from the control plane, not the snapshot:
// the app may have changed Environment since the backup was taken.
func TestRestoreConfig_carriesTheNetwork(t *testing.T) {
	rr := restoreConfig("shop", &pb.ProjectDescriptor{
		ComposeYaml: "services:\n  web:\n    image: nginx\n",
		Network:     "deplo-env-environ_now",
	}, projectSnapshot{}, false, false)
	if rr.GetNetwork() != "deplo-env-environ_now" {
		t.Fatalf("restore would be refused for having no network, got %q", rr.GetNetwork())
	}
}

// The scope reclaimed nothing for a reason no unit test could see: `{{.Created}}`
// renders Go's default layout, not RFC3339, so every network failed to parse and
// fell to the fail-closed branch. This pins the format the template must produce.
func TestNetworkState_readsDockersTimestamp(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	const n = "deplo-env-agent-timetest"
	if err := dockercli.EnsureNetwork(ctx, n); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() {
		_, _ = dockercli.Run(context.Background(), 20*time.Second, "network", "rm", n)
	})
	attached, created, ok := networkState(ctx, n)
	if !ok {
		t.Fatal("networkState could not read the network - every network would be skipped")
	}
	if attached != 0 {
		t.Errorf("a fresh network has no tenant on it, got %d", attached)
	}
	if created.IsZero() || time.Since(created) > time.Hour {
		t.Errorf("the creation time did not parse: %v", created)
	}
}
