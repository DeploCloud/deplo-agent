package server

import (
	"context"
	"strings"
	"testing"

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
