package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/DeploCloud/deplo-agent/internal/link"
	"github.com/DeploCloud/deplo-agent/internal/server"
)

// writeInstallEnv writes bootstrap.env the way the install script does.
func writeInstallEnv(t *testing.T, dir, url string) {
	t.Helper()
	body := "DEPLO_BOOTSTRAP_URL=" + url + "\nDEPLO_BOOTSTRAP_TOKEN=spent\nDEPLO_BOOTSTRAP_FINGERPRINT=aa:bb\n"
	if err := os.WriteFile(filepath.Join(dir, "bootstrap.env"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLinkAddresses_anAgentEnrolledBeforeTheLinkKeepsItsInstallURL(t *testing.T) {
	dir := t.TempDir()
	writeInstallEnv(t, dir, "https://panel.example.com")

	got, err := linkAddresses(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if got != (link.Addresses{Primary: "https://panel.example.com"}) {
		t.Fatalf("addresses = %+v", got)
	}
	// Saved once: a later restart no longer depends on the install's file.
	if err := os.Remove(filepath.Join(dir, "bootstrap.env")); err != nil {
		t.Fatal(err)
	}
	if again, _ := linkAddresses(dir, ""); again != got {
		t.Fatalf("after a restart without bootstrap.env: %+v, want %+v", again, got)
	}
}

func TestLinkAddresses_theSavedPairWinsOverWhatTheInstallNamed(t *testing.T) {
	dir := t.TempDir()
	writeInstallEnv(t, dir, "https://old.example.com")
	if err := link.SaveAddresses(dir, link.Addresses{Primary: "https://new.example.com"}); err != nil {
		t.Fatal(err)
	}
	got, err := linkAddresses(dir, "https://old.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if got.Primary != "https://new.example.com" {
		t.Fatalf("primary = %q, want the saved one", got.Primary)
	}
}

func TestLinkAddresses_noAddressAnywhereMeansNoLink(t *testing.T) {
	dir := t.TempDir()
	got, err := linkAddresses(dir, "")
	if err != nil || got.Primary != "" {
		t.Fatalf("addresses = %+v, %v; want none", got, err)
	}
	if _, err := os.Stat(link.AddressesPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("nothing may be saved when there is no address (stat err = %v)", err)
	}
}

func TestPrintCapabilities_tellsAnInstallerThisAgentLinks(t *testing.T) {
	var out strings.Builder
	printCapabilities(&out)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if !slices.Contains(lines, "agent-link") {
		t.Fatalf("--capabilities printed %q, want an agent-link line", out.String())
	}
	if len(lines) != len(server.Capabilities) {
		t.Fatalf("--capabilities printed %d lines, Hello advertises %d", len(lines), len(server.Capabilities))
	}
}

// With no port of its own, an agent whose saved pair cannot be read has only its install URL left.
func TestLinkAddresses_anUnreadablePairFallsBackToTheInstallURL(t *testing.T) {
	dir := t.TempDir()
	writeInstallEnv(t, dir, "https://panel.example.com")
	if err := os.WriteFile(link.AddressesPath(dir), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := linkAddresses(dir, "")
	if got.Primary != "https://panel.example.com" {
		t.Fatalf("addresses = %+v (%v), want the install URL", got, err)
	}
	if again, err := linkAddresses(dir, ""); again != got || err != nil {
		t.Fatalf("after a restart: %+v, %v; want %+v saved", again, err, got)
	}
}
