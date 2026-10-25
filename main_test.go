package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/DeploCloud/deplo-agent/internal/link"
)

func TestLoopbackAddr(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:9443": true,
		"[::1]:9443":     true,
		"localhost:9443": true,
		":9443":          false,
		"0.0.0.0:9443":   false,
		"10.0.0.5:9443":  false,
		"[::]:9443":      false,
		"127.0.0.1":      false,
	} {
		if got := loopbackAddr(addr); got != want {
			t.Errorf("loopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}

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

	got, err := linkAddresses(dir)
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
	if again, _ := linkAddresses(dir); again != got {
		t.Fatalf("after a restart without bootstrap.env: %+v, want %+v", again, got)
	}
}

func TestLinkAddresses_theSavedPairWinsOverTheInstallFile(t *testing.T) {
	dir := t.TempDir()
	writeInstallEnv(t, dir, "https://old.example.com")
	if err := link.SaveAddresses(dir, link.Addresses{Primary: "https://new.example.com"}); err != nil {
		t.Fatal(err)
	}
	got, err := linkAddresses(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.Primary != "https://new.example.com" {
		t.Fatalf("primary = %q, want the saved one", got.Primary)
	}
}

func TestLinkAddresses_noAddressAnywhereMeansNoLink(t *testing.T) {
	dir := t.TempDir()
	got, err := linkAddresses(dir)
	if err != nil || got.Primary != "" {
		t.Fatalf("addresses = %+v, %v; want none", got, err)
	}
	if _, err := os.Stat(link.AddressesPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("nothing may be saved when there is no address (stat err = %v)", err)
	}
}

func TestLinkConfig_neverLinksWithoutMTLS(t *testing.T) {
	dir := t.TempDir()
	if err := link.SaveAddresses(dir, link.Addresses{Primary: "https://panel.example.com"}); err != nil {
		t.Fatal(err)
	}
	if lc, err := linkConfig(true, dir, "agent.crt"); lc != nil || err != nil {
		t.Fatalf("--insecure linked to %+v (err %v): gRPC would cross the internet in the clear", lc, err)
	}
	lc, err := linkConfig(false, dir, "agent.crt")
	if err != nil || lc == nil || lc.URL != "https://panel.example.com" || lc.CertFile != "agent.crt" {
		t.Fatalf("with mTLS: %+v, %v", lc, err)
	}
}
