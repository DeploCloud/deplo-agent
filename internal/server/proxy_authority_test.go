package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// issue makes a certificate for name, signed by parent (self-signed when nil); it answers the PEM and the cert.
func issue(t *testing.T, name string, ca bool, parent *x509.Certificate, parentKey ed25519.PrivateKey) ([]byte, *x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	tpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		DNSNames: []string{name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: ca, BasicConstraintsValid: true}
	if parent == nil {
		parent, parentKey = tpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, parent, pub, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), c, key
}

func TestSettleProxyAuthority_setsAsideAnotherAuthoritysAccountAndCertificates(t *testing.T) {
	dir := t.TempDir()
	traefik := filepath.Join(dir, "traefik")
	if err := os.MkdirAll(filepath.Join(traefik, "acme"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, root, rootKey := issue(t, "public root", true, nil, nil)
	public, _, _ := issue(t, "fise.example.com", false, root, rootKey)
	lab, _, _ := issue(t, "notes.lab.test", false, nil, nil)
	pool := x509.NewCertPool()
	pool.AddCert(root)
	old, oldStop := publicRoots, stopProxy
	publicRoots = func() (*x509.CertPool, error) { return pool, nil }
	stops := 0
	stopProxy = func(context.Context) error { stops++; return nil }
	t.Cleanup(func() { publicRoots, stopProxy = old, oldStop })

	// The proxy orders from Let's Encrypt now; the test resolver keeps its own authority and everything it has.
	compose := filepath.Join(traefik, "docker-compose.yml")
	_ = os.WriteFile(compose, []byte(`services:
  traefik:
    command:
      - --certificatesresolvers.letsencrypt.acme.storage=/acme/acme.json
      - --certificatesresolvers.lab.acme.storage=/acme/acme.json
      - --certificatesresolvers.lab.acme.caserver=https://pebble:14000/dir
    volumes:
      - ./acme:/acme
`), 0o600)
	entry := func(c []byte) map[string]any {
		return map[string]any{"domain": map[string]any{"main": "x"}, "certificate": c, "key": []byte("k"), "Store": "default"}
	}
	store := filepath.Join(traefik, "acme", "acme.json")
	raw, _ := json.Marshal(map[string]any{
		"letsencrypt": map[string]any{
			"Account":      map[string]any{"Email": "a@acme.com", "Registration": map[string]any{"uri": "https://pebble:14000/my-account/1"}},
			"Certificates": []any{entry(public), entry(lab)},
		},
		"lab": map[string]any{
			"Account":      map[string]any{"Registration": map[string]any{"uri": "https://pebble:14000/my-account/2"}},
			"Certificates": []any{entry(lab)},
		},
	})
	_ = os.WriteFile(store, raw, 0o600)

	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.SetAgentDir(dir)
	s.settleProxyAuthority(context.Background(), compose)

	st, err := readACMEStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := st.resolvers["letsencrypt"]["Account"]; ok {
		t.Error("an account registered with another authority still orders")
	}
	if got := st.entries("letsencrypt"); len(got) != 1 || string(got[0].Certificate) != string(public) {
		t.Errorf("letsencrypt keeps %d certificates", len(got))
	}
	if _, ok := st.resolvers["lab"]["Account"]; !ok || len(st.entries("lab")) != 1 {
		t.Error("a resolver on its own authority lost what it holds")
	}
	if stops != 1 {
		t.Fatalf("the proxy was stopped %d times for its store", stops)
	}
	to := onlySetAside(t, dir)
	var gone map[string]map[string]json.RawMessage
	raw, _ = os.ReadFile(filepath.Join(to, "acme.json"))
	if json.Unmarshal(raw, &gone) != nil || gone["letsencrypt"]["Account"] == nil || gone["letsencrypt"]["Certificates"] == nil {
		t.Fatalf("set aside: %s", raw)
	}

	// Settled: the next start of the proxy finds nothing to set aside.
	s.settleProxyAuthority(context.Background(), compose)
	if stops != 1 {
		t.Fatal("a settled store stopped the proxy again")
	}
}
