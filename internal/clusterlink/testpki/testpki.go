// Package testpki mints a throwaway CA and server-style leaf certificates, as the instance's agent CA does, for tests.
package testpki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

// CA signs leaves.
type CA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

// New returns a fresh CA.
func New(t testing.TB) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test agent ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &CA{cert: cert, key: key, pool: pool}
}

// Leaf issues an identity with the server-auth use only, which is what an agent certificate has.
func (ca *CA) Leaf(t testing.TB, name string) *wire.Identity {
	t.Helper()
	return ca.leaf(t, name, time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
}

// Expired issues an identity that is already past its end.
func (ca *CA) Expired(t testing.TB, name string) *wire.Identity {
	t.Helper()
	return ca.leaf(t, name, time.Now().Add(-3*time.Hour), time.Now().Add(-time.Hour))
}

func (ca *CA) leaf(t testing.TB, name string, from, to time.Time) *wire.Identity {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    from,
		NotAfter:     to,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{name},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return &wire.Identity{
		Cert:        tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key},
		Fingerprint: wire.Fingerprint(der),
		Roots:       ca.pool,
	}
}

// Provision writes an agent's materials (agent.crt, agent.key, ca.crt) to dir and returns the identity they hold.
func (ca *CA) Provision(t testing.TB, dir, name string) *wire.Identity {
	t.Helper()
	id := ca.Leaf(t, name)
	keyDER, err := x509.MarshalPKCS8PrivateKey(id.Cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{
		"agent.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: id.Cert.Certificate[0]}),
		"agent.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		"ca.crt":    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw}),
	}
	for f, body := range files {
		if err := os.WriteFile(filepath.Join(dir, f), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return id
}

// Foreign is an identity from a CA the instance does not know.
func Foreign(t testing.TB, trusted *CA) *wire.Identity {
	t.Helper()
	id := New(t).Leaf(t, "stranger")
	id.Roots = trusted.pool
	return id
}
