package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// mkCA returns a self-signed CA cert+key for signing agent leaves in the test.
func mkCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey, []byte) {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "deplo-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	caCert, _ := x509.ParseCertificate(der)
	caPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return caCert, priv, caPem
}

// signLeaf mimics the control plane's CA signing a CSR into a leaf certificate.
func signLeaf(t *testing.T, caCert *x509.Certificate, caKey ed25519.PrivateKey, csrPem string, serial int64) []byte {
	t.Helper()
	block, _ := pem.Decode([]byte(csrPem))
	if block == nil {
		t.Fatal("csr not PEM")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatalf("parse CSR: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		t.Fatalf("CSR self-signature invalid: %v", err)
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      csr.Subject,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, caCert, csr.PublicKey, caKey)
	if err != nil {
		t.Fatalf("sign leaf: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// leafSerial reads the serial of the cert currently on disk, to prove a swap.
func leafSerial(t *testing.T, certFile string) *big.Int {
	t.Helper()
	b, _ := os.ReadFile(certFile)
	block, _ := pem.Decode(b)
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse on-disk cert: %v", err)
	}
	return c.SerialNumber
}

func TestCertRenewal_roundTrip(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "agent.crt")
	keyFile := filepath.Join(dir, "agent.key")
	caFile := filepath.Join(dir, "ca.crt")

	caCert, caKey, caPem := mkCA(t)
	if err := os.WriteFile(caFile, caPem, 0o644); err != nil {
		t.Fatal(err)
	}
	// Initial leaf (serial 100) for a fresh agent keypair.
	_, initPriv, _ := ed25519.GenerateKey(rand.Reader)
	initCSRDER, _ := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "deplo-agent"}}, initPriv)
	initCSR := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: initCSRDER})
	initCert := signLeaf(t, caCert, caKey, string(initCSR), 100)
	initKeyDER, _ := x509.MarshalPKCS8PrivateKey(initPriv)
	os.WriteFile(certFile, initCert, 0o644)
	os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: initKeyDER}), 0o600)

	cm, err := NewCertManager(certFile, keyFile, caFile)
	if err != nil {
		t.Fatalf("NewCertManager: %v", err)
	}
	svc := New(dir, dir, dir, dir)
	svc.EnableCertRenewal(cm)

	if got := leafSerial(t, certFile); got.Int64() != 100 {
		t.Fatalf("pre-renewal serial = %d, want 100", got)
	}

	// 1. Agent generates a fresh CSR (new key held pending).
	csrResp, err := svc.RenewalCSR(context.Background(), &pb.RenewalCSRRequest{})
	if err != nil {
		t.Fatalf("RenewalCSR: %v", err)
	}
	if csrResp.GetCsrPem() == "" {
		t.Fatal("empty CSR")
	}
	// 2. Control plane signs it (serial 200).
	newCert := signLeaf(t, caCert, caKey, csrResp.GetCsrPem(), 200)
	// 3. Agent installs the renewed cert.
	res, err := svc.InstallRenewedCert(context.Background(), &pb.InstallRenewedCertRequest{CertPem: string(newCert)})
	if err != nil {
		t.Fatalf("InstallRenewedCert: %v", err)
	}
	if !res.GetOk() {
		t.Fatalf("install not ok: %s", res.GetError())
	}
	// On-disk cert is the new one (serial 200) and key/cert still form a valid pair.
	if got := leafSerial(t, certFile); got.Int64() != 200 {
		t.Fatalf("post-renewal on-disk serial = %d, want 200", got)
	}
	if _, err := loadPairForTest(certFile, keyFile); err != nil {
		t.Fatalf("renewed cert/key not a valid pair: %v", err)
	}
	// The live TLS config serves the new leaf.
	cfg, _ := cm.ServerTLSConfig().GetConfigForClient(nil)
	if cfg.Certificates[0].Leaf == nil {
		leaf, _ := x509.ParseCertificate(cfg.Certificates[0].Certificate[0])
		cfg.Certificates[0].Leaf = leaf
	}
	served, _ := x509.ParseCertificate(cfg.Certificates[0].Certificate[0])
	if served.SerialNumber.Int64() != 200 {
		t.Fatalf("live cert serial = %d, want 200 (hot-swap failed)", served.SerialNumber.Int64())
	}

	// A replay of the same install now fails (pending key consumed).
	if r, _ := svc.InstallRenewedCert(context.Background(), &pb.InstallRenewedCertRequest{CertPem: string(newCert)}); r.GetOk() {
		t.Fatal("replay install unexpectedly succeeded — pending key not consumed")
	}
}

func TestCertRenewal_rejectsMismatchedCert(t *testing.T) {
	dir := t.TempDir()
	certFile := filepath.Join(dir, "agent.crt")
	keyFile := filepath.Join(dir, "agent.key")
	caFile := filepath.Join(dir, "ca.crt")
	caCert, caKey, caPem := mkCA(t)
	os.WriteFile(caFile, caPem, 0o644)
	_, initPriv, _ := ed25519.GenerateKey(rand.Reader)
	initCSRDER, _ := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "deplo-agent"}}, initPriv)
	initCert := signLeaf(t, caCert, caKey, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: initCSRDER})), 1)
	initKeyDER, _ := x509.MarshalPKCS8PrivateKey(initPriv)
	os.WriteFile(certFile, initCert, 0o644)
	os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: initKeyDER}), 0o600)

	cm, _ := NewCertManager(certFile, keyFile, caFile)
	svc := New(dir, dir, dir, dir)
	svc.EnableCertRenewal(cm)

	// Get a CSR (pending key A), but sign a DIFFERENT key's CSR and try to install.
	if _, err := svc.RenewalCSR(context.Background(), &pb.RenewalCSRRequest{}); err != nil {
		t.Fatal(err)
	}
	_, otherPriv, _ := ed25519.GenerateKey(rand.Reader)
	otherCSRDER, _ := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "deplo-agent"}}, otherPriv)
	otherCert := signLeaf(t, caCert, caKey, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: otherCSRDER})), 2)
	res, err := svc.InstallRenewedCert(context.Background(), &pb.InstallRenewedCertRequest{CertPem: string(otherCert)})
	if err != nil {
		t.Fatalf("unexpected rpc error: %v", err)
	}
	if res.GetOk() {
		t.Fatal("install of a cert not matching the pending key succeeded — must be rejected")
	}
	// The on-disk cert is untouched (still serial 1).
	if got := leafSerial(t, certFile); got.Int64() != 1 {
		t.Fatalf("on-disk serial = %d, want 1 (must be untouched on mismatch)", got)
	}
}

func loadPairForTest(certFile, keyFile string) (any, error) {
	// tls.LoadX509KeyPair equivalent without importing tls into the test file.
	c, err := os.ReadFile(certFile)
	if err != nil {
		return nil, err
	}
	k, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, err
	}
	cb, _ := pem.Decode(c)
	kb, _ := pem.Decode(k)
	leaf, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, err
	}
	key, err := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if err != nil {
		return nil, err
	}
	if !leaf.PublicKey.(ed25519.PublicKey).Equal(key.(ed25519.PrivateKey).Public()) {
		return nil, os.ErrInvalid
	}
	return key, nil
}
