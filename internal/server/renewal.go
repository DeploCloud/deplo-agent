package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// CertManager owns the agent's live mTLS server materials and can hot-swap the
// leaf cert WITHOUT a restart. main.go builds it from the on-disk cert/key/ca and
// hands the gRPC server a TLS config whose GetConfigForClient reads the CURRENT
// materials on every handshake, so an InstallRenewedCert takes effect immediately
// for new connections. All access is guarded by a RWMutex.
type CertManager struct {
	certFile, keyFile, caFile string

	mu   sync.RWMutex
	cert *tls.Certificate
	pool *x509.CertPool
}

// NewCertManager loads the current materials from disk (failing if they are
// missing/invalid, exactly like the old static load).
func NewCertManager(certFile, keyFile, caFile string) (*CertManager, error) {
	m := &CertManager{certFile: certFile, keyFile: keyFile, caFile: caFile}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load keypair: %w", err)
	}
	caPem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPem) {
		return nil, fmt.Errorf("ca file %q contained no certificates", caFile)
	}
	m.cert = &cert
	m.pool = pool
	return m, nil
}

// ServerTLSConfig returns a config whose per-handshake GetConfigForClient reads
// the CURRENT cert + client-CA pool, so a renewed leaf (and, if the CA rotated,
// a new pool) is picked up without restarting the listener.
func (m *CertManager) ServerTLSConfig() *tls.Config {
	return &tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS12,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			m.mu.RLock()
			defer m.mu.RUnlock()
			return &tls.Config{
				Certificates: []tls.Certificate{*m.cert},
				ClientAuth:   tls.RequireAndVerifyClientCert,
				ClientCAs:    m.pool,
				MinVersion:   tls.VersionTLS12,
			}, nil
		},
	}
}

// install verifies the new cert matches keyPEM, swaps the in-memory materials so
// LIVE handshakes use the new leaf immediately, then persists all three files
// atomically (temp + rename) for restart survival. A persist failure is returned
// but the in-memory swap already happened — the running server keeps serving with
// the new cert and the control plane's next renewal attempt re-persists.
func (m *CertManager) install(certPEM, keyPEM, caPEM []byte) error {
	// The cert MUST correspond to the pending key, else a swap would break every
	// future handshake. tls.X509KeyPair fails closed on a mismatch.
	newCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("renewed cert does not match the pending key: %w", err)
	}
	var newPool *x509.CertPool
	if len(caPEM) > 0 {
		newPool = x509.NewCertPool()
		if !newPool.AppendCertsFromPEM(caPEM) {
			return fmt.Errorf("renewed ca pem contained no certificates")
		}
	}
	// Swap in memory first so the running listener serves the new leaf at once.
	m.mu.Lock()
	m.cert = &newCert
	if newPool != nil {
		m.pool = newPool
	}
	m.mu.Unlock()

	// Persist for restart survival. Written key-then-cert-then-ca via temp+rename;
	// the window in which the two files could disagree on disk is microseconds and
	// recoverable by re-bootstrap while the (weeks-from-expiry) old cert is valid.
	if err := atomicWriteFile(m.keyFile, keyPEM, 0o600); err != nil {
		return fmt.Errorf("persist key: %w", err)
	}
	if err := atomicWriteFile(m.certFile, certPEM, 0o644); err != nil {
		return fmt.Errorf("persist cert: %w", err)
	}
	if len(caPEM) > 0 {
		if err := atomicWriteFile(m.caFile, caPEM, 0o644); err != nil {
			return fmt.Errorf("persist ca: %w", err)
		}
	}
	return nil
}

// atomicWriteFile writes data to a sibling temp file then renames it into place,
// so a reader never sees a half-written file.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after a successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// EnableCertRenewal wires the (mTLS-only) cert manager into the service so the
// RenewalCSR / InstallRenewedCert RPCs are live. Left nil for --insecure / tests,
// where the two RPCs return Unimplemented.
func (s *Service) EnableCertRenewal(cm *CertManager) { s.certMgr = cm }

// RenewalCSR generates a FRESH keypair and returns a CSR for it; the new private
// key is held PENDING in memory (never sent) until InstallRenewedCert confirms
// the control-plane-signed cert matches it. This is driven by the control plane
// over the still-valid pinned mTLS channel before the current leaf expires.
func (s *Service) RenewalCSR(ctx context.Context, req *pb.RenewalCSRRequest) (*pb.RenewalCSRResponse, error) {
	if s.certMgr == nil {
		return nil, status.Error(codes.Unimplemented, "cert renewal is not enabled on this agent")
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate key: %v", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "deplo-agent"},
	}, priv)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create CSR: %v", err)
	}
	s.pendingMu.Lock()
	s.pendingKey = priv
	s.pendingMu.Unlock()
	csrPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
	return &pb.RenewalCSRResponse{CsrPem: string(csrPem)}, nil
}

// InstallRenewedCert installs the CA-signed leaf produced from the last
// RenewalCSR: it checks the cert's public key matches the pending private key,
// then hot-swaps + persists the materials. Idempotent-safe: a stale/mismatched
// cert is rejected without touching the live materials.
func (s *Service) InstallRenewedCert(ctx context.Context, req *pb.InstallRenewedCertRequest) (*pb.StackResult, error) {
	if s.certMgr == nil {
		return nil, status.Error(codes.Unimplemented, "cert renewal is not enabled on this agent")
	}
	s.pendingMu.Lock()
	priv := s.pendingKey
	s.pendingMu.Unlock()
	if priv == nil {
		return &pb.StackResult{Ok: false, Error: "no pending renewal — call RenewalCSR first"}, nil
	}
	block, _ := pem.Decode([]byte(req.GetCertPem()))
	if block == nil {
		return &pb.StackResult{Ok: false, Error: "cert_pem is not valid PEM"}, nil
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return &pb.StackResult{Ok: false, Error: "parse cert: " + err.Error()}, nil
	}
	// The signed leaf must carry the public half of our pending key.
	certPub, ok := leaf.PublicKey.(ed25519.PublicKey)
	if !ok || !certPub.Equal(priv.Public()) {
		return &pb.StackResult{Ok: false, Error: "renewed cert does not match the pending renewal key"}, nil
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := s.certMgr.install([]byte(req.GetCertPem()), keyPEM, []byte(req.GetCaPem())); err != nil {
		return &pb.StackResult{Ok: false, Error: err.Error()}, nil
	}
	// Consume the pending key so a replay can't re-install it.
	s.pendingMu.Lock()
	s.pendingKey = nil
	s.pendingMu.Unlock()
	return &pb.StackResult{Ok: true}, nil
}
