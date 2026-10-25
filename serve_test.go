package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/bootstrap"
	"github.com/DeploCloud/deplo-agent/internal/link"
	"github.com/DeploCloud/deplo-agent/internal/server"
)

// testPKI issues like the control plane: the agent's leaf is serverAuth only, the panel's clientAuth only.
type testPKI struct {
	ca   *x509.Certificate
	key  ed25519.PrivateKey
	pool *x509.CertPool
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Deplo test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return &testPKI{ca: ca, key: priv, pool: pool}
}

func (p *testPKI) issue(t *testing.T, serial int64, usage x509.ExtKeyUsage) (der []byte, key ed25519.PrivateKey) {
	t.Helper()
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "deplo"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}, p.ca, pub, p.key)
	if err != nil {
		t.Fatal(err)
	}
	return der, key
}

// provisionAgent writes the agent's mTLS materials where bootstrap would.
func (p *testPKI) provisionAgent(t *testing.T, agentDir string) {
	t.Helper()
	der, key := p.issue(t, 2, x509.ExtKeyUsageServerAuth)
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	m := bootstrap.Paths(agentDir)
	for path, block := range map[string]*pem.Block{
		m.CertPath: {Type: "CERTIFICATE", Bytes: der},
		m.KeyPath:  {Type: "PRIVATE KEY", Bytes: keyDER},
		m.CAPath:   {Type: "CERTIFICATE", Bytes: p.ca.Raw},
	} {
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// panelTLS is the control plane's side of the mTLS; peer receives the agent leaf it verified.
func (p *testPKI) panelTLS(t *testing.T, peer **x509.Certificate) *tls.Config {
	t.Helper()
	der, key := p.issue(t, 3, x509.ExtKeyUsageClientAuth)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		RootCAs:      p.pool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if peer != nil {
				*peer = cs.PeerCertificates[0]
			}
			return nil
		},
	}
}

// noDocker puts a `docker` that always fails first on PATH, so Hello never reaches the host's Docker.
func noDocker(t *testing.T) {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/bin/sh\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
}

type linked struct {
	claim string
	ws    *websocket.Conn
	conn  net.Conn
}

// panelDoor accepts links like the panel's door and hands each one over.
func panelDoor(t *testing.T) (string, <-chan linked) {
	t.Helper()
	links := make(chan linked, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != link.Path {
			http.NotFound(w, r)
			return
		}
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{link.Subprotocol}})
		if err != nil {
			return
		}
		links <- linked{claim: r.Header.Get(link.CertHeader), ws: c, conn: websocket.NetConn(context.Background(), c, websocket.MessageBinary)}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, links
}

func nextLinked(t *testing.T, links <-chan linked) linked {
	t.Helper()
	select {
	case l := <-links:
		return l
	case <-time.After(10 * time.Second):
		t.Fatal("the agent never linked")
		return linked{}
	}
}

// overLink is the control plane's client on a link the agent opened.
func overLink(t *testing.T, conn net.Conn, cfg *tls.Config) pb.AgentClient {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///agent-link",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return conn, nil }),
		grpc.WithTransportCredentials(credentials.NewTLS(cfg)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return pb.NewAgentClient(cc)
}

func hello(t *testing.T, c pb.AgentClient) *pb.HelloResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := c.Hello(ctx, &pb.HelloRequest{ContractVersion: pb.ContractVersion_CONTRACT_VERSION_V1})
	if err != nil {
		t.Fatalf("Hello: %v", err)
	}
	return resp
}

type agentUnderTest struct {
	pki    *testPKI
	lis    net.Listener
	links  <-chan linked
	stop   context.CancelFunc
	served chan error
}

// startAgent serves the real Agent service with main's gRPC server, on a listener and over a link.
func startAgent(t *testing.T) *agentUnderTest {
	t.Helper()
	noDocker(t)
	pki := newTestPKI(t)
	dir := t.TempDir()
	pki.provisionAgent(t, dir)
	m := bootstrap.Paths(dir)
	cm, err := server.NewCertManager(m.CertPath, m.KeyPath, m.CAPath)
	if err != nil {
		t.Fatal(err)
	}
	srv := newGRPCServer(cm)
	pb.RegisterAgentServer(srv, server.New(t.TempDir(), t.TempDir(), "/", ""))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url, links := panelDoor(t)
	ctx, cancel := context.WithCancel(context.Background())
	a := &agentUnderTest{pki: pki, lis: lis, links: links, stop: cancel, served: make(chan error, 1)}
	go func() {
		a.served <- serveAgent(ctx, srv, lis, &link.Config{URL: url, CertFile: m.CertPath, Logf: t.Logf})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-a.served:
		case <-time.After(30 * time.Second):
			t.Error("serveAgent did not return")
		}
	})
	return a
}

func TestServeAgent_answersOverTheLinkWithItsMTLSIdentity(t *testing.T) {
	a := startAgent(t)
	l := nextLinked(t, a.links)
	var peer *x509.Certificate
	resp := hello(t, overLink(t, l.conn, a.pki.panelTLS(t, &peer)))

	if !slices.Contains(resp.GetCapabilities(), "agent-link") {
		t.Errorf("Hello over the link does not advertise agent-link: %v", resp.GetCapabilities())
	}
	if resp.GetContractVersion() != pb.ContractVersion_CONTRACT_VERSION_V1 {
		t.Errorf("contract = %v, want V1", resp.GetContractVersion())
	}
	sum := sha256.Sum256(peer.Raw)
	if l.claim != hex.EncodeToString(sum[:]) {
		t.Errorf("the door claim %q is not the certificate the agent proved inside the link", l.claim)
	}
}

func TestServeAgent_keepsItsListenerBesideTheLink(t *testing.T) {
	a := startAgent(t)
	l := nextLinked(t, a.links)
	hello(t, overLink(t, l.conn, a.pki.panelTLS(t, nil)))
	cc, err := grpc.NewClient(a.lis.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(a.pki.panelTLS(t, nil))))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	hello(t, pb.NewAgentClient(cc))
}

func TestServeAgent_keepsServingAcrossALinkReconnect(t *testing.T) {
	a := startAgent(t)
	first := nextLinked(t, a.links)
	hello(t, overLink(t, first.conn, a.pki.panelTLS(t, nil)))

	_ = first.ws.CloseNow() // the panel restarts
	second := nextLinked(t, a.links)
	hello(t, overLink(t, second.conn, a.pki.panelTLS(t, nil)))
	select {
	case err := <-a.served:
		t.Fatalf("serving ended on a reconnect: %v", err)
	default:
	}
}

func TestServeAgent_stopsWithoutWaitingOnStreamsOverTheLink(t *testing.T) {
	a := startAgent(t)
	l := nextLinked(t, a.links)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := overLink(t, l.conn, a.pki.panelTLS(t, nil)).StreamMetrics(ctx, &pb.MetricsStreamRequest{IntervalMs: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first sample: %v", err)
	}

	started := time.Now()
	a.stop()
	select {
	case err := <-a.served:
		if err != nil {
			t.Fatalf("serveAgent: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("serveAgent never returned")
	}
	if took := time.Since(started); took > 10*time.Second {
		t.Errorf("stopping took %v: the open stream on the link held the drain", took)
	}
	a.served <- nil // for the cleanup
}
