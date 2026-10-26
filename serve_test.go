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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

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
	dir    string
	links  <-chan linked
	stop   context.CancelFunc
	served chan error
}

// startAgent serves the real Agent service with main's gRPC server over a link to a test door.
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
	url, links := panelDoor(t)
	if err := link.SaveAddresses(dir, link.Addresses{Primary: url}); err != nil {
		t.Fatal(err)
	}
	lc, err := linkConfig(dir, m.CertPath, "")
	if err != nil {
		t.Fatal(err)
	}
	lc.Logf = t.Logf
	srv := newGRPCServer(cm)
	svc := server.New(t.TempDir(), t.TempDir(), "/", "")
	svc.EnableAgentLink(lc.Addresses)
	pb.RegisterAgentServer(srv, svc)
	ctx, cancel := context.WithCancel(context.Background())
	a := &agentUnderTest{pki: pki, dir: dir, links: links, stop: cancel, served: make(chan error, 1)}
	go func() {
		a.served <- serveAgent(ctx, srv, *lc)
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

func setPair(t *testing.T, c pb.AgentClient, primary, fallback string) (*pb.LinkAddresses, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.SetLinkAddresses(ctx, &pb.LinkAddresses{Primary: primary, Fallback: fallback})
}

func TestServeAgent_takesANewPairOverItsLinkAndLinksThereNext(t *testing.T) {
	a := startAgent(t)
	first := nextLinked(t, a.links)
	c := overLink(t, first.conn, a.pki.panelTLS(t, nil))
	if got := hello(t, c).GetLinkAddresses(); got.GetPrimary() == "" || got.GetFallback() != "" {
		t.Fatalf("Hello reports %v, want the saved primary alone", got)
	}

	moved, moves := panelDoor(t)
	kept, err := setPair(t, c, moved, "https://deplo-cb00710a.deplo.site")
	if err != nil {
		t.Fatal(err)
	}
	want := &pb.LinkAddresses{Primary: moved, Fallback: "https://deplo-cb00710a.deplo.site"}
	if kept.GetPrimary() != want.Primary || kept.GetFallback() != want.Fallback {
		t.Fatalf("kept %v, want %v", kept, want)
	}
	// Still on the link that carried it, which now reports the new pair.
	if got := hello(t, c).GetLinkAddresses(); got.GetPrimary() != want.Primary || got.GetFallback() != want.Fallback {
		t.Fatalf("Hello after the change reports %v, want %v", got, want)
	}
	if saved, err := linkAddresses(a.dir, ""); err != nil || saved != (link.Addresses{Primary: want.Primary, Fallback: want.Fallback}) {
		t.Fatalf("what a restart reads: %+v, %v", saved, err)
	}

	_ = first.ws.CloseNow()
	again := nextLinked(t, moves)
	hello(t, overLink(t, again.conn, a.pki.panelTLS(t, nil)))
}

func TestServeAgent_keepsItsPairWhenGivenOneItCouldNotDial(t *testing.T) {
	a := startAgent(t)
	l := nextLinked(t, a.links)
	c := overLink(t, l.conn, a.pki.panelTLS(t, nil))
	was := hello(t, c).GetLinkAddresses().GetPrimary()

	if _, err := setPair(t, c, "", "https://deplo-cb00710a.deplo.site"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a pair with no primary: %v, want InvalidArgument", err)
	}
	if got := hello(t, c).GetLinkAddresses(); got.GetPrimary() != was || got.GetFallback() != "" {
		t.Fatalf("Hello reports %v after a refused pair, want the old one", got)
	}
}

func stopListening(c pb.AgentClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.StopListening(ctx, &pb.StopListeningRequest{})
	return err
}

// A panel from before #122 still asks an agent to stop listening; with no port of its own, it just agrees.
func TestServeAgent_agreesToStopListeningAndGoesOnOverItsLink(t *testing.T) {
	a := startAgent(t)
	l := nextLinked(t, a.links)
	c := overLink(t, l.conn, a.pki.panelTLS(t, nil))
	if hello(t, c).GetListening() {
		t.Error("Hello reports a listener")
	}

	if err := stopListening(c); err != nil {
		t.Fatalf("StopListening: %v", err)
	}
	if hello(t, c).GetListening() {
		t.Error("Hello reports a listener after StopListening")
	}
	_ = l.ws.CloseNow()
	again := nextLinked(t, a.links)
	hello(t, overLink(t, again.conn, a.pki.panelTLS(t, nil)))
}
