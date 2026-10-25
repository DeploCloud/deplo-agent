package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/bootstrap"
	"github.com/DeploCloud/deplo-agent/internal/capture"
	"github.com/DeploCloud/deplo-agent/internal/link"
	"github.com/DeploCloud/deplo-agent/internal/server"
)

func main() {
	// The same binary, mounted into the screenshot browser's container.
	if len(os.Args) > 1 && os.Args[1] == "capture" {
		os.Exit(capture.Main(os.Stdin, os.Stdout))
	}
	var (
		addr        = flag.String("addr", "", "listen address (host:port); none by default, the agent links out to its panel")
		showCaps    = flag.Bool("capabilities", false, "print the capabilities Hello advertises, one per line, and exit")
		certFile    = flag.String("cert", envOr("DEPLO_AGENT_CERT", ""), "agent server certificate (PEM)")
		keyFile     = flag.String("key", envOr("DEPLO_AGENT_KEY", ""), "agent server private key (PEM)")
		caFile      = flag.String("ca", envOr("DEPLO_AGENT_CA", ""), "CA certificate to verify the control plane (PEM)")
		agentDir    = flag.String("agent-dir", envOr("DEPLO_AGENT_DIR", ""), "directory holding mTLS materials (bootstrap writes them here)")
		stackDir    = flag.String("stack-dir", envOr("DEPLO_AGENT_STACK_DIR", "/data/stacks"), "where rendered stack files are written")
		buildTmpDir = flag.String("build-tmp", envOr("DEPLO_AGENT_BUILD_TMP", os.TempDir()), "where upload build contexts are extracted")
		dataDir     = flag.String("data-dir", envOr("DEPLO_AGENT_DATA_DIR", "/"), "filesystem measured for disk metrics")
		dataBase    = flag.String("data-base", envOr("DEPLO_AGENT_DATA_BASE", ""), "host data root for dev workspaces + the SSH gateway (empty => parent of --stack-dir)")
		insecure    = flag.Bool("insecure", os.Getenv("DEPLO_AGENT_INSECURE") == "1", "DANGEROUS: serve without mTLS (tests/local only)")

		bootstrapURL   = flag.String("bootstrap-url", envOr("DEPLO_BOOTSTRAP_URL", ""), "control-plane URL to call home to on first run")
		bootstrapTok   = flag.String("bootstrap-token", envOr("DEPLO_BOOTSTRAP_TOKEN", ""), "one-time bootstrap token")
		bootstrapFP    = flag.String("bootstrap-fingerprint", envOr("DEPLO_BOOTSTRAP_FINGERPRINT", ""), "expected control-plane cert sha256 (HTTPS only)")
		advertisedHost = flag.String("advertised-host", envOr("DEPLO_AGENT_ADVERTISED_HOST", ""), "address the agent reports it is reachable at (informational)")
	)
	flag.Parse()
	for _, k := range []string{"DEPLO_BOOTSTRAP_URL", "DEPLO_BOOTSTRAP_TOKEN", "DEPLO_BOOTSTRAP_FINGERPRINT"} {
		_ = os.Unsetenv(k)
	}
	if *showCaps {
		printCapabilities(os.Stdout)
		return
	}

	if *insecure && !loopbackAddr(*addr) {
		log.Fatalf("deplo-agent: --insecure serves without mTLS, so it needs a loopback --addr (got %q)", *addr)
	}

	if err := os.MkdirAll(*buildTmpDir, 0o755); err != nil {
		log.Fatalf("deplo-agent: build-tmp: %v", err)
	}

	cert, key, ca := *certFile, *keyFile, *caFile
	if cert == "" && key == "" && ca == "" && *agentDir != "" {
		m := bootstrap.Paths(*agentDir)
		if !bootstrap.Provisioned(*agentDir) {
			if *bootstrapTok == "" || *bootstrapURL == "" {
				log.Fatalf("deplo-agent: not provisioned and no bootstrap token/url given (run via the dashboard's install command)")
			}
			port := 0 // a linking agent has no port to report
			if *addr != "" {
				port = portFromAddr(*addr)
			}
			log.Printf("deplo-agent: bootstrapping against %s", *bootstrapURL)
			if _, err := bootstrap.Run(bootstrap.Config{
				ControlPlaneURL: *bootstrapURL,
				Token:           *bootstrapTok,
				Fingerprint:     *bootstrapFP,
				AgentPort:       port,
				AdvertisedHost:  *advertisedHost,
				AgentDir:        *agentDir,
			}); err != nil {
				log.Fatalf("deplo-agent: bootstrap failed: %v", err)
			}
			log.Printf("deplo-agent: bootstrap complete; materials in %s", *agentDir)
		}
		cert, key, ca = m.CertPath, m.KeyPath, m.CAPath
	}

	var certMgr *server.CertManager
	if !*insecure {
		cm, err := server.NewCertManager(cert, key, ca)
		if err != nil {
			log.Fatalf("deplo-agent: mTLS setup: %v", err)
		}
		certMgr = cm
	} else {
		log.Printf("deplo-agent: WARNING serving WITHOUT mTLS (--insecure)")
	}

	srv := newGRPCServer(certMgr)
	svc := server.New(*stackDir, *buildTmpDir, *dataDir, *dataBase)
	svc.SetAgentDir(*agentDir)
	if certMgr != nil {
		svc.EnableCertRenewal(certMgr)
	}
	pb.RegisterAgentServer(srv, svc)

	lis, err := listen(*addr, *agentDir)
	if err != nil {
		log.Fatalf("deplo-agent: listen %s: %v", *addr, err)
	}
	svc.SetListener(lis)
	if lis != nil {
		log.Printf("deplo-agent %s listening on %s (mtls=%v)", server.AgentVersion, *addr, !*insecure)
	} else if *addr != "" {
		log.Printf("deplo-agent %s: no listener, it closed its port once its link worked (--addr %s unused)", server.AgentVersion, *addr)
	} else {
		log.Printf("deplo-agent %s: no listener, it links out to its panel", server.AgentVersion)
	}

	lc, err := linkConfig(*insecure, *agentDir, cert)
	if err != nil {
		log.Printf("deplo-agent: agent link: %v", err)
	}
	if lc != nil {
		svc.EnableAgentLink(lc.Addresses)
		if a := lc.Addresses.Current(); a.Primary != "" {
			log.Printf("deplo-agent: linking to %s", a.Primary)
		}
	}
	if lis == nil && (lc == nil || lc.Addresses.Current().Primary == "") {
		log.Fatalf("deplo-agent: no --addr and no panel address to link to, so nothing could reach this agent")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go svc.WarmRailpack(ctx)
	if err := serveAgent(ctx, srv, lis, lc); err != nil {
		log.Fatalf("deplo-agent: serve: %v", err)
	}
}

// newGRPCServer is the agent's one gRPC server; every listener and link it serves shares these credentials.
func newGRPCServer(certMgr *server.CertManager) *grpc.Server {
	var opts []grpc.ServerOption
	if certMgr != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(certMgr.ServerTLSConfig())))
	}
	opts = append(opts,
		grpc.MaxRecvMsgSize(256*1024*1024),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             15 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	)
	return grpc.NewServer(opts...)
}

// listen opens the listener for --addr; none when it is empty or the agent closed it once linked (ADR-0040).
func listen(addr, agentDir string) (*link.Direct, error) {
	if addr == "" || agentDir != "" && link.ListenerStopped(agentDir) {
		return nil, nil
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return link.NewDirect(lis, agentDir), nil
}

// printCapabilities answers an installer asking what this binary can do, e.g. whether it links.
func printCapabilities(w io.Writer) {
	for _, c := range server.Capabilities {
		fmt.Fprintln(w, c)
	}
}

// serveAgent serves srv on lis (when there is one) and, given lc, over an agent link, until ctx ends or lis fails.
func serveAgent(ctx context.Context, srv *grpc.Server, lis *link.Direct, lc *link.Config) error {
	serveErr := make(chan error, 2)
	if lis != nil {
		go func() { serveErr <- srv.Serve(lis) }()
	}
	linkCtx, closeLink := context.WithCancel(context.Background())
	defer closeLink()
	unlinked := make(chan struct{})
	if lc != nil {
		ll := link.NewListener()
		go func() { serveErr <- srv.Serve(ll) }()
		go func() { link.Run(linkCtx, *lc, ll); close(unlinked) }()
	} else {
		close(unlinked)
	}
	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}
	log.Printf("deplo-agent: signal received - draining in-flight RPCs…")
	// The link closes first: a stream riding it would otherwise hold the drain to its cap.
	closeLink()
	<-unlinked
	drained := make(chan struct{})
	go func() { srv.GracefulStop(); close(drained) }()
	select {
	case <-drained:
		log.Printf("deplo-agent: drained cleanly, exiting")
	case <-time.After(25 * time.Second):
		log.Printf("deplo-agent: drain timed out, forcing stop")
		srv.Stop()
	}
	return nil
}

// linkConfig is where the agent links, or nil: never without mTLS, since the link crosses the internet.
// With no saved address (or an unreadable one) it waits for the panel to send a pair over the listener.
func linkConfig(insecure bool, agentDir, certFile string) (*link.Config, error) {
	if insecure || agentDir == "" {
		return nil, nil
	}
	a, err := linkAddresses(agentDir)
	return &link.Config{Addresses: link.NewStore(agentDir, a), CertFile: certFile, Logf: log.Printf}, err
}

// linkAddresses is the saved pair; an agent enrolled before the agent link saves its install URL once.
func linkAddresses(agentDir string) (link.Addresses, error) {
	a, err := link.LoadAddresses(agentDir)
	if err != nil || a.Primary != "" {
		return a, err
	}
	if a.Primary = bootstrap.InstallURL(agentDir); a.Primary == "" {
		return a, nil
	}
	return a, link.SaveAddresses(agentDir, a)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return host == "localhost" || ip != nil && ip.IsLoopback()
}

func portFromAddr(addr string) int {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 9443
	}
	port := 0
	for _, c := range strings.TrimSpace(p) {
		if c < '0' || c > '9' {
			return 9443
		}
		port = port*10 + int(c-'0')
	}
	if port == 0 {
		return 9443
	}
	return port
}
