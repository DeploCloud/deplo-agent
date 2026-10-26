package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
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
	"github.com/DeploCloud/deplo-agent/internal/pkgupdate"
	"github.com/DeploCloud/deplo-agent/internal/server"
)

func main() {
	// The same binary, mounted into the screenshot browser's container.
	if len(os.Args) > 1 && os.Args[1] == "capture" {
		os.Exit(capture.Main(os.Stdin, os.Stdout))
	}
	// And into the container that moves one dependency with the app's own package manager.
	if len(os.Args) > 1 && os.Args[1] == "pkgupdate" {
		os.Exit(pkgupdate.Main(os.Stdin, os.Stdout))
	}
	var (
		// Units written before the agent link pass --addr, and self-update keeps their argv: taken, hidden, ignored.
		addr        = flag.String(legacyAddr, "", "")
		showCaps    = flag.Bool("capabilities", false, "print the capabilities Hello advertises, one per line, and exit")
		certFile    = flag.String("cert", envOr("DEPLO_AGENT_CERT", ""), "agent server certificate (PEM)")
		keyFile     = flag.String("key", envOr("DEPLO_AGENT_KEY", ""), "agent server private key (PEM)")
		caFile      = flag.String("ca", envOr("DEPLO_AGENT_CA", ""), "CA certificate to verify the control plane (PEM)")
		agentDir    = flag.String("agent-dir", envOr("DEPLO_AGENT_DIR", ""), "directory holding mTLS materials (bootstrap writes them here)")
		stackDir    = flag.String("stack-dir", envOr("DEPLO_AGENT_STACK_DIR", "/data/stacks"), "where rendered stack files are written")
		buildTmpDir = flag.String("build-tmp", envOr("DEPLO_AGENT_BUILD_TMP", os.TempDir()), "where upload build contexts are extracted")
		dataDir     = flag.String("data-dir", envOr("DEPLO_AGENT_DATA_DIR", "/"), "filesystem measured for disk metrics")
		dataBase    = flag.String("data-base", envOr("DEPLO_AGENT_DATA_BASE", ""), "host data root for dev workspaces + the SSH gateway (empty => parent of --stack-dir)")

		bootstrapURL   = flag.String("bootstrap-url", envOr("DEPLO_BOOTSTRAP_URL", ""), "control-plane URL to call home to on first run")
		bootstrapTok   = flag.String("bootstrap-token", envOr("DEPLO_BOOTSTRAP_TOKEN", ""), "one-time bootstrap token")
		bootstrapFP    = flag.String("bootstrap-fingerprint", envOr("DEPLO_BOOTSTRAP_FINGERPRINT", ""), "expected control-plane cert sha256 (HTTPS only)")
		advertisedHost = flag.String("advertised-host", envOr("DEPLO_AGENT_ADVERTISED_HOST", ""), "address the agent reports it is reachable at (informational)")
	)
	flag.Usage = usage
	flag.Parse()
	for _, k := range []string{"DEPLO_BOOTSTRAP_URL", "DEPLO_BOOTSTRAP_TOKEN", "DEPLO_BOOTSTRAP_FINGERPRINT"} {
		_ = os.Unsetenv(k)
	}
	if *showCaps {
		printCapabilities(os.Stdout)
		return
	}

	if *addr != "" {
		log.Printf("deplo-agent: ignoring --addr %s: the agent listens nowhere, it links out to its panel", *addr)
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
			log.Printf("deplo-agent: bootstrapping against %s", *bootstrapURL)
			if _, err := bootstrap.Run(bootstrap.Config{
				ControlPlaneURL: *bootstrapURL,
				Token:           *bootstrapTok,
				Fingerprint:     *bootstrapFP,
				AdvertisedHost:  *advertisedHost,
				AgentDir:        *agentDir,
			}); err != nil {
				log.Fatalf("deplo-agent: bootstrap failed: %v", err)
			}
			log.Printf("deplo-agent: bootstrap complete; materials in %s", *agentDir)
		}
		cert, key, ca = m.CertPath, m.KeyPath, m.CAPath
	}

	certMgr, err := server.NewCertManager(cert, key, ca)
	if err != nil {
		log.Fatalf("deplo-agent: mTLS setup: %v", err)
	}

	srv := newGRPCServer(certMgr)
	svc := server.New(*stackDir, *buildTmpDir, *dataDir, *dataBase)
	svc.SetAgentDir(*agentDir)
	svc.EnableCertRenewal(certMgr)
	pb.RegisterAgentServer(srv, svc)

	lc, err := linkConfig(*agentDir, cert, *bootstrapURL)
	if err != nil {
		log.Printf("deplo-agent: agent link: %v", err)
	}
	if lc == nil || lc.Addresses.Current().Primary == "" {
		log.Fatalf("deplo-agent: no panel address to link to, so nothing could reach this agent")
	}
	svc.EnableAgentLink(lc.Addresses)
	log.Printf("deplo-agent %s: linking to %s", server.AgentVersion, lc.Addresses.Current().Primary)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go svc.WarmRailpack(ctx)
	if err := serveAgent(ctx, srv, *lc); err != nil {
		log.Fatalf("deplo-agent: serve: %v", err)
	}
}

// legacyAddr is the listen-address flag of agents from before the link, hidden from the usage text.
const legacyAddr = "addr"

// usage is flag's own, without the flags kept only so that old units still start.
func usage() {
	shown := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	shown.SetOutput(flag.CommandLine.Output())
	flag.VisitAll(func(f *flag.Flag) {
		if f.Name != legacyAddr {
			shown.Var(f.Value, f.Name, f.Usage)
		}
	})
	fmt.Fprintf(shown.Output(), "Usage of %s:\n", os.Args[0])
	shown.PrintDefaults()
}

// newGRPCServer is the agent's one gRPC server, served over each agent link in turn.
func newGRPCServer(certMgr *server.CertManager) *grpc.Server {
	return grpc.NewServer(
		grpc.Creds(credentials.NewTLS(certMgr.ServerTLSConfig())),
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
}

// printCapabilities answers an installer asking what this binary can do, e.g. whether it links.
func printCapabilities(w io.Writer) {
	for _, c := range server.Capabilities {
		fmt.Fprintln(w, c)
	}
}

// serveAgent serves srv over the agent link lc keeps open, until ctx ends (ADR-0040).
func serveAgent(ctx context.Context, srv *grpc.Server, lc link.Config) error {
	serveErr := make(chan error, 1)
	ll := link.NewListener()
	go func() { serveErr <- srv.Serve(ll) }()
	linkCtx, closeLink := context.WithCancel(context.Background())
	defer closeLink()
	unlinked := make(chan struct{})
	go func() { link.Run(linkCtx, lc, ll); close(unlinked) }()
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

// linkConfig is where the agent links, or nil with no agent dir to keep its addresses in.
func linkConfig(agentDir, certFile, bootstrapURL string) (*link.Config, error) {
	if agentDir == "" {
		return nil, nil
	}
	a, err := linkAddresses(agentDir, bootstrapURL)
	return &link.Config{Addresses: link.NewStore(agentDir, a), CertFile: certFile, Logf: log.Printf}, err
}

// linkAddresses is the saved pair. Without one, or with an unreadable one, the agent saves its install URL:
// from bootstrap.env or, under the first units, from --bootstrap-url. Nothing else could reach it.
func linkAddresses(agentDir, bootstrapURL string) (link.Addresses, error) {
	a, err := link.LoadAddresses(agentDir)
	if a.Primary != "" {
		return a, nil
	}
	if a.Primary = bootstrap.InstallURL(agentDir); a.Primary == "" {
		a.Primary = strings.TrimRight(strings.TrimSpace(bootstrapURL), "/")
	}
	if a.Primary == "" {
		return a, err
	}
	return a, errors.Join(err, link.SaveAddresses(agentDir, a))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
