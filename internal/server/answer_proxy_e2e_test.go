package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// End-to-end (real docker, real proxy): fise on eu-1 (22 Nov 2026). Its database failed its health check, its web
// service never started, and every visitor's TLS through the proxy's passthrough router waited for an App that
// would not get ready: the ClientHello got no answer for 30 s. A broken App is passed on to the proxy at once.
func TestE2E_aBrokenAppsVisitorGetsAnAnswerThroughTheRealProxy(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	old := answerWait
	answerWait = 10 * time.Second
	t.Cleanup(func() { answerWait = old })
	const app, project = "prj_e2eproxy", "deplo-agent-test-proxy-broken"
	dir := t.TempDir()

	// The App: a database that never gets healthy, and the web service that waits on it.
	file := filepath.Join(dir, "compose.yml")
	stack := "services:\n" +
		"  db:\n    image: busybox:1.37\n    command: [\"sleep\", \"600\"]\n    labels: [\"deplo.project=" + app + "\"]\n" +
		"    healthcheck: {test: [\"CMD\", \"false\"], interval: 1s, retries: 1, timeout: 1s}\n" +
		"  web:\n    image: busybox:1.37\n    command: [\"sleep\", \"600\"]\n    labels: [\"deplo.project=" + app + "\"]\n" +
		"    depends_on: {db: {condition: service_healthy}}\n"
	if err := os.WriteFile(file, []byte(stack), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = dockercli.Run(context.Background(), 60*time.Second, "compose", "-p", project, "-f", file, "down", "-t", "1")
	})
	_, _ = dockercli.Run(ctx, 120*time.Second, "compose", "-p", project, "-f", file, "up", "-d")

	// The answer forwarder's port, where the proxy passes the domain on, PROXY header first.
	fwd, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fwd.Close() })
	websecure, internal := freePort(t), freePort(t)
	routes := fmt.Sprintf(`tcp:
  routers:
    deplo-answer-%[1]s:
      rule: HostSNI(`+"`shop.example.com`"+`)
      entryPoints: [websecure]
      service: deplo-answer-%[1]s
      tls: {passthrough: true}
  services:
    deplo-answer-%[1]s:
      loadBalancer:
        serversTransport: deplo-answer
        servers: [{address: "%[2]s"}]
  serversTransports:
    deplo-answer: {proxyProtocol: {version: 2}}
http:
  routers:
    shop:
      rule: Host(`+"`shop.example.com`"+`)
      entryPoints: [answer]
      service: noop@internal
      tls: {}
`, app, fwd.Addr().String())
	if err := os.MkdirAll(filepath.Join(dir, "dynamic"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dynamic", "answers.yml"), []byte(routes), 0o644); err != nil {
		t.Fatal(err)
	}
	name := "deplo-agent-test-proxy-" + fmt.Sprint(websecure)
	t.Cleanup(func() { _, _ = dockercli.Run(context.Background(), 30*time.Second, "rm", "-f", name) })
	if res, err := dockercli.Run(ctx, 120*time.Second, "run", "-d", "--name", name, "--network", "host",
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-v", filepath.Join(dir, "dynamic")+":/deplo-dynamic:ro", "traefik:v3.7.17",
		fmt.Sprintf("--entrypoints.websecure.address=127.0.0.1:%d", websecure),
		fmt.Sprintf("--entrypoints.answer.address=127.0.0.1:%d", internal),
		"--entrypoints.answer.proxyprotocol.trustedips=127.0.0.1/32",
		"--providers.file.directory=/deplo-dynamic"); err != nil || res.Code != 0 {
		t.Fatalf("proxy: %v %s", err, res.Stderr)
	}

	// This agent: the App placed here, docker's real readiness, its proxy's internal entrypoint on the host.
	s := New(t.TempDir(), t.TempDir(), "/", "")
	f := s.forwarders()
	f.mu.Lock()
	f.st.Cluster = "cl1"
	f.st.Answers = []answer{{AppID: app, TLSPort: 40443, WebPort: 40080}}
	f.st.ServeAnswers = []string{app}
	f.st.AnswerPort = uint32(internal)
	f.mu.Unlock()
	// Docker's own readiness; the proxy is reached on the host here, where the agent enters its network.
	ops := dockerForwardOps
	ops.proxyDial = func(ctx context.Context, port uint32) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	}
	s.fwdOps = ops
	go func() {
		for {
			c, err := fwd.Accept()
			if err != nil {
				return
			}
			go f.serveAnswer(&prefixed{Conn: c, r: io.MultiReader(strings.NewReader("PORT 40443\n"), c)})
		}
	}()
	waitWithin(t, 60*time.Second, "the database to fail its health check", func() bool {
		res, err := dockercli.Run(ctx, 10*time.Second, "ps", "-q", "--filter", "label=deplo.project="+app, "--filter", "health=unhealthy")
		return err == nil && strings.TrimSpace(res.Stdout) != ""
	})
	waitWithin(t, 30*time.Second, "the proxy to listen", func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", websecure), time.Second)
		if err == nil {
			c.Close()
		}
		return err == nil
	})

	// The leader's word, just now: a lease is current for a few seconds only.
	placeOn(t, s, "srvA", lease.Entry{Kind: "app", ID: app, Member: "srvA", Slug: "proxy-broken"})
	start := time.Now()
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: 15 * time.Second}, "tcp", fmt.Sprintf("127.0.0.1:%d", websecure),
		&tls.Config{ServerName: "shop.example.com", InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("no TLS after %v: %v", time.Since(start), err)
	}
	defer c.Close()
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the visitor waited %v for a broken App", took)
	}
}

type prefixed struct {
	net.Conn
	r io.Reader
}

func (p *prefixed) Read(b []byte) (int, error) { return p.r.Read(b) }

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
