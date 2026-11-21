package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The real proxy and a real certificate authority: a server back from a power cut, its store holding the zone's pair
// and a passed-on App's, both inside their renewal window, gets no certificate whatever the CA's reuse policy, and
// renews both once they are its own again. Pebble validates every check, so only the hold can stop an order.
const (
	pebbleImage = "ghcr.io/letsencrypt/pebble@sha256:0c81f05db371f436c97fb894bd6d2ab8fc9c8e2ce40f8bef8a8e15941bcd75ef"
	whoamiImage = "traefik/whoami:v1.11.0"
)

func dockerOut(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// holdLab is one CA, one check endpoint and one proxy on a network of their own, all removed with the test.
type holdLab struct {
	name, dir, agentDir string
	proxyArgs           []string
}

func newHoldLab(t *testing.T, reuse int) *holdLab {
	t.Helper()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	l := &holdLab{name: fmt.Sprintf("deplo-agent-test-hold-%d-%s", reuse, hex.EncodeToString(b)), dir: t.TempDir()}
	l.agentDir = filepath.Join(l.dir, "agent")
	tdir := filepath.Join(l.agentDir, "traefik")
	if err := os.MkdirAll(filepath.Join(tdir, "acme"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, c := range []string{"proxy", "pebble", "whoami"} {
			_ = exec.Command("docker", "rm", "-f", l.name+"-"+c).Run()
		}
		_ = exec.Command("docker", "network", "rm", l.name).Run()
	})
	dockerOut(t, "network", "create", l.name)
	cfg := `{"pebble":{"listenAddress":"0.0.0.0:14000","managementListenAddress":"0.0.0.0:15000",` +
		`"certificate":"test/certs/localhost/cert.pem","privateKey":"test/certs/localhost/key.pem","httpPort":5002,"tlsPort":5001,` +
		`"keyAlgorithm":"ecdsa","profiles":{"default":{"description":"ten minutes","validityPeriod":600}}}}`
	if err := os.WriteFile(filepath.Join(l.dir, "pebble.json"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	dockerOut(t, "run", "-d", "--name", l.name+"-pebble", "--network", l.name, "--network-alias", "pebble",
		"-e", "PEBBLE_VA_ALWAYS_VALID=1", "-e", "PEBBLE_VA_NOSLEEP=1", "-e", "PEBBLE_WFE_NONCEREJECT=0",
		"-e", fmt.Sprintf("PEBBLE_AUTHZREUSE=%d", reuse), "-v", filepath.Join(l.dir, "pebble.json")+":/pebble.json:ro",
		pebbleImage, "-config", "/pebble.json")
	dockerOut(t, "run", "-d", "--name", l.name+"-whoami", "--network", l.name, "--network-alias", "checks", whoamiImage)
	id := dockerOut(t, "create", pebbleImage)
	dockerOut(t, "cp", id+":/test/certs/pebble.minica.pem", filepath.Join(l.dir, "ca.pem"))
	dockerOut(t, "rm", id)

	var flags []string
	for _, r := range []string{"letsencrypt", zoneResolver} {
		p := "--certificatesresolvers." + r + ".acme."
		flags = append(flags, p+"caserver=https://pebble:14000/dir", p+"email=ops@example.com", p+"storage=/acme/acme.json",
			p+"certificatesduration=1")
	}
	flags = append(flags, "--certificatesresolvers.letsencrypt.acme.httpchallenge.entrypoint=web",
		"--certificatesresolvers.deplo-dns.acme.dnschallenge.provider=httpreq",
		"--certificatesresolvers.deplo-dns.acme.dnschallenge.propagation.disableChecks=true",
		"--log.level=INFO", "--providers.file.directory=/deplo-dynamic", "--entrypoints.web.address=:8000", "--entrypoints.websecure.address=:8443")
	compose := "services:\n  traefik:\n    image: traefik:v3.7.17\n    command:\n"
	for _, f := range flags {
		compose += "      - " + f + "\n"
	}
	compose += "    volumes:\n      - ./acme:/acme\n"
	if err := os.WriteFile(filepath.Join(tdir, "docker-compose.yml"), []byte(compose), 0o600); err != nil {
		t.Fatal(err)
	}
	// The passed-on shop App, the zone's order router, and the router a copy of the App left running here would carry.
	routes := `{"tcp":{"routers":{"deplo-answer-prj1":{"rule":"HostSNI(` + "`shop.example.com`" + `)","entryPoints":["websecure"],` +
		`"service":"deplo-answer-prj1","tls":{"passthrough":true}}},"services":{"deplo-answer-prj1":{"loadBalancer":{"servers":[{"address":"127.0.0.1:1"}]}}}},` +
		`"http":{"routers":{"deplo-zone-1":{"rule":"Host(` + "`deplo-zone-certificate.invalid`" + `)","entryPoints":["websecure"],` +
		`"service":"noop@internal","tls":{"certResolver":"deplo-dns","domains":[{"main":"go.example.com","sans":["*.go.example.com"]}]}}}}}`
	if err := os.WriteFile(filepath.Join(tdir, answerRoutesFile), []byte(routes), 0o600); err != nil {
		t.Fatal(err)
	}
	app := "http:\n  routers:\n    shop:\n      rule: Host(`shop.example.com`)\n      entryPoints: [websecure]\n" +
		"      service: noop@internal\n      tls:\n        certResolver: letsencrypt\n"
	if err := os.MkdirAll(filepath.Join(l.dir, "app"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(l.dir, "app", "shop.yml"), []byte(app), 0o644); err != nil {
		t.Fatal(err)
	}
	l.proxyArgs = append([]string{"run", "-d", "--name", l.name + "-proxy", "--network", l.name,
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-e", "LEGO_CA_CERTIFICATES=/ca.pem", "-e", "HTTPREQ_ENDPOINT=http://checks",
		"-v", filepath.Join(l.dir, "ca.pem") + ":/ca.pem:ro", "-v", filepath.Join(tdir, "acme") + ":/acme",
		"-v", filepath.Join(tdir, answerRoutesFile) + ":/deplo-dynamic/deplo-answers.yml:ro",
		"-v", filepath.Join(l.dir, "app", "shop.yml") + ":/deplo-dynamic/shop.yml:ro", "traefik:v3.7.17"}, flags...)
	return l
}

func (l *holdLab) issued(t *testing.T) int {
	t.Helper()
	return strings.Count(dockerOut(t, "logs", l.name+"-pebble"), "Issued certificate serial")
}

func (l *holdLab) store(t *testing.T) *acmeStore {
	t.Helper()
	st, err := readACMEStore(filepath.Join(l.agentDir, "traefik", "acme", "acme.json"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func waitWithin(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	end := time.Now().Add(within)
	for !ok() {
		if time.Now().After(end) {
			t.Fatalf("waited %s for %s", within, what)
		}
		time.Sleep(time.Second)
	}
}

func TestE2E_aServerBackFromAPowerCutGetsNoCertificateWhateverTheCAReuses(t *testing.T) {
	needsRealDocker(t)
	for _, reuse := range []int{100, 0, 50} {
		t.Run(fmt.Sprintf("reuse%d", reuse), func(t *testing.T) {
			t.Parallel()
			l := newHoldLab(t, reuse)
			proxy := l.name + "-proxy"
			dockerOut(t, l.proxyArgs...)
			waitWithin(t, 90*time.Second, "both certificates", func() bool {
				st := l.store(t)
				return len(st.entries(zoneResolver)) == 1 && len(st.entries("letsencrypt")) == 1
			})
			// The power goes: the proxy stops where it stands, and the hold runs before it starts again.
			dockerOut(t, "kill", proxy)
			if n, err := ProxyHold(l.agentDir); err != nil || n != 2 {
				t.Fatalf("held %d (%v)", n, err)
			}
			before := l.issued(t)
			dockerOut(t, "start", proxy)
			// Each pair is inside its window: unheld, the proxy renews at start and again every minute.
			time.Sleep(75 * time.Second)
			if got := l.issued(t) - before; got != 0 {
				t.Fatalf("the CA issued %d certificates to a server back from a power cut\n%s", got, dockerOut(t, "logs", proxy))
			}
			if !strings.Contains(dockerOut(t, "logs", proxy), "Testing certificate renew") {
				t.Fatal("the proxy never looked at its store, so the run proves nothing")
			}

			// Deplo and the leader say both are still this server's: back in the store, both are renewed.
			dockerOut(t, "stop", proxy)
			s := &Service{agentDir: l.agentDir}
			var keep []proxyCert
			keep = append(keep, s.forwarders().installedCerts()...)
			m := storeMerge{keep: keep, dns: map[string]bool{"go.example.com": true, "*.go.example.com": true}}
			acme, err := readProxyACME(s.traefikCompose())
			if err != nil {
				t.Fatal(err)
			}
			st := l.store(t)
			if changed, _ := m.apply(st, acme); !changed {
				t.Fatal("nothing went back into the store")
			}
			if err := st.write(acme.Store); err != nil {
				t.Fatal(err)
			}
			before = l.issued(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if out, err := exec.CommandContext(ctx, "docker", "start", proxy).CombinedOutput(); err != nil {
				t.Fatalf("%v: %s", err, out)
			}
			waitWithin(t, 90*time.Second, "the renewer's renewals", func() bool { return l.issued(t)-before >= 2 })
		})
	}
}
