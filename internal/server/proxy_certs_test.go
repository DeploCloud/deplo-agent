package server

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// testPair is a self-signed certificate for names, valid until notAfter, and its key, as PEM.
func testPair(t *testing.T, notAfter time.Time, names ...string) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: names[0]}, DNSNames: names,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: notAfter}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kder, _ := x509.MarshalECPrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}))
}

// keyBody is the base64 body of a PEM key: what a leak would show, wrapped or not.
func keyBody(keyPEM string) string {
	b, _ := pem.Decode([]byte(keyPEM))
	return base64.StdEncoding.EncodeToString(b.Bytes)[:40]
}

const proxyCompose = `services:
  traefik:
    image: traefik:v3.7.17
    container_name: deplo-traefik
    command:
      - --providers.file.directory=/deplo-dynamic
      - --certificatesresolvers.letsencrypt.acme.httpchallenge=true
      - --certificatesresolvers.letsencrypt.acme.storage=/acme/acme.json
      - --certificatesresolvers.letsencrypt.acme.certificatesduration=1
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./acme:/acme
      - ./answers.yml:/deplo-dynamic/deplo-answers.yml:ro
`

// proxyHost is a Service with a proxy stack on disk, an empty ACME store and the shop App passed on here.
type proxyHost struct {
	s       *Service
	store   string
	routes  string
	runs    map[string]bool
	steps   []string
	applied int
}

func newProxyHost(t *testing.T) *proxyHost {
	t.Helper()
	s := newTestService(t)
	s.SetAgentDir(t.TempDir())
	dir := s.traefikDir()
	if err := os.MkdirAll(filepath.Join(dir, "acme"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "docker-compose.yml"), []byte(proxyCompose), 0o600); err != nil {
		t.Fatal(err)
	}
	h := &proxyHost{s: s, store: filepath.Join(dir, "acme", "acme.json"), routes: filepath.Join(dir, answerRoutesFile),
		runs: map[string]bool{"prj1": true}}
	if err := os.WriteFile(h.store, []byte(`{"letsencrypt":{"Account":{"Email":"ops@example.com"},"Certificates":null}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s.fwdOps = forwardOps{ensure: func(context.Context, forwardSpec) error { return nil },
		runs: func(_ context.Context, id string) (bool, error) { return h.runs[id], nil }}
	var st forwardState
	if err := validateAnswers(&pb.SetClusterForwardersRequest{AnswerPort: 8443, Answers: []*pb.ClusterAnswer{shop()}}, &st); err != nil {
		t.Fatal(err)
	}
	st.Cluster = "clu_1"
	s.forwarders().st = st
	s.traefikApply = func(context.Context, string, bool) error {
		h.steps = append(h.steps, "start")
		h.applied++
		return nil
	}
	old := stopProxy
	stopProxy = func(context.Context) error { h.steps = append(h.steps, "stop"); return nil }
	settle := certSettle
	certSettle = 0
	t.Cleanup(func() { stopProxy, certSettle = old, settle })
	return h
}

// placedOn is a lease from the leader that runs the shop App on another member.
func (h *proxyHost) placedOn(t *testing.T, member string) {
	t.Helper()
	h.s.leaseGuard().Renew(lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 1,
		Placement: []lease.Entry{{Kind: "app", ID: "prj1", Member: member, Slug: "shop"}}})
}

// putInStore writes a pair into the store the way Traefik does, base64 in JSON.
func (h *proxyHost) putInStore(t *testing.T, cert, key string, names ...string) {
	t.Helper()
	st, err := readACMEStore(h.store)
	if err != nil {
		t.Fatal(err)
	}
	var e acmeEntry
	e.Domain.Main, e.Domain.SANs = names[0], names[1:]
	e.Certificate, e.Key, e.Store = []byte(cert), []byte(key), "default"
	st.setEntries("letsencrypt", append(st.entries("letsencrypt"), e))
	if err := st.write(h.store); err != nil {
		t.Fatal(err)
	}
}

func (h *proxyHost) storeSums(t *testing.T) []string {
	t.Helper()
	st, err := readACMEStore(h.store)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, c := range st.certs() {
		out = append(out, c.Sum)
	}
	return out
}

func bundleCerts(pairs ...[2]string) json.RawMessage {
	var list []failoverCert
	for _, p := range pairs {
		list = append(list, failoverCert{Cert: p[0], Key: p[1]})
	}
	raw, _ := json.Marshal(list)
	return raw
}

func mustCert(t *testing.T, c, k string) proxyCert {
	t.Helper()
	pc, err := parseProxyCert(c, k)
	if err != nil {
		t.Fatal(err)
	}
	return pc
}

var shopNames = []string{"shop.example.com", "www.shop.example.com"}

func TestProxyCerts_aPairThatDoesNotMatchIsRefusedWithoutQuotingIt(t *testing.T) {
	c, _ := testPair(t, time.Now().Add(time.Hour), "a.example.com")
	_, k := testPair(t, time.Now().Add(time.Hour), "a.example.com")
	_, err := parseProxyCert(c, k)
	if err == nil {
		t.Fatal("a certificate with another key was taken")
	}
	if strings.Contains(err.Error(), keyBody(k)) || strings.Contains(err.Error(), "PRIVATE KEY") {
		t.Fatalf("the error quotes the key: %v", err)
	}
}

func TestProxyCerts_theStoreIsFoundFromTheProxysComposeFile(t *testing.T) {
	h := newProxyHost(t)
	acme, err := readProxyACME(h.s.traefikCompose())
	if err != nil {
		t.Fatal(err)
	}
	if acme.Store != h.store || acme.Resolver != "letsencrypt" || acme.Duration != time.Hour {
		t.Fatalf("got %+v, want %s, letsencrypt, 1h", acme, h.store)
	}
	abs := strings.Replace(proxyCompose, "./acme:/acme", "/opt/deplo/acme:/acme", 1)
	path := filepath.Join(t.TempDir(), "c.yml")
	_ = os.WriteFile(path, []byte(abs), 0o600)
	if acme, _ := readProxyACME(path); acme.Store != filepath.Join("/opt/deplo/acme", "acme.json") {
		t.Fatalf("an absolute mount reads as %q", acme.Store)
	}
	if renewBefore(time.Hour) != 20*time.Minute || renewBefore(2160*time.Hour) != 30*24*time.Hour {
		t.Fatal("the renewal window is not Traefik's")
	}
}

func TestProxyCerts_mergeKeepsTheNewerPairAndTheRestOfTheStore(t *testing.T) {
	h := newProxyHost(t)
	oldC, oldK := testPair(t, time.Now().Add(time.Hour), shopNames...)
	otherC, otherK := testPair(t, time.Now().Add(time.Hour), "blog.example.com")
	goneC, goneK := testPair(t, time.Now().Add(time.Hour), "away.example.com")
	h.putInStore(t, oldC, oldK, shopNames...)
	h.putInStore(t, otherC, otherK, "blog.example.com")
	h.putInStore(t, goneC, goneK, "away.example.com")
	newC, newK := testPair(t, time.Now().Add(3*time.Hour), shopNames...)
	stale, staleK := testPair(t, time.Now().Add(30*time.Minute), "blog.example.com")

	st, _ := readACMEStore(h.store)
	m := storeMerge{keep: []proxyCert{mustCert(t, newC, newK), mustCert(t, stale, staleK)}, drop: []string{"away.example.com"}}
	if changed, _ := m.apply(st, proxyACME{Resolver: "letsencrypt"}); !changed {
		t.Fatal("the store did not change")
	}
	if err := st.write(h.store); err != nil {
		t.Fatal(err)
	}
	got := h.storeSums(t)
	want := []string{mustCert(t, newC, newK).Sum, mustCert(t, otherC, otherK).Sum}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatal("the store does not hold the newer shop pair and the blog's own")
	}
	raw, _ := os.ReadFile(h.store)
	if !strings.Contains(string(raw), `"Email": "ops@example.com"`) {
		t.Fatalf("the account was lost: %s", raw)
	}
	if fi, _ := os.Stat(h.store); fi.Mode().Perm() != 0o600 {
		t.Fatalf("the store is %v", fi.Mode().Perm())
	}
}

func TestProxyCerts_aFailoverInstallsOnlyPairsForHostnamesPassedOnHere(t *testing.T) {
	h := newProxyHost(t)
	shopC, shopK := testPair(t, time.Now().Add(time.Hour), shopNames...)
	evilC, evilK := testPair(t, time.Now().Add(time.Hour), "bank.example.com")
	oldC, oldK := testPair(t, time.Now().Add(-time.Minute), shopNames...)
	n, err := h.s.forwarders().installCerts(bundleCerts([2]string{shopC, shopK}, [2]string{evilC, evilK}, [2]string{oldC, oldK}), time.Now())
	if err != nil || n != 1 {
		t.Fatalf("installed %d (%v), want the shop's one pair", n, err)
	}
	raw, _ := os.ReadFile(h.routes)
	if !strings.Contains(string(raw), `"certificates"`) || strings.Contains(string(raw), keyBody(evilK)) {
		t.Fatalf("the routes file holds the wrong pairs:\n%s", raw)
	}
	if fi, _ := os.Stat(h.routes); fi.Mode().Perm() != 0o600 {
		t.Fatalf("the routes file holding a key is %v", fi.Mode().Perm())
	}
	// An agent that restarts reads back what it installed.
	again := New(h.s.stackDir, h.s.buildTmpDir, h.s.dataDir, h.s.dataBase)
	again.SetAgentDir(h.s.agentDir)
	if got := again.forwarders().installedCerts(); len(got) != 1 || got[0].Sum != mustCert(t, shopC, shopK).Sum {
		t.Fatalf("a restarted agent holds %d installed pairs", len(got))
	}
	// A table never carries certificates of its own.
	var r map[string]any
	_ = json.Unmarshal([]byte(routesFor("prj1", shopTLS, shopWeb, "shop.example.com")), &r)
	r["tls"] = map[string]any{"certificates": []any{map[string]any{"certFile": evilC, "keyFile": evilK}}}
	bad, _ := json.Marshal(r)
	var st forwardState
	if validateAnswers(&pb.SetClusterForwardersRequest{AnswerPort: 8443, Answers: []*pb.ClusterAnswer{{AppId: "prj1",
		TlsPort: shopTLS, WebPort: shopWeb, Routes: string(bad)}}}, &st) == nil {
		t.Fatal("a table with certificates was taken")
	}
}

func TestProxyCerts_aPairOlderThanTheStoresIsNotInstalled(t *testing.T) {
	h := newProxyHost(t)
	newC, newK := testPair(t, time.Now().Add(3*time.Hour), shopNames...)
	h.putInStore(t, newC, newK, shopNames...)
	oldC, oldK := testPair(t, time.Now().Add(time.Hour), shopNames...)
	if n, _ := h.s.forwarders().installCerts(bundleCerts([2]string{oldC, oldK}), time.Now()); n != 0 {
		t.Fatal("an older pair hides the store's newer one")
	}
}

func TestProxyCerts_theStoreChangesOnlyWhileTheProxyIsStopped(t *testing.T) {
	h := newProxyHost(t)
	c, k := testPair(t, time.Now().Add(time.Hour), shopNames...)
	if _, err := h.s.forwarders().installCerts(bundleCerts([2]string{c, k}), time.Now()); err != nil {
		t.Fatal(err)
	}
	stopProxy = func(context.Context) error {
		h.steps = append(h.steps, "stop")
		if slices.Contains(h.storeSums(t), mustCert(t, c, k).Sum) {
			t.Error("the store changed before the proxy stopped")
		}
		return nil
	}
	merged, err := h.s.startProxy(context.Background(), h.s.traefikCompose(), true)
	if err != nil || !slices.Equal(merged, shopNames) {
		t.Fatalf("merged %v (%v)", merged, err)
	}
	if !slices.Equal(h.steps, []string{"stop", "start"}) || !slices.Contains(h.storeSums(t), mustCert(t, c, k).Sum) {
		t.Fatalf("steps %v, store holds the pair: %v", h.steps, slices.Contains(h.storeSums(t), mustCert(t, c, k).Sum))
	}
	raw, _ := os.ReadFile(h.routes)
	if strings.Contains(string(raw), "certificates") || len(h.s.forwarders().installedCerts()) != 0 {
		t.Fatal("the file stays after its pair went into the store: it would hide every renewal")
	}
	// Nothing left to change: the next start is a plain one.
	h.steps = nil
	if _, err := h.s.startProxy(context.Background(), h.s.traefikCompose(), true); err != nil || !slices.Equal(h.steps, []string{"start"}) {
		t.Fatalf("steps %v (%v)", h.steps, err)
	}
}

func TestProxyCerts_aServerThatNoLongerRunsTheAppDropsItsEntry(t *testing.T) {
	h := newProxyHost(t)
	c, k := testPair(t, time.Now().Add(time.Hour), shopNames...)
	otherC, otherK := testPair(t, time.Now().Add(time.Hour), "blog.example.com")
	h.putInStore(t, c, k, shopNames...)
	h.putInStore(t, otherC, otherK, "blog.example.com")
	h.runs["prj1"] = false
	h.placedOn(t, "srv_b")
	if _, err := h.s.startProxy(context.Background(), h.s.traefikCompose(), false); err != nil {
		t.Fatal(err)
	}
	if got := h.storeSums(t); len(got) != 1 || got[0] != mustCert(t, otherC, otherK).Sum {
		t.Fatal("the stale entry of an App that moved away stays, or another went with it")
	}
}

func TestProxyCerts_deplosReadComesBackWithoutAKeyItHolds(t *testing.T) {
	h := newProxyHost(t)
	oldC, oldK := testPair(t, time.Now().Add(time.Hour), shopNames...)
	newC, newK := testPair(t, time.Now().Add(2*time.Hour), shopNames...)
	h.putInStore(t, oldC, oldK, shopNames...)
	if _, err := h.s.forwarders().installCerts(bundleCerts([2]string{newC, newK}), time.Now()); err != nil {
		t.Fatal(err)
	}
	newest := mustCert(t, newC, newK)
	res, err := h.s.ReadProxyCertificates(context.Background(), &pb.ReadProxyCertificatesRequest{Hosts: shopNames})
	if err != nil || len(res.GetCertificates()) != 1 {
		t.Fatalf("%v (%v)", res, err)
	}
	got := res.GetCertificates()[0]
	if got.GetSha256() != newest.Sum || got.GetKeyPem() != newK || !slices.Equal(got.GetHosts(), shopNames) {
		t.Fatal("the read is not the newest pair for both hostnames")
	}
	res, _ = h.s.ReadProxyCertificates(context.Background(), &pb.ReadProxyCertificatesRequest{Hosts: shopNames,
		Known: map[string]string{"shop.example.com": newest.Sum, "www.shop.example.com": newest.Sum}})
	if res.GetCertificates()[0].GetKeyPem() != "" {
		t.Fatal("a pair Deplo holds came back with its key")
	}
	if _, err := h.s.ReadProxyCertificates(context.Background(), &pb.ReadProxyCertificatesRequest{Hosts: []string{"../etc"}}); err == nil {
		t.Fatal("a hostname that is none was taken")
	}
}

func TestProxyCerts_theRestartComesAtTheQuietHourBeforeTheWindow(t *testing.T) {
	loc := time.UTC
	now := time.Date(2026, 11, 19, 10, 0, 0, 0, loc)
	at, urgent := restartAt(now, now.Add(60*24*time.Hour), 4)
	if urgent || at.Hour() != 4 || at.After(now.Add(59*24*time.Hour)) || at.Before(now.Add(58*24*time.Hour)) {
		t.Fatalf("restart at %v (urgent %v), want 04:00 a day before the window", at, urgent)
	}
	if at, urgent := restartAt(now, now.Add(30*time.Minute), 4); !urgent || !at.Equal(now) {
		t.Fatalf("a window half an hour away restarts at %v", at)
	}
	p := &trafficProfile{Hours: map[int64]uint64{}, loaded: true}
	if p.quietHour(loc) != 4 {
		t.Fatal("with no profile the quiet hour is not 04:00")
	}
	start := now.Add(-6 * 24 * time.Hour).Truncate(time.Hour)
	for h := start; h.Before(now); h = h.Add(time.Hour) {
		b := uint64(1000)
		if h.Hour() == 2 {
			b = 10
		}
		p.Hours[h.Unix()/3600] = b
	}
	if q := p.quietHour(loc); q != 2 {
		t.Fatalf("the quietest hour is %d, want 2", q)
	}
}

func TestProxyCerts_aRestartForARenewalIsRecordedUntilDeploReadsIt(t *testing.T) {
	h := newProxyHost(t)
	// Ordered for an hour, so renewed 20 minutes before the end: with 25 minutes left the window is 5 away.
	c, k := testPair(t, time.Now().Add(25*time.Minute), shopNames...)
	if _, err := h.s.forwarders().installCerts(bundleCerts([2]string{c, k}), time.Now()); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	h.s.restartForRenewal(context.Background(), now)
	if !slices.Equal(h.steps, []string{"stop", "start"}) {
		t.Fatalf("steps %v, want one restart with the store changed", h.steps)
	}
	got := h.s.proxyRestarts().take(0)
	if len(got) != 1 || !got[0].GetUrgent() || !slices.Equal(got[0].GetHosts(), shopNames) {
		t.Fatalf("recorded %v", got)
	}
	h.s.restartForRenewal(context.Background(), now.Add(time.Minute))
	if h.applied != 1 {
		t.Fatal("the proxy restarted again with nothing to put in its store")
	}
	if left := h.s.proxyRestarts().take(got[0].GetAtUnixMs()); len(left) != 0 {
		t.Fatal("a restart Deplo recorded is handed again")
	}
}

func TestProxyCerts_aFailoverInstallsTheCertificateBeforeTheAppStarts(t *testing.T) {
	h := newProxyHost(t)
	stubs := stubFailover(t)
	c, k := testPair(t, time.Now().Add(time.Hour), shopNames...)
	var seen bool
	runFailoverDeploy = func(_ *Service, _ context.Context, _ *pb.DeployRequest, e *emitter) {
		raw, _ := os.ReadFile(h.routes)
		seen = strings.Contains(string(raw), "certificates")
		e.result(true, "", "")
	}
	_ = stubs
	err := h.s.startFromBundle(context.Background(), &lease.Start{DeploymentID: "dpl1", Slug: "shop",
		ComposeYAML: "services: {web: {image: shop}}", Certificates: bundleCerts([2]string{c, k})})
	if err != nil || !seen {
		t.Fatalf("started %v, the certificate was installed first: %v", err, seen)
	}
}

// The key is written where the proxy reads it and nowhere else, and no log line carries it.
func TestProxyCerts_theKeyIsNeverLoggedNorWrittenOutsideTheProxysFiles(t *testing.T) {
	h := newProxyHost(t)
	c, k := testPair(t, time.Now().Add(time.Hour), shopNames...)
	logs := captureStderr(t, func() {
		_, _ = h.s.forwarders().installCerts(bundleCerts([2]string{c, k}), time.Now())
		_, wrongK := testPair(t, time.Now().Add(time.Hour), shopNames...)
		_, _ = h.s.forwarders().installCerts(bundleCerts([2]string{c, wrongK}), time.Now())
		_ = os.WriteFile(h.store, []byte("{not json"), 0o600)
		_, _ = h.s.startProxy(context.Background(), h.s.traefikCompose(), true)
		_ = h.s.startFromBundle(context.Background(), &lease.Start{Slug: "shop", ComposeYAML: "services: {}",
			Certificates: json.RawMessage(`[{"cert":` + strings.Repeat(`"x`, 1) + `"}]`)})
	})
	if strings.Contains(logs, keyBody(k)) || strings.Contains(logs, "PRIVATE KEY") {
		t.Fatalf("a log line carries the key:\n%s", logs)
	}
	h.s.forwarders().save()
	allowed := map[string]bool{h.routes: true, h.store: true}
	_ = filepath.Walk(h.s.agentDir, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi.IsDir() {
			return nil
		}
		raw, _ := os.ReadFile(p)
		pemB64 := base64.StdEncoding.EncodeToString([]byte(k))[:40]
		if !bytes.Contains(raw, []byte(keyBody(k))) && !bytes.Contains(raw, []byte("PRIVATE KEY")) && !bytes.Contains(raw, []byte(pemB64)) {
			return nil
		}
		if !allowed[p] {
			t.Errorf("%s holds the key", p)
		} else if fi.Mode().Perm() != 0o600 {
			t.Errorf("%s holds the key with mode %v", p, fi.Mode().Perm())
		}
		return nil
	})
}

func captureStderr(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	done := make(chan string)
	go func() { b, _ := io.ReadAll(r); done <- string(b) }()
	f()
	os.Stderr = old
	_ = w.Close()
	return <-done
}

func TestProxyCerts_advertised(t *testing.T) {
	if !slices.Contains(Capabilities, "cluster.answers.certificates") {
		t.Fatal("Hello does not name cluster.answers.certificates")
	}
}

// A pair of an App that never started here restarts nothing; it goes from the routes file once it is stale.
func TestProxyCerts_aPairOfAnAppThatDoesNotRunHereRestartsNothingAndGoes(t *testing.T) {
	h := newProxyHost(t)
	c, k := testPair(t, time.Now().Add(25*time.Minute), shopNames...)
	if _, err := h.s.forwarders().installCerts(bundleCerts([2]string{c, k}), time.Now()); err != nil {
		t.Fatal(err)
	}
	h.runs["prj1"] = false
	now := time.Now()
	for i := 0; i < 3; i++ {
		h.s.restartForRenewal(context.Background(), now.Add(time.Duration(i)*time.Minute))
	}
	if h.applied != 0 {
		t.Fatalf("the proxy restarted %d times for an App that does not run here", h.applied)
	}
	h.s.forwarders().pruneFiles(context.Background(), now.Add(time.Minute))
	if len(h.s.forwarders().installedCerts()) != 1 {
		t.Fatal("a pair just installed for a failover went before its App could start")
	}
	h.s.forwarders().pruneFiles(context.Background(), now.Add(staleAfter+time.Minute))
	if len(h.s.forwarders().installedCerts()) != 0 || h.applied != 0 {
		t.Fatal("a stale pair stays, or its removal restarted the proxy")
	}
}

func TestProxyCerts_anExpiredPairGoesAndAFailedMergeIsNotRetriedEveryMinute(t *testing.T) {
	h := newProxyHost(t)
	c, k := testPair(t, time.Now().Add(25*time.Minute), shopNames...)
	if _, err := h.s.forwarders().installCerts(bundleCerts([2]string{c, k}), time.Now()); err != nil {
		t.Fatal(err)
	}
	stopProxy = func(context.Context) error { return errors.New("docker is gone") }
	now := time.Now()
	h.s.restartForRenewal(context.Background(), now)
	h.s.restartForRenewal(context.Background(), now.Add(time.Minute))
	if h.applied != 1 {
		t.Fatalf("the proxy restarted %d times, want once before backing off", h.applied)
	}
	h.s.forwarders().pruneFiles(context.Background(), now.Add(26*time.Minute))
	if len(h.s.forwarders().installedCerts()) != 0 {
		t.Fatal("an expired pair is still served from the routes file")
	}
}

// Found on the lab: a server the App left renewed its stale entry on its account's earlier authorization, with no
// check at all. Once the App has run elsewhere for a while, the proxy restarts once and the entry goes.
func TestProxyCerts_aServerTheAppLeftDropsItsEntryOnceAndSaysSo(t *testing.T) {
	h := newProxyHost(t)
	c, k := testPair(t, time.Now().Add(time.Hour), shopNames...)
	h.putInStore(t, c, k, shopNames...)
	h.runs["prj1"] = false
	now := time.Now()
	h.s.dropStaleEntries(context.Background(), now.Add(time.Hour))
	if h.applied != 0 {
		t.Fatal("an App stopped on its own server lost its certificate")
	}
	h.placedOn(t, "srv_b")
	h.s.dropStaleEntries(context.Background(), now)
	if h.applied != 0 {
		t.Fatal("the proxy restarted the moment the App left: a move back would have cost it")
	}
	h.s.dropStaleEntries(context.Background(), now.Add(staleFor+time.Second))
	if h.applied != 1 || len(h.storeSums(t)) != 0 {
		t.Fatalf("restarts %d, entries left %d: want one restart that drops it", h.applied, len(h.storeSums(t)))
	}
	got := h.s.proxyRestarts().take(0)
	if len(got) != 1 || !got[0].GetDropped() || !slices.Equal(got[0].GetHosts(), shopNames) {
		t.Fatalf("recorded %v", got)
	}
	h.s.dropStaleEntries(context.Background(), now.Add(time.Hour))
	if h.applied != 1 {
		t.Fatal("the proxy restarted again with nothing left to drop")
	}
}

func TestProxyCerts_aServerThatRunsTheAppKeepsItsEntry(t *testing.T) {
	h := newProxyHost(t)
	c, k := testPair(t, time.Now().Add(time.Hour), shopNames...)
	h.putInStore(t, c, k, shopNames...)
	h.s.dropStaleEntries(context.Background(), time.Now())
	h.s.dropStaleEntries(context.Background(), time.Now().Add(time.Hour))
	if h.applied != 0 || len(h.storeSums(t)) != 1 {
		t.Fatal("the App's own server restarted its proxy or lost its certificate")
	}
}
