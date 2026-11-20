package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/acme"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// fakeCA answers the ACME calls an authorization close makes: a new order that reuses the account's two valid
// authorizations, and each one read and deactivated.
type fakeCA struct {
	mu     sync.Mutex
	authz  map[string]string
	orders int
	srv    *httptest.Server
}

func newFakeCA(t *testing.T) *fakeCA {
	ca := &fakeCA{authz: map[string]string{"1": "valid", "2": "valid"}}
	mux := http.NewServeMux()
	base := func() string { return ca.srv.URL }
	nonce := func(w http.ResponseWriter) { w.Header().Set("Replay-Nonce", fmt.Sprint(time.Now().UnixNano())) }
	mux.HandleFunc("/dir", func(w http.ResponseWriter, r *http.Request) {
		nonce(w)
		_ = json.NewEncoder(w).Encode(map[string]string{"newNonce": base() + "/nonce", "newAccount": base() + "/acct", "newOrder": base() + "/order"})
	})
	mux.HandleFunc("/nonce", func(w http.ResponseWriter, r *http.Request) { nonce(w); w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		ca.mu.Lock()
		ca.orders++
		ca.mu.Unlock()
		nonce(w)
		w.Header().Set("Location", base()+"/order/1")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "pending", "finalize": base() + "/finalize",
			"identifiers":    []map[string]string{{"type": "dns", "value": "go.example.com"}, {"type": "dns", "value": "*.go.example.com"}},
			"authorizations": []string{base() + "/authz/1", base() + "/authz/2"}})
	})
	mux.HandleFunc("/authz/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/authz/")
		var jws struct {
			Payload string `json:"payload"`
		}
		_ = json.NewDecoder(r.Body).Decode(&jws)
		body, _ := base64.RawURLEncoding.DecodeString(jws.Payload)
		ca.mu.Lock()
		if strings.Contains(string(body), "deactivated") {
			ca.authz[id] = "deactivated"
		}
		st := ca.authz[id]
		ca.mu.Unlock()
		nonce(w)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": st, "identifier": map[string]string{"type": "dns", "value": "go.example.com"}})
	})
	ca.srv = httptest.NewServer(mux)
	t.Cleanup(ca.srv.Close)
	return ca
}

func (ca *fakeCA) states() (map[string]string, int) {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	out := map[string]string{}
	for k, v := range ca.authz {
		out[k] = v
	}
	return out, ca.orders
}

// renewerOf is a proxy host whose DNS resolver orders from the fake CA and whose store holds the zone's pair.
func renewerOf(t *testing.T, ca *fakeCA, renew bool) *proxyHost {
	t.Helper()
	h := zoneCertHost(t)
	compose := strings.Replace(proxyCompose, "    volumes:\n",
		"      - --certificatesresolvers.deplo-dns.acme.storage=/acme/acme.json\n"+
			"      - --certificatesresolvers.deplo-dns.acme.dnschallenge.provider=httpreq\n"+
			"      - --certificatesresolvers.deplo-dns.acme.caserver="+ca.srv.URL+"/dir\n    volumes:\n", 1)
	if err := os.WriteFile(h.s.traefikCompose(), []byte(compose), 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	c, k := testPair(t, time.Now().Add(48*time.Hour), zoneNames()...)
	st, err := readACMEStore(h.store)
	if err != nil {
		t.Fatal(err)
	}
	acct, _ := json.Marshal(map[string]any{"Email": "ops@example.com", "Registration": map[string]string{"uri": ca.srv.URL + "/acct/1"},
		"PrivateKey": x509.MarshalPKCS1PrivateKey(key)})
	st.order = append(st.order, zoneResolver)
	st.resolvers[zoneResolver] = map[string]json.RawMessage{"Account": acct}
	var e acmeEntry
	e.Domain.Main, e.Domain.SANs = "go.example.com", []string{"*.go.example.com"}
	e.Certificate, e.Key, e.Store = []byte(c), []byte(k), "default"
	st.setEntries(zoneResolver, []acmeEntry{e})
	if err := st.write(h.store); err != nil {
		t.Fatal(err)
	}
	setZone(t, h.s, certRequest(&pb.ClusterZoneCertificate{Renew: renew, Order: renew, Sha256: mustCert(t, c, k).Sum}))
	return h
}

func waitAuthz(t *testing.T, h *proxyHost) {
	t.Helper()
	until(t, 5*time.Second, "done", func() string {
		z := h.s.zones()
		z.mu.Lock()
		defer z.mu.Unlock()
		if r := z.authz["go.example.com"]; r != nil && !r.busy {
			return "done"
		}
		return "busy"
	})
}

func TestZoneCert_theRenewerClosesItsChecksSoNoOtherServerReusesThem(t *testing.T) {
	ca := newFakeCA(t)
	h := renewerOf(t, ca, true)
	renews := map[string]bool{"go.example.com": true}
	h.s.settleZoneAuthz(time.Now(), renews)
	waitAuthz(t, h)
	st, orders := ca.states()
	if st["1"] != "deactivated" || st["2"] != "deactivated" || orders != 1 {
		t.Fatalf("authorizations %v after %d orders", st, orders)
	}
	// The same pair: nothing more is asked of the CA.
	h.s.settleZoneAuthz(time.Now(), renews)
	waitAuthz(t, h)
	if _, orders := ca.states(); orders != 1 {
		t.Fatalf("%d orders for one pair", orders)
	}
}

func TestZoneCert_aServerThatOnlyHoldsThePairAsksNothingOfTheCA(t *testing.T) {
	ca := newFakeCA(t)
	h := renewerOf(t, ca, false)
	h.s.tendZoneCerts(time.Now().Add(time.Hour))
	time.Sleep(100 * time.Millisecond)
	if st, orders := ca.states(); orders != 0 || st["1"] != "valid" {
		t.Fatalf("a holder asked the CA: %v, %d orders", st, orders)
	}
}

func TestZoneCert_aCloseThatFailsIsTriedAgainLater(t *testing.T) {
	ca := newFakeCA(t)
	h := renewerOf(t, ca, true)
	old := closeZoneAuthz
	calls := 0
	closeZoneAuthz = func(context.Context, *acme.Client, []string) (int, error) {
		calls++
		return 0, fmt.Errorf("the CA is away")
	}
	t.Cleanup(func() { closeZoneAuthz = old })
	renews := map[string]bool{"go.example.com": true}
	now := time.Now()
	h.s.settleZoneAuthz(now, renews)
	waitAuthz(t, h)
	h.s.settleZoneAuthz(now.Add(time.Minute), renews)
	waitAuthz(t, h)
	if calls != 1 {
		t.Fatalf("%d tries within the wait", calls)
	}
	h.s.settleZoneAuthz(time.Now().Add(zoneAuthzRetry+time.Second), renews)
	waitAuthz(t, h)
	if calls != 2 {
		t.Fatalf("%d tries after the wait", calls)
	}
}

func TestZoneCert_theCAIsReachedAsTheProxyReachesIt(t *testing.T) {
	dir := t.TempDir()
	compose := dir + "/docker-compose.yml"
	pem, _ := testPair(t, time.Now().Add(time.Hour), "pebble")
	if err := os.WriteFile(dir+"/ca.pem", []byte(pem), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(compose, []byte(`services:
  traefik:
    command:
      - --certificatesresolvers.deplo-dns.acme.storage=/acme/acme.json
      - --certificatesresolvers.deplo-dns.acme.caserver=https://pebble:14000/dir
    environment:
      - LEGO_CA_CERTIFICATES=/pebble-ca.pem
    volumes:
      - ./ca.pem:/pebble-ca.pem:ro
    extra_hosts:
      - "pebble:10.0.0.53"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	u, hc, err := proxyCAClient(compose)
	if err != nil || u != "https://pebble:14000/dir" || hc == nil {
		t.Fatalf("%q %v", u, err)
	}
	if _, _, err := proxyCAClient(dir + "/missing.yml"); err == nil {
		t.Fatal("a missing compose file is read")
	}
}
