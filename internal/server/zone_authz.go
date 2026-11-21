package server

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
	"golang.org/x/crypto/acme"
)

// Once the renewer's proxy holds a new certificate of its zone, its account's authorizations for the zone are
// deactivated: a CA may reuse them, and a server that stops renewing would then order without any DNS check, so
// the other servers' refusal of its record would not stop a second certificate (ADR-0049, found on the lab).

const letsEncryptDirectory = "https://acme-v02.api.letsencrypt.org/directory"

var (
	zoneAuthzRetry   = 10 * time.Minute
	zoneAuthzTimeout = 30 * time.Second
	// closeZoneAuthz is a var so tests can count the calls.
	closeZoneAuthz = closeAuthorizations
)

type zoneAuthzRun struct {
	sum     string
	busy    bool
	retryAt time.Time
}

// proxyAccount is the DNS resolver's ACME account as Traefik keeps it.
type proxyAccount struct {
	Registration struct {
		URI string `json:"uri"`
	} `json:"Registration"`
	PrivateKey []byte `json:"PrivateKey"`
}

// settleZoneAuthz closes the authorizations behind each zone pair this server's proxy renewed, once per pair.
func (s *Service) settleZoneAuthz(now time.Time, renews map[string]bool) {
	compose := s.traefikCompose()
	acmeCfg, err := readProxyACME(compose)
	if err != nil || !acmeCfg.Resolvers[zoneResolver] {
		return
	}
	st, err := readACMEStore(acmeCfg.Store)
	if err != nil {
		return
	}
	z := s.zones()
	sets := s.currentZoneView().sets
	for zone := range renews {
		sum := ""
		for _, e := range st.entries(zoneResolver) {
			c, err := parseProxyCert(string(e.Certificate), string(e.Key))
			if err == nil && len(sets[zone]) > 0 && sameNames(c.Names, sets[zone]) && c.NotAfter.After(now) {
				sum = c.Sum
			}
		}
		z.mu.Lock()
		if z.authz == nil {
			z.authz = map[string]*zoneAuthzRun{}
		}
		run := z.authz[zone]
		if run == nil {
			run = &zoneAuthzRun{}
			z.authz[zone] = run
		}
		due := sum != "" && sum != run.sum && !run.busy && !now.Before(run.retryAt)
		if due {
			run.busy = true
		}
		z.mu.Unlock()
		if !due {
			continue
		}
		go func(zone, sum string) {
			ctx, cancel := context.WithTimeout(context.Background(), zoneAuthzTimeout)
			defer cancel()
			n, err := s.closeZoneAuthzFor(ctx, compose, st, sets[zone])
			z.mu.Lock()
			run.busy = false
			if err != nil {
				run.retryAt = time.Now().Add(zoneAuthzRetry)
			} else {
				run.sum = sum
			}
			z.mu.Unlock()
			if err != nil {
				log.Printf("deplo-agent: could not close the certificate checks of %s after its renewal: %v", strings.Join(sets[zone], ", "), err)
			} else if n > 0 {
				log.Printf("deplo-agent: closed %d certificate checks of %s after its renewal", n, strings.Join(sets[zone], ", "))
			}
		}(zone, sum)
	}
}

func (s *Service) closeZoneAuthzFor(ctx context.Context, compose string, st *acmeStore, names []string) (int, error) {
	var acct proxyAccount
	raw, ok := st.resolvers[zoneResolver]["Account"]
	if !ok || json.Unmarshal(raw, &acct) != nil || acct.Registration.URI == "" {
		return 0, errors.New("the proxy has no account for certificates over DNS")
	}
	key, err := accountKey(acct.PrivateKey)
	if err != nil {
		return 0, err
	}
	dir, hc, err := proxyCAClient(compose)
	if err != nil {
		return 0, err
	}
	return closeZoneAuthz(ctx, &acme.Client{Key: key, KID: acme.KeyID(acct.Registration.URI), DirectoryURL: dir, HTTPClient: hc,
		UserAgent: "deplo-agent"}, names)
}

// closeAuthorizations asks for an order of the names, which brings back every authorization the CA would reuse,
// and deactivates each one still valid or pending. The order is left to expire.
func closeAuthorizations(ctx context.Context, c *acme.Client, names []string) (int, error) {
	o, err := c.AuthorizeOrder(ctx, acme.DomainIDs(names...))
	if err != nil {
		return 0, err
	}
	n := 0
	for _, u := range o.AuthzURLs {
		a, err := c.GetAuthorization(ctx, u)
		if err != nil {
			return n, err
		}
		if a.Status != acme.StatusValid && a.Status != acme.StatusPending {
			continue
		}
		if err := c.RevokeAuthorization(ctx, u); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func accountKey(der []byte) (crypto.Signer, error) {
	if k, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return k, nil
	}
	if k, err := x509.ParseECPrivateKey(der); err == nil {
		return k, nil
	}
	if k, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		if s, ok := k.(crypto.Signer); ok {
			return s, nil
		}
	}
	return nil, errors.New("the proxy's account key is unreadable")
}

// proxyCAClient is the DNS resolver's CA and an HTTP client that reaches it as the proxy does: the CA roots it is
// told to trust and the host names its compose file maps.
func proxyCAClient(compose string) (string, *http.Client, error) {
	raw, err := os.ReadFile(compose)
	if err != nil {
		return "", nil, err
	}
	var doc struct {
		Services map[string]struct {
			Command     any `yaml:"command"`
			Environment any `yaml:"environment"`
			Volumes     any `yaml:"volumes"`
			ExtraHosts  any `yaml:"extra_hosts"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return "", nil, errors.New("the proxy's compose file is unreadable")
	}
	dir := letsEncryptDirectory
	var roots *x509.CertPool
	hosts := map[string]string{}
	for _, svc := range doc.Services {
		flags := composeList(svc.Command, " ")
		if !strings.Contains(strings.Join(flags, "\n"), "--certificatesresolvers."+zoneResolver+".") {
			continue
		}
		for _, f := range flags {
			if v, ok := strings.CutPrefix(f, "--certificatesresolvers."+zoneResolver+".acme.caserver="); ok {
				dir = v
			}
		}
		mounts := map[string]string{}
		for _, v := range composeList(svc.Volumes, "") {
			parts := strings.Split(v, ":")
			if len(parts) >= 2 {
				src := parts[0]
				if !filepath.IsAbs(src) {
					src = filepath.Join(filepath.Dir(compose), src)
				}
				mounts[parts[1]] = src
			}
		}
		for _, e := range composeList(svc.Environment, "") {
			v, ok := strings.CutPrefix(e, "LEGO_CA_CERTIFICATES=")
			if !ok {
				continue
			}
			for _, p := range strings.FieldsFunc(v, func(r rune) bool { return r == ',' || r == os.PathListSeparator }) {
				host, ok := mounts[p]
				if !ok {
					continue
				}
				pem, err := os.ReadFile(host)
				if err != nil {
					return "", nil, fmt.Errorf("the CA roots the proxy trusts are unreadable: %w", err)
				}
				if roots == nil {
					if roots, err = x509.SystemCertPool(); err != nil {
						roots = x509.NewCertPool()
					}
				}
				roots.AppendCertsFromPEM(pem)
			}
		}
		for _, h := range composeList(svc.ExtraHosts, "") {
			if name, ip, ok := strings.Cut(strings.Replace(h, "=", ":", 1), ":"); ok {
				hosts[strings.TrimSpace(name)] = strings.TrimSpace(ip)
			}
		}
	}
	d := &net.Dialer{Timeout: 10 * time.Second}
	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if host, port, err := net.SplitHostPort(addr); err == nil && hosts[host] != "" {
				addr = net.JoinHostPort(hosts[host], port)
			}
			return d.DialContext(ctx, network, addr)
		}}
	return dir, &http.Client{Transport: tr, Timeout: zoneAuthzTimeout}, nil
}

// composeList reads a compose list, a "KEY: value" map as KEY=value, or a single string split on sep.
func composeList(v any, sep string) []string {
	var out []string
	switch c := v.(type) {
	case []any:
		for _, x := range c {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
	case map[string]any:
		for k, x := range c {
			out = append(out, fmt.Sprintf("%s=%v", k, x))
		}
	case string:
		if sep == "" {
			return []string{c}
		}
		out = strings.Fields(c)
	}
	return out
}
