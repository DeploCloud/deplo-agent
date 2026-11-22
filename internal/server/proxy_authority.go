package server

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"go.yaml.in/yaml/v3"
)

// A resolver's account and certificates from an authority it no longer orders from are set aside (ADR-0050): the
// account would fail every order, and the certificates come from a CA the Deplo that wrote the proxy does not use.

var caserverFlag = regexp.MustCompile(`^--certificatesresolvers\.([^.=]+)\.acme\.caserver=(.+)$`)

// publicRoots is what a certificate from Let's Encrypt chains to; a seam for the tests.
var publicRoots = x509.SystemCertPool

// resolverAuthorities is the ACME directory each resolver of the proxy orders from: its caserver, else Let's Encrypt.
func resolverAuthorities(composePath string, resolvers map[string]bool) map[string]string {
	out := map[string]string{}
	for r := range resolvers {
		out[r] = letsEncryptDirectory
	}
	raw, err := os.ReadFile(composePath)
	if err != nil {
		return out
	}
	var doc struct {
		Services map[string]struct {
			Command any `yaml:"command"`
		} `yaml:"services"`
	}
	if yaml.Unmarshal(raw, &doc) != nil {
		return out
	}
	for _, svc := range doc.Services {
		for _, f := range composeList(svc.Command, " ") {
			if m := caserverFlag.FindStringSubmatch(f); m != nil && resolvers[m[1]] {
				out[m[1]] = m[2]
			}
		}
	}
	return out
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return u.Host
}

// fromPublicCA says a chain verifies to the public roots, whatever its dates.
func fromPublicCA(chain []byte, roots *x509.CertPool) bool {
	var certs []*x509.Certificate
	for rest := chain; ; {
		var b *pem.Block
		if b, rest = pem.Decode(rest); b == nil {
			break
		}
		if c, err := x509.ParseCertificate(b.Bytes); err == nil {
			certs = append(certs, c)
		}
	}
	if len(certs) == 0 {
		return false
	}
	inter := x509.NewCertPool()
	for _, c := range certs[1:] {
		inter.AddCert(c)
	}
	_, err := certs[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter,
		CurrentTime: certs[0].NotBefore.Add(time.Second), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
	return err == nil
}

// foreignAuthority takes out of st what came from another authority than each resolver's, and returns it by resolver.
func foreignAuthority(st *acmeStore, cas map[string]string) map[string]map[string]json.RawMessage {
	gone := map[string]map[string]json.RawMessage{}
	roots, rootsErr := publicRoots()
	for _, name := range st.order {
		dir, ok := cas[name]
		if !ok {
			continue
		}
		r, g := st.resolvers[name], map[string]json.RawMessage{}
		var acct struct {
			Registration struct {
				URI string `json:"uri"`
			} `json:"Registration"`
		}
		if raw, ok := r["Account"]; ok && json.Unmarshal(raw, &acct) == nil && acct.Registration.URI != "" &&
			hostOf(acct.Registration.URI) != hostOf(dir) {
			g["Account"] = raw
			delete(r, "Account")
		}
		// Only Let's Encrypt's roots are known here: a test authority's certificates stay as they are.
		if dir == letsEncryptDirectory && rootsErr == nil {
			var keep, drop []acmeEntry
			for _, e := range st.entries(name) {
				if fromPublicCA(e.Certificate, roots) {
					keep = append(keep, e)
				} else {
					drop = append(drop, e)
				}
			}
			if len(drop) > 0 {
				g["Certificates"], _ = json.Marshal(drop)
				st.setEntries(name, keep)
			}
		}
		if len(g) > 0 {
			gone[name] = g
		}
	}
	return gone
}

// settleProxyAuthority sets aside, with the proxy stopped, what its store holds from another authority. The caller
// holds proxyStartMu and starts the proxy after.
func (s *Service) settleProxyAuthority(ctx context.Context, composePath string) {
	acme, err := readProxyACME(composePath)
	if err != nil {
		return
	}
	st, err := readACMEStore(acme.Store)
	if err != nil {
		return
	}
	gone := foreignAuthority(st, resolverAuthorities(composePath, acme.Resolvers))
	if len(gone) == 0 || s.agentDir == "" {
		return
	}
	to := filepath.Join(s.agentDir, setAsideRoot, time.Now().UTC().Format("20060102T150405Z"))
	raw, _ := json.MarshalIndent(gone, "", "  ")
	if err := os.MkdirAll(to, 0o700); err == nil {
		err = writeFileAtomic(filepath.Join(to, "acme.json"), raw, 0o600)
	}
	if err != nil {
		log.Printf("deplo-agent: could not set aside the proxy's certificates from another authority: %v", err)
		return
	}
	if err := stopProxy(ctx); err != nil {
		log.Printf("deplo-agent: proxy certificates from another authority: %v", err)
		return
	}
	if err := st.write(acme.Store); err != nil {
		log.Printf("deplo-agent: proxy certificates from another authority: could not write the store: %v", err)
		return
	}
	log.Printf("deplo-agent: set aside the proxy's account and certificates from another authority in %s", to)
}
