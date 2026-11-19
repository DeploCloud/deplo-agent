package server

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"

	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The certificate of a domain that answers on every server travels with its App (ADR-0048): Deplo reads it from
// the App's server's proxy, a failover installs it as a file on the next server, and this agent moves it into
// that proxy's ACME store only while the proxy is stopped, since a running proxy erases what it did not write.

// proxyCert is one certificate a proxy serves: its chain and key as PEM, and the names it covers.
type proxyCert struct {
	Names    []string
	Cert     string
	Key      string
	NotAfter time.Time
	Sum      string
}

var errBadCert = errors.New("a certificate and its key do not make a valid pair")

// parseProxyCert checks a chain and its key and reads what they cover. Its errors never quote either.
func parseProxyCert(certPEM, keyPEM string) (proxyCert, error) {
	pair, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	if err != nil || len(pair.Certificate) == 0 {
		return proxyCert{}, errBadCert
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return proxyCert{}, errBadCert
	}
	names := slices.Clone(leaf.DNSNames)
	if len(names) == 0 && leaf.Subject.CommonName != "" {
		names = []string{leaf.Subject.CommonName}
	}
	for i := range names {
		names[i] = strings.ToLower(names[i])
	}
	sum := sha256.Sum256(pair.Certificate[0])
	return proxyCert{Names: names, Cert: certPEM, Key: keyPEM, NotAfter: leaf.NotAfter, Sum: hex.EncodeToString(sum[:])}, nil
}

// covers says whether the certificate is valid for host: by name, or by a wildcard one label deep.
func (c proxyCert) covers(host string) bool {
	for _, n := range c.Names {
		if n == host {
			return true
		}
		if rest, ok := strings.CutPrefix(n, "*."); ok {
			if _, after, found := strings.Cut(host, "."); found && after == rest {
				return true
			}
		}
	}
	return false
}

// sameNames says whether two certificates cover the same set of names.
func sameNames(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(slices.Compact(x), slices.Compact(y))
}

// acmeEntry is one certificate of Traefik's ACME store, in the store's own form.
type acmeEntry struct {
	Domain struct {
		Main string   `json:"main"`
		SANs []string `json:"sans,omitempty"`
	} `json:"domain"`
	Certificate []byte `json:"certificate"`
	Key         []byte `json:"key"`
	Store       string `json:"Store"`
}

// acmeStore is the store file: per resolver its account and certificates, and anything else it holds kept as is.
type acmeStore struct {
	order     []string
	resolvers map[string]map[string]json.RawMessage
}

func readACMEStore(path string) (*acmeStore, error) {
	st := &acmeStore{resolvers: map[string]map[string]json.RawMessage{}}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) || (err == nil && len(strings.TrimSpace(string(raw))) == 0) {
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	top := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, errors.New("the proxy's certificate store is unreadable")
	}
	for name, body := range top {
		r := map[string]json.RawMessage{}
		if string(body) != "null" && json.Unmarshal(body, &r) != nil {
			return nil, errors.New("the proxy's certificate store is unreadable")
		}
		st.order = append(st.order, name)
		st.resolvers[name] = r
	}
	slices.Sort(st.order)
	return st, nil
}

func (st *acmeStore) entries(resolver string) []acmeEntry {
	var out []acmeEntry
	if raw, ok := st.resolvers[resolver]["Certificates"]; ok {
		_ = json.Unmarshal(raw, &out)
	}
	return out
}

func (st *acmeStore) setEntries(resolver string, list []acmeEntry) {
	r := st.resolvers[resolver]
	if r == nil {
		r = map[string]json.RawMessage{}
		st.resolvers[resolver] = r
		st.order = append(st.order, resolver)
	}
	if list == nil {
		list = []acmeEntry{}
	}
	raw, _ := json.Marshal(list)
	r["Certificates"] = raw
}

// certs is every valid pair of the store, whichever resolver holds it.
func (st *acmeStore) certs() []proxyCert {
	var out []proxyCert
	for _, name := range st.order {
		for _, e := range st.entries(name) {
			if c, err := parseProxyCert(string(e.Certificate), string(e.Key)); err == nil {
				out = append(out, c)
			}
		}
	}
	return out
}

// write replaces the store in one rename, readable by its owner only, as Traefik keeps it.
func (st *acmeStore) write(path string) error {
	top := map[string]map[string]json.RawMessage{}
	for _, name := range st.order {
		top[name] = st.resolvers[name]
	}
	raw, err := json.MarshalIndent(top, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".acme-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// storeMerge is what a proxy start puts right in its store: these pairs in, these names' entries out.
type storeMerge struct {
	keep []proxyCert
	drop []string
}

func (m storeMerge) empty() bool { return len(m.keep) == 0 && len(m.drop) == 0 }

// apply keeps each pair unless the store holds a newer one for the same names, and takes out every entry for a
// dropped name that no kept pair covers. It reports whether the store changed.
func (m storeMerge) apply(st *acmeStore, resolver string) bool {
	changed := false
	for _, name := range st.order {
		list := st.entries(name)
		kept := list[:0:0]
		for _, e := range list {
			c, err := parseProxyCert(string(e.Certificate), string(e.Key))
			if err == nil && slices.ContainsFunc(m.drop, c.covers) && !slices.ContainsFunc(m.keep, func(k proxyCert) bool { return sameNames(k.Names, c.Names) }) {
				changed = true
				continue
			}
			kept = append(kept, e)
		}
		if len(kept) != len(list) {
			st.setEntries(name, kept)
		}
	}
	for _, k := range m.keep {
		placed := false
		for _, name := range st.order {
			list := st.entries(name)
			for i, e := range list {
				c, err := parseProxyCert(string(e.Certificate), string(e.Key))
				if err != nil || !sameNames(c.Names, k.Names) {
					continue
				}
				placed = true
				if k.NotAfter.After(c.NotAfter) {
					list[i].Certificate, list[i].Key = []byte(k.Cert), []byte(k.Key)
					st.setEntries(name, list)
					changed = true
				}
			}
		}
		if placed {
			continue
		}
		var e acmeEntry
		e.Domain.Main, e.Domain.SANs = k.Names[0], slices.Clone(k.Names[1:])
		e.Certificate, e.Key, e.Store = []byte(k.Cert), []byte(k.Key), "default"
		st.setEntries(resolver, append(st.entries(resolver), e))
		changed = true
	}
	return changed
}

// proxyACME is where this server's proxy keeps its ACME store, which resolver Deplo's routers name, and how long
// its certificates are ordered for, all read from the proxy's compose file.
type proxyACME struct {
	Store    string
	Resolver string
	Duration time.Duration
}

var storageFlag = regexp.MustCompile(`^--certificatesresolvers\.([^.=]+)\.acme\.storage=(.+)$`)
var durationFlag = regexp.MustCompile(`^--certificatesresolvers\.([^.=]+)\.acme\.certificatesduration=(\d+)$`)

func readProxyACME(composePath string) (proxyACME, error) {
	out := proxyACME{Resolver: "letsencrypt", Duration: 2160 * time.Hour}
	raw, err := os.ReadFile(composePath)
	if err != nil {
		return out, err
	}
	var doc struct {
		Services map[string]struct {
			Command any   `yaml:"command"`
			Volumes []any `yaml:"volumes"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return out, errors.New("the proxy's compose file is unreadable")
	}
	for _, svc := range doc.Services {
		var flags []string
		switch c := svc.Command.(type) {
		case []any:
			for _, v := range c {
				if s, ok := v.(string); ok {
					flags = append(flags, s)
				}
			}
		case string:
			flags = strings.Fields(c)
		}
		inside := ""
		for _, f := range flags {
			if m := storageFlag.FindStringSubmatch(f); m != nil && inside == "" {
				out.Resolver, inside = m[1], m[2]
			}
		}
		if inside == "" {
			continue
		}
		for _, f := range flags {
			if m := durationFlag.FindStringSubmatch(f); m != nil && m[1] == out.Resolver {
				if h, err := strconv.Atoi(m[2]); err == nil && h > 0 {
					out.Duration = time.Duration(h) * time.Hour
				}
			}
		}
		for _, v := range svc.Volumes {
			spec, ok := v.(string)
			if !ok {
				continue
			}
			parts := strings.Split(spec, ":")
			if len(parts) < 2 {
				continue
			}
			src, dst := parts[0], strings.TrimRight(parts[1], "/")
			if rel, ok := strings.CutPrefix(inside, dst+"/"); ok {
				if !filepath.IsAbs(src) {
					src = filepath.Join(filepath.Dir(composePath), src)
				}
				out.Store = filepath.Join(src, rel)
				return out, nil
			}
		}
	}
	return out, errors.New("the proxy keeps no certificate store on this host")
}

// renewBefore is how long before it expires Traefik renews a certificate ordered for d.
func renewBefore(d time.Duration) time.Duration {
	switch {
	case d >= 265*24*time.Hour:
		return 4 * 30 * 24 * time.Hour
	case d >= 90*24*time.Hour:
		return 30 * 24 * time.Hour
	case d >= 7*24*time.Hour:
		return 24 * time.Hour
	case d >= 24*time.Hour:
		return 6 * time.Hour
	}
	return 20 * time.Minute
}

const maxCertHosts = 400

var certHost = regexp.MustCompile(`^` + answerHost + `$`)

// ReadProxyCertificates answers, for each hostname asked, the newest valid certificate this proxy serves for it.
func (s *Service) ReadProxyCertificates(ctx context.Context, req *pb.ReadProxyCertificatesRequest) (*pb.ReadProxyCertificatesResponse, error) {
	if len(req.GetHosts()) > maxCertHosts {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d hostnames at once", maxCertHosts)
	}
	for _, h := range req.GetHosts() {
		if !certHost.MatchString(h) {
			return nil, status.Errorf(codes.InvalidArgument, "%q is not a hostname", h)
		}
	}
	all := s.forwarders().installedCerts()
	if acme, err := readProxyACME(s.traefikCompose()); err == nil {
		st, err := readACMEStore(acme.Store)
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		all = append(all, st.certs()...)
	}
	return &pb.ReadProxyCertificatesResponse{Certificates: pickCerts(all, req.GetHosts(), req.GetKnown(), time.Now())}, nil
}

// pickCerts is the newest valid pair for each host, one answer per pair; a pair Deplo already holds goes keyless.
func pickCerts(all []proxyCert, hosts []string, known map[string]string, now time.Time) []*pb.ProxyCertificate {
	var out []*pb.ProxyCertificate
	bySum := map[string]*pb.ProxyCertificate{}
	for _, h := range hosts {
		var best *proxyCert
		for i := range all {
			c := &all[i]
			if c.NotAfter.After(now) && c.covers(h) && (best == nil || c.NotAfter.After(best.NotAfter)) {
				best = c
			}
		}
		if best == nil {
			continue
		}
		if p := bySum[best.Sum]; p != nil {
			p.Hosts = append(p.Hosts, h)
			if known[h] != best.Sum && p.KeyPem == "" {
				p.KeyPem = best.Key
			}
			continue
		}
		p := &pb.ProxyCertificate{Hosts: []string{h}, CertPem: best.Cert, Sha256: best.Sum, NotAfterUnixMs: best.NotAfter.UnixMilli()}
		if known[h] != best.Sum {
			p.KeyPem = best.Key
		}
		bySum[best.Sum] = p
		out = append(out, p)
	}
	return out
}
