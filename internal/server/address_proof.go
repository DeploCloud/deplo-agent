package server

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// Deplo proves an address reaches this server by comparing what answers there with what this server says it is
// (ADR-0050): https://deplo.build/docs/operations/servers/addresses
const (
	// addressCheckName is a name no route has, so the proxy presents its own default certificate.
	addressCheckName = "deplo-address-check.invalid"
	addressTimeout   = 5 * time.Second
)

// proxyAt is where this machine's proxy listens; a var so tests can point it at their own.
var proxyAt = []string{"127.0.0.1:443", "[::1]:443"}

// serverIdent is the TXT this server's name server answers for _deplo-server.<zone>: its certificate, hashed.
func serverIdent(fingerprint string) string {
	if fingerprint == "" {
		return ""
	}
	h := sha256.Sum256([]byte("deplo-server:" + fingerprint))
	return hex.EncodeToString(h[:16])
}

// zoneIdent is what this server's name server says it is under every zone: its current certificate, hashed. Without
// one readable, the Self Deplo gave its own cluster's zone, never a cluster another Deplo left behind. The caller
// holds z.mu.
func (s *Service) zoneIdent(clusters map[string]*zoneCluster) string {
	if id, err := s.clusterIdentity(); err == nil && id.Fingerprint != "" {
		return serverIdent(id.Fingerprint)
	}
	if c := clusters[s.loadMember().Cluster]; c != nil {
		return serverIdent(c.Self)
	}
	ids := make([]string, 0, len(clusters))
	for id := range clusters {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return ""
	}
	return serverIdent(clusters[ids[0]].Self)
}

// ProveAddresses says what tells this server apart; see the RPC's contract in proto/agent.proto.
func (s *Service) ProveAddresses(ctx context.Context, req *pb.ProveAddressesRequest) (*pb.ProveAddressesResponse, error) {
	resp := &pb.ProveAddressesResponse{ProxyCertificateSha256: proxyCertificate(ctx)}
	z := s.zones()
	z.mu.Lock()
	if len(z.st.Clusters) > 0 {
		resp.DnsIdent = s.zoneIdent(z.st.Clusters)
	}
	z.mu.Unlock()
	if u, err := url.Parse(req.GetEchoUrl()); err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Host != "" {
		for _, family := range []string{"tcp4", "tcp6"} {
			if a := trafficAddress(ctx, u.String(), family); a != "" {
				resp.TrafficAddresses = append(resp.TrafficAddresses, a)
			}
		}
	}
	return resp, nil
}

// proxyCertificate is the digest of the certificate the proxy on this machine presents for a name no route has.
func proxyCertificate(ctx context.Context) string {
	for _, at := range proxyAt {
		d := &tls.Dialer{NetDialer: &net.Dialer{Timeout: addressTimeout},
			// Read, never trusted: Deplo compares it with what it sees at the address.
			Config: &tls.Config{ServerName: addressCheckName, InsecureSkipVerify: true}}
		c, err := d.DialContext(ctx, "tcp", at)
		if err != nil {
			continue
		}
		certs := c.(*tls.Conn).ConnectionState().PeerCertificates
		c.Close()
		if len(certs) > 0 {
			h := sha256.Sum256(certs[0].Raw)
			return hex.EncodeToString(h[:])
		}
	}
	return ""
}

// trafficAddress asks Deplo, over one family only, which address this server's traffic arrives from.
func trafficAddress(ctx context.Context, echo, family string) string {
	d := &net.Dialer{Timeout: addressTimeout}
	// The answer is only a candidate: Deplo publishes it once it proves the address reaches this server.
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) { return d.DialContext(ctx, family, addr) },
	}
	defer tr.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(ctx, 2*addressTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, echo, nil)
	if err != nil {
		return ""
	}
	res, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return ""
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64))
	a, err := netip.ParseAddr(strings.TrimSpace(string(raw)))
	a = a.Unmap()
	if res.StatusCode != http.StatusOK || err != nil || !a.IsGlobalUnicast() || a.IsPrivate() || a.Is4() != (family == "tcp4") {
		return ""
	}
	return a.String()
}
