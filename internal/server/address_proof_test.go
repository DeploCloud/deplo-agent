package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The proxy's certificate is what answers on this machine's 443 for a name no route has, and an echo that says
// a private address, or one of the other family, is no traffic address.
func TestProveAddresses_namesTheProxysCertificateAndOnlyPublicEchoes(t *testing.T) {
	proxy := httptest.NewTLSServer(http.NotFoundHandler())
	defer proxy.Close()
	old := proxyAt
	proxyAt = []string{proxy.Listener.Addr().String()}
	t.Cleanup(func() { proxyAt = old })
	sum := sha256.Sum256(proxy.Certificate().Raw)

	for answer, want := range map[string]string{"198.51.100.7\n": "", "10.0.0.54": "", "2001:db8::1": "", "not an address": ""} {
		echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(answer)) }))
		s := New(t.TempDir(), t.TempDir(), "/", "")
		resp, err := s.ProveAddresses(context.Background(), &pb.ProveAddressesRequest{EchoUrl: echo.URL})
		echo.Close()
		if err != nil || resp.GetProxyCertificateSha256() != hex.EncodeToString(sum[:]) {
			t.Fatalf("proxy certificate %q, err %v", resp.GetProxyCertificateSha256(), err)
		}
		// The echo listens on IPv4 loopback only: what it says is kept only when it is public and IPv4.
		got := ""
		if len(resp.GetTrafficAddresses()) > 0 {
			got = resp.GetTrafficAddresses()[0]
		}
		if answer == "198.51.100.7\n" {
			want = "198.51.100.7"
		}
		if got != want {
			t.Fatalf("echo %q gave traffic address %q, want %q", answer, got, want)
		}
	}
}

func TestServerIdent_isTheCertificateHashed(t *testing.T) {
	h := sha256.Sum256([]byte("deplo-server:abc"))
	if serverIdent("abc") != hex.EncodeToString(h[:16]) || serverIdent("") != "" {
		t.Fatal("the ident is not the certificate hashed")
	}
}
