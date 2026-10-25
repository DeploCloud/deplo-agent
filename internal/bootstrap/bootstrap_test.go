package bootstrap

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/link"
)

func fakeControlPlane(t *testing.T, token string, tamperMAC bool) *httptest.Server {
	return fakeControlPlaneIssuing(t, token, tamperMAC, "", "")
}

// fakeControlPlaneIssuing answers like the control plane, naming linkURL as the link address when set;
// forge "key" signs a stranger's key, forge "ca" signs the CSR with a CA other than the one it returns.
func fakeControlPlaneIssuing(t *testing.T, token string, tamperMAC bool, forge, linkURL string) *httptest.Server {
	t.Helper()
	caPub, caPriv, _ := ed25519.GenerateKey(rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caPub, caPriv)
	caCert, _ := x509.ParseCertificate(caDER)
	caPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req callHomeRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Token != token {
			w.WriteHeader(401)
			_ = json.NewEncoder(w).Encode(callHomeResponse{Error: "unknown-token"})
			return
		}
		block, _ := pem.Decode([]byte(req.CSRPem))
		csr, err := x509.ParseCertificateRequest(block.Bytes)
		if err != nil || csr.CheckSignature() != nil {
			w.WriteHeader(401)
			_ = json.NewEncoder(w).Encode(callHomeResponse{Error: "bad-csr"})
			return
		}
		leafTmpl := &x509.Certificate{
			SerialNumber: big.NewInt(2),
			Subject:      pkix.Name{CommonName: "deplo-agent"},
			NotBefore:    time.Now().Add(-time.Minute),
			NotAfter:     time.Now().Add(time.Hour),
		}
		leafPub, signer, signerKey := csr.PublicKey, caCert, caPriv
		switch forge {
		case "key":
			leafPub, _, _ = ed25519.GenerateKey(rand.Reader)
		case "ca":
			otherPub, otherPriv, _ := ed25519.GenerateKey(rand.Reader)
			otherDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, otherPub, otherPriv)
			signer, _ = x509.ParseCertificate(otherDER)
			signerKey = otherPriv
		}
		leafDER, _ := x509.CreateCertificate(rand.Reader, leafTmpl, signer, leafPub, signerKey)
		leafPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})

		payload, _ := json.Marshal(callHomeResponse{CertPem: string(leafPem), CAPem: string(caPem), LinkURL: linkURL})
		h := hmac.New(sha256.New, []byte(token))
		h.Write(payload)
		mac := hex.EncodeToString(h.Sum(nil))
		if tamperMAC {
			mac = "deadbeef"
		}
		w.Header().Set("x-deplo-bootstrap-mac", mac)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write(payload)
	}))
}

func TestBootstrap_happyPathOverHTTP(t *testing.T) {
	srv := fakeControlPlane(t, "tok-123", false)
	defer srv.Close()
	dir := t.TempDir()

	mats, err := Run(Config{
		ControlPlaneURL: srv.URL,
		Token:           "tok-123",
		Fingerprint:     "",
		AgentPort:       9443,
		AgentDir:        dir,
	})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if !Provisioned(dir) {
		t.Fatalf("materials not written")
	}
	keyPem, _ := os.ReadFile(mats.KeyPath)
	certPem, _ := os.ReadFile(mats.CertPath)
	if len(keyPem) == 0 || len(certPem) == 0 {
		t.Fatalf("empty materials")
	}
	kb, _ := pem.Decode(keyPem)
	if _, err := x509.ParsePKCS8PrivateKey(kb.Bytes); err != nil {
		t.Fatalf("bad key: %v", err)
	}
}

func TestBootstrap_savesTheCallHomeURLAsThePrimaryAddress(t *testing.T) {
	srv := fakeControlPlane(t, "tok-123", false)
	defer srv.Close()
	dir := t.TempDir()
	if _, err := Run(Config{ControlPlaneURL: srv.URL + "/", Token: "tok-123", AgentDir: dir}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// A restart reads the saved pair back from disk, with nothing held in memory.
	got, err := link.LoadAddresses(dir)
	if err != nil {
		t.Fatalf("load addresses: %v", err)
	}
	if got != (link.Addresses{Primary: srv.URL}) {
		t.Fatalf("saved addresses = %+v, want primary %q and no fallback", got, srv.URL)
	}
	info, err := os.Stat(link.AddressesPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("addresses file mode = %o, want 600", perm)
	}
}

func TestBootstrap_savesTheLinkAddressThePanelNames(t *testing.T) {
	// The panel's own server calls home on the dashboard's port and links on another (ADR-0040).
	srv := fakeControlPlaneIssuing(t, "tok-123", false, "", "http://127.0.0.1:3102/")
	defer srv.Close()
	dir := t.TempDir()
	if _, err := Run(Config{ControlPlaneURL: srv.URL, Token: "tok-123", AgentDir: dir}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	got, err := link.LoadAddresses(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got != (link.Addresses{Primary: "http://127.0.0.1:3102"}) {
		t.Fatalf("saved addresses = %+v, want the one the panel named", got)
	}
}

func TestBootstrap_anAddressThatIsNotTheWebKeepsTheCallHomeURL(t *testing.T) {
	for _, named := range []string{"ftp://127.0.0.1:3102", "127.0.0.1:3102", " "} {
		srv := fakeControlPlaneIssuing(t, "tok-123", false, "", named)
		dir := t.TempDir()
		_, err := Run(Config{ControlPlaneURL: srv.URL, Token: "tok-123", AgentDir: dir})
		srv.Close()
		if err != nil {
			t.Fatalf("%q: bootstrap: %v", named, err)
		}
		if got, _ := link.LoadAddresses(dir); got.Primary != srv.URL {
			t.Errorf("%q: primary = %q, want the call-home URL %q", named, got.Primary, srv.URL)
		}
	}
}

func TestBootstrap_rejectsTamperedMACOverHTTP(t *testing.T) {
	srv := fakeControlPlane(t, "tok-123", true)
	defer srv.Close()
	_, err := Run(Config{
		ControlPlaneURL: srv.URL,
		Token:           "tok-123",
		Fingerprint:     "",
		AgentPort:       9443,
		AgentDir:        t.TempDir(),
	})
	if err == nil {
		t.Fatalf("expected bootstrap to reject a tampered MAC")
	}
}

func TestBootstrap_rejectsBadToken(t *testing.T) {
	srv := fakeControlPlane(t, "right-token", false)
	defer srv.Close()
	_, err := Run(Config{
		ControlPlaneURL: srv.URL,
		Token:           "wrong-token",
		AgentDir:        t.TempDir(),
	})
	if err == nil {
		t.Fatalf("expected rejection for a bad token")
	}
}

// http plus a hand-added fingerprint used to skip the MAC while pinning nothing.
func TestBootstrap_httpWithAFingerprintStillNeedsTheMAC(t *testing.T) {
	srv := fakeControlPlane(t, "tok-123", true)
	defer srv.Close()
	dir := t.TempDir()
	_, err := Run(Config{
		ControlPlaneURL: srv.URL,
		Token:           "tok-123",
		Fingerprint:     "aa:bb",
		AgentDir:        dir,
	})
	if err == nil || Provisioned(dir) {
		t.Fatalf("a plain-HTTP answer with a bad MAC must be refused whatever the flags say (err=%v)", err)
	}
}

func TestBootstrap_refusesACertThatIsNotOurs(t *testing.T) {
	for _, forge := range []string{"key", "ca"} {
		srv := fakeControlPlaneIssuing(t, "tok-123", false, forge, "")
		dir := t.TempDir()
		_, err := Run(Config{ControlPlaneURL: srv.URL, Token: "tok-123", AgentDir: dir})
		srv.Close()
		if err == nil {
			t.Errorf("forge %q: a certificate that is not for our key or not from the returned CA must be refused", forge)
		}
		if entries, _ := os.ReadDir(dir); len(entries) != 0 {
			t.Errorf("forge %q: nothing may be written, found %d files", forge, len(entries))
		}
	}
}
