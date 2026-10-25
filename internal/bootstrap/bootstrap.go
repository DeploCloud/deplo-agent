package bootstrap

import (
	"bytes"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/link"
)

// Materials are the persisted mTLS files the agent serves with after bootstrap.
type Materials struct {
	CertPath string
	KeyPath  string
	CAPath   string
}

// Paths returns the standard material paths under agentDir.
func Paths(agentDir string) Materials {
	return Materials{
		CertPath: filepath.Join(agentDir, "agent.crt"),
		KeyPath:  filepath.Join(agentDir, "agent.key"),
		CAPath:   filepath.Join(agentDir, "ca.crt"),
	}
}

// Provisioned reports whether the agent already has its materials (so a restart skips bootstrap and serves straight away).
func Provisioned(agentDir string) bool {
	m := Paths(agentDir)
	for _, p := range []string{m.CertPath, m.KeyPath, m.CAPath} {
		if _, err := os.Stat(p); err != nil {
			return false
		}
	}
	return true
}

// InstallURL is the call-home URL the install script left in agentDir/bootstrap.env, or "".
// The file, not the environment: a self-update re-exec starts with no DEPLO_BOOTSTRAP_* set.
func InstallURL(agentDir string) string {
	raw, err := os.ReadFile(filepath.Join(agentDir, "bootstrap.env"))
	if err != nil {
		return ""
	}
	url := ""
	for _, line := range strings.Split(string(raw), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "DEPLO_BOOTSTRAP_URL="); ok {
			url = strings.Trim(strings.TrimSpace(v), `"'`)
		}
	}
	return strings.TrimRight(url, "/")
}

// Config carries the one-time inputs from the install command.
type Config struct {
	// ControlPlaneURL is the base URL to call home to (http(s)://host[:port]).
	ControlPlaneURL string
	// Token is the single-use bootstrap token.
	Token string
	// Fingerprint is the expected control-plane cert sha256 (HTTPS only); "" => HTTP.
	Fingerprint string
	// AgentPort is the port the agent will serve gRPC on (reported to the control plane so it knows where to dial back).
	AgentPort int
	// AdvertisedHost is what the agent believes its address is (informational).
	AdvertisedHost string
	// AgentDir is where to write the resulting materials.
	AgentDir string
}

type callHomeRequest struct {
	Token          string `json:"token"`
	CSRPem         string `json:"csrPem"`
	AgentPort      int    `json:"agentPort,omitempty"`
	AdvertisedHost string `json:"advertisedHost,omitempty"`
}

type callHomeResponse struct {
	CertPem string `json:"certPem"`
	CAPem   string `json:"caPem"`
	// LinkURL is where the panel wants this agent to link (ADR-0040); an older panel names none.
	LinkURL string `json:"linkUrl,omitempty"`
	Error   string `json:"error,omitempty"`
}

// Run performs the bootstrap and writes the materials.
func Run(cfg Config) (Materials, error) {
	mats := Paths(cfg.AgentDir)
	if err := os.MkdirAll(cfg.AgentDir, 0o700); err != nil {
		return mats, fmt.Errorf("create agent dir: %w", err)
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return mats, fmt.Errorf("generate key: %w", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "deplo-agent"},
	}, priv)
	if err != nil {
		return mats, fmt.Errorf("create CSR: %w", err)
	}
	csrPem := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}))

	resp, rawBody, mac, err := callHome(cfg, csrPem)
	if err != nil {
		return mats, err
	}
	if resp.Error != "" {
		return mats, fmt.Errorf("control plane rejected bootstrap: %s", resp.Error)
	}
	if resp.CertPem == "" || resp.CAPem == "" {
		return mats, fmt.Errorf("control plane returned no certificate")
	}

	// The scheme decides, not the flags: a fingerprint does nothing for a plain-HTTP call.
	if !isHTTPS(cfg.ControlPlaneURL) {
		if mac == "" {
			return mats, fmt.Errorf("control plane did not sign the bootstrap response (refusing over plain HTTP)")
		}
		if !verifyMAC(cfg.Token, rawBody, mac) {
			return mats, fmt.Errorf("bootstrap response HMAC did not verify - possible tampering")
		}
	}
	if err := verifyIssued(pub, resp.CertPem, resp.CAPem); err != nil {
		return mats, err
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return mats, fmt.Errorf("marshal key: %w", err)
	}
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	type material struct {
		tmp, final string
		data       []byte
		perm       os.FileMode
	}
	staged := []material{
		{tmp: mats.KeyPath + ".tmp", final: mats.KeyPath, data: keyPem, perm: 0o600},
		{tmp: mats.CertPath + ".tmp", final: mats.CertPath, data: []byte(resp.CertPem), perm: 0o600},
		{tmp: mats.CAPath + ".tmp", final: mats.CAPath, data: []byte(resp.CAPem), perm: 0o644},
	}
	cleanupTmps := func() {
		for _, m := range staged {
			_ = os.Remove(m.tmp)
		}
	}
	for _, m := range staged {
		_ = os.Remove(m.tmp)
		if err := os.WriteFile(m.tmp, m.data, m.perm); err != nil {
			cleanupTmps()
			return mats, fmt.Errorf("stage %s: %w", filepath.Base(m.final), err)
		}
	}
	// Saved before the PEMs commit: an agent is provisioned only once its link address is on disk too.
	if err := link.SaveAddresses(cfg.AgentDir, link.Addresses{Primary: primaryAddress(cfg.ControlPlaneURL, resp.LinkURL)}); err != nil {
		cleanupTmps()
		return mats, fmt.Errorf("save link address: %w", err)
	}
	for _, m := range staged {
		if err := os.Rename(m.tmp, m.final); err != nil {
			cleanupTmps()
			return mats, fmt.Errorf("commit %s: %w", filepath.Base(m.final), err)
		}
	}
	return mats, nil
}

func callHome(cfg Config, csrPem string) (callHomeResponse, []byte, string, error) {
	body, _ := json.Marshal(callHomeRequest{
		Token:          cfg.Token,
		CSRPem:         csrPem,
		AgentPort:      cfg.AgentPort,
		AdvertisedHost: cfg.AdvertisedHost,
	})
	url := strings.TrimRight(cfg.ControlPlaneURL, "/") + "/api/agent/bootstrap"

	client := &http.Client{Timeout: 30 * time.Second}
	if isHTTPS(url) {
		if cfg.Fingerprint == "" {
			return callHomeResponse{}, nil, "", fmt.Errorf("HTTPS control plane requires a pinned fingerprint")
		}
		client.Transport = pinnedTransport(cfg.Fingerprint)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return callHomeResponse{}, nil, "", err
	}
	req.Header.Set("content-type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return callHomeResponse{}, nil, "", fmt.Errorf("call home: %w", err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return callHomeResponse{}, nil, "", err
	}
	var parsed callHomeResponse
	_ = json.Unmarshal(raw, &parsed)
	if res.StatusCode != http.StatusOK && parsed.Error == "" {
		parsed.Error = fmt.Sprintf("HTTP %d", res.StatusCode)
	}
	return parsed, raw, res.Header.Get("x-deplo-bootstrap-mac"), nil
}

// primaryAddress is the link address the panel named, signed with the certificate, or else the call-home URL.
func primaryAddress(callHome, named string) string {
	named = strings.TrimSpace(named)
	lower := strings.ToLower(named)
	if !strings.HasPrefix(lower, "https://") && !strings.HasPrefix(lower, "http://") {
		named = callHome
	}
	return strings.TrimRight(named, "/")
}

func isHTTPS(url string) bool {
	return strings.HasPrefix(strings.ToLower(url), "https://")
}

// verifyIssued refuses a certificate that is not for this agent's key or does not chain to the
// returned CA, before anything is written.
func verifyIssued(pub ed25519.PublicKey, certPem, caPem string) error {
	block, _ := pem.Decode([]byte(certPem))
	if block == nil {
		return fmt.Errorf("control plane returned an unreadable certificate")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("control plane returned an unreadable certificate: %w", err)
	}
	if !pub.Equal(leaf.PublicKey) {
		return fmt.Errorf("control plane returned a certificate for a different key")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(caPem)) {
		return fmt.Errorf("control plane returned an unreadable CA")
	}
	// Checked at the cert's own start, so a host clock behind the control plane's still bootstraps.
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:       roots,
		CurrentTime: leaf.NotBefore,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return fmt.Errorf("control plane returned a certificate its CA did not sign: %w", err)
	}
	return nil
}

func pinnedTransport(expected string) *http.Transport {
	want := strings.ToLower(strings.ReplaceAll(expected, ":", ""))
	return &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			VerifyConnection: func(cs tls.ConnectionState) error {
				if len(cs.PeerCertificates) == 0 {
					return fmt.Errorf("control plane presented no certificate")
				}
				sum := sha256.Sum256(cs.PeerCertificates[0].Raw)
				got := hex.EncodeToString(sum[:])
				if !hmac.Equal([]byte(got), []byte(want)) {
					return fmt.Errorf("control-plane cert fingerprint mismatch: pinned %s, got %s", want, got)
				}
				return nil
			},
		},
	}
}

func verifyMAC(token string, body []byte, mac string) bool {
	h := hmac.New(sha256.New, []byte(token))
	h.Write(body)
	expected := hex.EncodeToString(h.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(mac))
}
