// Package wire is what a cluster relay and its parties agree on. A contract with deplo-cluster internal/wire: keep the two in step (ADR-0044).
package wire

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// The door's wire protocol.
const (
	Path        = "/api/cluster/relay"
	Subprotocol = "deplo-cluster-relay.v1"
	CertHeader  = "Deplo-Cluster-Cert"
	RoleHeader  = "Deplo-Cluster-Role"
	// DefaultPort is where a relay listens beside its host's proxy.
	DefaultPort = 3002
)

// Role is which process of a server dials the relay: the leader, or the server agent as a member.
type Role string

const (
	RoleLeader Role = "leader"
	RoleMember Role = "member"
)

// Party is one end the relay carries streams between: a certificate and the role it plays.
type Party struct {
	Fingerprint string `json:"fingerprint"`
	Role        Role   `json:"role"`
}

// Key is the party's name on the wire.
func (p Party) Key() string { return p.Fingerprint + "/" + string(p.Role) }

// Valid reports whether p names a certificate fingerprint and a known role.
func (p Party) Valid() bool {
	if len(p.Fingerprint) != 64 || p.Fingerprint != strings.ToLower(p.Fingerprint) {
		return false
	}
	if _, err := hex.DecodeString(p.Fingerprint); err != nil {
		return false
	}
	return p.Role == RoleLeader || p.Role == RoleMember
}

// ParseParty reads a party's wire name.
func ParseParty(s string) (Party, error) {
	fp, role, ok := strings.Cut(s, "/")
	p := Party{Fingerprint: fp, Role: Role(role)}
	if !ok || !p.Valid() {
		return Party{}, fmt.Errorf("%q is not a party", s)
	}
	return p, nil
}

// Stream header lines: an initiator asks the relay to CONNECT, the relay tells the target FROM and the initiator OK or ERR.
const (
	VerbConnect = "CONNECT"
	VerbFrom    = "FROM"
	VerbOK      = "OK"
	VerbErr     = "ERR"
	// HeaderTimeout bounds how long a stream may take to say what it wants.
	HeaderTimeout = 10 * time.Second
	maxHeader     = 256
)

// ReadHeader reads one line a byte at a time, so nothing past it is consumed.
func ReadHeader(r io.Reader) (verb, arg string, err error) {
	var line []byte
	one := make([]byte, 1)
	for len(line) <= maxHeader {
		if _, err := io.ReadFull(r, one); err != nil {
			return "", "", err
		}
		if one[0] == '\n' {
			verb, arg, _ = strings.Cut(string(line), " ")
			return verb, arg, nil
		}
		line = append(line, one[0])
	}
	return "", "", errors.New("stream header too long")
}

// WriteHeader writes one header line.
func WriteHeader(w io.Writer, verb, arg string) error {
	line := verb
	if arg != "" {
		line += " " + arg
	}
	_, err := w.Write([]byte(line + "\n"))
	return err
}

// Fingerprint is the sha256 of a certificate's DER, lowercase hex: the name the control plane pins.
func Fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:])
}

// Identity is a party's certificate, key and the CA that signed every certificate it trusts.
type Identity struct {
	Cert        tls.Certificate
	Fingerprint string
	Roots       *x509.CertPool
}

// LoadIdentity reads PEM files; callers read it per connection so a renewed certificate counts.
func LoadIdentity(certFile, keyFile, caFile string) (*Identity, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load the certificate: %w", err)
	}
	raw, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read the CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, errors.New("no certificate in the CA file")
	}
	return &Identity{Cert: cert, Fingerprint: Fingerprint(cert.Certificate[0]), Roots: pool}, nil
}

// PEMFingerprint is the fingerprint of the first certificate in a PEM file.
func PEMFingerprint(file string) (string, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return "", errors.New("no PEM block in the certificate")
	}
	return Fingerprint(block.Bytes), nil
}

// verify checks the peer's chain leads to the instance's CA for any use: agent certificates are issued for servers and serve as clients here.
func (id *Identity) verify(pin string) func([][]byte, [][]*x509.Certificate) error {
	return func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) == 0 {
			return errors.New("the peer sent no certificate")
		}
		leaf, err := x509.ParseCertificate(raw[0])
		if err != nil {
			return err
		}
		inter := x509.NewCertPool()
		for _, der := range raw[1:] {
			if c, err := x509.ParseCertificate(der); err == nil {
				inter.AddCert(c)
			}
		}
		if _, err := leaf.Verify(x509.VerifyOptions{
			Roots: id.Roots, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		}); err != nil {
			return fmt.Errorf("the peer's certificate is not from this instance: %w", err)
		}
		if pin != "" && Fingerprint(raw[0]) != pin {
			return errors.New("the peer's certificate is not the one expected")
		}
		return nil
	}
}

// ServerName is a name every certificate of the instance carries: the CA says who may answer, the pin says which one.
const ServerName = "localhost"

// ClientConfig dials a peer; with a pin, only that exact certificate passes.
func (id *Identity) ClientConfig(pin string) *tls.Config {
	return &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{id.Cert},
		RootCAs:               id.Roots,
		ServerName:            ServerName,
		VerifyPeerCertificate: id.verify(pin),
	}
}

// ServerConfig answers a peer and requires its certificate from the same CA.
func (id *Identity) ServerConfig() *tls.Config {
	return &tls.Config{
		MinVersion:            tls.VersionTLS13,
		Certificates:          []tls.Certificate{id.Cert},
		ClientAuth:            tls.RequireAnyClientCert,
		VerifyPeerCertificate: id.verify(""),
	}
}

// PeerFingerprint is the fingerprint of the certificate the other end of a finished handshake presented.
func PeerFingerprint(st tls.ConnectionState) string {
	if len(st.PeerCertificates) == 0 {
		return ""
	}
	return Fingerprint(st.PeerCertificates[0].Raw)
}
