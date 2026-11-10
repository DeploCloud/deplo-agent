package wire_test

import (
	"bytes"
	"crypto/tls"
	"net"
	"strings"
	"testing"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/testpki"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/wire"
)

// handshake runs both ends over a loopback pair, which buffers, unlike a pipe.
func handshake(t *testing.T, client, server *tls.Config) (cerr, serr error) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	done := make(chan error, 1)
	go func() {
		raw, err := l.Accept()
		if err != nil {
			done <- err
			return
		}
		defer raw.Close()
		s := tls.Server(raw, server)
		err = s.Handshake()
		if err == nil {
			_, err = s.Read(make([]byte, 1))
		}
		done <- err
	}()
	raw, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	c := tls.Client(raw, client)
	cerr = c.Handshake()
	if cerr == nil {
		_, _ = c.Write([]byte("x"))
	}
	raw.Close()
	return cerr, <-done
}

func TestStreamHandshakePinsThePeerAndAcceptsServerStyleClientCertificates(t *testing.T) {
	ca := testpki.New(t)
	a, b, c := ca.Leaf(t, "a"), ca.Leaf(t, "b"), ca.Leaf(t, "c")

	if cerr, serr := handshake(t, a.ClientConfig(b.Fingerprint), b.ServerConfig()); cerr != nil || serr != nil {
		t.Fatalf("pinned handshake failed: %v / %v", cerr, serr)
	}
	// Another party of the same instance answering for b is not b.
	if cerr, _ := handshake(t, a.ClientConfig(b.Fingerprint), c.ServerConfig()); cerr == nil {
		t.Fatal("a certificate other than the pinned one passed")
	}
	// A certificate from another CA does not pass as a client.
	if _, serr := handshake(t, testpki.Foreign(t, ca).ClientConfig(""), b.ServerConfig()); serr == nil {
		t.Fatal("a client from another CA passed")
	}
	// An expired one neither.
	if cerr, serr := handshake(t, ca.Expired(t, "old").ClientConfig(""), b.ServerConfig()); cerr == nil && serr == nil {
		t.Fatal("an expired client passed")
	}
}

func TestPartyNames(t *testing.T) {
	fp := strings.Repeat("ab", 32)
	p, err := wire.ParseParty(fp + "/leader")
	if err != nil || p.Role != wire.RoleLeader || p.Key() != fp+"/leader" {
		t.Fatalf("%v %v", p, err)
	}
	for _, bad := range []string{"", fp, fp + "/root", "xyz/member", strings.ToUpper(fp) + "/member", fp[:10] + "/member"} {
		if _, err := wire.ParseParty(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestHeadersConsumeOneLineOnly(t *testing.T) {
	var buf bytes.Buffer
	_ = wire.WriteHeader(&buf, wire.VerbConnect, "peer")
	buf.WriteString("tail")
	verb, arg, err := wire.ReadHeader(&buf)
	if err != nil || verb != wire.VerbConnect || arg != "peer" || buf.String() != "tail" {
		t.Fatalf("%q %q %v, rest %q", verb, arg, err, buf.String())
	}
	long := bytes.NewReader(bytes.Repeat([]byte("a"), 1000))
	if _, _, err := wire.ReadHeader(long); err == nil {
		t.Fatal("an endless header was read")
	}
}
