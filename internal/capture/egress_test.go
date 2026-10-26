package capture

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DeploCloud/deplo-agent/internal/netguard"
)

func fakeDNS(t *testing.T, answers map[string][]string) {
	t.Helper()
	orig := netguard.LookupIP
	netguard.LookupIP = func(_ context.Context, host string) ([]net.IP, error) {
		var out []net.IP
		for _, a := range answers[host] {
			out = append(out, net.ParseIP(a))
		}
		if out == nil {
			return nil, fmt.Errorf("no such host %q", host)
		}
		return out, nil
	}
	t.Cleanup(func() { netguard.LookupIP = orig })
}

func recordingEgress(dialed *[]string) *Egress {
	return &Egress{
		OwnHost: "app.example.com",
		Proxy:   map[string]string{"80": "127.0.0.1:8080", "443": "127.0.0.1:8443"},
		Local:   func() ([]net.IP, error) { return []net.IP{net.ParseIP("203.0.113.7"), net.ParseIP("127.0.0.1")}, nil },
		DialContext: func(_ context.Context, _, addr string) (net.Conn, error) {
			*dialed = append(*dialed, addr)
			return nil, errors.New("not dialing in a test")
		},
	}
}

func TestEgressDial_policy(t *testing.T) {
	fakeDNS(t, map[string][]string{
		"cdn.example.net":   {"93.184.216.34"},
		"api.example.com":   {"203.0.113.7"},
		"intra.example.com": {"10.0.0.5"},
		"meta.example.com":  {"169.254.169.254"},
		"loop.example.com":  {"127.0.0.1"},
		"cgnat.example.com": {"100.64.0.9"},
		"mixed.example.com": {"93.184.216.34", "192.168.1.10"},
	})
	cases := []struct {
		target, want string
		refused      bool
	}{
		{"app.example.com:443", "127.0.0.1:8443", false},
		{"APP.example.com.:80", "127.0.0.1:8080", false},
		{"cdn.example.net:443", "93.184.216.34:443", false},
		{"api.example.com:443", "127.0.0.1:8443", false},
		{"loop.example.com:80", "127.0.0.1:8080", false},
		{"api.example.com:9443", "", true},
		{"203.0.113.7:22", "", true},
		{"intra.example.com:443", "", true},
		{"meta.example.com:80", "", true},
		{"169.254.169.254:80", "", true},
		{"cgnat.example.com:443", "", true},
		{"mixed.example.com:443", "", true},
		{"loop.example.com:2375", "", true},
	}
	for _, c := range cases {
		var dialed []string
		e := recordingEgress(&dialed)
		_, err := e.dial(context.Background(), c.target)
		if c.refused {
			if !errors.Is(err, errRefused) || len(dialed) != 0 || e.Refused() != 1 {
				t.Errorf("%s: want refused with nothing dialed, got err=%v dialed=%v", c.target, err, dialed)
			}
			continue
		}
		if len(dialed) != 1 || dialed[0] != c.want {
			t.Errorf("%s: want %s dialed, got %v (err %v)", c.target, c.want, dialed, err)
		}
	}
}

func serveEgress(t *testing.T, e *Egress) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "egress.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go e.Serve(ctx, l)
	return sock
}

func TestEgress_connectTunnels(t *testing.T) {
	e := &Egress{
		OwnHost: "app.example.com",
		Proxy:   map[string]string{"443": "upstream"},
		DialContext: func(_ context.Context, _, addr string) (net.Conn, error) {
			if addr != "upstream" {
				return nil, fmt.Errorf("unexpected %s", addr)
			}
			a, b := net.Pipe()
			go func() { _, _ = io.Copy(b, b) }()
			return a, nil
		},
	}
	c, err := net.Dial("unix", serveEgress(t, e))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprint(c, "CONNECT app.example.com:443 HTTP/1.1\r\nHost: app.example.com:443\r\n\r\n")
	br := bufio.NewReader(c)
	line, _ := br.ReadString('\n')
	if !strings.Contains(line, "200") {
		t.Fatalf("want 200, got %q", line)
	}
	_, _ = br.ReadString('\n')
	fmt.Fprint(c, "ping")
	got := make([]byte, 4)
	if _, err := io.ReadFull(br, got); err != nil || string(got) != "ping" {
		t.Fatalf("tunnel did not echo: %q %v", got, err)
	}
}

func TestEgress_forwardsPlainHTTPWithTheRealHost(t *testing.T) {
	var gotHost string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		fmt.Fprint(w, "hello")
	}))
	defer up.Close()
	e := &Egress{OwnHost: "app.example.com", Proxy: map[string]string{"80": up.Listener.Addr().String()}}
	sock := serveEgress(t, e)
	client := &http.Client{Transport: &http.Transport{
		Proxy: http.ProxyURL(mustURL(t, "http://egress")),
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return net.Dial("unix", sock)
		},
	}}
	resp, err := client.Get("http://app.example.com/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "hello" || gotHost != "app.example.com" {
		t.Fatalf("want hello with Host app.example.com, got %q host %q", body, gotHost)
	}
}

func TestEgress_refusedTargetAnswers502(t *testing.T) {
	fakeDNS(t, map[string][]string{})
	e := &Egress{OwnHost: "app.example.com", Proxy: map[string]string{}}
	c, err := net.Dial("unix", serveEgress(t, e))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprint(c, "CONNECT 169.254.169.254:443 HTTP/1.1\r\nHost: 169.254.169.254:443\r\n\r\n")
	line, _ := bufio.NewReader(c).ReadString('\n')
	if !strings.Contains(line, "502") || e.Refused() != 1 {
		t.Fatalf("want a 502 and one refusal, got %q (%d)", line, e.Refused())
	}
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestEgressDial_skipsTrackersButNeverTheApp(t *testing.T) {
	var dialed []string
	e := recordingEgress(&dialed)
	e.OwnHost = "www.google-analytics.com"
	for _, h := range []string{"securepubads.g.doubleclick.net:443", "www.googletagmanager.com:443", "cdn.taboola.com:443"} {
		if _, err := e.dial(context.Background(), h); !errors.Is(err, errTracker) {
			t.Errorf("%s: want skipped as a tracker, got %v", h, err)
		}
	}
	if e.Refused() != 0 {
		t.Error("a tracker is not an SSRF refusal")
	}
	_, _ = e.dial(context.Background(), "www.google-analytics.com:443")
	if len(dialed) != 1 {
		t.Fatalf("the deployment's own host is never skipped, dialed %v", dialed)
	}
	if isTracker("notdoubleclick.net") || !isTracker("doubleclick.net") {
		t.Error("suffix match must be on a label boundary")
	}
}

func TestEgress_allowListRefusesEverythingElse(t *testing.T) {
	e := &Egress{Allow: func(h string) bool { return h == "registry.npmjs.org" }}
	if _, err := e.dial(context.Background(), "example.com:443"); err == nil || e.Refused() != 1 {
		t.Fatalf("a host off the list must be refused, got %v (refused %d)", err, e.Refused())
	}
}
