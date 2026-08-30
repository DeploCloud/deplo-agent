package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func TestCapabilities_advertisesHttpProbe(t *testing.T) {
	if !containsString(Capabilities, "http-probe") {
		t.Error("Capabilities must advertise \"http-probe\"")
	}
}

func TestValidateProbePath(t *testing.T) {
	ok := []string{"/", "/favicon.ico", "/static/icon-32x32.png?v=2", "/a%20b"}
	for _, p := range ok {
		if err := validateProbePath(p); err != nil {
			t.Errorf("path %q should be accepted: %v", p, err)
		}
	}
	// A space or a newline in the path is request smuggling, not a typo - the
	// path lands verbatim in the request line.
	bad := []string{"", "favicon.ico", "/a b", "/a\r\nX-Injected: 1", "/a\nb", "/\x7f", strings.Repeat("/a", 2000)}
	for _, p := range bad {
		if err := validateProbePath(p); err == nil {
			t.Errorf("path %q should be refused", p)
		} else if status.Code(err) != codes.InvalidArgument {
			t.Errorf("path %q: want InvalidArgument, got %v", p, status.Code(err))
		}
	}
}

func TestProbeHostGrammar(t *testing.T) {
	for _, h := range []string{"app.example.com", "app-1.nip.io", "localhost", "example.com:8080"} {
		if !probeHostRe.MatchString(h) {
			t.Errorf("host %q should be accepted", h)
		}
	}
	for _, h := range []string{"", "a b", "http://example.com", "example.com\r\nX: 1", "user@example.com", "/path"} {
		if probeHostRe.MatchString(h) {
			t.Errorf("host %q should be refused", h)
		}
	}
}

func TestProbeHttp_refusesAnUnscopedRequest(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), t.TempDir(), "")
	// No project_id => no label filter => every container on the host would be a
	// candidate. Refused before anything touches Docker.
	_, err := s.ProbeHttp(context.Background(), &pb.ProbeHttpRequest{Port: 80, Path: "/"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestProbeHttp_validatesPortPathAndHost(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), t.TempDir(), "")
	base := func() *pb.ProbeHttpRequest {
		return &pb.ProbeHttpRequest{ProjectId: "prj_1", Slug: "app", Port: 80, Path: "/"}
	}
	cases := map[string]func(*pb.ProbeHttpRequest){
		"port 0":        func(r *pb.ProbeHttpRequest) { r.Port = 0 },
		"port 70000":    func(r *pb.ProbeHttpRequest) { r.Port = 70000 },
		"relative path": func(r *pb.ProbeHttpRequest) { r.Path = "favicon.ico" },
		"host is a URL": func(r *pb.ProbeHttpRequest) { r.Host = "http://evil/" },
	}
	for name, mutate := range cases {
		req := base()
		mutate(req)
		if _, err := s.ProbeHttp(context.Background(), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: want InvalidArgument, got %v", name, err)
		}
	}
}

func TestProbeOnce_sendsTheHostHeaderWhileDiallingTheAddress(t *testing.T) {
	var gotHost, gotPath, gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost, gotPath, gotUA = r.Host, r.URL.RequestURI(), r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("PNGDATA"))
	}))
	defer srv.Close()

	resp, err := probeOnce(context.Background(), addrOf(srv), "/favicon.png", "app.example.com", 0)
	if err != nil {
		t.Fatalf("probeOnce: %v", err)
	}
	// The app is reached at the container's address, but told it is being asked
	// for its own hostname - what an app with host authorization requires.
	if gotHost != "app.example.com" {
		t.Errorf("Host header = %q, want app.example.com", gotHost)
	}
	if gotPath != "/favicon.png" {
		t.Errorf("path = %q", gotPath)
	}
	if !strings.HasPrefix(gotUA, "deplo-agent/") {
		t.Errorf("User-Agent = %q", gotUA)
	}
	if resp.GetStatus() != 200 || string(resp.GetBody()) != "PNGDATA" {
		t.Errorf("status/body = %d/%q", resp.GetStatus(), resp.GetBody())
	}
	if resp.GetContentType() != "image/png" {
		t.Errorf("content type = %q", resp.GetContentType())
	}
	if resp.GetTruncated() {
		t.Error("a body that fits must not be reported as truncated")
	}
}

func TestProbeOnce_capsTheBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 5000)))
	}))
	defer srv.Close()

	resp, err := probeOnce(context.Background(), addrOf(srv), "/", "", 100)
	if err != nil {
		t.Fatalf("probeOnce: %v", err)
	}
	if len(resp.GetBody()) != 100 || !resp.GetTruncated() {
		t.Errorf("body=%d truncated=%v, want 100/true", len(resp.GetBody()), resp.GetTruncated())
	}
}

func TestProbeOnce_reportsARedirectWithoutFollowingIt(t *testing.T) {
	followed := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/elsewhere" {
			followed = true
			return
		}
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	}))
	defer srv.Close()

	resp, err := probeOnce(context.Background(), addrOf(srv), "/", "", 0)
	if err != nil {
		t.Fatalf("probeOnce: %v", err)
	}
	if followed {
		t.Error("the agent must not follow a redirect - that is the control plane's call")
	}
	if resp.GetStatus() != 302 || resp.GetLocation() != "/elsewhere" {
		t.Errorf("status=%d location=%q", resp.GetStatus(), resp.GetLocation())
	}
}

func TestProbeOnce_aDeadPortIsUnavailableNotAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := addrOf(srv)
	srv.Close() // nothing listens there now

	_, err := probeOnce(context.Background(), addr, "/", "", 0)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable, got %v", err)
	}
}

func TestPickContainerIP(t *testing.T) {
	// The TENANT network wins: it is the address Traefik itself reaches the app on.
	// It used to be the one literally named `deplo`, which no app has been on since
	// ADR-0028 - so that preference matched nothing and the pick fell to whichever
	// name sorted first, which can be the stack's private network.
	ip, err := pickContainerIP(
		`{"deplo-env-environ_x":{"IPAddress":"10.200.5.2"},"app_default":{"IPAddress":"172.19.0.2"}}`)
	if err != nil || ip != "10.200.5.2" {
		t.Fatalf("got %q, %v", ip, err)
	}
	// A team network is a tenant network too, and sorts AFTER the private one.
	ip, err = pickContainerIP(
		`{"a_default":{"IPAddress":"172.19.0.2"},"deplo-team-team_x":{"IPAddress":"10.200.7.3"}}`)
	if err != nil || ip != "10.200.7.3" {
		t.Fatalf("team network should win, got %q, %v", ip, err)
	}
	// Without it, the choice must not depend on Go's random map order.
	for i := 0; i < 20; i++ {
		ip, err = pickContainerIP(`{"zeta":{"IPAddress":"10.0.0.9"},"alpha":{"IPAddress":"10.0.0.2"}}`)
		if err != nil || ip != "10.0.0.2" {
			t.Fatalf("unstable pick: got %q, %v", ip, err)
		}
	}
	// IPv6-only container.
	ip, err = pickContainerIP(`{"deplo-env-e":{"IPAddress":"","GlobalIPv6Address":"fd00::2"}}`)
	if err != nil || ip != "fd00::2" {
		t.Fatalf("got %q, %v", ip, err)
	}
	// network_mode: host, or networking gone - nothing to probe.
	if _, err := pickContainerIP(`{"host":{"IPAddress":""}}`); err == nil {
		t.Error("a container with no address must be an error")
	}
	if _, err := pickContainerIP("not json"); err == nil {
		t.Error("unreadable inspect output must be an error")
	}
}

// addrOf is the host:port of a test server, without the scheme.
func addrOf(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "http://")
}
