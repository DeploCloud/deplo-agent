package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// captureTestBinary builds the agent the browser's container runs, or takes DEPLO_CAPTURE_TEST_BINARY.
func captureTestBinary(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("DEPLO_CAPTURE_TEST_BINARY"); p != "" {
		return p
	}
	out := filepath.Join(t.TempDir(), "deplo-agent")
	cmd := exec.Command("go", "build", "-o", out, "../..")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the agent: %v\n%s", err, b)
	}
	return out
}

func TestE2E_CaptureScreenshot(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	var mu sync.Mutex
	seen := map[string]bool{}
	var sitePort string
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen[r.URL.Path] = true
		mu.Unlock()
		switch r.URL.Path {
		case "/":
			fmt.Fprint(w, `<!doctype html><style>body{background:#fff;color:#111;font:48px sans-serif}
@media (prefers-color-scheme: dark){body{background:#111;color:#eee}}</style><h1>Deplo screenshot test</h1>`)
		case "/blank":
			fmt.Fprint(w, `<!doctype html><body style="background:#fff"></body>`)
		case "/auth":
			if u, p, ok := r.BasicAuth(); !ok || u != "ada" || p != "s3cret" {
				w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
				http.Error(w, "nope", http.StatusUnauthorized)
				return
			}
			fmt.Fprint(w, `<!doctype html><h1 style="font:48px sans-serif">Behind basic auth</h1>`)
		case "/ssrf":
			fmt.Fprintf(w, `<!doctype html><h1 style="font:48px sans-serif">Probe</h1>
<img src="http://169.254.169.254/latest/meta-data/"><iframe src="http://127.0.0.1:%s/leak"></iframe>`, sitePort)
		default:
			http.NotFound(w, r)
		}
	}))
	defer site.Close()
	sitePort = portOf(site.Listener.Addr().String())

	exe := captureTestBinary(t)
	origExe, origPorts := captureExe, captureProxyPorts
	captureExe = func() (string, error) { return exe, nil }
	captureProxyPorts = func(context.Context) map[string]string {
		return map[string]string{"80": site.Listener.Addr().String()}
	}
	t.Cleanup(func() { captureExe, captureProxyPorts = origExe, origPorts })

	b := make([]byte, 6)
	_, _ = rand.Read(b)
	host := "capture-" + hex.EncodeToString(b) + ".invalid"
	s := &Service{buildTmpDir: t.TempDir()}
	shoot := func(path, user, pass string) *pb.CaptureScreenshotResponse {
		t.Helper()
		cctx, cancel := context.WithTimeout(ctx, 6*time.Minute)
		defer cancel()
		resp, err := s.CaptureScreenshot(cctx, &pb.CaptureScreenshotRequest{Url: "http://" + host + path, BasicAuthUsername: user, BasicAuthPassword: pass})
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		return resp
	}

	if r := shoot("/", "", ""); r.Outcome != pb.CaptureOutcome_CAPTURE_OUTCOME_OK || len(r.LightWebp) == 0 || len(r.DarkWebp) == 0 {
		t.Errorf("/: want OK with light and dark, got %v %d/%d bytes (%s)", r.Outcome, len(r.LightWebp), len(r.DarkWebp), r.Detail)
	}
	if r := shoot("/missing", "", ""); r.Outcome != pb.CaptureOutcome_CAPTURE_OUTCOME_HTTP_ERROR || r.HttpStatus != 404 {
		t.Errorf("/missing: want HTTP_ERROR 404, got %v %d", r.Outcome, r.HttpStatus)
	}
	if r := shoot("/blank", "", ""); r.Outcome != pb.CaptureOutcome_CAPTURE_OUTCOME_BLANK {
		t.Errorf("/blank: want BLANK, got %v", r.Outcome)
	}
	if r := shoot("/auth", "ada", "s3cret"); r.Outcome != pb.CaptureOutcome_CAPTURE_OUTCOME_OK {
		t.Errorf("/auth with the credential: want OK, got %v %d", r.Outcome, r.HttpStatus)
	}
	if r := shoot("/auth", "", ""); r.Outcome != pb.CaptureOutcome_CAPTURE_OUTCOME_HTTP_ERROR || r.HttpStatus != 401 {
		t.Errorf("/auth without it: want HTTP_ERROR 401, got %v %d", r.Outcome, r.HttpStatus)
	}
	shoot("/ssrf", "", "")
	mu.Lock()
	defer mu.Unlock()
	if !seen["/ssrf"] || seen["/leak"] {
		t.Errorf("the page must load but never reach this host's loopback: %v", seen)
	}
}

func portOf(hostport string) string {
	for i := len(hostport) - 1; i >= 0; i-- {
		if hostport[i] == ':' {
			return hostport[i+1:]
		}
	}
	return ""
}
