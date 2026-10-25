package server

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func TestCapabilities_advertisesScreenshot(t *testing.T) {
	if !containsString(Capabilities, "screenshot") {
		t.Error(`Capabilities must advertise "screenshot"`)
	}
}

func TestCaptureRunArgs_hardened(t *testing.T) {
	args := strings.Join(captureRunArgs("/usr/local/bin/deplo-agent", "/var/lib/deplo-agent/capture-1/egress.sock"), " ")
	for _, want := range []string{
		"--rm", "--network none", "--user 65534:65534", "--cap-drop ALL", "--security-opt no-new-privileges",
		"--read-only", "--memory 1g", "--cpu-shares 256", "--oom-score-adj 1000", "--pids-limit 1024", "--log-driver none",
		"/usr/local/bin/deplo-agent:/deplo-agent:ro", "egress.sock:/egress/egress.sock:ro",
		"--entrypoint /deplo-agent " + captureImage + " capture",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("run args miss %q:\n%s", want, args)
		}
	}
	if !strings.Contains(captureImage, "@sha256:") {
		t.Error("the browser image must be pinned by digest")
	}
}

func TestCaptureScreenshot_refusesABadURL(t *testing.T) {
	s := &Service{}
	for _, u := range []string{"", "ftp://app.example.com/", "https://user:pw@app.example.com/", "https:///nohost", "https://app example.com/"} {
		_, err := s.CaptureScreenshot(context.Background(), &pb.CaptureScreenshotRequest{Url: u})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%q: want InvalidArgument, got %v", u, err)
		}
	}
}

func TestCaptureScreenshot_waitsForTheSlotOnlyAsLongAsTheCaller(t *testing.T) {
	captureSlot <- struct{}{}
	defer func() { <-captureSlot }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := (&Service{}).CaptureScreenshot(ctx, &pb.CaptureScreenshotRequest{Url: "https://app.example.com/"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("want DeadlineExceeded while another capture holds the slot, got %v", err)
	}
}

func webp(n int) []byte {
	b := append([]byte("RIFF\x00\x00\x00\x00WEBPVP8 "), make([]byte, n)...)
	return b
}

func TestCaptureResponse(t *testing.T) {
	buf := func(s string) *cappedBuffer {
		c := &cappedBuffer{limit: captureMaxOutput}
		_, _ = c.Write([]byte(s))
		return c
	}
	ok, err := captureResponse(buf(`{"outcome":"ok","status":200,"light":"` + b64(webp(10)) + `","dark":"` + b64([]byte("<svg/>")) + `"}`))
	if err != nil || ok.Outcome != pb.CaptureOutcome_CAPTURE_OUTCOME_OK || len(ok.LightWebp) == 0 || ok.DarkWebp != nil {
		t.Fatalf("ok with a bad dark picture: want light kept and dark dropped, got %+v %v", ok, err)
	}
	if _, err := captureResponse(buf(`{"outcome":"ok","light":"` + b64([]byte("<html>")) + `"}`)); err == nil {
		t.Fatal("a light picture that is not WebP must be refused")
	}
	if _, err := captureResponse(buf(`{"outcome":"ok","light":"` + b64(webp(captureMaxImage)) + `"}`)); err == nil {
		t.Fatal("an oversized picture must be refused")
	}
	he, _ := captureResponse(buf(`{"outcome":"http_error","status":502}`))
	if he.Outcome != pb.CaptureOutcome_CAPTURE_OUTCOME_HTTP_ERROR || he.HttpStatus != 502 {
		t.Fatalf("http_error: got %+v", he)
	}
	over := &cappedBuffer{limit: 4}
	if n, _ := over.Write([]byte("0123456789")); n != 10 || !over.overflow || over.Len() != 4 {
		t.Fatalf("capped buffer must drain and flag, got n=%d overflow=%v len=%d", n, over.overflow, over.Len())
	}
	if _, err := captureResponse(over); err == nil {
		t.Fatal("overflowing output must be refused")
	}
}

func TestTraefikPorts(t *testing.T) {
	dockerOnPath(t, `echo '{"443/tcp":[{"HostIp":"","HostPort":"8443"}],"80/tcp":[{"HostIp":"127.0.0.1","HostPort":"8080"}]}'`)
	got := traefikPorts(context.Background())
	if got["80"] != "127.0.0.1:8080" || got["443"] != "127.0.0.1:8443" {
		t.Fatalf("got %v", got)
	}
	dockerOnPath(t, `exit 1`)
	if got := traefikPorts(context.Background()); got["443"] != "127.0.0.1:443" {
		t.Fatalf("without the proxy container: want the default ports, got %v", got)
	}
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
