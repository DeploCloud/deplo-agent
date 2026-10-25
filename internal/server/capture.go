package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/capture"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// captureImage is the browser the screenshots run in. Bump the digest with agent releases.
const captureImage = "chromedp/headless-shell:157.0.8103.15@sha256:f76fce2964611eb1e25b90d906727884db12a8918a6d85a0df2619a9ef845c7c"

const (
	captureRunTimeout  = 90 * time.Second
	capturePullTimeout = 5 * time.Minute
	captureMinFree     = 5 << 30
	captureMaxOutput   = 8 << 20
	captureMaxImage    = 2 << 20
)

// ponytail: one capture per host; a queue if busy fleets want parallel screenshots.
var captureSlot = make(chan struct{}, 1)

// captureExe is the binary mounted into the browser's container; tests point it at a real build.
var captureExe = os.Executable

// captureProxyPorts finds this server's proxy; tests point it at their own server.
var captureProxyPorts = traefikPorts

// CaptureScreenshot photographs a deployment in a browser that has no network of its own.
func (s *Service) CaptureScreenshot(ctx context.Context, req *pb.CaptureScreenshotRequest) (*pb.CaptureScreenshotResponse, error) {
	target, err := url.Parse(req.GetUrl())
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.User != nil || !probeHostRe.MatchString(target.Host) {
		return nil, status.Error(codes.InvalidArgument, "url must be http(s)://host, with no credentials in it")
	}
	select {
	case captureSlot <- struct{}{}:
		defer func() { <-captureSlot }()
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err := ensureCaptureImage(ctx); err != nil {
		return nil, err
	}
	exe, err := captureExe()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "locate the agent binary: %v", err)
	}
	dir, err := os.MkdirTemp(s.captureDir(), "capture-")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "socket dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	sock := filepath.Join(dir, "egress.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "egress socket: %v", err)
	}
	// The browser runs as nobody; the 0700 directory around the socket still keeps every other host user out.
	_ = os.Chmod(sock, 0o666)
	egress := &capture.Egress{OwnHost: target.Hostname(), Proxy: captureProxyPorts(ctx)}
	ectx, stop := context.WithCancel(ctx)
	defer stop()
	go egress.Serve(ectx, l)

	spec, _ := json.Marshal(capture.Spec{URL: req.GetUrl(), Username: req.GetBasicAuthUsername(), Password: req.GetBasicAuthPassword()})
	out := &cappedBuffer{limit: captureMaxOutput}
	_, runErr := dockercli.PipeThrough(ctx, captureRunTimeout, bytes.NewReader(spec), out, nil, captureRunArgs(exe, sock)...)
	stop()
	if n := egress.Refused(); n > 0 {
		log.Printf("deplo-agent: screenshot of %s: refused %d connections", target.Hostname(), n)
	}
	if runErr != nil {
		return nil, status.Errorf(codes.Unavailable, "browser: %v", runErr)
	}
	return captureResponse(out)
}

func (s *Service) captureDir() string {
	if s.agentDir != "" {
		return s.agentDir
	}
	return s.buildTmpDir
}

func captureRunArgs(exe, sock string) []string {
	return []string{
		"run", "--rm", "-i", "--network", "none", "--user", "65534:65534",
		"--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev,size=256m",
		"--memory", "1g", "--memory-swap", "1g", "--cpus", "1", "--cpu-shares", "256", "--pids-limit", "1024", "--oom-score-adj", "1000",
		"--log-driver", "none", "--label", "deplo.helper=screenshot",
		"-e", "HOME=/tmp", "-e", "XDG_CACHE_HOME=/tmp", "-e", "XDG_CONFIG_HOME=/tmp",
		"-v", exe + ":/deplo-agent:ro", "-v", sock + ":" + capture.SocketPath + ":ro",
		"--entrypoint", "/deplo-agent", captureImage, "capture",
	}
}

func ensureCaptureImage(ctx context.Context) error {
	if res, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", captureImage); err == nil && res.Code == 0 {
		return nil
	}
	if root := dockerRootDir(ctx); root != "" && filesystemFree(root) < captureMinFree {
		return status.Error(codes.FailedPrecondition, "not enough free disk to download the screenshot browser")
	}
	res, err := dockercli.Run(ctx, capturePullTimeout, "pull", captureImage)
	if err != nil {
		return status.Errorf(codes.Unavailable, "download the screenshot browser: %v", err)
	}
	if res.Code != 0 {
		return status.Errorf(codes.Unavailable, "download the screenshot browser: %s", strings.TrimSpace(res.Stderr))
	}
	return nil
}

// traefikPorts reads where this server's proxy listens, so a loopback-only or non-default bind still works.
func traefikPorts(ctx context.Context) map[string]string {
	ports := map[string]string{"80": "127.0.0.1:80", "443": "127.0.0.1:443"}
	res, err := dockercli.Run(ctx, 10*time.Second, "inspect", "-f", "{{json .HostConfig.PortBindings}}", traefikContainer)
	if err != nil || res.Code != 0 {
		return ports
	}
	var bindings map[string][]struct{ HostIp, HostPort string }
	if json.Unmarshal([]byte(strings.TrimSpace(res.Stdout)), &bindings) != nil {
		return ports
	}
	for _, p := range []string{"80", "443"} {
		for _, b := range bindings[p+"/tcp"] {
			if b.HostPort == "" {
				continue
			}
			ip := b.HostIp
			if ip == "" || ip == "0.0.0.0" || ip == "::" {
				ip = "127.0.0.1"
			}
			ports[p] = net.JoinHostPort(ip, b.HostPort)
			break
		}
	}
	return ports
}

var webpMagic = []byte("WEBP")

func captureResponse(out *cappedBuffer) (*pb.CaptureScreenshotResponse, error) {
	if out.overflow {
		return nil, status.Error(codes.Internal, "browser output too large")
	}
	var res capture.Result
	if err := json.Unmarshal(out.Bytes(), &res); err != nil {
		return nil, status.Errorf(codes.Internal, "browser output: %v", err)
	}
	resp := &pb.CaptureScreenshotResponse{HttpStatus: int32(res.Status), Detail: res.Detail}
	switch res.Outcome {
	case capture.OutcomeOK:
		if err := validWebp(res.Light); err != nil {
			return nil, status.Errorf(codes.Internal, "light picture: %v", err)
		}
		if len(res.Dark) > 0 && validWebp(res.Dark) != nil {
			res.Dark = nil
		}
		resp.Outcome, resp.LightWebp, resp.DarkWebp = pb.CaptureOutcome_CAPTURE_OUTCOME_OK, res.Light, res.Dark
	case capture.OutcomeHTTPError:
		resp.Outcome = pb.CaptureOutcome_CAPTURE_OUTCOME_HTTP_ERROR
	case capture.OutcomeBlank:
		resp.Outcome = pb.CaptureOutcome_CAPTURE_OUTCOME_BLANK
	case capture.OutcomeTimeout:
		resp.Outcome = pb.CaptureOutcome_CAPTURE_OUTCOME_TIMEOUT
	default:
		resp.Outcome = pb.CaptureOutcome_CAPTURE_OUTCOME_UNREACHABLE
	}
	return resp, nil
}

func validWebp(b []byte) error {
	if len(b) > captureMaxImage {
		return errors.New("too large")
	}
	if len(b) < 12 || !bytes.Equal(b[:4], []byte("RIFF")) || !bytes.Equal(b[8:12], webpMagic) {
		return errors.New("not a WebP image")
	}
	return nil
}

// cappedBuffer keeps the first limit bytes and drains the rest, so a runaway child is never blocked on its pipe.
type cappedBuffer struct {
	bytes.Buffer
	limit    int
	overflow bool
}

func (c *cappedBuffer) Write(p []byte) (int, error) {
	if room := c.limit - c.Len(); room < len(p) {
		c.overflow = true
		if room > 0 {
			c.Buffer.Write(p[:room])
		}
		return len(p), nil
	}
	return c.Buffer.Write(p)
}
