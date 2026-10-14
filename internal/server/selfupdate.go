package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const selfUpdateGrace = 750 * time.Millisecond

var reexec = func(path string, argv []string, env []string) error {
	return syscall.Exec(path, argv, env)
}

const maxAgentBinary = 256 << 20

var updateClient = &http.Client{
	Timeout: 10 * time.Minute,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return checkDownloadURL(req.URL)
	},
}

// checkDownloadURL allows https only; plain http is for a loopback test server.
func checkDownloadURL(u *url.URL) error {
	host := u.Hostname()
	ip := net.ParseIP(host)
	if u.Scheme == "https" || u.Scheme == "http" && (host == "localhost" || ip != nil && ip.IsLoopback()) {
		return nil
	}
	return fmt.Errorf("refusing to download the agent from %s://%s: only https is allowed", u.Scheme, u.Host)
}

var downloadFile = func(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if err := checkDownloadURL(u); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := updateClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("download %s: HTTP %d", rawURL, resp.StatusCode)
	}
	return resp.Body, nil
}

// SelfUpdate replaces the agent's own binary in place with a newer release and restarts to run it, WITHOUT touching the mTLS materials, so the server keeps its identity and pinned fingerprint across the upgrade (see the RPC's contract in proto/agent.proto).
func (s *Service) SelfUpdate(ctx context.Context, req *pb.SelfUpdateRequest) (*pb.SelfUpdateResponse, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot locate the agent binary to update: %v (re-run the installer to upgrade)", err)
	}
	if resolved, rerr := filepath.EvalSymlinks(exe); rerr == nil {
		exe = resolved
	}
	return s.applyUpdate(ctx, exe, req)
}

func (s *Service) applyUpdate(ctx context.Context, exePath string, req *pb.SelfUpdateRequest) (*pb.SelfUpdateResponse, error) {
	bin := req.GetBinaries()[runtime.GOARCH]
	if bin == nil || bin.GetUrl() == "" || bin.GetSha256() == "" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"the agent release has no binary for this host's architecture (%s); re-run the installer once a release includes it", runtime.GOARCH)
	}

	// Forward-only, like the control plane (DeploCloud/deplo docs/agents/fleet-rollout.md section 10).
	if versionNewer(AgentVersion, req.GetVersion()) {
		return nil, status.Errorf(codes.FailedPrecondition,
			"refusing to downgrade the agent from v%s to v%s", AgentVersion, req.GetVersion())
	}
	staged, err := s.stageVerifiedBinary(ctx, exePath, bin.GetUrl(), bin.GetSha256())
	if err != nil {
		return nil, err
	}

	if err := os.Rename(staged, exePath); err != nil {
		os.Remove(staged)
		return nil, status.Errorf(codes.FailedPrecondition,
			"cannot replace the agent binary at %s: %v (is the install dir writable? re-run the installer to upgrade)", exePath, err)
	}
	log.Printf("deplo-agent: self-update staged v%s at %s; restarting to apply", req.GetVersion(), exePath)

	argv := append([]string{exePath}, os.Args[1:]...)
	env := os.Environ()
	go func() {
		time.Sleep(selfUpdateGrace)
		log.Printf("deplo-agent: re-execing %s to complete self-update to v%s", exePath, req.GetVersion())
		if err := reexec(exePath, argv, env); err != nil {
			log.Printf("deplo-agent: re-exec failed: %v (new binary is on disk; restart the service to apply)", err)
		}
	}()

	return &pb.SelfUpdateResponse{Version: req.GetVersion(), Restarting: true}, nil
}

func (s *Service) stageVerifiedBinary(ctx context.Context, exe, url, wantSha256 string) (string, error) {
	body, err := downloadFile(ctx, url)
	if err != nil {
		return "", status.Errorf(codes.Unavailable, "download new agent binary: %v", err)
	}
	defer body.Close()

	dir := filepath.Dir(exe)
	tmp, err := os.CreateTemp(dir, ".deplo-agent-update-*")
	if err != nil {
		return "", status.Errorf(codes.FailedPrecondition,
			"cannot stage the update in %s: %v (re-run the installer to upgrade)", dir, err)
	}
	tmpPath := tmp.Name()
	cleanup := func(e error) (string, error) {
		tmp.Close()
		os.Remove(tmpPath)
		return "", e
	}
	// Streamed to disk while hashed: the binary never sits whole in memory.
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(body, maxAgentBinary+1))
	if err != nil {
		return cleanup(status.Errorf(codes.Unavailable, "download new agent binary: %v", err))
	}
	if n > maxAgentBinary {
		return cleanup(status.Errorf(codes.FailedPrecondition, "agent binary is larger than %d bytes", maxAgentBinary))
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != wantSha256 {
		return cleanup(status.Errorf(codes.FailedPrecondition,
			"agent binary checksum mismatch: expected %s, got %s (refusing to install an unverified binary)", wantSha256, got))
	}
	if err := tmp.Chmod(0o755); err != nil {
		return cleanup(status.Errorf(codes.Internal, "chmod staged binary: %v", err))
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return "", status.Errorf(codes.Internal, "close staged binary: %v", err)
	}
	return tmpPath, nil
}

var (
	semverPattern  = regexp.MustCompile(`^v?(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?`)
	describeSuffix = regexp.MustCompile(`^\d+-g[0-9a-f]{4,}(-dirty)?$`)
)

// versionNewer reports whether a outranks b by the control plane's isNewer (lib/version.ts);
// an unparseable side ("dev") never outranks anything.
func versionNewer(a, b string) bool {
	ma, mb := semverPattern.FindStringSubmatch(strings.TrimSpace(a)), semverPattern.FindStringSubmatch(strings.TrimSpace(b))
	if ma == nil || mb == nil {
		return false
	}
	for i := 1; i <= 3; i++ {
		na, _ := strconv.Atoi(ma[i])
		nb, _ := strconv.Atoi(mb[i])
		if na != nb {
			return na > nb
		}
	}
	return comparePre(preRelease(ma[4]), preRelease(mb[4])) > 0
}

// preRelease drops a `git describe` suffix (0.2.0-8-gb39f8a7), a build after the tag, not a canary.
func preRelease(p string) []string {
	if p == "" || describeSuffix.MatchString(p) {
		return nil
	}
	return strings.Split(p, ".")
}

func comparePre(a, b []string) int {
	if len(a) == 0 || len(b) == 0 {
		return len(b) - len(a)
	}
	for i := 0; i < len(a) || i < len(b); i++ {
		if i >= len(a) {
			return -1
		}
		if i >= len(b) {
			return 1
		}
		na, oka := preNum(a[i])
		nb, okb := preNum(b[i])
		switch {
		case oka && okb && na != nb:
			return na - nb
		case oka && !okb:
			return -1
		case !oka && okb:
			return 1
		case !oka && !okb && a[i] != b[i]:
			return strings.Compare(a[i], b[i])
		}
	}
	return 0
}

func preNum(s string) (int, bool) {
	if s == "" || strings.Trim(s, "0123456789") != "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	return n, err == nil
}
