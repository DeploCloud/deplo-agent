package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// probehttp.go answers ProbeHttp: ONE bounded HTTP GET to a container of an
// app's own stack, from the host, over Docker's network.
//
// It exists because a compose app's icon is not a file. Such an app runs
// prebuilt images — its favicon lives inside the image and is only ever SERVED,
// so the only honest way to read it is to ask the running app, exactly as a
// browser does. Everything ABOVE that read (which path to ask for, how to rank
// what comes back) stays in the control plane; this file is the host-coupled
// half and nothing more.
//
// The security property is the address: the CALLER never supplies one. It names
// an app (a `deplo.project` label it must own) and a compose service; the agent
// resolves that to a container and takes the IP from Docker itself. There is no
// DNS lookup, no scheme, no caller-chosen host — so this cannot be turned into a
// general outbound fetch from the host, and it can never reach a container
// belonging to another app. Redirects are reported, never followed, for the same
// reason: following one is a decision about where to go, which belongs to the
// control plane.

// The network Deplo attaches routed services to. A container is reachable on
// every network it joins, but this one is preferred: it is the network Traefik
// itself reaches the app on, so it is the address whose behaviour matches what
// the outside world sees.
const deploNetwork = "deplo"

const (
	// Default and ceiling for the returned body. The ceiling is what makes the
	// RPC bounded no matter what a caller asks for; it comfortably clears the
	// control plane's 512 KiB logo cap plus a large HTML document.
	probeDefaultMaxBytes = 512 * 1024
	probeMaxBytes        = 2 * 1024 * 1024
	// Whole-request budget, dial included. An app that cannot answer this fast is
	// treated as having no icon — detection is cosmetic and must never hold a
	// deploy or a page open.
	probeTimeout = 6 * time.Second
	// Longest path we will send. Far past any real icon URL; a guard against a
	// caller trying to smuggle a large payload into the request line.
	probeMaxPathLen = 2048
)

// probeHostRe is the Host header grammar: a plain hostname, optionally with a
// port. No spaces, no CR/LF, no userinfo, no scheme — a Host header is a single
// token and anything richer is a request-splitting attempt, not a hostname.
var probeHostRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,253}(:[0-9]{1,5})?$`)

// probeClient is shared: one client keeps connection reuse across the handful of
// requests an icon detection makes, and pins the redirect policy in one place.
var probeClient = &http.Client{
	Timeout: probeTimeout,
	// Never follow: the response says where it was sent, and the control plane
	// decides whether that target is still this app.
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// ProbeHttp performs one GET against a container of the app's stack.
func (s *Service) ProbeHttp(ctx context.Context, req *pb.ProbeHttpRequest) (*pb.ProbeHttpResponse, error) {
	projectID := req.GetProjectId()
	if projectID == "" {
		// Without the label filter the container lookup would range over every
		// container on the host — the same cross-tenant enumeration ListInstances
		// refuses.
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	port := req.GetPort()
	if port < 1 || port > 65535 {
		return nil, status.Errorf(codes.InvalidArgument, "port %d is out of range (1-65535)", port)
	}
	path := req.GetPath()
	if err := validateProbePath(path); err != nil {
		return nil, err
	}
	host := req.GetHost()
	if host != "" && !probeHostRe.MatchString(host) {
		return nil, status.Errorf(codes.InvalidArgument, "host %q is not a hostname", host)
	}
	ip, err := resolveStackContainerIP(ctx, projectID, req.GetSlug(), req.GetService())
	if err != nil {
		return nil, err
	}
	return probeOnce(ctx, net.JoinHostPort(ip, strconv.Itoa(int(port))), path, host, int(req.GetMaxBytes()))
}

// probeOnce is the request itself, against an address the caller has already
// resolved. Split from the handler so the wire behaviour that actually matters —
// the Host header override, the body cap, not following a redirect — is testable
// without a Docker daemon. It owns the cap (0 => the default, over the ceiling =>
// the ceiling), so there is exactly one place a body bound can be got wrong.
func probeOnce(ctx context.Context, addr, path, host string, maxBytes int) (*pb.ProbeHttpResponse, error) {
	if maxBytes <= 0 {
		maxBytes = probeDefaultMaxBytes
	}
	if maxBytes > probeMaxBytes {
		maxBytes = probeMaxBytes
	}
	url := "http://" + addr + path
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bad request target: %v", err)
	}
	if host != "" {
		// Go sends req.Host as the Host header while still dialling the URL's
		// address — the container IP stays the destination.
		hreq.Host = host
	}
	hreq.Header.Set("User-Agent", "deplo-agent/"+AgentVersion)
	hreq.Header.Set("Accept", "*/*")

	resp, err := probeClient.Do(hreq)
	if err != nil {
		// The app is not answering on that port (still booting, wrong service,
		// listening elsewhere). Unavailable, not an agent failure.
		return nil, status.Errorf(codes.Unavailable, "probe %s: %v", path, err)
	}
	defer resp.Body.Close()

	// Read one byte past the cap so a body that exactly fills it is not reported
	// as truncated, and a longer one always is.
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxBytes)+1))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read %s: %v", path, err)
	}
	truncated := len(body) > maxBytes
	if truncated {
		body = body[:maxBytes]
	}
	return &pb.ProbeHttpResponse{
		Status:      int32(resp.StatusCode),
		ContentType: strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type"))),
		Body:        body,
		Truncated:   truncated,
		Location:    strings.TrimSpace(resp.Header.Get("Location")),
	}, nil
}

// validateProbePath rejects anything that is not a plain absolute request path.
// A path arrives off the wire and goes into a request line, so a space or a
// CR/LF in it is request smuggling, not a typo.
func validateProbePath(path string) error {
	if path == "" || !strings.HasPrefix(path, "/") {
		return status.Error(codes.InvalidArgument, "path must start with /")
	}
	if len(path) > probeMaxPathLen {
		return status.Errorf(codes.InvalidArgument, "path is longer than %d bytes", probeMaxPathLen)
	}
	for _, r := range path {
		if r <= ' ' || r == 0x7f {
			return status.Error(codes.InvalidArgument, "path contains a control character or space")
		}
	}
	return nil
}

// resolveStackContainerIP finds the container serving `service` in the app's
// stack and returns the IP to talk to it on.
//
// Only RUNNING containers qualify: a stopped one has no address, and reporting
// "no icon" is the truthful answer for an app that is not up. An empty service
// takes the app's single running container — the single-image shape, where the
// stack has exactly one and naming it would be ceremony.
func resolveStackContainerIP(ctx context.Context, projectID, slug, service string) (string, error) {
	rows, err := listProjectContainers(ctx, projectID)
	if err != nil {
		return "", status.Errorf(codes.Unavailable, "list containers: %v", err)
	}
	var match string
	for _, c := range rows {
		if c.State != "running" {
			continue
		}
		if service != "" && serviceOf(slug, c.Name) != service {
			continue
		}
		match = c.Name
		break
	}
	if match == "" {
		if service != "" {
			return "", status.Errorf(codes.NotFound, "no running container for service %q", service)
		}
		return "", status.Error(codes.NotFound, "the app has no running container")
	}
	ip, err := containerIP(ctx, match)
	if err != nil {
		return "", err
	}
	return ip, nil
}

// containerIP reads a container's address from Docker, preferring the `deplo`
// network (the one Traefik reaches it on) and falling back to any other network
// it joined — a container that is only on its own compose network is still
// perfectly reachable from the host.
func containerIP(ctx context.Context, container string) (string, error) {
	res, err := dockercli.Run(ctx, 10*time.Second,
		"inspect", "-f", "{{json .NetworkSettings.Networks}}", container)
	if err != nil {
		return "", status.Errorf(codes.Unavailable, "inspect %s: %v", container, err)
	}
	if res.Code != 0 {
		return "", status.Errorf(codes.NotFound, "no such container %q", container)
	}
	ip, err := pickContainerIP(res.Stdout)
	if err != nil {
		return "", status.Errorf(codes.FailedPrecondition, "container %q: %v", container, err)
	}
	return ip, nil
}

// pickContainerIP parses `docker inspect`'s network map and picks the address to
// use. Split out from the docker call so the preference order is directly
// testable. Networks are a map, so iteration order is random — the fallback picks
// the alphabetically first name rather than whichever the runtime handed over, so
// the same container always yields the same address.
func pickContainerIP(stdout string) (string, error) {
	var nets map[string]struct {
		IPAddress         string `json:"IPAddress"`
		GlobalIPv6Address string `json:"GlobalIPv6Address"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &nets); err != nil {
		return "", fmt.Errorf("unreadable network settings: %w", err)
	}
	addrOf := func(name string) string {
		n, ok := nets[name]
		if !ok {
			return ""
		}
		if n.IPAddress != "" {
			return n.IPAddress
		}
		return n.GlobalIPv6Address
	}
	if ip := addrOf(deploNetwork); ip != "" {
		return ip, nil
	}
	names := make([]string, 0, len(nets))
	for name := range nets {
		names = append(names, name)
	}
	best := ""
	for _, name := range names {
		if addrOf(name) == "" {
			continue
		}
		if best == "" || name < best {
			best = name
		}
	}
	if best == "" {
		// A container on `network_mode: host` (or one whose networking is gone)
		// has no address of its own. Nothing to probe.
		return "", fmt.Errorf("no container IP on any network")
	}
	return addrOf(best), nil
}
