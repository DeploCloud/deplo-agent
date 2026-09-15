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

const (
	probeDefaultMaxBytes = 512 * 1024
	probeMaxBytes        = 2 * 1024 * 1024
	probeTimeout         = 6 * time.Second
	probeMaxPathLen      = 2048
)

var probeHostRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,253}(:[0-9]{1,5})?$`)

var probeClient = &http.Client{
	Timeout: probeTimeout,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// ProbeHttp performs one GET against a container of the app's stack.
func (s *Service) ProbeHttp(ctx context.Context, req *pb.ProbeHttpRequest) (*pb.ProbeHttpResponse, error) {
	projectID := req.GetProjectId()
	if projectID == "" {
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
		hreq.Host = host
	}
	hreq.Header.Set("User-Agent", "deplo-agent/"+AgentVersion)
	hreq.Header.Set("Accept", "*/*")

	resp, err := probeClient.Do(hreq)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "probe %s: %v", path, err)
	}
	defer resp.Body.Close()

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
	for name := range nets {
		if dockercli.IsTenantNetwork(name) {
			if ip := addrOf(name); ip != "" {
				return ip, nil
			}
		}
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
		return "", fmt.Errorf("no container IP on any network")
	}
	return addrOf(best), nil
}
