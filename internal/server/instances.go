package server

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// ListInstances enumerates a project's attachable containers.
func (s *Service) ListInstances(ctx context.Context, req *pb.ListInstancesRequest) (*pb.ListInstancesResponse, error) {
	projectID := req.GetProjectId()
	if projectID == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}
	cs, err := listProjectContainers(ctx, projectID)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(cs))
	for _, c := range cs {
		names = append(names, c.Name)
	}
	details := inspectContainers(ctx, names)

	out := make([]*pb.ConsoleInstance, 0, len(cs))
	for _, c := range cs {
		d := details[c.Name]
		state := d.State
		if state == "" {
			state = c.State
		}
		service := serviceOf(req.GetSlug(), c.Name)
		out = append(out, &pb.ConsoleInstance{
			Name:          c.Name,
			Service:       service,
			Image:         c.Image,
			Running:       state == "running",
			Exposed:       isExposed(service, req.GetExposeService()),
			User:          d.User,
			Workdir:       d.Workdir,
			OpenStdin:     d.OpenStdin,
			Tty:           d.Tty,
			State:         state,
			Health:        d.Health,
			RestartCount:  d.RestartCount,
			StartedAtUnix: startedAtUnix(d.StartedAt),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Exposed != b.Exposed {
			return a.Exposed
		}
		if a.Running != b.Running {
			return a.Running
		}
		return a.Service < b.Service
	})
	return &pb.ListInstancesResponse{Instances: out}, nil
}

func isExposed(service, exposeService string) bool {
	return exposeService != "" && service == exposeService
}

type containerRow struct {
	Name  string
	Image string
	State string
}

func listProjectContainers(ctx context.Context, projectID string) ([]containerRow, error) {
	args := []string{"ps", "-a", "--format", "{{json .}}"}
	if projectID != "" {
		args = append(args, "--filter", "label=deplo.project="+projectID)
	}
	res, err := dockercli.Run(ctx, 15*time.Second, args...)
	if err != nil {
		return nil, err
	}
	rows := []containerRow{}
	for _, line := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var raw struct {
			Names string `json:"Names"`
			Image string `json:"Image"`
			State string `json:"State"`
		}
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}
		rows = append(rows, containerRow{Name: raw.Names, Image: raw.Image, State: raw.State})
	}
	return rows, nil
}

func startedAtUnix(ts string) int64 {
	t, ok := parseDockerTime(ts)
	if !ok || t.IsZero() || t.Unix() <= 0 {
		return 0
	}
	return t.Unix()
}

type containerDetail struct {
	Name         string `json:"name"`
	User         string `json:"user"`
	Workdir      string `json:"workdir"`
	OpenStdin    bool   `json:"openStdin"`
	Tty          bool   `json:"tty"`
	State        string `json:"state"`
	Health       string `json:"health"`
	RestartCount int32  `json:"restartCount"`
	StartedAt    string `json:"startedAt"`
}

const inspectTemplate = `{"name":{{json .Name}},` +
	`"user":{{json .Config.User}},` +
	`"workdir":{{json .Config.WorkingDir}},` +
	`"openStdin":{{json .Config.OpenStdin}},` +
	`"tty":{{json .Config.Tty}},` +
	`"state":{{json .State.Status}},` +
	`"restartCount":{{json .RestartCount}},` +
	`"startedAt":{{json .State.StartedAt}},` +
	`"health":{{if .State.Health}}{{json .State.Health.Status}}{{else}}""{{end}}}`

func inspectContainers(ctx context.Context, names []string) map[string]containerDetail {
	out := map[string]containerDetail{}
	if len(names) == 0 {
		return out
	}
	args := append([]string{"inspect", "-f", inspectTemplate}, names...)
	res, err := dockercli.Run(ctx, 20*time.Second, args...)
	if err != nil {
		return out
	}
	return parseInspectLines(res.Stdout)
}

func parseInspectLines(stdout string) map[string]containerDetail {
	out := map[string]containerDetail{}
	for _, line := range strings.Split(strings.TrimSpace(stdout), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var d containerDetail
		if err := json.Unmarshal([]byte(line), &d); err != nil {
			continue
		}
		d.Name = strings.TrimPrefix(d.Name, "/")
		if d.Name == "" {
			continue
		}
		if d.User == "" {
			d.User = "root"
		}
		if d.Workdir == "" {
			d.Workdir = "/"
		}
		out[d.Name] = d
	}
	return out
}

func serviceOf(slug, containerName string) string {
	prefix := "deplo-" + slug + "-"
	if strings.HasPrefix(containerName, prefix) {
		rest := containerName[len(prefix):]
		return trimTrailingReplicaIndex(rest)
	}
	return strings.TrimPrefix(containerName, "deplo-")
}

func trimTrailingReplicaIndex(s string) string {
	i := strings.LastIndex(s, "-")
	if i < 0 || i == len(s)-1 {
		return s
	}
	for _, ch := range s[i+1:] {
		if ch < '0' || ch > '9' {
			return s
		}
	}
	return s[:i]
}
