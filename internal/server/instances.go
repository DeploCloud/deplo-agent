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

// instances.go ports lib/data/console.ts listInstances (+ the inspectRuntime /
// inspectStdio / serviceOf helpers from lib/infra/docker.ts) to the agent. It
// lists every attachable container in a project's stack with the runtime/stdio
// metadata the console needs, ordered exposed -> running -> service.
//
// It also reports each container's RAW docker state (running / restarting /
// exited / …), its healthcheck verdict and its restart count. The bool `running`
// alone cannot distinguish a container docker is crash-looping from one that is
// cleanly stopped — both are simply "not running" — which is how the control
// plane came to show an app in a restart loop as "Online".
//
// DELIBERATELY no synthetic fallback entry: when a project has no containers the
// response is empty. The TS listInstances returns ONE fabricated entry so the
// console still renders a name — that is a control-plane UI affordance and stays
// control-plane-side, LOCALHOST-ONLY. Fabricating a container for a remote would
// be the "stored status that lies" the plan forbids; a remote with no containers
// truthfully reports none.

// ListInstances enumerates a project's attachable containers.
func (s *Service) ListInstances(ctx context.Context, req *pb.ListInstancesRequest) (*pb.ListInstancesResponse, error) {
	projectID := req.GetProjectId()
	// An empty project_id would drop the label filter and list EVERY container on
	// the host (cross-tenant enumeration). Reject it: ListInstances is always
	// scoped to one project, mirroring assertOwned's empty-projectID refusal on
	// the other container RPCs.
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
	// ONE `docker inspect` for the whole stack, keyed by container name — not two
	// per container. A container that vanished between the `ps` and the inspect
	// is simply absent from the map (never silently mistaken for another one).
	details := inspectContainers(ctx, names)

	out := make([]*pb.ConsoleInstance, 0, len(cs))
	for _, c := range cs {
		d := details[c.Name]
		// `docker ps` already told us the state; the inspect confirms it. Prefer
		// the inspect (same daemon, richer read), fall back to ps.
		state := d.State
		if state == "" {
			state = c.State
		}
		service := serviceOf(req.GetSlug(), c.Name)
		out = append(out, &pb.ConsoleInstance{
			Name:    c.Name,
			Service: service,
			Image:   c.Image,
			Running: state == "running",
			Exposed:      isExposed(service, req.GetExposeService()),
			User:         d.User,
			Workdir:      d.Workdir,
			OpenStdin:    d.OpenStdin,
			Tty:          d.Tty,
			State:        state,
			Health:       d.Health,
			RestartCount: d.RestartCount,
		})
	}
	// Exposed app first, then running, then alphabetical by service — the same
	// order listInstances produces (so instances[0] is the default target).
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

// isExposed reports whether a container's service is the Traefik-exposed one.
//
// It compares the SERVICE, not the container name. The old test asked whether the
// name contained "-<exposeService>-", which is true of every container in the
// stack: a compose project is named deplo-<slug>, so "deplo-activepieces-postgres-1"
// contains "-activepieces-" exactly as the app's own container does — and the
// whole stack came back flagged as exposed, taking the instance ordering with it.
func isExposed(service, exposeService string) bool {
	return exposeService != "" && service == exposeService
}

type containerRow struct {
	Name  string
	Image string
	State string
}

// listProjectContainers runs `docker ps -a --filter label=deplo.project=<id>`
// and parses the JSON lines, mirroring lib/infra/docker.ts listContainers.
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

// containerDetail is everything one `docker inspect` pass yields per container.
type containerDetail struct {
	Name         string `json:"name"`
	User         string `json:"user"`
	Workdir      string `json:"workdir"`
	OpenStdin    bool   `json:"openStdin"`
	Tty          bool   `json:"tty"`
	State        string `json:"state"`
	Health       string `json:"health"`
	RestartCount int32  `json:"restartCount"`
}

// The inspect template emits one JSON object per container. It carries the name
// so the answers can be matched back even when a container disappears mid-call,
// and guards .State.Health, which is nil for an image with no healthcheck (a bare
// {{json .State.Health.Status}} would fail the whole template).
const inspectTemplate = `{"name":{{json .Name}},` +
	`"user":{{json .Config.User}},` +
	`"workdir":{{json .Config.WorkingDir}},` +
	`"openStdin":{{json .Config.OpenStdin}},` +
	`"tty":{{json .Config.Tty}},` +
	`"state":{{json .State.Status}},` +
	`"restartCount":{{json .RestartCount}},` +
	`"health":{{if .State.Health}}{{json .State.Health.Status}}{{else}}""{{end}}}`

// inspectContainers inspects every named container in ONE call, keyed by name.
// Best-effort: a container that cannot be inspected is simply missing from the
// map, and the caller falls back to the `docker ps` row. Replaces the old pair of
// per-container calls (inspectRuntime + inspectStdio), whose tab-separated format
// dropped a field whenever Config.User was empty: TrimSpace ate the LEADING tab,
// so the split shifted and the WORKDIR was returned as the user — the console
// then exec'd as `-u /app`.
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
	// A non-zero code means at least one name was not found; the lines for the
	// ones that WERE found are still on stdout, so parse whatever came back.
	return parseInspectLines(res.Stdout)
}

// parseInspectLines turns the inspect template's output into details by name.
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
		// docker reports the name as "/deplo-foo".
		d.Name = strings.TrimPrefix(d.Name, "/")
		if d.Name == "" {
			continue
		}
		// An image that declares no USER runs as root, and no WORKDIR means "/".
		// Defaulted HERE, on the parsed field — never by string-splitting, which
		// is what shifted workdir into user when Config.User was empty.
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

// serviceOf extracts the compose service name from a container name
// (deplo-<slug>-<service>-N), falling back to the slug for single-image deploys.
// Mirrors lib/data/console.ts serviceOf.
func serviceOf(slug, containerName string) string {
	prefix := "deplo-" + slug + "-"
	if strings.HasPrefix(containerName, prefix) {
		rest := containerName[len(prefix):]
		return trimTrailingReplicaIndex(rest)
	}
	return strings.TrimPrefix(containerName, "deplo-")
}

// trimTrailingReplicaIndex strips a compose "-N" replica suffix (.replace(/-\d+$/,"")).
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
