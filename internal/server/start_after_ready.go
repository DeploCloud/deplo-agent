package server

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// lateLabel marks a service that waits for the app in the stack file itself, so a later StartStack
// or Reroute, which carry no list, still hold it back.
const lateLabel = "deplo.start-after-ready"

// lateServices are the stack's services that wait for the app: a tunnel connector must not send
// visitors to an app that is not answering yet. The request names them, or the stack marks them.
func lateServices(composeYaml string, named []string) []string {
	var doc struct {
		Services map[string]struct {
			Labels any `yaml:"labels"`
		} `yaml:"services"`
	}
	if yaml.Unmarshal([]byte(composeYaml), &doc) != nil {
		return nil
	}
	var out []string
	for name, svc := range doc.Services {
		if servicePattern.MatchString(name) && (slices.Contains(named, name) || marksLate(svc.Labels)) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

func marksLate(labels any) bool {
	switch l := labels.(type) {
	case map[string]any:
		return fmt.Sprint(l[lateLabel]) == "true"
	case []any:
		return slices.Contains(l, any(lateLabel+"=true"))
	}
	return false
}

// heldBack are the late services with no running container. One that already runs is left alone:
// stopping it would take the app's address offline for the length of every redeploy.
func heldBack(ctx context.Context, project string, late []string) []string {
	var out []string
	for _, n := range late {
		res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-q", "--filter", "status=running",
			"--filter", "label=com.docker.compose.project="+project, "--filter", "label=com.docker.compose.service="+n)
		if err != nil || res.Code != 0 || strings.TrimSpace(res.Stdout) == "" {
			out = append(out, n)
		}
	}
	return out
}

// withoutLate keeps the held services out of an `up`.
func withoutLate(args, held []string) []string {
	for _, n := range held {
		args = append(args, "--scale", n+"=0")
	}
	return args
}

// lateUp is the command that starts the late services beside a stack that is already up.
func lateUp(base, late []string) []string {
	return append(append(slices.Clone(base), "up", "-d", "--no-deps"), late...)
}

// startLate brings the late services up once the app is ready, and turns the deploy into a failure
// when one will not start: an App nobody can reach through its tunnel is not up.
func (s *Service) startLate(ctx context.Context, req *pb.DeployRequest, res *pb.DeployResult, e *emitter) *pb.DeployResult {
	if !res.GetReady() || len(e.lateArgs) == 0 {
		return res
	}
	defer s.lockStack(req.GetSlug())()
	late := strings.Join(lateServices(req.GetComposeYaml(), req.GetStartAfterReady()), ", ")
	e.log("info", "The app is ready. Starting "+late+"…")
	out, err := dockercli.RunEnv(ctx, 2*time.Minute, dockerConfigEnv(req), e.lateArgs...)
	if err == nil && out.Code == 0 {
		return res
	}
	msg := strings.TrimSpace(out.Stderr)
	if err != nil {
		msg = err.Error()
	}
	return &pb.DeployResult{CommitSha: res.GetCommitSha(), Error: fmt.Sprintf("the app is ready, but %s did not start: %s", late, msg)}
}

// startHolding starts a stopped stack in two steps: everything else, then the late services.
// ponytail: StartStack carries no ready check, so "ready" here is the rest of the stack having started.
func (s *Service) startHolding(ctx context.Context, slug string, late []string) (dockercli.Result, error) {
	ls, err := dockercli.Run(ctx, 15*time.Second, "ps", "-a", "--filter", "label=com.docker.compose.project=deplo-"+slug,
		"--format", `{{.Label "com.docker.compose.service"}}`)
	if err != nil || ls.Code != 0 {
		return ls, err
	}
	var rest, held []string
	for _, n := range splitLines(ls.Stdout) {
		switch {
		case !servicePattern.MatchString(n) || slices.Contains(rest, n) || slices.Contains(held, n):
		case slices.Contains(late, n):
			held = append(held, n)
		default:
			rest = append(rest, n)
		}
	}
	for _, step := range [][]string{rest, held} {
		if len(step) == 0 {
			continue
		}
		if res, err := dockercli.Run(ctx, time.Minute, s.composeCtl(slug, append([]string{"start"}, step...)...)...); err != nil || res.Code != 0 {
			return res, err
		}
	}
	return dockercli.Result{}, nil
}
