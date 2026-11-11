package server

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A slug never holds "_", so no other app's stack or container can take the stand-in's name.
const standInSuffix = "_next"

// startStandIn runs the new version beside the live container (see DeployRequest.stand_in_compose_yaml).
// standIn reports that it serves and has to be removed once the live container is replaced; failed
// ends the deploy with the live container untouched. Neither: nothing to keep online, swap in place.
func (s *Service) startStandIn(ctx context.Context, req *pb.DeployRequest, live string, started time.Time, commitSha string, e *emitter) (standIn bool, failed *pb.DeployResult) {
	replicas := replicaCount(req.GetStandInComposeYaml())
	stack := isStack(req)
	project := stack || runsAsProject(req.GetStandInComposeYaml())
	if !dockercli.IsRunning(ctx, live) && len(projectContainers(ctx, live)) == 0 {
		return false, nil
	}
	slug := req.GetSlug()
	if err := os.WriteFile(s.stackPath(slug+standInSuffix), []byte(req.GetStandInComposeYaml()), 0o600); err != nil {
		e.log("warn", "Could not keep the app online during this deploy ("+err.Error()+"), so it goes offline briefly.")
		return false, nil
	}
	base := s.standInCompose(slug)
	if req.GetBuildKind() == pb.BuildKind_BUILD_KIND_COMPOSE {
		// The stand-in reads the same working copy, so it passes the same checks as the stack it copies.
		err := checkSourceStack(ctx, req, base, s.srcDir(slug))
		if err == nil {
			err = s.stripSourceLinks(slug)
		}
		if err != nil {
			_ = os.Remove(s.stackPath(slug + standInSuffix))
			return false, &pb.DeployResult{Error: err.Error(), CommitSha: commitSha}
		}
	}
	created := func() bool {
		defer s.lockStack(slug)()
		// A stand-in an interrupted deploy left behind still serves the old release.
		_, _ = dockercli.Run(ctx, 2*time.Minute, append(base, "down", "--remove-orphans")...)
		res, err := dockercli.RunEnv(ctx, 5*time.Minute, dockerConfigEnv(req), append(base, "up", "--no-start")...)
		return err == nil && res.Code == 0
	}()
	same := created
	if created && project {
		same = sameProjectRoutes(ctx, live, live+standInSuffix)
	} else if created {
		same = sameRoutes(ctx, live, live+standInSuffix)
	}
	if !same {
		s.removeStandIn(slug, e)
		if created {
			// ponytail: replicas restart together here too. Keeping them up needs a stand-in rendered with its own
			// router names, which is the control plane's to do (the Traefik label grammar never moves into Go).
			e.log("info", "How this app is reached changed (a domain, a header or sticky sessions), so it goes offline briefly while the new version starts.")
		} else {
			e.log("warn", "Could not start the new version beside the old one, so the app goes offline briefly.")
		}
		return false, nil
	}

	if stack {
		e.log("info", "Starting the new version of the stack beside the one that is online.")
	} else if replicas > 1 {
		e.log("info", fmt.Sprintf("Starting %d replicas of the new version beside the ones that are online.", replicas))
	} else {
		e.log("info", "Starting the new version beside the one that is online.")
	}
	up := func() bool {
		defer s.lockStack(slug)()
		// The stand-in never runs a late service: the live stack's own serves, or starts once it is ready.
		args := withoutLate(append(base, "up", "-d", "--remove-orphans"),
			lateServices(req.GetStandInComposeYaml(), req.GetStartAfterReady()))
		code, err := dockercli.StreamEnv(ctx, 15*time.Minute, func(l string) { e.log("info", l) },
			dockerConfigEnv(req), args...)
		return err == nil && code == 0
	}()
	res := &pb.DeployResult{Error: "compose up of the new version failed", CommitSha: commitSha}
	if up {
		res = s.awaitStandIn(ctx, req, live+standInSuffix, replicas, started, commitSha, e)
	}
	if !res.GetReady() {
		s.removeStandIn(slug, e)
		// Storage may refuse a second copy (a lock file, an embedded database): swap in place instead.
		// Replicas never swap in place: that would stop every copy at once.
		if !project && hasStorage(ctx, live) {
			e.log("info", "The new version could not start beside the old one while they share storage, "+
				"so the app goes offline briefly while it starts on its own.")
			return false, nil
		}
		res.Error = "The new version did not start, so the old one is still online: " + res.GetError()
		return false, res
	}
	e.log("info", "The new version is up and serving. Replacing the old one…")
	return true, nil
}

// awaitStandIn waits for the stand-in like the release it copies: a stack by every container of its
// project, each service under the name it has there.
func (s *Service) awaitStandIn(ctx context.Context, req *pb.DeployRequest, project string, replicas int, started time.Time, commitSha string, e *emitter) *pb.DeployResult {
	if !isStack(req) {
		return s.awaitStarted(ctx, req, project, false, replicas, started, commitSha, e)
	}
	ready := &pb.ReadyCheck{Port: req.GetReady().GetPort(), PortRequired: req.GetReady().GetPortRequired()}
	if svc := req.GetReady().GetService(); svc != "" {
		ready.Service = svc + standInSuffix
	}
	next := &pb.DeployRequest{Slug: req.GetSlug(), Ready: ready, ReadyTimeoutMs: req.GetReadyTimeoutMs()}
	return s.waitReadyWith(ctx, next, true, started, commitSha, e, func() []readySample { return sampleProject(ctx, project) })
}

// standInCompose addresses the stand-in's project with the env file its app's stack is read with.
func (s *Service) standInCompose(slug string) []string {
	args := s.composeCtl(slug)
	args[2], args[4] = "deplo-"+slug+standInSuffix, s.stackPath(slug+standInSuffix)
	return args
}

// standIn: the container belongs to a deploy's stand-in, by its own name or by its compose project.
func (c readySample) standIn() bool {
	return strings.HasSuffix(c.name, standInSuffix) || strings.HasSuffix(c.project, standInSuffix)
}

// removeStandIn takes the stand-in down even when the deploy was canceled. Its volumes are the app's.
func (s *Service) removeStandIn(slug string, e *emitter) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	defer s.lockStack(slug)()
	res, err := dockercli.Run(ctx, 2*time.Minute, append(s.standInCompose(slug), "down", "--remove-orphans")...)
	if err != nil || res.Code != 0 {
		e.log("warn", "Could not remove the copy that kept the app online; the next deploy removes it.")
		return
	}
	_ = os.Remove(s.stackPath(slug + standInSuffix))
}

// hasStorage: the container mounts a volume or a folder, which the stand-in shares with it.
func hasStorage(ctx context.Context, container string) bool {
	res, err := dockercli.Run(ctx, 15*time.Second, "inspect", "-f", "{{len .Mounts}}", container)
	return err == nil && res.Code == 0 && strings.TrimSpace(res.Stdout) != "0"
}

// sameProjectRoutes is sameRoutes over every container of two compose projects, started or only created.
func sameProjectRoutes(ctx context.Context, a, b string) bool {
	ra, okA := projectRoutes(ctx, a)
	rb, okB := projectRoutes(ctx, b)
	return okA && okB && maps.Equal(ra, rb)
}

func projectRoutes(ctx context.Context, project string) (map[string]bool, bool) {
	res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-aq", "--filter", "label=com.docker.compose.project="+project)
	ids := splitLines(res.Stdout)
	if err != nil || res.Code != 0 || len(ids) == 0 {
		return nil, false
	}
	res, err = dockercli.Run(ctx, 15*time.Second, append([]string{"inspect", "-f", "{{json .Config.Labels}}"}, ids...)...)
	if err != nil || res.Code != 0 {
		return nil, false
	}
	out := map[string]bool{}
	for _, line := range splitLines(res.Stdout) {
		var all map[string]string
		if json.Unmarshal([]byte(line), &all) != nil {
			return nil, false
		}
		for k, v := range all {
			if strings.HasPrefix(k, "traefik.") {
				out[k+"="+v] = true
			}
		}
	}
	return out, true
}

// sameRoutes: the proxy load-balances two containers only when they declare identical routes.
func sameRoutes(ctx context.Context, a, b string) bool {
	ra, okA := traefikLabels(ctx, a)
	rb, okB := traefikLabels(ctx, b)
	return okA && okB && maps.Equal(ra, rb)
}

func traefikLabels(ctx context.Context, container string) (map[string]string, bool) {
	res, err := dockercli.Run(ctx, 15*time.Second, "inspect", "-f", "{{json .Config.Labels}}", container)
	if err != nil || res.Code != 0 {
		return nil, false
	}
	var all map[string]string
	if json.Unmarshal([]byte(strings.TrimSpace(res.Stdout)), &all) != nil {
		return nil, false
	}
	out := map[string]string{}
	for k, v := range all {
		if strings.HasPrefix(k, "traefik.") {
			out[k] = v
		}
	}
	return out, true
}

// stackReplicas is what each service of a stack declares through deploy.replicas; one that declares none is absent.
func stackReplicas(composeYaml string) map[string]int {
	var doc struct {
		Services map[string]struct {
			Deploy struct {
				Replicas *int `yaml:"replicas"`
			} `yaml:"deploy"`
		} `yaml:"services"`
	}
	if yaml.Unmarshal([]byte(composeYaml), &doc) != nil {
		return nil
	}
	out := map[string]int{}
	for name, svc := range doc.Services {
		if svc.Deploy.Replicas != nil {
			out[name] = *svc.Deploy.Replicas
		}
	}
	return out
}

// runsAsProject: a stack that declares deploy.replicas has no container of its own name, even with one copy
// (a member of a cluster holding one replica of an App spread over several).
func runsAsProject(composeYaml string) bool { return len(stackReplicas(composeYaml)) > 0 }

// replicaCount is the most copies a service of the stack asks for through deploy.replicas (1 when none).
func replicaCount(composeYaml string) int {
	most := 1
	for _, n := range stackReplicas(composeYaml) {
		most = max(most, n)
	}
	return most
}

// projectContainers lists the running containers of a compose project. A project is named like
// its single container, so this also finds the one copy an app ran before it had replicas.
func projectContainers(ctx context.Context, project string) []string {
	res, err := dockercli.Run(ctx, 15*time.Second, "ps", "-q",
		"--filter", "label=com.docker.compose.project="+project, "--filter", "status=running")
	if err != nil || res.Code != 0 {
		return nil
	}
	return splitLines(res.Stdout)
}

// waitProjectRunning holds until every one of n copies of the project runs.
func waitProjectRunning(ctx context.Context, project string, n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(projectContainers(ctx, project)) >= n {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
	}
	return false
}
