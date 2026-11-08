package server

import (
	"context"
	"encoding/json"
	"maps"
	"os"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A slug never holds "_", so no other app's stack or container can take the stand-in's name.
const standInSuffix = "_next"

// startStandIn runs the new version beside the live container (see DeployRequest.stand_in_compose_yaml).
// standIn reports that it serves and has to be removed once the live container is replaced; failed
// ends the deploy with the live container untouched. Neither: nothing to keep online, swap in place.
func (s *Service) startStandIn(ctx context.Context, req *pb.DeployRequest, live string, started time.Time, commitSha string, e *emitter) (standIn bool, failed *pb.DeployResult) {
	if !dockercli.IsRunning(ctx, live) {
		return false, nil
	}
	slug := req.GetSlug()
	base := s.standInCompose(slug)
	if err := os.WriteFile(s.stackPath(slug+standInSuffix), []byte(req.GetStandInComposeYaml()), 0o600); err != nil {
		e.log("warn", "Could not keep the app online during this deploy ("+err.Error()+"), so it goes offline briefly.")
		return false, nil
	}
	created := func() bool {
		defer s.lockStack(slug)()
		// A stand-in an interrupted deploy left behind still serves the old release.
		_, _ = dockercli.Run(ctx, 2*time.Minute, append(base, "down", "--remove-orphans")...)
		res, err := dockercli.RunEnv(ctx, 5*time.Minute, dockerConfigEnv(req), append(base, "up", "--no-start")...)
		return err == nil && res.Code == 0
	}()
	if !created || !sameRoutes(ctx, live, live+standInSuffix) {
		s.removeStandIn(slug, e)
		if created {
			e.log("info", "The app's domains changed, so it goes offline briefly while the new version starts.")
		} else {
			e.log("warn", "Could not start the new version beside the old one, so the app goes offline briefly.")
		}
		return false, nil
	}

	e.log("info", "Starting the new version beside the one that is online…")
	up := func() bool {
		defer s.lockStack(slug)()
		code, err := dockercli.StreamEnv(ctx, 15*time.Minute, func(l string) { e.log("info", l) },
			dockerConfigEnv(req), append(base, "up", "-d", "--remove-orphans")...)
		return err == nil && code == 0
	}()
	res := &pb.DeployResult{Error: "compose up of the new version failed", CommitSha: commitSha}
	if up {
		res = s.awaitStarted(ctx, req, live+standInSuffix, false, started, commitSha, e)
	}
	if !res.GetReady() {
		s.removeStandIn(slug, e)
		// Storage may refuse a second copy (a lock file, an embedded database): swap in place instead.
		if hasStorage(ctx, live) {
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

func (s *Service) standInCompose(slug string) []string {
	return composeBaseArgs("deplo-"+slug+standInSuffix, s.stackPath(slug+standInSuffix), "", "")
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
