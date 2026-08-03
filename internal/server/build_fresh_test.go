package server

import (
	"slices"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The two "give me a genuinely fresh one" switches: no_build_cache must put
// --no-cache on the docker build, force_recreate must put --force-recreate on the
// compose up. Both default OFF — an ordinary deploy's argv has to stay exactly
// what it was, or every unchanged reroute starts restarting containers.

func TestBuildArgvNoCache(t *testing.T) {
	plain := buildArgv(&pb.DeployRequest{}, "-f", "Dockerfile")
	if slices.Contains(plain, "--no-cache") {
		t.Fatalf("an ordinary build must keep the cache: %v", plain)
	}
	if strings.Join(plain, " ") != "build -f Dockerfile" {
		t.Fatalf("argv = %v, want the unchanged `build -f Dockerfile`", plain)
	}

	fresh := buildArgv(&pb.DeployRequest{NoBuildCache: true}, "-f", "Dockerfile")
	// docker parses build flags after the verb; the rest of the argv must be
	// untouched so the caller's -f/--build-arg/context ordering still holds.
	if strings.Join(fresh, " ") != "build --no-cache -f Dockerfile" {
		t.Fatalf("argv = %v, want `build --no-cache -f Dockerfile`", fresh)
	}
}

func TestComposeUpArgsForceRecreate(t *testing.T) {
	plain := composeUpArgs("deplo-app", "/stacks/app.yml", "", false, nil)
	if strings.Join(plain, " ") != "compose -p deplo-app -f /stacks/app.yml up -d --remove-orphans" {
		t.Fatalf("argv = %v, want the unchanged single-image bring-up", plain)
	}

	stack := composeUpArgs("deplo-app", "/stacks/app.yml", "/stacks/app.env", true, nil)
	if strings.Join(stack, " ") != "compose -p deplo-app -f /stacks/app.yml --env-file /stacks/app.env up -d --remove-orphans --force-recreate" {
		t.Fatalf("argv = %v, want the env-file stack forced to recreate", stack)
	}
}

// Both switches are capability-gated, so an older agent's control plane can warn
// instead of silently caching / silently not recreating.
func TestFreshBuildCapabilitiesAdvertised(t *testing.T) {
	for _, c := range []string{"deploy.nocache", "deploy.force-recreate"} {
		if !slices.Contains(Capabilities, c) {
			t.Errorf("Hello must advertise %q", c)
		}
	}
}
