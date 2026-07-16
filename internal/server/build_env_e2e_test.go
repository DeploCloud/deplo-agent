package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// End-to-end (real docker): a deploy's env must reach the BUILD, not just the
// runtime stack — the whole point of build-time env parity (NEXT_PUBLIC_* is
// inlined while the build command runs). Builds a generated Dockerfile that
// bakes the var into the image AT BUILD TIME, runs it, and asserts the value
// came through — while never appearing on a logged command line.
//
// Run with: go test ./internal/server/ -run E2E -v  (with docker available).
func TestE2E_BuildEnvReachesDockerfileBuild(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	s := New(t.TempDir(), t.TempDir(), "/", "")
	buildDir := t.TempDir()

	const image = "deplo-test/buildenv:e2e"
	const value = "https://api.example.test/v1"

	// What the control plane's generateDockerfile now renders: ARG+ENV per var,
	// then build steps that consume it. `RUN` writes the var to a file — if the
	// value survives into the file, it was present AT BUILD TIME.
	df := `FROM busybox
ARG NEXT_PUBLIC_API
ENV NEXT_PUBLIC_API=$NEXT_PUBLIC_API
RUN printf '%s' "$NEXT_PUBLIC_API" > /baked
CMD ["cat", "/baked"]
`
	req := &pb.DeployRequest{
		Slug:       "buildenv",
		ProjectId:  "prj_buildenv",
		ImageRef:   image,
		BuildKind:  pb.BuildKind_BUILD_KIND_DOCKERFILE,
		Dockerfile: &pb.DockerfileBuild{Generated: true, GeneratedDockerfile: df},
		Env: map[string]string{
			"NEXT_PUBLIC_API": value,
			// NOT declared as ARG — must never be passed (warning-free builds).
			"RUNTIME_ONLY": "never-a-build-arg",
		},
	}

	var mu sync.Mutex
	var commands []string
	e := &emitter{send: func(ev *pb.DeployEvent) error {
		if l := ev.GetLog(); l != nil && l.GetLevel() == "command" {
			mu.Lock()
			commands = append(commands, l.GetText())
			mu.Unlock()
		}
		return nil
	}}

	if !s.buildImage(ctx, req, buildDir, e) {
		t.Fatal("buildImage failed (see deploy events)")
	}
	defer func() { _, _ = dockercli.Run(ctx, 30*time.Second, "rmi", "-f", image) }()

	res, err := dockercli.Run(ctx, 60*time.Second, "run", "--rm", image)
	if err != nil || res.Code != 0 {
		t.Fatalf("docker run: %v / exit %d (%s)", err, res.Code, res.Stderr)
	}
	if res.Stdout != value {
		t.Errorf("baked build-time value = %q; want %q", res.Stdout, value)
	}

	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(commands, "\n")
	if !strings.Contains(joined, "--build-arg NEXT_PUBLIC_API") {
		t.Errorf("declared ARG not forwarded as a bare build arg:\n%s", joined)
	}
	if strings.Contains(joined, value) || strings.Contains(joined, "never-a-build-arg") {
		t.Errorf("an env VALUE leaked onto a logged command line:\n%s", joined)
	}
	if strings.Contains(joined, "RUNTIME_ONLY") {
		t.Errorf("undeclared var must not be passed as a build arg:\n%s", joined)
	}
}
