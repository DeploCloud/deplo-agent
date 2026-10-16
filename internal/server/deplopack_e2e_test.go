package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

func TestE2E_DeplopackPreparedImagesAndCache(t *testing.T) {
	builder := os.Getenv("DEPLOPACK_E2E_BUILDER")
	if builder == "" {
		t.Skip("set DEPLOPACK_E2E_BUILDER on an isolated test host")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	tools := t.TempDir()
	installDir := filepath.Join(tools, "tools", "deplopack")
	if err := os.MkdirAll(installDir, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(builder)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(installDir, "builder-0.1.0"), data, 0755); err != nil {
		t.Fatal(err)
	}
	marker, _ := json.Marshal(detectorInstall{Version: "0.1.0", SHA256: fmt.Sprintf("%x", sha256.Sum256(data))})
	if err := os.WriteFile(filepath.Join(installDir, "builder-0.1.0.json"), marker, 0600); err != nil {
		t.Fatal(err)
	}
	s := New(t.TempDir(), t.TempDir(), "/", tools)
	s.dependencies = dependencyState{ctx: ctx, cancel: cancel, binaries: map[string]bool{}, preparations: map[string]*dependencyPreparation{}, jobs: map[string]*dependencyJob{}}
	defer s.StopDeplopackPreparation()
	source := t.TempDir()
	pkg := map[string]any{"name": "deplo-cache-check", "version": "1.0.0", "dependencies": map[string]string{"is-number": "7.0.0"}, "scripts": map[string]string{"build": `node -e "require('fs').writeFileSync('baked.txt',process.env.CACHE_TEST+':'+require('is-number')(7))"`, "start": "node server.js"}}
	encoded, _ := json.Marshal(pkg)
	if err := os.WriteFile(filepath.Join(source, "package.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "server.js"), []byte(`console.log("cache-check");`), 0600); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	build := func(app string, run int) string {
		t.Helper()
		image := "deplo-test/cache-check:" + suffix + fmt.Sprint(run)
		appID := "prj_cache_check_" + suffix + app
		cacheKey := fmt.Sprintf("%x", sha256.Sum256([]byte(appID)))[:20]
		t.Cleanup(func() {
			_, _ = dockercli.Run(context.Background(), time.Minute, "image", "rm", image)
			_, _ = dockercli.Run(context.Background(), time.Minute, "volume", "rm", "deplo-deplopack-cache-"+cacheKey)
		})
		req := &pb.DeployRequest{DeployId: "dpl_cache_check_" + suffix + fmt.Sprint(run), Slug: "cache-check-" + suffix, ProjectId: appID, ImageRef: image, BuildKind: pb.BuildKind_BUILD_KIND_DEPLOPACK, BuildSpec: &pb.BuildSpec{Method: "deplopack", Port: 3000, DeplopackVersion: "0.1.0", DeplopackProvider: "node"}, Env: map[string]string{"CACHE_TEST": "one"}}
		rec := &e2eBuildRecorder{}
		if !s.buildDeplopack(ctx, req, source, rec.emitter()) {
			t.Fatalf("build %d failed:\n%s", run, rec.joined())
		}
		logs := rec.joined()
		if strings.Contains(logs, "Unable to find image") || !strings.Contains(logs, "local image") {
			t.Fatalf("build %d did not reuse preparation:\n%s", run, logs)
		}
		result, err := dockercli.Run(ctx, time.Minute, "run", "--rm", "--entrypoint", "cat", image, "/app/baked.txt")
		if err != nil || result.Code != 0 || strings.TrimSpace(result.Stdout) != "one:true" {
			t.Fatalf("build output: %v %+v", err, result)
		}
		t.Logf("build %d complete; cached steps: %d", run, strings.Count(logs, "CACHED"))
		return logs
	}
	build("a", 1)
	warm := build("a", 2)
	for _, command := range []string{"npm install", "npm run build", "install mise packages:"} {
		match := regexp.MustCompile(`(?m)^#([0-9]+) ` + regexp.QuoteMeta(command)).FindStringSubmatch(warm)
		if len(match) != 2 || !strings.Contains(warm, "#"+match[1]+" CACHED") {
			t.Fatalf("%s was not cached:\n%s", command, warm)
		}
	}
	build("b", 3)
	spec, err := readDependencySpec(ctx, builder)
	if err != nil || spec == nil {
		t.Fatalf("catalog: %v", err)
	}
	dryRun, err := s.DockerCleanup(ctx, &pb.DockerCleanupRequest{DryRun: true, Scopes: []pb.CleanupScope{pb.CleanupScope_CLEANUP_SCOPE_UNUSED_PULLED_IMAGES}})
	if err != nil || !dryRun.GetOk() {
		t.Fatalf("cleanup dry-run: %v %+v", err, dryRun)
	}
	cli, err := dockercli.Run(ctx, time.Minute, "image", "inspect", "docker:29-cli", "--format", "{{.Id}}")
	if err != nil || cli.Code != 0 {
		t.Fatalf("inspect CLI: %v %+v", err, cli)
	}
	for _, scope := range dryRun.Results {
		for _, image := range scope.Items {
			if image == strings.TrimSpace(cli.Stdout) {
				t.Fatal("cleanup would remove Docker CLI")
			}
		}
	}
	if os.Getenv("DEPLOPACK_E2E_REPAIR") != "1" {
		return
	}
	active, err := dockercli.Run(ctx, time.Minute, "ps", "--filter", "name=deplo-buildkit-", "-q")
	if err != nil || active.Code != 0 || strings.TrimSpace(active.Stdout) != "" {
		t.Fatalf("refusing CLI removal while a build is active: %v %+v", err, active)
	}
	removed, err := dockercli.Run(ctx, time.Minute, "image", "rm", "docker:29-cli")
	if err != nil || removed.Code != 0 {
		t.Fatalf("remove CLI: %v %+v", err, removed)
	}
	if _, err := s.awaitDeplopackPreparation(ctx, spec, false, nil); err != nil {
		t.Fatalf("repair: %v", err)
	}
	restored, err := dockercli.Run(ctx, time.Minute, "image", "inspect", "docker:29-cli", "--format", "{{.Id}}")
	if err != nil || restored.Code != 0 || restored.Stdout != cli.Stdout {
		t.Fatalf("CLI was not restored: %v %+v", err, restored)
	}
	t.Log("cleanup preserves tooling; missing CLI repaired after successful preparation")
}
