package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

func TestE2E_RepositoryCompose(t *testing.T) {
	if os.Getenv("DEPLO_COMPOSE_GIT_E2E") != "1" {
		t.Skip("enable only with an isolated Docker test app")
	}
	for _, composePath := range []string{"compose.yml", "deploy/compose.yml"} {
		t.Run(composePath, func(t *testing.T) { testRepositoryCompose(t, composePath) })
	}
}

func testRepositoryCompose(t *testing.T, composePath string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	workspace := t.TempDir()
	source := filepath.Join(workspace, "repo")
	if err := os.MkdirAll(filepath.Join(source, "deploy"), 0755); err != nil {
		t.Fatal(err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	slug := "compose-git-test-" + suffix
	image := "deplo-compose-git-test:" + suffix
	network := "deplo-team-composegit-" + suffix
	base := filepath.Dir(composePath)
	contextPath, bindPath, dockerfile, build := ".", "./payload", "Dockerfile", "build: ."
	if base != "." {
		contextPath, bindPath, dockerfile = "..", "../payload", "dockerfile"
		build = "build: {context: .., dockerfile: dockerfile}"
	}
	compose := fmt.Sprintf(`services:
  app:
    image: %s
    labels: {deplo.slug: %s}
    %s
    command: [sleep, "300"]
    environment:
      CONFIG_VALUE: ${CONFIG_VALUE}
    env_file: service.env
    volumes: ["%s:/bound:ro"]
  sidecar:
    image: busybox:1.37
    labels: {deplo.slug: %s}
    command: [sleep, "300"]
networks:
  default: {external: true, name: %s}
`, image, slug, build, bindPath, slug, network)
	for path, contents := range map[string]string{
		dockerfile:                         "FROM busybox:1.37\nCOPY payload /payload\n",
		".dockerignore":                    ".git\n",
		"payload":                          "first",
		composePath:                        compose,
		filepath.Join(base, ".env"):        "CONFIG_VALUE=repository\n",
		filepath.Join(base, "service.env"): "FILE_VALUE=from-repository-file\n",
	} {
		if err := os.WriteFile(filepath.Join(source, path), []byte(contents), 0644); err != nil {
			t.Fatal(err)
		}
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "git", args...)
		cmd.Dir = source
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	git("init", "-q")
	git("config", "user.email", "test@example.com")
	git("config", "user.name", "Compose test")
	git("add", ".")
	git("commit", "-qm", "first")
	firstSHA := git("rev-parse", "HEAD")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	daemon := dockercli.Command(ctx, "git", "daemon", "--reuseaddr", "--export-all", "--base-path="+workspace, "--listen=127.0.0.1", fmt.Sprintf("--port=%d", port))
	var daemonErrors bytes.Buffer
	daemon.Stderr = &daemonErrors
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = daemon.Cancel(); _ = daemon.Wait() }()
	address := fmt.Sprintf("127.0.0.1:%d", port)
	listening := false
	for attempt := 0; attempt < 50; attempt++ {
		if connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond); err == nil {
			connection.Close()
			listening = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !listening {
		t.Fatalf("git daemon did not start: %s", daemonErrors.String())
	}
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	t.Cleanup(func() {
		_, _ = s.DestroyStack(context.Background(), &pb.StackRef{Slug: slug, RemoveVolumes: true})
		_, _ = dockercli.Run(context.Background(), time.Minute, "image", "rm", image)
		_, _ = dockercli.Run(context.Background(), time.Minute, "network", "rm", network)
	})
	sourceRef := &pb.GitSource{Url: "git://" + address + "/repo", Commit: firstSHA}
	read, err := s.ReadRepoCompose(ctx, &pb.ReadRepoComposeRequest{Source: sourceRef, ComposePath: composePath})
	if err != nil || read.GetCommitSha() != firstSHA || read.GetComposeYaml() != compose {
		t.Fatalf("read: %v %+v", err, read)
	}
	deploy := func(sha, expected string) {
		t.Helper()
		read, err := s.ReadRepoCompose(ctx, &pb.ReadRepoComposeRequest{Source: &pb.GitSource{Url: sourceRef.Url, Commit: sha}, ComposePath: composePath})
		if err != nil {
			t.Fatal(err)
		}
		request := &pb.DeployRequest{DeployId: "dpl_test_" + suffix, Slug: slug, ProjectId: "prj_test_" + suffix, SourceKind: pb.SourceKind_SOURCE_KIND_COMPOSE,
			Git: &pb.GitSource{Url: sourceRef.Url, Commit: sha}, ComposePath: composePath, ComposeYaml: read.GetComposeYaml(), Network: network,
			Env: map[string]string{"CONFIG_VALUE": "app-value"}, ReadyTimeoutMs: 30000}
		var result *pb.DeployResult
		var logs []string
		s.runDeploy(ctx, request, &emitter{send: func(event *pb.DeployEvent) error {
			if line := event.GetLog(); line != nil {
				logs = append(logs, line.Text)
			}
			if terminal := event.GetResult(); terminal != nil {
				result = terminal
			}
			return nil
		}})
		if result == nil || !result.Ready || result.CommitSha != sha {
			t.Fatalf("deploy failed: %+v\n%s", result, strings.Join(logs, "\n"))
		}
		args := append(s.composeCtl(slug, "exec", "-T", "app"), "sh", "-c", `cat /payload; cat /bound; printf '%s:%s' "$CONFIG_VALUE" "$FILE_VALUE"`)
		output, err := dockercli.Run(ctx, time.Minute, args...)
		if err != nil || output.Code != 0 || strings.TrimSpace(output.Stdout) != expected+expected+"app-value:from-repository-file" {
			t.Fatalf("runtime files/env: %v %+v", err, output)
		}
		if strings.Contains(strings.Join(logs, "\n"), "from-repository-file") {
			t.Fatal("environment file value leaked into logs")
		}
	}
	deploy(firstSHA, "first")
	rerouted, err := s.Reroute(ctx, &pb.RerouteRequest{Slug: slug, ComposeYaml: compose, Env: map[string]string{"CONFIG_VALUE": "app-value"}, Network: network})
	if err != nil || !rerouted.GetOk() {
		t.Fatalf("reroute: %v %+v", err, rerouted)
	}
	if err := os.WriteFile(filepath.Join(source, "payload"), []byte("second"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(source, "next"), 0755); err != nil {
		t.Fatal(err)
	}
	for file, contents := range map[string]string{
		"next/Dockerfile.next": "FROM busybox:1.37\nCOPY payload /payload\n",
		"next/payload":         "second",
	} {
		if err := os.WriteFile(filepath.Join(source, file), []byte(contents), 0644); err != nil {
			t.Fatal(err)
		}
	}
	compose = strings.Replace(compose, build, "build: {context: "+filepath.Join(contextPath, "next")+", dockerfile: Dockerfile.next}", 1)
	if err := os.WriteFile(filepath.Join(source, composePath), []byte(compose), 0644); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "second")
	deploy(git("rev-parse", "HEAD"), "second")
	entries, err := os.ReadDir(filepath.Join(s.filesRoot(slug), "repositories"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("obsolete checkout not cleaned: %v %d", err, len(entries))
	}
	if result, err := s.DestroyStack(ctx, &pb.StackRef{Slug: slug, RemoveVolumes: true}); err != nil || !result.Ok {
		t.Fatalf("destroy: %v %+v", err, result)
	}
	if _, err := os.Stat(s.filesRoot(slug)); !os.IsNotExist(err) {
		t.Fatal("checkout retained after app deletion")
	}
}
