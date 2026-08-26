package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// bcrypt of "s3cret" for user "deplo-e2e" - a fixture for the throwaway registry
// below, which lives for the length of one test.
const e2eHtpasswd = "deplo-e2e:$2b$10$6aH1T6bAeVkmA5BIxIfV/e7RtcW6TBA91a2BKTqgEvGyDl5YfHG1e"

const (
	e2eRegistryPort = "5999"
	e2eRegistryHost = "127.0.0.1:" + e2eRegistryPort
	e2eRegistryName = "deplo-e2e-registry"
)

// End-to-end (real docker, real registry): a deploy's registry credentials must be a
// docker config the CLI actually accepts, and the same image must NOT pull without
// them. This is the whole feature - a credential the pull never reads is what this
// replaced.
func TestE2E_RegistryAuthPullsAnImageAnonymousPullCannot(t *testing.T) {
	ctx := context.Background()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}

	authDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(authDir, "htpasswd"), []byte(e2eHtpasswd+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, _ = dockercli.Run(ctx, 30*time.Second, "rm", "-f", e2eRegistryName)
	res, err := dockercli.Run(ctx, 3*time.Minute, "run", "-d", "--name", e2eRegistryName,
		"-p", "127.0.0.1:"+e2eRegistryPort+":5000",
		"-v", authDir+":/auth",
		"-e", "REGISTRY_AUTH=htpasswd",
		"-e", "REGISTRY_AUTH_HTPASSWD_REALM=deplo-e2e",
		"-e", "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd",
		"registry:2")
	if err != nil || res.Code != 0 {
		t.Skipf("cannot start the test registry (%v / %s)", err, res.Stderr)
	}
	defer func() {
		_, _ = dockercli.Run(context.Background(), 30*time.Second, "rm", "-f", e2eRegistryName)
	}()
	waitForPort(t, e2eRegistryHost)

	// The credentials exactly as the control plane sends them for a deploy.
	req := &pb.DeployRequest{
		DeployId: "dep_e2e_regauth",
		RegistryAuth: []*pb.RegistryAuth{
			{Host: e2eRegistryHost, Username: "deplo-e2e", Password: "s3cret"},
		},
	}
	cleanup, err := writeDockerConfig(req)
	if err != nil {
		t.Fatalf("writeDockerConfig: %v", err)
	}
	defer cleanup()

	// Something small that is already on any host that just started the registry.
	private := e2eRegistryHost + "/deplo/private:e2e"
	if r, err := dockercli.Run(ctx, 30*time.Second, "tag", "registry:2", private); err != nil || r.Code != 0 {
		t.Fatalf("tag: %v %s", err, r.Stderr)
	}
	defer func() {
		_, _ = dockercli.Run(context.Background(), 60*time.Second, "rmi", "-f", private)
	}()

	pushed := false
	for i := 0; i < 5 && !pushed; i++ {
		r, err := dockercli.RunEnv(ctx, 2*time.Minute, dockerConfigEnv(req), "push", private)
		if err == nil && r.Code == 0 {
			pushed = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !pushed {
		t.Fatal("could not push to the test registry")
	}
	// Force a real network pull for both attempts below.
	if r, err := dockercli.Run(ctx, 60*time.Second, "rmi", "-f", private); err != nil || r.Code != 0 {
		t.Fatalf("rmi: %v %s", err, r.Stderr)
	}

	// 1. Without the credentials the image is simply not pullable.
	anon := []string{"DOCKER_CONFIG=" + t.TempDir()}
	r, err := dockercli.RunEnv(ctx, 2*time.Minute, anon, "pull", private)
	if err == nil && r.Code == 0 {
		t.Fatal("the image pulled ANONYMOUSLY: the registry is not actually private, so this proves nothing")
	}
	if !strings.Contains(strings.ToLower(r.Stderr), "auth") {
		t.Logf("anonymous pull failed with: %s", strings.TrimSpace(r.Stderr))
	}

	// 2. With the deploy's own docker config it pulls. Same env deploy.go passes.
	r, err = dockercli.RunEnv(ctx, 2*time.Minute, dockerConfigEnv(req), "pull", private)
	if err != nil || r.Code != 0 {
		t.Fatalf("authenticated pull failed: %v code=%d %s", err, r.Code, r.Stderr)
	}
}

func waitForPort(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Skip(fmt.Sprintf("test registry never listened on %s", addr))
}
