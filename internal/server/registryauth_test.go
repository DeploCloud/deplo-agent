package server

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The credentials the control plane decrypts become a docker config the CLI reads
// through DOCKER_CONFIG, and they leave nothing on the host once the deploy ends.

func TestWriteDockerConfigWritesAuthsAndCleansUp(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	req := &pb.DeployRequest{
		DeployId: "dep_abc",
		RegistryAuth: []*pb.RegistryAuth{
			{Host: "ghcr.io", Username: "octocat", Password: "ghp_secret"},
		},
	}
	cleanup, err := writeDockerConfig(req)
	if err != nil {
		t.Fatalf("writeDockerConfig: %v", err)
	}
	dir := dockerConfigDir("dep_abc")
	path := filepath.Join(dir, "config.json")

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat config.json: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("config.json mode = %v, want 0600 (it holds a plaintext token)", info.Mode().Perm())
	}

	var parsed struct {
		Auths map[string]struct {
			Auth string `json:"auth"`
		} `json:"auths"`
	}
	blob, _ := os.ReadFile(path)
	if err := json.Unmarshal(blob, &parsed); err != nil {
		t.Fatalf("config.json is not valid docker config: %v", err)
	}
	want := base64.StdEncoding.EncodeToString([]byte("octocat:ghp_secret"))
	if got := parsed.Auths["ghcr.io"].Auth; got != want {
		t.Fatalf("auths[ghcr.io].auth = %q, want %q", got, want)
	}

	if env := dockerConfigEnv(req); len(env) != 1 || env[0] != "DOCKER_CONFIG="+dir {
		t.Fatalf("dockerConfigEnv = %v, want DOCKER_CONFIG=%s", env, dir)
	}

	cleanup()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("credentials survived the deploy at %s", dir)
	}
}

// A team with no registry must not get a DOCKER_CONFIG at all: an empty config dir
// would MASK the host's own ~/.docker/config.json instead of adding to it.
func TestWriteDockerConfigIsANoOpWithoutCredentials(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	req := &pb.DeployRequest{DeployId: "dep_none"}
	cleanup, err := writeDockerConfig(req)
	if err != nil {
		t.Fatalf("writeDockerConfig: %v", err)
	}
	defer cleanup()
	if _, err := os.Stat(dockerConfigDir("dep_none")); !os.IsNotExist(err) {
		t.Fatal("wrote a docker config for a deploy that has no credentials")
	}
	if env := dockerConfigEnv(req); env != nil {
		t.Fatalf("dockerConfigEnv = %v, want nil", env)
	}
}

// The deploy id keys a path, so it may never climb out of the temp dir.
func TestDockerConfigDirRefusesAPathClimb(t *testing.T) {
	dir := dockerConfigDir("../../etc/deplo")
	if filepath.Dir(dir) != os.TempDir() {
		t.Fatalf("dir = %q, want a direct child of %q", dir, os.TempDir())
	}
}
