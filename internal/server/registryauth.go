package server

// https://deplo.build/docs/guides/server/container-registries

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// safeToken keeps a control-plane id usable as a single path component.
func safeToken(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, s)
}

// dockerConfigDir holds THIS deploy's registry credentials. Derived from the deploy
// id so every docker invocation can find it without threading a path around.
func dockerConfigDir(deployID string) string {
	return filepath.Join(os.TempDir(), "deplo-dockercfg-"+safeToken(deployID))
}

// dockerConfigEnv points the docker CLI at this deploy's credentials. Nil when the
// team connected no registry, which leaves the host's own config untouched.
func dockerConfigEnv(req *pb.DeployRequest) []string {
	if len(req.GetRegistryAuth()) == 0 {
		return nil
	}
	return []string{"DOCKER_CONFIG=" + dockerConfigDir(req.GetDeployId())}
}

// writeDockerConfig materialises the deploy's credentials as a 0600 config.json.
// The returned cleanup removes it, and is a no-op when there was nothing to write.
func writeDockerConfig(req *pb.DeployRequest) (func(), error) {
	auths := req.GetRegistryAuth()
	if len(auths) == 0 {
		return func() {}, nil
	}
	entries := map[string]map[string]string{}
	for _, a := range auths {
		host := strings.TrimSpace(a.GetHost())
		if host == "" {
			continue
		}
		entries[host] = map[string]string{
			"auth": base64.StdEncoding.EncodeToString(
				[]byte(a.GetUsername() + ":" + a.GetPassword()),
			),
		}
	}
	if len(entries) == 0 {
		return func() {}, nil
	}
	blob, err := json.Marshal(map[string]any{"auths": entries})
	if err != nil {
		return nil, err
	}
	dir := dockerConfigDir(req.GetDeployId())
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	if err := os.WriteFile(filepath.Join(dir, "config.json"), blob, 0o600); err != nil {
		cleanup()
		return nil, err
	}
	return cleanup, nil
}

// sweepDockerConfigs removes credential dirs an agent that died mid-deploy left in
// place. Called at startup; a live deploy's own dir is minted after this runs.
func sweepDockerConfigs() {
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "deplo-dockercfg-*"))
	if err != nil {
		return
	}
	for _, m := range matches {
		_ = os.RemoveAll(m)
	}
}
