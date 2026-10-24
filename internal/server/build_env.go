package server

import (
	"sort"
	"strings"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A tenant variable reaches root host processes (the docker client, railpack), so it
// never takes a name that configures one: the loader, docker, BuildKit, or glibc's setuid list.
var reservedEnvPrefixes = []string{"LD_", "DOCKER_", "BUILDKIT_", "BUILDX_", "COMPOSE_", "XDG_"}

var reservedEnvNames = map[string]bool{
	"HOME": true, "PATH": true, dockercli.MarkerEnv: true,
	"GCONV_PATH": true, "GETCONF_DIR": true, "GLIBC_TUNABLES": true, "HOSTALIASES": true,
	"LOCALDOMAIN": true, "LOCPATH": true, "MALLOC_TRACE": true, "NIS_PATH": true, "NLSPATH": true,
	"RESOLV_HOST_CONF": true, "RES_OPTIONS": true, "TMPDIR": true, "TZDIR": true,
}

func reservedEnvName(k string) bool {
	if reservedEnvNames[k] {
		return true
	}
	for _, p := range reservedEnvPrefixes {
		if strings.HasPrefix(k, p) {
			return true
		}
	}
	return false
}

func buildEnvKeys(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for k := range env {
		if validRailpackSecret.MatchString(k) && !reservedEnvName(k) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func envKV(env map[string]string, keys []string) []string {
	kv := make([]string, 0, len(keys))
	for _, k := range keys {
		kv = append(kv, k+"="+env[k])
	}
	return kv
}

func filterKeys(keys []string, keep func(string) bool) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if keep(k) {
			out = append(out, k)
		}
	}
	return out
}

func declaredArgNames(dockerfile string) map[string]struct{} {
	names := map[string]struct{}{}
	lines := strings.Split(dockerfile, "\n")
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		for strings.HasSuffix(line, "\\") && i+1 < len(lines) {
			i++
			line = strings.TrimSuffix(line, "\\") + " " + strings.TrimSpace(lines[i])
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || !strings.EqualFold(fields[0], "ARG") {
			continue
		}
		for _, f := range fields[1:] {
			name := f
			if eq := strings.IndexByte(f, '='); eq >= 0 {
				name = f[:eq]
			}
			if validRailpackSecret.MatchString(name) {
				names[name] = struct{}{}
			}
		}
	}
	return names
}

func dockerfileEnvKeys(dockerfile string, env map[string]string) []string {
	declared := declaredArgNames(dockerfile)
	return filterKeys(buildEnvKeys(env), func(k string) bool {
		_, ok := declared[k]
		return ok
	})
}

func appendBuildArgKeys(args []string, keys []string) []string {
	for _, k := range keys {
		args = append(args, "--build-arg", k)
	}
	return args
}
