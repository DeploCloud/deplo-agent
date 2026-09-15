package server

import (
	"sort"
	"strings"
)

func buildEnvKeys(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for k := range env {
		if validRailpackSecret.MatchString(k) {
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
	return dropReservedBuildEnv(filterKeys(buildEnvKeys(env), func(k string) bool {
		_, ok := declared[k]
		return ok
	}))
}

func appendBuildArgValues(args []string, vars map[string]string) []string {
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		args = append(args, "--build-arg", k+"="+vars[k])
	}
	return args
}

func appendBuildArgKeys(args []string, keys []string) []string {
	for _, k := range keys {
		args = append(args, "--build-arg", k)
	}
	return args
}
