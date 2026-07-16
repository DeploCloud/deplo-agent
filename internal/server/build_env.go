package server

import (
	"sort"
	"strings"
)

// Build-time env, in parity with the runtime env: every build method makes the
// request's decrypted env (req.env — the same map the runtime stack gets)
// available to the BUILD, so build-time-inlined configuration (Next.js
// NEXT_PUBLIC_*, Vite VITE_*, CRA REACT_APP_*) works without the user knowing
// what a build arg is. The value-handling rule is uniform across methods:
//
//   - NAMES ride argv as bare flags (`--build-arg KEY` / `-e KEY` / `--env KEY`
//     — each tool resolves a bare name from its caller's process env), because
//     the deploy log echoes command lines and a value must never appear there.
//   - VALUES ride the spawned client's process env (StreamEnv/SpawnEnv), so
//     they never touch argv, the deploy log, or a shell string.
//
// Keys arrive off the wire (the control plane resolved them), so — same threat
// model as the railpack plan secrets — only identifier-shaped names are used;
// anything else is dropped rather than quoted (a legitimate env var name is
// always an identifier).

// buildEnvKeys returns the request env's identifier-shaped names, sorted for
// deterministic argv (and therefore deterministic docker layer caching).
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

// envKV renders "KEY=VALUE" process-env entries for the given keys — the ONLY
// place a build-env VALUE lives on the way into a build tool.
func envKV(env map[string]string, keys []string) []string {
	kv := make([]string, 0, len(keys))
	for _, k := range keys {
		kv = append(kv, k+"="+env[k])
	}
	return kv
}

// filterKeys returns the keys for which keep() is true, preserving order.
func filterKeys(keys []string, keep func(string) bool) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if keep(k) {
			out = append(out, k)
		}
	}
	return out
}

// declaredArgNames scans a Dockerfile body for the ARG names it declares —
// single-name (`ARG FOO`, `ARG FOO=default`) and BuildKit's multi-name
// (`ARG FOO BAR=x`) forms, in any stage. Line continuations are followed the
// way docker's parser does (a trailing `\` joins the next line).
func declaredArgNames(dockerfile string) map[string]struct{} {
	names := map[string]struct{}{}
	lines := strings.Split(dockerfile, "\n")
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		// Join continuations so `ARG FOO \` + `    BAR` declares both.
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

// dockerfileEnvKeys returns the env keys this Dockerfile declares as ARGs — the
// set that becomes bare `--build-arg KEY` flags. Passing only DECLARED names
// keeps builds warning-free (docker flags unconsumed build args) and means a
// custom Dockerfile opts into a variable simply by declaring `ARG NAME`, while
// the generated Dockerfile (which declares every resolved var) gets them all.
func dockerfileEnvKeys(dockerfile string, env map[string]string) []string {
	declared := declaredArgNames(dockerfile)
	return filterKeys(buildEnvKeys(env), func(k string) bool {
		_, ok := declared[k]
		return ok
	})
}

// appendBuildArgKeys appends one bare `--build-arg KEY` per key (docker reads a
// bare name's value from the client's process env — pass envKV alongside).
func appendBuildArgKeys(args []string, keys []string) []string {
	for _, k := range keys {
		args = append(args, "--build-arg", k)
	}
	return args
}
