package server

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Why changing one environment variable used to rebuild every dependency.
//
// Nixpacks declares the whole build environment in a single block, and it puts
// that block ABOVE the install step:
//
//	ARG DATABASE_URL NODE_ENV PAYLOAD_SECRET RESEND_API_KEY …
//	ENV DATABASE_URL=$DATABASE_URL NODE_ENV=$NODE_ENV …
//	# install phase
//	COPY package.json bun.lock /app/.
//	RUN bun i                          <- 1.88 GB layer on a real app here
//	# build phase
//	COPY . /app/.
//	RUN bun run build
//
// A RUN step's cache key includes the environment it runs with, so editing ANY
// variable — including one the build never reads, like a mail API key used only
// at runtime — changed the environment of that install RUN, which re-ran it and
// forced its dependency layer to be rebuilt and re-EXPORTED. Measured here: a
// deploy where nothing but one runtime secret changed cost ~104 s against the
// ~40 s the same app pays for an actual code change.
//
// Deplo passes the app's whole environment to the build on purpose — that is what
// makes NEXT_PUBLIC_* / VITE_* work without the user having to know what a build
// arg is, and it is not up for negotiation (asking the user to tick "needed at
// build time" per variable is the kind of knob Deplo exists to not have). The
// placement is the problem, not the parity: declaring those variables one step
// LOWER, immediately before the build phase, leaves them just as available to the
// build while putting them out of the install step's cache key.
//
// Two properties of BuildKit make the move safe and worth it:
//   - ENV is metadata, so it creates no layer of its own; moving it costs nothing.
//   - COPY is keyed on CONTENT, not on the environment, so the source COPY that
//     follows the moved block stays cached too. Only the build RUN re-runs, which
//     is exactly the step that has to.
//
// Measured on the synthetic case: with the block on top, changing one secret
// re-ran install; with it moved, install reported CACHED and only the build ran.
//
// This only fires for a repository whose install step is provably a pure
// dependency install (the same gate as nixpacks_install_copy.go), and any
// variable the install step could legitimately read stays exactly where Nixpacks
// put it — see movableBuildEnv.

// installEnvPrefixes name the environment a package manager or language
// toolchain reads while INSTALLING, so a variable starting with one of these
// stays above the install step even though it is the app's own. The list is
// deliberately generous: a false positive costs one app one cache hit, while a
// false negative would change how dependencies resolve. Matched on the
// upper-cased name, so npm's lowercase `npm_config_*` form is covered too.
var installEnvPrefixes = []string{
	"NPM_", "NODE_", "YARN_", "PNPM_", "BUN_", "COREPACK_", "NIXPACKS_",
	"PIP_", "PYTHON", "POETRY_", "PIPENV_", "PDM_", "UV_",
	"COMPOSER_", "BUNDLE_", "GEM_", "CARGO_", "RUSTUP_",
	"GOPROXY", "GOPRIVATE", "GONOSUMDB", "GOSUMDB", "GOFLAGS",
	"MIX_", "HEX_", "MAVEN_", "GRADLE_", "JAVA_", "DOTNET_", "NUGET_",
	"SSL_", "CURL_", "GIT_", "NETRC",
}

// installEnvNames are the exact names outside those prefixes that still steer an
// install: the proxy set every fetcher honours, and the CI flag package managers
// read to pick their non-interactive behaviour.
var installEnvNames = map[string]bool{
	"CI":          true,
	"HTTP_PROXY":  true,
	"HTTPS_PROXY": true,
	"NO_PROXY":    true,
	"ALL_PROXY":   true,
	"FTP_PROXY":   true,
	"HOME":        true,
	"TMPDIR":      true,
	"TMP":         true,
	"TEMP":        true,
}

// installConfigFiles are the repo files that can interpolate an environment
// variable into the install itself — the registry/auth config every Node package
// manager reads (`//registry.npmjs.org/:_authToken=${NPM_TOKEN}`). Any name
// referenced from one of these stays above the install step whatever it is
// called, which is what covers private-registry tokens with app-specific names.
var installConfigFiles = []string{
	".npmrc", ".yarnrc", ".yarnrc.yml", ".pnpmrc", ".bunfig.toml", ".netrc",
}

// envRefPattern matches a shell-style variable reference, `$NAME` or `${NAME}`.
var envRefPattern = regexp.MustCompile(`\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?`)

// movableBuildEnv returns the subset of the app's build variables that may be
// declared below the install step: everything except the names that could change
// how dependencies are installed. Three sources are consulted — the toolchain
// prefixes/names above, every `$NAME` referenced by the repo's package-manager
// config, and every `$NAME` referenced by a custom install command the user set.
func movableBuildEnv(keys []string, buildDir, installCmd string) map[string]bool {
	sensitive := map[string]bool{}
	for _, name := range envRefsIn(installCmd) {
		sensitive[name] = true
	}
	for _, file := range installConfigFiles {
		body, err := os.ReadFile(filepath.Join(buildDir, file))
		if err != nil {
			continue
		}
		for _, name := range envRefsIn(string(body)) {
			sensitive[name] = true
		}
	}

	movable := make(map[string]bool, len(keys))
	for _, k := range keys {
		if sensitive[k] || installSensitiveName(k) {
			continue
		}
		movable[k] = true
	}
	return movable
}

// installSensitiveName reports whether a variable name is one an install step
// reads by convention.
func installSensitiveName(key string) bool {
	upper := strings.ToUpper(key)
	if installEnvNames[upper] {
		return true
	}
	for _, prefix := range installEnvPrefixes {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}

// envRefsIn returns every variable name referenced as $NAME / ${NAME} in text.
func envRefsIn(text string) []string {
	matches := envRefPattern.FindAllStringSubmatch(text, -1)
	names := make([]string, 0, len(matches))
	for _, m := range matches {
		names = append(names, m[1])
	}
	return names
}

// deferEnvBelowInstall rewrites the generated Dockerfile's lines so the movable
// variables are declared just before the phase that follows the install, and
// returns the names it moved.
//
// It recognises exactly the shape Nixpacks emits and leaves the file untouched
// on anything else — a bail costs the cache hit we were trying to win, which is
// simply today's behaviour, while a wrong rewrite would break the build. The
// single-FROM check matters in particular: ARG is scoped to a stage, so the same
// move across a multi-stage file would silently drop the variables.
func deferEnvBelowInstall(lines []string, movable map[string]bool) ([]string, []string) {
	if countLinesWithPrefix(lines, "FROM ") != 1 {
		return lines, nil
	}
	argIdx := firstLineWithPrefix(lines, "ARG ")
	if argIdx < 0 || argIdx+1 >= len(lines) || !strings.HasPrefix(lines[argIdx+1], "ENV ") {
		return lines, nil
	}
	names := strings.Fields(strings.TrimPrefix(lines[argIdx], "ARG "))
	pairs := strings.Fields(strings.TrimPrefix(lines[argIdx+1], "ENV "))
	if len(names) == 0 || len(names) != len(pairs) {
		return lines, nil
	}
	// Every entry must be the plain `ARG NAME` + `ENV NAME=$NAME` pair Nixpacks
	// writes. A default value or a literal would mean a different generator and a
	// different set of assumptions.
	for i, name := range names {
		if strings.ContainsAny(name, "=$") || pairs[i] != name+"=$"+name {
			return lines, nil
		}
	}

	anchor := installFollowerIndex(lines, argIdx)
	if anchor < 0 {
		return lines, nil
	}

	var stay, move []string
	for _, name := range names {
		if movable[name] {
			move = append(move, name)
		} else {
			stay = append(stay, name)
		}
	}
	if len(move) == 0 {
		return lines, nil
	}

	out := make([]string, 0, len(lines)+5)
	out = append(out, lines[:argIdx]...)
	if len(stay) > 0 {
		out = append(out, argLine(stay), envLine(stay))
	}
	out = append(out, lines[argIdx+2:anchor]...)
	out = append(out,
		"# app build variables, declared below the install step so changing one",
		"# cannot invalidate the installed dependencies",
		argLine(move), envLine(move), "")
	out = append(out, lines[anchor:]...)
	return out, move
}

// installFollowerIndex returns the index of the section comment that follows the
// install phase — the build phase when there is one, otherwise whatever Nixpacks
// emits next. That line is where the deferred block is inserted: after every
// install instruction, before everything that may need the app's variables.
func installFollowerIndex(lines []string, after int) int {
	install := -1
	for i := after; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "# install phase" {
			install = i
			break
		}
	}
	if install < 0 {
		return -1
	}
	for i := install + 1; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		// `# noop` marks an empty phase rather than starting a new one.
		if strings.HasPrefix(trimmed, "#") && trimmed != "# noop" {
			return i
		}
	}
	return -1
}

func argLine(names []string) string { return "ARG " + strings.Join(names, " ") }

func envLine(names []string) string {
	pairs := make([]string, 0, len(names))
	for _, name := range names {
		pairs = append(pairs, name+"=$"+name)
	}
	return "ENV " + strings.Join(pairs, " ")
}

func firstLineWithPrefix(lines []string, prefix string) int {
	for i, line := range lines {
		if strings.HasPrefix(line, prefix) {
			return i
		}
	}
	return -1
}

func countLinesWithPrefix(lines []string, prefix string) int {
	n := 0
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			n++
		}
	}
	return n
}

// deferAppEnvBelowInstall applies the rewrite to the Dockerfile Nixpacks
// generated at path, returning the variable names it moved (none when it left
// the file alone). Best-effort by design: every failure path keeps the generated
// file exactly as Nixpacks wrote it.
func deferAppEnvBelowInstall(path string, envKeys []string, buildDir, installCmd string) ([]string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Preserve the file's own line endings by splitting on "\n" and keeping any
	// trailing "\r" inside the line — the prefix checks below are unaffected and
	// an untouched line is written back byte-identical.
	lines := strings.Split(string(body), "\n")
	rewritten, moved := deferEnvBelowInstall(lines, movableBuildEnv(envKeys, buildDir, installCmd))
	if len(moved) == 0 {
		return nil, nil
	}
	if err := os.WriteFile(path, []byte(strings.Join(rewritten, "\n")), 0o644); err != nil {
		return nil, err
	}
	return moved, nil
}
