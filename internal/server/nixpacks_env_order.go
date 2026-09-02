package server

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Why changing one environment variable used to rebuild every dependency.

// installEnvPrefixes name the environment a package manager or language toolchain reads
// while INSTALLING, so a variable starting with one of these stays above the install
// step even though it is the app's own.
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

// installConfigFiles are the repo files that can interpolate an environment variable
// into the install itself - the registry/auth config every Node package manager reads
// (`//registry.npmjs.org/:_authToken=${NPM_TOKEN}`).
var installConfigFiles = []string{
	".npmrc", ".yarnrc", ".yarnrc.yml", ".pnpmrc", ".bunfig.toml", ".netrc",
}

// envRefPattern matches a shell-style variable reference, `$NAME` or `${NAME}`.
var envRefPattern = regexp.MustCompile(`\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?`)

// movableBuildEnv returns the subset of the app's build variables that may be declared
// below the install step: everything except the names that could change how
// dependencies are installed.
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
// variables are declared just before the phase that follows the install, and returns
// the names it moved.
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
// install phase - the build phase when there is one, otherwise whatever Nixpacks emits
// next.
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

// deferAppEnvBelowInstall applies the rewrite to the Dockerfile Nixpacks generated at
// path, returning the variable names it moved (none when it left the file alone).
func deferAppEnvBelowInstall(path string, envKeys []string, buildDir, installCmd string) ([]string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Preserve the file's own line endings by splitting on "\n" and keeping any
	// trailing "\r" inside the line - the prefix checks below are unaffected and
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

// stripAppEnv drops the app's own variables from every `ENV KEY=$KEY` line Nixpacks
// wrote. The matching `ARG` still puts the value in each RUN's environment, so the
// build is unchanged while the image stops carrying it in its config.
func stripAppEnv(lines []string, app map[string]bool) ([]string, []string) {
	var dropped []string
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		body, ok := strings.CutPrefix(line, "ENV ")
		if !ok {
			out = append(out, line)
			continue
		}
		// Only the plain `KEY=$KEY` form Nixpacks generates. A literal or an
		// interpolation (`ENV NIXPACKS_PATH=/app/bin:$NIXPACKS_PATH`) is a value
		// the build itself needs, so the line is left byte-identical.
		pairs := strings.Fields(body)
		keep, gone := make([]string, 0, len(pairs)), []string(nil)
		plain := len(pairs) > 0
		for _, pair := range pairs {
			name, _, _ := strings.Cut(pair, "=")
			if name == "" || pair != name+"=$"+name {
				plain = false
				break
			}
			if app[name] {
				gone = append(gone, name)
			} else {
				keep = append(keep, name)
			}
		}
		switch {
		case !plain || len(gone) == 0:
			out = append(out, line)
		case len(keep) > 0:
			out = append(out, envLine(keep))
		}
		dropped = append(dropped, gone...)
	}
	return out, dropped
}

// stripAppEnvFromDockerfile applies stripAppEnv to the Dockerfile at path. Run it
// AFTER deferAppEnvBelowInstall, which needs the `ARG`/`ENV` pair still intact.
func stripAppEnvFromDockerfile(path string, envKeys []string) ([]string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	app := make(map[string]bool, len(envKeys))
	for _, k := range envKeys {
		app[k] = true
	}
	rewritten, dropped := stripAppEnv(strings.Split(string(body), "\n"), app)
	if len(dropped) == 0 {
		return nil, nil
	}
	if err := os.WriteFile(path, []byte(strings.Join(rewritten, "\n")), 0o644); err != nil {
		return nil, err
	}
	return dropped, nil
}
