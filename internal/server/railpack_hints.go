package server

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// railpackInfo is the part of railpack-info.json the agent reads.
type railpackInfo struct {
	ResolvedPackages map[string]struct {
		ResolvedVersion string `json:"resolvedVersion"`
	} `json:"resolvedPackages"`
	DetectedProviders []string `json:"detectedProviders"`
	Logs              []struct {
		Level string
		Msg   string
	} `json:"logs"`
}

func readRailpackInfo(path string) railpackInfo {
	var info railpackInfo
	if b, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(b, &info)
	}
	return info
}

func (i railpackInfo) errors() []string {
	var out []string
	for _, l := range i.Logs {
		if strings.EqualFold(l.Level, "error") && strings.TrimSpace(l.Msg) != "" {
			out = append(out, strings.TrimSpace(l.Msg))
		}
	}
	return out
}

func (i railpackInfo) major(pkg string) string {
	return majorVersion(i.ResolvedPackages[pkg].ResolvedVersion, "")
}

// railpackTransientExit is prepare's exit code for a failure worth retrying (EX_TEMPFAIL).
const railpackTransientExit = 75

// prepareFailure is the deploy error for a failed `railpack prepare`: what railpack said, then what to change.
func prepareFailure(code int, info railpackInfo, haveToken bool, buildDir string) string {
	errs := info.errors()
	text := strings.Join(errs, "\n")
	hint := prepareHint(text, haveToken)
	if strings.Contains(strings.ToLower(text), "could not determine how to build the app") {
		if h := undetectedHint(buildDir); h != "" {
			hint = h
		}
	}
	if hint == "" && code == railpackTransientExit {
		hint = "Railpack could not download what it needs, usually a passing network problem: deploy again."
	}
	msg := fmt.Sprintf("railpack prepare failed (exit %d)", code)
	if len(errs) > 0 {
		msg = headline(errs[0])
	}
	if hint != "" {
		msg += " " + hint
	}
	return msg
}

var versionErrors = []*regexp.Regexp{
	regexp.MustCompile("failed to resolve version (?P<version>\\S+) of (?P<pkg>[a-z0-9_-]+)"),
	regexp.MustCompile("no version available for (?P<pkg>[a-z0-9_-]+) (?P<version>\\S+)"),
	regexp.MustCompile("failed to get latest version for package `(?P<pkg>[a-z0-9_-]+)` after trying versions: (?P<version>[^:]+)"),
}

// The app variable each mise package's version is pinned with.
var railpackVersionVars = map[string]string{
	"python": "RAILPACK_PYTHON_VERSION", "go": "RAILPACK_GO_VERSION", "ruby": "RAILPACK_RUBY_VERSION",
	"bun": "RAILPACK_BUN_VERSION", "deno": "RAILPACK_DENO_VERSION", "java": "RAILPACK_JDK_VERSION",
	"rust": "RAILPACK_RUST_VERSION", "elixir": "RAILPACK_ELIXIR_VERSION", "erlang": "RAILPACK_ERLANG_VERSION",
	"dotnet": "RAILPACK_DOTNET_VERSION", "gradle": "RAILPACK_GRADLE_VERSION",
}

func prepareHint(text string, haveToken bool) string {
	low := strings.ToLower(text)
	if githubRateLimited(low) {
		return githubHint(haveToken)
	}
	for _, re := range versionErrors {
		if m := re.FindStringSubmatch(text); m != nil {
			version, _, _ := strings.Cut(m[re.SubexpIndex("version")], ",")
			return versionHint(m[re.SubexpIndex("pkg")], strings.TrimSpace(version))
		}
	}
	switch {
	case strings.Contains(low, "no start command detected"):
		return "Set a Start command in Settings -> Build & output."
	case strings.Contains(low, "could not determine how to build the app"):
		return "Railpack does not recognise this folder: set the Root directory, or add a Dockerfile."
	}
	return ""
}

func versionHint(pkg, version string) string {
	switch pkg {
	case "node":
		return fmt.Sprintf("Node.js %s is not available: pin a released Node.js version in Settings -> Build & output.", version)
	case "php":
		return fmt.Sprintf("PHP %s is not available: require a released PHP in composer.json, for example \"php\": \"^8.3\".", version)
	}
	if v := railpackVersionVars[pkg]; v != "" {
		return fmt.Sprintf("%s %s is not available: set %s to a released version in the app's variables.", pkg, version, v)
	}
	return fmt.Sprintf("%s %s is not available to Railpack.", pkg, version)
}

func githubRateLimited(low string) bool {
	return strings.Contains(low, "github") && (strings.Contains(low, "rate limit") || strings.Contains(low, "403"))
}

func githubHint(haveToken bool) string {
	if haveToken {
		return "GitHub refused a download with its rate limit: deploy again in a few minutes."
	}
	return "GitHub refused a download with its rate limit. Connecting a GitHub App to the team lets Deplo " +
		"sign these lookups; otherwise deploy again in an hour."
}

// buildHint names a known cause in a failed build's last lines, or "". nodeMajor is the Node
// railpack resolved ("" for any other build).
func buildHint(lines []string, haveToken bool, nodeMajor string) string {
	low := strings.ToLower(strings.Join(lines, "\n"))
	switch {
	case githubRateLimited(low):
		return githubHint(haveToken)
	case nodeMajor == "24" && strings.Contains(low, "gyp err!"):
		return "A native module failed to compile under Node 24: pin Node 22 in Settings -> Build & output."
	case strings.Contains(low, "no precompiled python"):
		return "There is no ready-made build of that Python version: set RAILPACK_PYTHON_VERSION to a " +
			"released version (for example 3.13) in the app's variables."
	}
	if strings.Contains(low, `module '"@prisma/client"' has no exported member`) || strings.Contains(low, "@prisma/client did not initialize yet") {
		return "Prisma's client was not generated before the build: put `npx prisma generate && ` in front of the Build command in Settings -> Build & output."
	}
	if strings.Contains(low, "your lockfile needs to be updated") || strings.Contains(low, "err_pnpm_outdated_lockfile") ||
		strings.Contains(low, "can only install packages when your package.json and package-lock.json") {
		return "The repository's lockfile does not match its package.json: commit an updated lockfile, or set the " +
			"Install command (npm install, yarn install or pnpm install) in Settings -> Build & output."
	}
	if exts := missingPHPExtensions(strings.Join(lines, "\n")); len(exts) > 0 {
		return "A package needs PHP's " + strings.Join(exts, ", ") + " extension: set RAILPACK_PHP_EXTENSIONS=" +
			strings.Join(exts, ",") + " in the app's variables."
	}
	if want, got := nodeTooOld(strings.Join(lines, "\n")); want != "" {
		return "The code needs Node " + want + " and the build used Node " + got +
			": pick a newer Node.js version in Settings -> Build & output."
	}
	if m := copyNotFound.FindStringSubmatch(strings.Join(lines, "\n")); m != nil {
		return "The Dockerfile copies " + strings.TrimPrefix(m[1], "/") + ", which is not in the repository " +
			"(another tool makes it before the build, or .dockerignore leaves it out): build it in the Dockerfile, " +
			"or pick Railpack in Settings -> Build & output to build from the source."
	}
	return ""
}

// What a framework or package manager prints when the build's Node is older than the code needs.
var nodeTooOldErrors = []*regexp.Regexp{
	regexp.MustCompile(`You are using Node\.js (?P<got>[\d.]+)\. For Next\.js, Node\.js version "(?P<want>[^"]+)" is required`),
	regexp.MustCompile(`You are using Node\.js (?P<got>[\d.]+)\. Vite requires Node\.js version (?P<want>[\d.+ or]+[\d+])`),
	regexp.MustCompile(`The engine "node" is incompatible with this module\. Expected version "(?P<want>[^"]+)"\. Got "(?P<got>[^"]+)"`),
	regexp.MustCompile(`Your Node version is incompatible with [^\n]*(?:\n[^\n]*){0,3}?Expected version: (?P<want>[^\n]+)\n[^\n]*Got: v?(?P<got>[\d.]+)`),
}

func nodeTooOld(text string) (want, got string) {
	for _, re := range nodeTooOldErrors {
		if m := re.FindStringSubmatch(text); m != nil {
			return m[re.SubexpIndex("want")], m[re.SubexpIndex("got")]
		}
	}
	return "", ""
}

var phpExtMissing = regexp.MustCompile(`requires ext-([a-z0-9_]+) \S* -> it is missing from your system`)

// missingPHPExtensions lists the extensions composer refused the install over, each once.
func missingPHPExtensions(text string) []string {
	var out []string
	for _, m := range phpExtMissing.FindAllStringSubmatch(text, -1) {
		if !slices.Contains(out, m[1]) {
			out = append(out, m[1])
		}
	}
	return out
}

var copyNotFound = regexp.MustCompile(`failed to calculate checksum of ref [^ ]+: "([^"]+)": not found`)

// undetectedHint names what hides a project Railpack would otherwise recognise.
func undetectedHint(buildDir string) string {
	has := func(name string) bool { _, err := os.Stat(filepath.Join(buildDir, name)); return err == nil }
	switch {
	case dockerignoreHidesAll(buildDir):
		return "Its .dockerignore leaves out every file (`*`), so Railpack sees an empty folder: remove that line to let it read the code."
	case (has("build.gradle") || has("build.gradle.kts") || has("settings.gradle") || has("settings.gradle.kts")) && !has("gradlew"):
		return "Railpack builds a Gradle project through its wrapper: commit gradlew and the gradle/wrapper folder (`gradle wrapper` writes them)."
	}
	return ""
}

// dockerignoreHidesAll reports a .dockerignore that leaves out every file, as Quarkus' own does for its prebuilt jars.
func dockerignoreHidesAll(buildDir string) bool {
	b, err := os.ReadFile(filepath.Join(buildDir, ".dockerignore"))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(b), "\n") {
		if l := strings.TrimSpace(line); l == "*" || l == "**" || l == "/*" {
			return true
		}
	}
	return false
}

// headline is the first sentence of a multi-line tool error, ending in a period.
func headline(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	if i := strings.Index(line, ". "); i >= 0 {
		line = line[:i]
	}
	if r := []rune(line); len(r) > 300 {
		line = string(r[:300])
	}
	return strings.TrimRight(strings.TrimSpace(line), ".") + "."
}

// lineTail keeps the last lines of a command's output for the failure hints.
type lineTail struct {
	lines []string
	max   int
}

func (t *lineTail) add(line string) {
	t.lines = append(t.lines, line)
	if len(t.lines) > t.max {
		t.lines = t.lines[len(t.lines)-t.max:]
	}
}
