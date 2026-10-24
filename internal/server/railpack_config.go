package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/DeploCloud/deplo-agent/internal/safepath"
)

const railpackSkipConfigName = "deplo-railpack.json"

// writeRailpackSkipConfig writes the repo's own railpack config (base, relative to buildDir) with
// the skipped steps emptied, under a Deplo name: --config-file replaces the repo's file, never merges.
func writeRailpackSkipConfig(buildDir, base string, skipInstall, skipBuild bool) (string, error) {
	cfg := map[string]any{}
	path, ok := safepath.Join(buildDir, orDefault(base, "railpack.json"))
	if !ok {
		return "", fmt.Errorf("the railpack config %q is outside the build folder", base)
	}
	switch raw, err := os.ReadFile(path); {
	case err == nil:
		if err := json.Unmarshal(jsoncToJSON(raw), &cfg); err != nil {
			return "", fmt.Errorf("%s is not valid JSON: %w", orDefault(base, "railpack.json"), err)
		}
	case os.IsNotExist(err) && base != "":
		return "", fmt.Errorf("%s, named by RAILPACK_CONFIG_FILE, is not in the build folder", base)
	case !os.IsNotExist(err):
		return "", fmt.Errorf("read the railpack config: %w", err)
	}
	steps, _ := cfg["steps"].(map[string]any)
	if steps == nil {
		steps = map[string]any{}
	}
	for name, skip := range map[string]bool{"install": skipInstall, "build": skipBuild} {
		if !skip {
			continue
		}
		step, _ := steps[name].(map[string]any)
		if step == nil {
			step = map[string]any{}
		}
		step["commands"] = []any{}
		steps[name] = step
	}
	cfg["steps"] = steps
	body, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	if err := writeBuildFile(filepath.Join(buildDir, railpackSkipConfigName), body); err != nil {
		return "", err
	}
	return railpackSkipConfigName, nil
}

// jsoncToJSON drops the comments and trailing commas railpack accepts in its config.
func jsoncToJSON(b []byte) []byte {
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		switch c := b[i]; {
		case c == '"':
			j := i + 1
			for j < len(b) && b[j] != '"' {
				if b[j] == '\\' {
					j++
				}
				j++
			}
			out = append(out, b[i:min(j+1, len(b))]...)
			i = j
		case c == '/' && i+1 < len(b) && b[i+1] == '/':
			for i+1 < len(b) && b[i+1] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(b) && b[i+1] == '*':
			end := bytes.Index(b[i+2:], []byte("*/"))
			if end < 0 {
				return out
			}
			i += end + 3
		case c == '}' || c == ']':
			trimmed := bytes.TrimRight(out, " \t\r\n")
			if len(trimmed) > 0 && trimmed[len(trimmed)-1] == ',' {
				out = trimmed[:len(trimmed)-1]
			}
			out = append(out, c)
		default:
			out = append(out, c)
		}
	}
	return out
}

// writeStaticfile asks railpack to serve the output as a single-page app, unless the repo
// already says how with its own Staticfile.
func writeStaticfile(buildDir, root string) (bool, error) {
	path := filepath.Join(buildDir, "Staticfile")
	if _, err := os.Lstat(path); err == nil {
		return false, nil
	}
	body := "index_fallback: true\n"
	if root != "" {
		body = "root: " + strconv.Quote(root) + "\n" + body
	}
	return true, writeBuildFile(path, []byte(body))
}

var gradleToolchain = regexp.MustCompile(`JavaLanguageVersion\.of\(\s*(\d{1,2})\s*\)`)

// javaFromGradle is the Java version a Gradle toolchain pins. Railpack does not read it and installs
// its default JDK, which such a build refuses (Spring's own project template pins one).
func javaFromGradle(buildDir string) string {
	for _, name := range []string{"build.gradle", "build.gradle.kts"} {
		f, err := os.Open(filepath.Join(buildDir, name))
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(f, 1<<20))
		_ = f.Close()
		if m := gradleToolchain.FindSubmatch(b); m != nil {
			return string(m[1])
		}
	}
	return ""
}

// railpackGradleStart is the jar lookup Railpack plans for Gradle; it only searches one folder down,
// so a single-module project (its jar in build/libs) starts java with no jar at all.
const railpackGradleStart = "ls -1 */build/libs/*jar"

// fixGradleStart widens that lookup to build/libs too; any other start command is left as it is.
func fixGradleStart(planPath string) (bool, error) {
	plan, err := readPlan(planPath)
	if err != nil {
		return false, err
	}
	deploy, _ := plan["deploy"].(map[string]any)
	start, _ := deploy["startCommand"].(string)
	if !strings.Contains(start, railpackGradleStart) {
		return false, nil
	}
	deploy["startCommand"] = strings.Replace(start, railpackGradleStart, "ls -1 build/libs/*jar */build/libs/*jar 2>/dev/null", 1)
	return true, writePlan(planPath, plan)
}

// The repository files that pin each tool; mise reads them over the version Railpack resolved.
var versionFiles = []struct {
	env, tool string
	files     []string
}{
	{"RAILPACK_PYTHON_VERSION", "Python", []string{".python-version"}},
	{"RAILPACK_NODE_VERSION", "Node", []string{".nvmrc", ".node-version"}},
	{"RAILPACK_RUBY_VERSION", "Ruby", []string{".ruby-version"}},
	{"RAILPACK_GO_VERSION", "Go", []string{".go-version"}},
	{"RAILPACK_JDK_VERSION", "Java", []string{".java-version"}},
}

// pinVersionFiles makes a version set for the app win over the repository's own version file in the build
// copy: the build's mise reads that file before the version Railpack planned. It returns what it changed.
func pinVersionFiles(buildDir string, pinned func(env string) string) []string {
	var changed []string
	for _, vf := range versionFiles {
		v := strings.TrimSpace(pinned(vf.env))
		if v == "" {
			continue
		}
		for _, name := range vf.files {
			path := filepath.Join(buildDir, name)
			fi, err := os.Lstat(path)
			if err != nil {
				continue
			}
			old := []byte("a link")
			if fi.Mode().IsRegular() {
				old, _ = os.ReadFile(path)
			}
			if strings.TrimSpace(string(old)) == v {
				continue
			}
			if err := writeBuildFile(path, []byte(v+"\n")); err == nil {
				changed = append(changed, fmt.Sprintf("%s %s over the repository's %s (%s)", vf.tool, v, name,
					orDefault(strings.TrimSpace(string(old)), "unreadable")))
			}
		}
	}
	return changed
}
