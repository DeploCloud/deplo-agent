// Package pkgupdate moves one dependency of an app with its own package manager, inside a
// container whose only way out is the host's egress socket, which reaches the npm registries alone.
package pkgupdate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/capture"
)

// SocketPath is where the host mounts its egress socket; WorkDir holds the app's directory.
const (
	SocketPath = "/egress/egress.sock"
	WorkDir    = "/work"
)

// Spec is what the host hands the container on stdin.
type Spec struct {
	// npm | pnpm | yarn (classic) | yarn-berry
	Manager string `json:"manager"`
	Package string `json:"package"`
	Version string `json:"version"`
	// Where package.json lists it (dependencies, devDependencies, optionalDependencies) and the
	// range prefix it keeps; none when the app does not list it itself.
	Section string `json:"section,omitempty"`
	Prefix  string `json:"prefix,omitempty"`
	// Also pin the copies other packages pull in: overrides, pnpm.overrides or resolutions.
	Nested bool `json:"nested,omitempty"`
	// The package manager to install first ("pnpm@9", "@yarnpkg/cli-dist@4"); npm and yarn 1 ship with node.
	Tool string `json:"tool,omitempty"`
}

// toolBin is where an installed package manager lands; npm installs it, since npm honours the
// proxy. /tmp is noexec, so node runs it.
const toolBin = "/tmp/pm/bin/"

// Result is the one JSON object the container writes to stdout.
type Result struct {
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// Commands are the steps for one update: package.json first, then the lockfile alone. No
// package's install scripts ever run.
func Commands(s Spec) [][]string {
	var refresh []string
	var pinKey string
	switch s.Manager {
	case "npm":
		refresh, pinKey = []string{"npm", "install", "--package-lock-only", "--ignore-scripts"}, "overrides"
	case "pnpm":
		refresh, pinKey = []string{"node", toolBin + "pnpm", "install", "--lockfile-only", "--ignore-scripts"}, "pnpm.overrides"
	case "yarn":
		refresh, pinKey = []string{"yarn", "install", "--ignore-scripts", "--non-interactive"}, "resolutions"
	case "yarn-berry":
		refresh, pinKey = []string{"node", toolBin + "yarn", "install", "--mode=update-lockfile"}, "resolutions"
	default:
		return nil
	}
	set := func(key, value string) []string {
		return []string{"npm", "pkg", "set", key + "[" + s.Package + "]=" + value}
	}
	var steps [][]string
	if s.Tool != "" {
		steps = append(steps, []string{"npm", "install", "--global", "--prefix", "/tmp/pm", "--ignore-scripts", s.Tool})
	}
	if s.Section != "" {
		steps = append(steps, set(s.Section, s.Prefix+s.Version))
	}
	if s.Section == "" || s.Nested {
		pin := s.Version
		// npm refuses an override that disagrees with a direct dependency; "$name" follows it.
		if s.Manager == "npm" && s.Section != "" {
			pin = "$" + s.Package
		}
		steps = append(steps, set(pinKey, pin))
	}
	return append(steps, refresh)
}

// Main runs inside the container: it opens the loopback proxy, then the steps.
func Main(in io.Reader, out io.Writer) int {
	write := func(r Result) int {
		_ = json.NewEncoder(out).Encode(r)
		if r.OK {
			return 0
		}
		return 1
	}
	var spec Spec
	if err := json.NewDecoder(in).Decode(&spec); err != nil {
		return write(Result{Detail: "read the spec: " + err.Error()})
	}
	steps := Commands(spec)
	if len(steps) == 0 {
		return write(Result{Detail: "no package manager named " + spec.Manager})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Minute)
	defer cancel()
	addr, err := capture.ListenSplice(ctx, SocketPath)
	if err != nil {
		return write(Result{Detail: err.Error()})
	}
	proxy := "http://" + addr
	env := append(os.Environ(),
		"HTTP_PROXY="+proxy, "HTTPS_PROXY="+proxy, "http_proxy="+proxy, "https_proxy="+proxy,
		"npm_config_proxy="+proxy, "npm_config_https_proxy="+proxy,
		"YARN_HTTP_PROXY="+proxy, "YARN_HTTPS_PROXY="+proxy, "YARN_ENABLE_TELEMETRY=0",
		"HOME=/tmp", "npm_config_cache=/tmp/npm", "YARN_CACHE_FOLDER=/tmp/yarn", "CI=1",
		"COREPACK_HOME=/tmp/corepack", "COREPACK_ENABLE_DOWNLOAD_PROMPT=0",
		"npm_config_update_notifier=false", "npm_config_fund=false", "npm_config_audit=false")
	for _, step := range steps {
		cmd := exec.CommandContext(ctx, step[0], step[1:]...)
		cmd.Dir, cmd.Env = WorkDir, env
		b, err := cmd.CombinedOutput()
		if err != nil {
			return write(Result{Detail: fmt.Sprintf("%s (%v): %s", strings.Join(step[:min(3, len(step))], " "), err, tail(string(b), 1500))})
		}
	}
	return write(Result{OK: true})
}

func tail(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) > n {
		return s[len(s)-n:]
	}
	return s
}
