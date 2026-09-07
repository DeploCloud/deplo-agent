package server

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"time"
)

// nixpacksOwnVariables returns the build variables NIXPACKS itself computes for a
// plan (NIXPACKS_SPA_OUTPUT_DIR, NODE_ENV, CI, …), minus the ones this build
// already feeds. `nixpacks build --out` only DECLARES them as `ARG name` with no
// default, so a `docker build` that does not pass them bakes an empty string:
// Caddy's `root * ../app/{$NIXPACKS_SPA_OUTPUT_DIR}` then served the repo root and
// every Vite SPA answered with its unbuilt index.html.
func nixpacksOwnVariables(ctx context.Context, bin, buildDir string, planFlags, spawnEnv, skip []string) (map[string]string, error) {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, append([]string{"plan", buildDir}, planFlags...)...)
	cmd.Env = append(cmd.Environ(), spawnEnv...)
	var out strings.Builder
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return planVariables([]byte(out.String()), skip)
}

// planVariables is the pure half: the plan's `variables` minus `skip`.
func planVariables(planJSON []byte, skip []string) (map[string]string, error) {
	var plan struct {
		Variables map[string]string `json:"variables"`
	}
	if err := json.Unmarshal(planJSON, &plan); err != nil {
		return nil, err
	}
	drop := map[string]bool{}
	for _, k := range skip {
		drop[k] = true
	}
	vars := map[string]string{}
	for k, v := range plan.Variables {
		// An app's own variable rides the bare `--build-arg KEY` + process-env path
		// so its VALUE never touches argv or the log. Never take it from here.
		if !drop[k] {
			vars[k] = v
		}
	}
	return vars, nil
}
