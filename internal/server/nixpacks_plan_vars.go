package server

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

func nixpacksOwnVariables(ctx context.Context, bin, buildDir string, planFlags, spawnEnv, skip []string) (map[string]string, error) {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := dockercli.Command(cctx, bin, append([]string{"plan", buildDir}, planFlags...)...)
	cmd.Env = append(cmd.Environ(), spawnEnv...)
	var out strings.Builder
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return planVariables([]byte(out.String()), skip)
}

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
		if !drop[k] {
			vars[k] = v
		}
	}
	return vars, nil
}
