package server

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"
)

// readPlan decodes a railpack plan keeping its numbers exact, so a rewrite changes only what it means to.
func readPlan(planPath string) (map[string]any, error) {
	b, err := os.ReadFile(planPath)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var plan map[string]any
	return plan, dec.Decode(&plan)
}

func writePlan(planPath string, plan map[string]any) error {
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(plan); err != nil {
		return err
	}
	return os.WriteFile(planPath, out.Bytes(), 0o644)
}

// tuneRailpackPlan makes the plan build faster without changing what it builds.
func tuneRailpackPlan(planPath string) error {
	plan, err := readPlan(planPath)
	if err != nil {
		return err
	}
	split := splitLockedCaches(plan)
	shared := shareRuntimeToolchain(plan)
	if pinned := pinRailpackImages(plan); !split && !shared && !pinned {
		return nil
	}
	return writePlan(planPath, plan)
}

func planStep(steps []any, name string) map[string]any {
	for _, s := range steps {
		if step, _ := s.(map[string]any); step != nil && step["name"] == name {
			return step
		}
	}
	return nil
}

var miseToolLine = regexp.MustCompile(`^\s*([A-Za-z0-9_-]+)\s*=\s*"([^"]+)"\s*$`)

// miseTools reads the [tools] of the mise config railpack generates for a step; nil when a line is not plain.
func miseTools(step map[string]any) map[string]string {
	assets, _ := step["assets"].(map[string]any)
	toml, _ := assets["generated-mise-toml"].(string)
	tools := map[string]string{}
	inTools := false
	for _, line := range strings.Split(toml, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") {
			inTools = trimmed == "[tools]"
			continue
		}
		if !inTools || trimmed == "" {
			continue
		}
		m := miseToolLine.FindStringSubmatch(line)
		if m == nil {
			return nil
		}
		tools[m[1]] = m[2]
	}
	return tools
}

// singleImage is the base image of a step built on nothing else.
func singleImage(step map[string]any) string {
	inputs, _ := step["inputs"].([]any)
	if len(inputs) != 1 {
		return ""
	}
	in, _ := inputs[0].(map[string]any)
	if len(in) != 1 {
		return ""
	}
	image, _ := in["image"].(string)
	return image
}

// shareRuntimeToolchain lets the runtime mise step (Java, Gleam) copy the toolchain the build's mise step
// installs, at the same versions on the same base, instead of installing it again beside it: a JDK
// downloaded twice at once, an Erlang that mise compiles built twice. mise then finds it installed.
func shareRuntimeToolchain(plan map[string]any) bool {
	steps, _ := plan["steps"].([]any)
	build, runtime := planStep(steps, "packages:mise"), planStep(steps, "packages:mise:runtime")
	if build == nil || runtime == nil {
		return false
	}
	base := singleImage(runtime)
	if base == "" || singleImage(build) != base {
		return false
	}
	have, want := miseTools(build), miseTools(runtime)
	if have == nil || len(want) == 0 {
		return false
	}
	include := make([]any, 0, len(want))
	for _, tool := range slices.Sorted(maps.Keys(want)) {
		if have[tool] != want[tool] {
			return false
		}
		include = append(include, "/mise/installs/"+tool)
	}
	runtime["inputs"] = append(runtime["inputs"].([]any), map[string]any{"step": "packages:mise", "include": include})
	return true
}

var cacheNameUnsafe = regexp.MustCompile(`[^A-Za-z0-9_-]+`)

// splitLockedCaches gives each later step its own copy of a locked cache an earlier step uses. Railpack
// shares one apt cache between the build and runtime apt steps, so the runtime one, whose small base
// image is ready first on a cold build, held the lock while the build's apt step waited.
func splitLockedCaches(plan map[string]any) bool {
	caches, _ := plan["caches"].(map[string]any)
	steps, _ := plan["steps"].([]any)
	used := map[string]bool{}
	changed := false
	for _, s := range steps {
		step, _ := s.(map[string]any)
		refs, _ := step["caches"].([]any)
		name, _ := step["name"].(string)
		for i, r := range refs {
			key, _ := r.(string)
			c, _ := caches[key].(map[string]any)
			if c == nil || c["type"] != "locked" {
				continue
			}
			if !used[key] {
				used[key] = true
				continue
			}
			own := key + "-" + cacheNameUnsafe.ReplaceAllString(name, "-")
			if _, taken := caches[own]; taken {
				continue
			}
			caches[own] = maps.Clone(c)
			refs[i] = own
			changed = true
		}
	}
	return changed
}

// pinRailpackImages names railpack's own base images by the digest Deplo tested (railpackImageDigests);
// an image the app's railpack config chose is left as it is.
func pinRailpackImages(node any) bool {
	changed := false
	switch v := node.(type) {
	case map[string]any:
		for k, child := range v {
			if ref, ok := child.(string); ok && k == "image" {
				if d := railpackImageDigests[ref]; d != "" {
					v[k] = ref + "@" + d
					changed = true
				}
				continue
			}
			changed = pinRailpackImages(child) || changed
		}
	case []any:
		for _, child := range v {
			changed = pinRailpackImages(child) || changed
		}
	}
	return changed
}
