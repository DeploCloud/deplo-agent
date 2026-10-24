package server

import (
	"slices"
	"strings"
	"testing"
)

// buildEnvKeys must sort deterministically and drop non-identifier names (keys arrive off the wire - same threat model as railpack plan secrets).
func TestBuildEnvKeys(t *testing.T) {
	got := buildEnvKeys(map[string]string{
		"NEXT_PUBLIC_API": "x",
		"A":               "1",
		"_UNDER":          "ok",
		"BAD-DASH":        "drop",
		"HAS SPACE":       "drop",
		"1LEADING":        "drop",
		"x;rm":            "drop",
		"":                "drop",
	})
	want := []string{"A", "NEXT_PUBLIC_API", "_UNDER"}
	if !slices.Equal(got, want) {
		t.Fatalf("buildEnvKeys = %v; want %v", got, want)
	}
}

// A tenant variable rides into root host processes, so a name that configures one never passes.
func TestBuildEnvKeysDropsReservedNames(t *testing.T) {
	env := map[string]string{"DOCKERFILE_FLAVOR": "kept", "APP_HOME": "kept"}
	for _, k := range []string{"HOME", "PATH", "LD_PRELOAD", "LD_AUDIT", "DOCKER_CONFIG", "DOCKER_CLI_PLUGIN_EXTRA_DIRS",
		"BUILDKIT_CACHE_MOUNT_NS", "BUILDX_CONFIG", "COMPOSE_FILE", "XDG_CONFIG_HOME", jobMarkerEnv, "GLIBC_TUNABLES"} {
		env[k] = "drop"
	}
	got := buildEnvKeys(env)
	want := []string{"APP_HOME", "DOCKERFILE_FLAVOR"}
	if !slices.Equal(got, want) {
		t.Fatalf("buildEnvKeys = %v; want %v", got, want)
	}
}

// declaredArgNames must catch single-name, =default, BuildKit multi-name and continuation forms, in any stage, case-insensitively, and only those.
func TestDeclaredArgNames(t *testing.T) {
	df := `FROM node:20 AS builder
ARG NEXT_PUBLIC_API
arg lower_case=default
ARG MULTI_A MULTI_B=x
ARG CONT_A \
    CONT_B
ENV NOT_AN_ARG=1
RUN echo "ARG NOT_THIS_ONE_EITHER" # inside a RUN, but the line starts with RUN
FROM nginx:alpine
ARG SECOND_STAGE
`
	got := declaredArgNames(df)
	for _, want := range []string{"NEXT_PUBLIC_API", "lower_case", "MULTI_A", "MULTI_B", "CONT_A", "CONT_B", "SECOND_STAGE"} {
		if _, ok := got[want]; !ok {
			t.Errorf("declaredArgNames missing %q (got %v)", want, got)
		}
	}
	for _, no := range []string{"NOT_AN_ARG", "NOT_THIS_ONE_EITHER", "default", "x"} {
		if _, ok := got[no]; ok {
			t.Errorf("declaredArgNames wrongly includes %q", no)
		}
	}
}

// dockerfileEnvKeys is the intersection: only env keys the Dockerfile declares as ARG become build args - an undeclared var is never passed (no unconsumed- build-arg warnings), a declared-but-absent ARG gets nothing injected.
func TestDockerfileEnvKeys(t *testing.T) {
	df := "FROM node:20\nARG NEXT_PUBLIC_API\nARG UNSET_BY_USER\n"
	env := map[string]string{"NEXT_PUBLIC_API": "x", "SECRET_ONLY_RUNTIME": "y"}
	got := dockerfileEnvKeys(df, env)
	if !slices.Equal(got, []string{"NEXT_PUBLIC_API"}) {
		t.Fatalf("dockerfileEnvKeys = %v; want [NEXT_PUBLIC_API]", got)
	}
}

// envKV pairs values to the selected keys only - the single place a value lives.
func TestEnvKV(t *testing.T) {
	got := envKV(map[string]string{"A": "1", "B": "2"}, []string{"B"})
	if !slices.Equal(got, []string{"B=2"}) {
		t.Fatalf("envKV = %v; want [B=2]", got)
	}
}

// appendBuildArgKeys must emit bare names (values NEVER ride argv - command lines are echoed into the user-visible deploy log).
func TestAppendBuildArgKeysBareNames(t *testing.T) {
	args := appendBuildArgKeys([]string{"build"}, []string{"NEXT_PUBLIC_API"})
	if !slices.Equal(args, []string{"build", "--build-arg", "NEXT_PUBLIC_API"}) {
		t.Fatalf("args = %v", args)
	}
	if strings.Contains(strings.Join(args, " "), "=") {
		t.Fatalf("a value leaked onto argv: %v", args)
	}
}

// The tag is moved rather than bumped, so the version proves nothing about which binary a host runs - the capability is what a rollout verifies against.
func TestCapabilities_advertisesEnvNotBaked(t *testing.T) {
	if !containsString(Capabilities, "build.env-not-baked") {
		t.Error("Capabilities must advertise \"build.env-not-baked\"")
	}
}
