package server

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// readPlanSecrets returns the plan's declared secrets and ok=true on a good read, and ok=false (so the caller falls back) only when the plan is missing/unparseable.
func TestReadPlanSecrets(t *testing.T) {
	dir := t.TempDir()

	good := filepath.Join(dir, "railpack-plan.json")
	if err := os.WriteFile(good, []byte(`{"secrets":["RAILPACK_NODE_VERSION","RAILPACK_BUILD_CMD"],"steps":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, ok := readPlanSecrets(good); !ok || !slices.Equal(got, []string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD"}) {
		t.Fatalf("good plan: got %v ok=%v", got, ok)
	}

	none := filepath.Join(dir, "none.json")
	if err := os.WriteFile(none, []byte(`{"steps":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, ok := readPlanSecrets(none); !ok || len(got) != 0 {
		t.Fatalf("no-secrets plan: got %v ok=%v; want empty,true", got, ok)
	}

	if got, ok := readPlanSecrets(filepath.Join(dir, "absent.json")); ok || got != nil {
		t.Fatalf("absent plan: got %v ok=%v; want nil,false", got, ok)
	}
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{not json`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok := readPlanSecrets(bad); ok {
		t.Fatalf("bad json: want ok=false")
	}
}

// sanitizeSecretNames keeps identifier-shaped names and drops anything an untrusted plan might use to smuggle buildctl CSV attributes.
func TestSanitizeSecretNames(t *testing.T) {
	in := []string{
		"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD", "_ok", "a1",
		"x; rm -rf / #",
		"foo,src=/etc/passwd",
		"has space", "-flag", "", "WITH-DASH", "uni¢ode", "HOME", "DOCKER_HOST",
	}
	got := sanitizeSecretNames(in)
	want := []string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD", "_ok", "a1"}
	if !slices.Equal(got, want) {
		t.Fatalf("sanitizeSecretNames = %v; want %v", got, want)
	}
	if in[4] != "x; rm -rf / #" {
		t.Fatalf("input slice was mutated: %v", in)
	}
}

// railpackBuildArgs must select the railpack frontend via BUILDKIT_SYNTAX, feed the plan as the Dockerfile, forward every plan secret as `--secret id=NAME,env=NAME`, and carry the caller's image output - all as discrete argv tokens, so a hostile name from an untrusted plan can never be word-split or shell-interpreted.
func TestRailpackBuildArgs(t *testing.T) {
	names := []string{"RAILPACK_NODE_VERSION", "RAILPACK_BUILD_CMD"}
	args := railpackBuildArgs(
		"ghcr.io/railwayapp/railpack-frontend:v0.43.0",
		"/tmp/plan/railpack-plan.json", "/tmp/ctx", names,
		[]string{"-t", "deplo/cwars:dpl_abc"}, false, "ns123", "hash456",
	)

	if slices.Contains(args, "sh") || slices.Contains(args, "-c") {
		t.Fatalf("argv must not invoke a shell: %v", args)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"build --build-arg BUILDKIT_SYNTAX=ghcr.io/railwayapp/railpack-frontend:v0.43.0",
		"-f /tmp/plan/railpack-plan.json",
		"--secret id=RAILPACK_NODE_VERSION,env=RAILPACK_NODE_VERSION",
		"--secret id=RAILPACK_BUILD_CMD,env=RAILPACK_BUILD_CMD",
		"-t deplo/cwars:dpl_abc",
		"--build-arg cache-key=ns123",
		"--build-arg secrets-hash=hash456",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("argv missing %q in: %s", want, joined)
		}
	}
	if slices.Contains(args, "--label") {
		t.Fatalf("labels must not ride the railpack frontend build: %v", args)
	}
	if args[len(args)-1] != "/tmp/ctx" {
		t.Fatalf("context must be the last arg: %v", args)
	}

	evil := "x; rm -rf / #"
	adv := railpackBuildArgs("front", "plan.json", "ctx", []string{evil}, nil, false, "", "")
	if !slices.Contains(adv, "--secret") || !slices.Contains(adv, "id="+evil+",env="+evil) {
		t.Fatalf("hostile name not a single --secret token: %v", adv)
	}

	fresh := railpackBuildArgs("front", "plan.json", "ctx", nil, nil, true, "", "")
	if fresh[0] != "build" || fresh[1] != "--no-cache" {
		t.Fatalf("no-cache railpack argv = %v, want build --no-cache first", fresh)
	}
}

// fakeRailpackTools puts a railpack and a docker on PATH that record their argv and env, and
// answers prepare with the given exit code and info file.
func fakeRailpackTools(t *testing.T, prepareExit int, info string) string {
	t.Helper()
	out := t.TempDir()
	railpack := "#!/bin/sh\n[ \"$1\" = --version ] && { echo railpack v" + railpackVersion + "; exit 0; }\n" +
		"echo \"$@\" >> " + out + "/railpack.args\nenv > " + out + "/railpack.env\n" +
		"while [ $# -gt 0 ]; do case \"$1\" in --plan-out) plan=$2;; --info-out) info=$2;; esac; shift; done\n" +
		"printf '%s' '{\"secrets\":[\"RAILPACK_START_CMD\",\"API_URL\"]}' > \"$plan\"\n" +
		"printf '%s' '" + info + "' > \"$info\"\nexit " + strconv.Itoa(prepareExit) + "\n"
	docker := "#!/bin/sh\n[ \"$1\" = info ] && exit 1\necho \"$@\" >> " + out + "/docker.args\nenv >> " + out + "/docker.env\nexit 0\n"
	for name, body := range map[string]string{"railpack": railpack, "docker": docker} {
		if err := os.WriteFile(filepath.Join(out, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", out+":"+os.Getenv("PATH"))
	return out
}

func railpackReq(spec *pb.BuildSpec) *pb.DeployRequest {
	return &pb.DeployRequest{
		DeployId: "dep_deplo-agent-test-rp", Slug: "deplo-agent-test-rp", ProjectId: "prj", ImageRef: "deplo/deplo-agent-test-rp:t",
		BuildKind: pb.BuildKind_BUILD_KIND_RAILPACK, BuildSpec: spec, GithubToken: "ghs_metadataonly",
		Env: map[string]string{"API_URL": "https://api.example.test", "RAILPACK_NODE_VERSION": "20"},
	}
}

// prepare runs with --error-missing-start, declares only what the app set, and gets the GitHub
// token in its own environment only: never an --env, a build secret, an argv or a log line.
func TestBuildRailpackPrepareGetsTheTokenAndOnlyTheSetValues(t *testing.T) {
	out := fakeRailpackTools(t, 0, `{"success":true}`)
	var logs []string
	e := &emitter{send: func(ev *pb.DeployEvent) error {
		if l := ev.GetLog(); l != nil {
			logs = append(logs, l.GetText())
		}
		return nil
	}}
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if !s.buildImage(context.Background(), railpackReq(&pb.BuildSpec{StartCommand: "node server.js"}), t.TempDir(), e) {
		t.Fatalf("build failed:\n%s", strings.Join(logs, "\n"))
	}
	args, _ := os.ReadFile(filepath.Join(out, "railpack.args"))
	env, _ := os.ReadFile(filepath.Join(out, "railpack.env"))
	dockerArgs, _ := os.ReadFile(filepath.Join(out, "docker.args"))
	dockerEnv, _ := os.ReadFile(filepath.Join(out, "docker.env"))
	for _, want := range []string{"--error-missing-start", "--env RAILPACK_START_CMD", "--env API_URL", "--env RAILPACK_NODE_VERSION"} {
		if !strings.Contains(string(args), want) {
			t.Errorf("prepare argv lacks %q: %s", want, args)
		}
	}
	for _, absent := range []string{"--env GITHUB_TOKEN", "RAILPACK_BUILD_CMD", "RAILPACK_SPA_OUTPUT_DIR", "node server.js", "ghs_"} {
		if strings.Contains(string(args), absent) {
			t.Errorf("prepare argv carries %q: %s", absent, args)
		}
	}
	if !strings.Contains(string(env), "GITHUB_TOKEN=ghs_metadataonly\n") || !strings.Contains(string(env), "RAILPACK_START_CMD=node server.js\n") {
		t.Errorf("prepare env lacks the token or the start command:\n%s", env)
	}
	if !strings.Contains(string(env), "RAILPACK_NODE_VERSION=20\n") {
		t.Errorf("an app's own RAILPACK_NODE_VERSION must reach railpack when no version is pinned:\n%s", env)
	}
	for what, b := range map[string][]byte{"docker argv": dockerArgs, "docker env": dockerEnv, "log": []byte(strings.Join(logs, "\n"))} {
		if strings.Contains(string(b), "ghs_metadataonly") {
			t.Errorf("the GitHub token reached the %s", what)
		}
	}
	if !strings.Contains(string(dockerArgs), "--secret id=API_URL,env=API_URL") || !strings.Contains(string(dockerArgs), "railpack-frontend:v0.43.0") {
		t.Errorf("build argv: %s", dockerArgs)
	}
}

// A pinned runtime version wins over an app's own RAILPACK_NODE_VERSION, and an app that sets
// GITHUB_TOKEN itself keeps it: Deplo's metadata token never replaces it.
func TestBuildRailpackPinnedVersionAndOwnGithubToken(t *testing.T) {
	out := fakeRailpackTools(t, 0, `{"success":true}`)
	req := railpackReq(&pb.BuildSpec{RuntimeVersion: "22.4.1"})
	req.Env["GITHUB_TOKEN"] = "users-own"
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if !s.buildImage(context.Background(), req, t.TempDir(), &emitter{send: func(*pb.DeployEvent) error { return nil }}) {
		t.Fatal("build failed")
	}
	env, _ := os.ReadFile(filepath.Join(out, "railpack.env"))
	if !strings.Contains(string(env), "RAILPACK_NODE_VERSION=22\n") || strings.Contains(string(env), "RAILPACK_NODE_VERSION=20") {
		t.Errorf("pinned major not applied:\n%s", env)
	}
	if !strings.Contains(string(env), "GITHUB_TOKEN=users-own\n") || strings.Contains(string(env), "ghs_metadataonly") {
		t.Errorf("the app's own GITHUB_TOKEN was replaced:\n%s", env)
	}
}

// A failed prepare reports railpack's reason from the info file with the fix, not just an exit code.
func TestBuildRailpackPrepareFailureReadsTheInfoFile(t *testing.T) {
	fakeRailpackTools(t, 1, `{"logs":[{"Level":"error","Msg":"No start command detected. Specify a start command: https://railpack.com/config/file#deploy","DocsPath":""}],"success":false}`)
	var res *pb.DeployResult
	e := &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return nil
	}}
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if s.buildImage(context.Background(), railpackReq(&pb.BuildSpec{}), t.TempDir(), e) {
		t.Fatal("a failed prepare built anyway")
	}
	if res == nil || res.GetError() != "No start command detected. Set a Start command in Settings -> Build & output." {
		t.Fatalf("result = %v", res)
	}
}

// An output folder serves a static site: railpack is told the root when there is no package.json.
func TestRailpackSettings(t *testing.T) {
	static := t.TempDir()
	node := t.TempDir()
	mustWrite(t, filepath.Join(node, "package.json"), "{}")
	spec := &pb.BuildSpec{OutputDirectory: "./dist", InstallCommand: "pnpm i", BuildCommand: "make", SkipBuild: true}
	got := railpackSettings(spec, static)
	if got["RAILPACK_SPA_OUTPUT_DIR"] != "dist" || got["RAILPACK_STATIC_FILE_ROOT"] != "dist" || got["RAILPACK_INSTALL_CMD"] != "pnpm i" {
		t.Errorf("static settings = %v", got)
	}
	if _, ok := got["RAILPACK_BUILD_CMD"]; ok {
		t.Errorf("a skipped build must not declare its command: %v", got)
	}
	if _, ok := railpackSettings(spec, node)["RAILPACK_STATIC_FILE_ROOT"]; ok {
		t.Error("a Node app's output is railpack's SPA, not a static file root")
	}
	if got := railpackSettings(&pb.BuildSpec{}, node); len(got) != 0 {
		t.Errorf("nothing set must declare nothing: %v", got)
	}
}

func TestCapabilities_advertisesGithubToken(t *testing.T) {
	if !containsString(Capabilities, "build.github-token") {
		t.Error(`Capabilities must advertise "build.github-token"`)
	}
}

// skip_build empties railpack's build step, which for a static site is what writes the web
// server config: such a site is planned again without the skip rather than built broken.
func TestBuildRailpackNeverSkipsAStaticSitesBuild(t *testing.T) {
	out := fakeRailpackTools(t, 0, `{"detectedProviders":["staticfile"],"success":true}`)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if !s.buildImage(context.Background(), railpackReq(&pb.BuildSpec{SkipBuild: true}), t.TempDir(),
		&emitter{send: func(*pb.DeployEvent) error { return nil }}) {
		t.Fatal("build failed")
	}
	b, _ := os.ReadFile(filepath.Join(out, "railpack.args"))
	runs := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(runs) != 2 || !strings.Contains(runs[0], "--config-file "+railpackSkipConfigName) || strings.Contains(runs[1], "--config-file") {
		t.Fatalf("prepare runs:\n%s", b)
	}
}
