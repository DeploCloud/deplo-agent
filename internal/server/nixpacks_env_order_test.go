package server

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The exact shape nixpacks 1.41 generates for a Node app, reduced to the lines
// that matter here.
const nixpacksGenerated = `FROM ghcr.io/railwayapp/nixpacks:ubuntu-1745885067

ENTRYPOINT ["/bin/bash", "-l", "-c"]
WORKDIR /app/


COPY .nixpacks/nixpkgs-ffee.nix .nixpacks/nixpkgs-ffee.nix
RUN nix-env -if .nixpacks/nixpkgs-ffee.nix && nix-collect-garbage -d


ARG CI DATABASE_URL NIXPACKS_METADATA NODE_ENV NPM_CONFIG_PRODUCTION PAYLOAD_SECRET PORT RESEND_API_KEY
ENV CI=$CI DATABASE_URL=$DATABASE_URL NIXPACKS_METADATA=$NIXPACKS_METADATA NODE_ENV=$NODE_ENV NPM_CONFIG_PRODUCTION=$NPM_CONFIG_PRODUCTION PAYLOAD_SECRET=$PAYLOAD_SECRET PORT=$PORT RESEND_API_KEY=$RESEND_API_KEY

# setup phase
# noop

# install phase
ENV NIXPACKS_PATH=/app/node_modules/.bin:$NIXPACKS_PATH
COPY package.json bun.lock /app/.
RUN --mount=type=cache,id=app-/root/bun,target=/root/.bun bun i

# build phase
COPY . /app/.
RUN --mount=type=cache,id=app-node_modules/cache,target=/app/node_modules/.cache bun run build


RUN printf '\nPATH=/app/node_modules/.bin:$PATH' >> /root/.profile


# start
COPY . /app

CMD ["bun run start"]
`

func lineIndex(t *testing.T, lines []string, prefix string) int {
	t.Helper()
	for i, l := range lines {
		if strings.HasPrefix(l, prefix) {
			return i
		}
	}
	t.Fatalf("no line starting with %q in:\n%s", prefix, strings.Join(lines, "\n"))
	return -1
}

// The whole point: the app's own variables end up below the install RUN and
// above the build RUN, so a value change can no longer invalidate the install.
func TestDeferEnvBelowInstall_movesAppVars(t *testing.T) {
	lines := strings.Split(nixpacksGenerated, "\n")
	movable := map[string]bool{"DATABASE_URL": true, "PAYLOAD_SECRET": true, "RESEND_API_KEY": true}

	out, moved := deferEnvBelowInstall(lines, movable)
	if !slices.Equal(moved, []string{"DATABASE_URL", "PAYLOAD_SECRET", "RESEND_API_KEY"}) {
		t.Fatalf("moved = %v", moved)
	}

	installRun := lineIndex(t, out, "RUN --mount=type=cache,id=app-/root/bun")
	buildRun := lineIndex(t, out, "RUN --mount=type=cache,id=app-node_modules/cache")
	movedArg := lineIndex(t, out, "ARG DATABASE_URL PAYLOAD_SECRET RESEND_API_KEY")
	movedEnv := lineIndex(t, out, "ENV DATABASE_URL=$DATABASE_URL PAYLOAD_SECRET=$PAYLOAD_SECRET RESEND_API_KEY=$RESEND_API_KEY")

	if movedArg < installRun {
		t.Errorf("the moved block must come AFTER the install RUN (%d vs %d)", movedArg, installRun)
	}
	if movedEnv != movedArg+1 {
		t.Errorf("ENV must immediately follow its ARG (%d vs %d)", movedEnv, movedArg)
	}
	if movedEnv > buildRun {
		t.Errorf("the moved block must come BEFORE the build RUN (%d vs %d)", movedEnv, buildRun)
	}

	// Everything nixpacks needs for the install keeps its original position, and
	// the moved names are gone from the top block.
	top := out[lineIndex(t, out, "ARG CI ")]
	if top != "ARG CI NIXPACKS_METADATA NODE_ENV NPM_CONFIG_PRODUCTION PORT" {
		t.Errorf("top ARG line = %q", top)
	}
	topEnv := out[lineIndex(t, out, "ENV CI=")]
	if strings.Contains(topEnv, "PAYLOAD_SECRET") {
		t.Errorf("a moved var is still declared on top: %q", topEnv)
	}

	// Every declared name still exists exactly once across the file, or the build
	// would fail on an undeclared build-arg.
	joined := strings.Join(out, "\n")
	for _, name := range []string{"CI", "DATABASE_URL", "NIXPACKS_METADATA", "NODE_ENV", "NPM_CONFIG_PRODUCTION", "PAYLOAD_SECRET", "PORT", "RESEND_API_KEY"} {
		if !strings.Contains(joined, name+"=$"+name) {
			t.Errorf("%s lost its ENV declaration", name)
		}
	}
	// The nix layer must stay above everything we touched - it is the expensive
	// one and no app variable may ever reach its cache key.
	if lineIndex(t, out, "RUN nix-env ") > movedArg {
		t.Error("the nix layer moved below the app variables")
	}
}

// When every declared name has to stay, the file is left byte-identical rather
// than rewritten into an equivalent-but-different form (which would itself be a
// cache miss).
func TestDeferEnvBelowInstall_nothingMovable(t *testing.T) {
	lines := strings.Split(nixpacksGenerated, "\n")
	out, moved := deferEnvBelowInstall(lines, map[string]bool{"UNRELATED": true})
	if len(moved) != 0 || !slices.Equal(out, lines) {
		t.Fatalf("expected an untouched file, moved = %v", moved)
	}
}

// Anything that is not the generator we know about is left alone: a bail costs a
// cache hit, a wrong rewrite costs the build.
func TestDeferEnvBelowInstall_bailsOnUnknownShapes(t *testing.T) {
	movable := map[string]bool{"DATABASE_URL": true, "PAYLOAD_SECRET": true, "RESEND_API_KEY": true}
	cases := map[string]string{
		// ARG is scoped per stage, so the same move across stages would silently
		// drop the variables.
		"multi-stage": nixpacksGenerated + "\nFROM alpine\nCOPY --from=0 /app /app\n",
		"ARG carries a default": strings.Replace(nixpacksGenerated,
			"ARG CI DATABASE_URL", "ARG CI=1 DATABASE_URL", 1),
		"ENV assigns a literal": strings.Replace(nixpacksGenerated,
			"ENV CI=$CI DATABASE_URL=$DATABASE_URL", "ENV CI=true DATABASE_URL=$DATABASE_URL", 1),
		"ENV is not adjacent to ARG": strings.Replace(nixpacksGenerated,
			"ARG CI DATABASE_URL", "ARG CI DATABASE_URL\n", 1),
		"no install phase":                strings.Replace(nixpacksGenerated, "# install phase", "# noop", 1),
		"nothing after the install phase": strings.Split(nixpacksGenerated, "# build phase")[0],
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			lines := strings.Split(body, "\n")
			out, moved := deferEnvBelowInstall(lines, movable)
			if len(moved) != 0 || !slices.Equal(out, lines) {
				t.Fatalf("%s must be left untouched, moved = %v", name, moved)
			}
		})
	}
}

// An app with no build command still gains: the variables land after the install
// so it stays cached, and they are still present in the final image.
func TestDeferEnvBelowInstall_noBuildPhase(t *testing.T) {
	body := strings.Replace(nixpacksGenerated, `# build phase
COPY . /app/.
RUN --mount=type=cache,id=app-node_modules/cache,target=/app/node_modules/.cache bun run build`, "", 1)
	out, moved := deferEnvBelowInstall(strings.Split(body, "\n"), map[string]bool{"PAYLOAD_SECRET": true})
	if len(moved) != 1 {
		t.Fatalf("moved = %v", moved)
	}
	if lineIndex(t, out, "ARG PAYLOAD_SECRET") < lineIndex(t, out, "RUN --mount=type=cache,id=app-/root/bun") {
		t.Error("the deferred block must still land after the install RUN")
	}
	if lineIndex(t, out, "ARG PAYLOAD_SECRET") > lineIndex(t, out, "CMD ") {
		t.Error("the deferred block must stay above CMD so the value is in the final image")
	}
}

func TestMovableBuildEnv(t *testing.T) {
	keys := []string{
		"DATABASE_URL", "PAYLOAD_SECRET", "NEXT_PUBLIC_URL", "S3_ACCESS_KEY_ID",
		"NODE_ENV", "NPM_CONFIG_PRODUCTION", "npm_config_registry", "YARN_NPM_AUTH_TOKEN",
		"CI", "HTTPS_PROXY", "NIXPACKS_METADATA",
		"ACME_REGISTRY_TOKEN", "PRIVATE_FEED_USER",
	}
	dir := writeRepo(t, map[string]string{
		".npmrc": "//npm.acme.test/:_authToken=${ACME_REGISTRY_TOKEN}\n//npm.acme.test/:username=$PRIVATE_FEED_USER\n",
	})
	movable := movableBuildEnv(keys, dir, "")

	for _, want := range []string{"DATABASE_URL", "PAYLOAD_SECRET", "NEXT_PUBLIC_URL", "S3_ACCESS_KEY_ID"} {
		if !movable[want] {
			t.Errorf("%s is a plain app variable and should move", want)
		}
	}
	// A toolchain variable changes how dependencies resolve; a name interpolated
	// into .npmrc authenticates the registry the install talks to. Both must keep
	// their position even though they are the app's own variables.
	for _, want := range []string{
		"NODE_ENV", "NPM_CONFIG_PRODUCTION", "npm_config_registry", "YARN_NPM_AUTH_TOKEN",
		"CI", "HTTPS_PROXY", "NIXPACKS_METADATA", "ACME_REGISTRY_TOKEN", "PRIVATE_FEED_USER",
	} {
		if movable[want] {
			t.Errorf("%s can steer the install and must NOT move", want)
		}
	}
}

// A user-supplied install command is part of the install step, so anything it
// reads has to stay above it.
func TestMovableBuildEnv_customInstallCommand(t *testing.T) {
	movable := movableBuildEnv([]string{"TURBO_TOKEN", "PAYLOAD_SECRET"}, t.TempDir(),
		`bun i --registry "$TURBO_TOKEN"`)
	if movable["TURBO_TOKEN"] {
		t.Error("a variable the install command reads must not move")
	}
	if !movable["PAYLOAD_SECRET"] {
		t.Error("an unrelated variable should still move")
	}
}

func TestDeferAppEnvBelowInstall_rewritesTheFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Dockerfile")
	if err := os.WriteFile(path, []byte(nixpacksGenerated), 0o644); err != nil {
		t.Fatal(err)
	}
	moved, err := deferAppEnvBelowInstall(path, []string{"PAYLOAD_SECRET", "NODE_ENV"}, dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(moved, []string{"PAYLOAD_SECRET"}) {
		t.Fatalf("moved = %v - NODE_ENV steers the install and must stay", moved)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "\nARG PAYLOAD_SECRET\nENV PAYLOAD_SECRET=$PAYLOAD_SECRET\n") {
		t.Fatalf("rewritten file lacks the deferred block:\n%s", body)
	}
}

// Nothing to move must leave the file's bytes exactly as nixpacks wrote them.
func TestDeferAppEnvBelowInstall_noopKeepsBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Dockerfile")
	if err := os.WriteFile(path, []byte(nixpacksGenerated), 0o644); err != nil {
		t.Fatal(err)
	}
	moved, err := deferAppEnvBelowInstall(path, []string{"NODE_ENV"}, dir, "")
	if err != nil || len(moved) != 0 {
		t.Fatalf("moved = %v, err = %v", moved, err)
	}
	body, _ := os.ReadFile(path)
	if string(body) != nixpacksGenerated {
		t.Fatal("the file was rewritten even though nothing moved")
	}
}

// The app's variables lose their ENV (the ARG still feeds every RUN) while Nixpacks'
// own declarations, and any line that is not the plain KEY=$KEY form, stay put.
func TestStripAppEnv_dropsOnlyTheAppsOwn(t *testing.T) {
	lines := strings.Split(nixpacksGenerated, "\n")
	app := map[string]bool{"DATABASE_URL": true, "PAYLOAD_SECRET": true, "RESEND_API_KEY": true}

	out, dropped := stripAppEnv(lines, app)
	if !slices.Equal(dropped, []string{"DATABASE_URL", "PAYLOAD_SECRET", "RESEND_API_KEY"}) {
		t.Fatalf("dropped = %v", dropped)
	}
	topArg := out[lineIndex(t, out, "ARG CI ")]
	if topArg != lines[lineIndex(t, lines, "ARG CI ")] {
		t.Errorf("the ARG line must be untouched, got %q", topArg)
	}
	topEnv := out[lineIndex(t, out, "ENV CI=")]
	if topEnv != "ENV CI=$CI NIXPACKS_METADATA=$NIXPACKS_METADATA NODE_ENV=$NODE_ENV NPM_CONFIG_PRODUCTION=$NPM_CONFIG_PRODUCTION PORT=$PORT" {
		t.Errorf("top ENV line = %q", topEnv)
	}
	joined := strings.Join(out, "\n")
	if !strings.Contains(joined, "ENV NIXPACKS_PATH=/app/node_modules/.bin:$NIXPACKS_PATH") {
		t.Error("an ENV holding a real value must survive")
	}
	for name := range app {
		if strings.Contains(joined, "ENV "+name) || strings.Contains(joined, " "+name+"=$") {
			t.Errorf("%s is still declared as ENV:\n%s", name, joined)
		}
	}
}

// An ENV line made up entirely of the app's variables - what the deferred block below
// the install step is - goes away rather than being left empty.
func TestStripAppEnv_dropsAWhollyAppEnvLine(t *testing.T) {
	out, dropped := stripAppEnv([]string{"ENV A=$A B=$B", "RUN true"}, map[string]bool{"A": true, "B": true})
	if !slices.Equal(out, []string{"RUN true"}) || len(dropped) != 2 {
		t.Fatalf("out = %v, dropped = %v", out, dropped)
	}
}

// Ordering contract: the deferred block keeps its ARG and loses its ENV.
func TestStripAppEnv_afterDeferLeavesTheMovedBlockArgOnly(t *testing.T) {
	app := map[string]bool{"DATABASE_URL": true, "PAYLOAD_SECRET": true, "RESEND_API_KEY": true}
	moved, _ := deferEnvBelowInstall(strings.Split(nixpacksGenerated, "\n"), app)
	out, _ := stripAppEnv(moved, app)

	joined := strings.Join(out, "\n")
	if !strings.Contains(joined, "ARG DATABASE_URL PAYLOAD_SECRET RESEND_API_KEY") {
		t.Errorf("the moved ARG must survive:\n%s", joined)
	}
	if strings.Contains(joined, "ENV DATABASE_URL") {
		t.Errorf("the moved ENV must be gone:\n%s", joined)
	}
	argIdx := lineIndex(t, out, "ARG DATABASE_URL ")
	installRun := lineIndex(t, out, "RUN --mount=type=cache,id=app-/root/bun")
	buildRun := lineIndex(t, out, "RUN --mount=type=cache,id=app-node_modules/cache")
	if argIdx < installRun || argIdx > buildRun {
		t.Errorf("the moved ARG must stay between the install and build RUNs (%d, %d, %d)", installRun, argIdx, buildRun)
	}
}

// The rewrite on disk: the Dockerfile is written back without the app's ENV.
func TestStripAppEnvFromDockerfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "Dockerfile")
	if err := os.WriteFile(path, []byte(nixpacksGenerated), 0o644); err != nil {
		t.Fatal(err)
	}
	dropped, err := stripAppEnvFromDockerfile(path, []string{"PAYLOAD_SECRET", "NOT_IN_THE_FILE"})
	if err != nil || !slices.Equal(dropped, []string{"PAYLOAD_SECRET"}) {
		t.Fatalf("dropped = %v, err = %v", dropped, err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "PAYLOAD_SECRET=$PAYLOAD_SECRET") {
		t.Errorf("still declared as ENV:\n%s", body)
	}
	if !strings.Contains(string(body), "ARG CI DATABASE_URL NIXPACKS_METADATA NODE_ENV NPM_CONFIG_PRODUCTION PAYLOAD_SECRET PORT RESEND_API_KEY") {
		t.Errorf("the ARG line must be untouched:\n%s", body)
	}
}
