package server

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

const lateSvc = "deplo-connector_web"

// A stack whose connector is marked in the file itself, which is all a StartStack or a Reroute has.
const lateStack = "services:\n  deplo-web: {image: deplo/web:2}\n  " + lateSvc +
	": {image: cloudflare/cloudflared:1, labels: {deplo.start-after-ready: \"true\"}}\n"

// The same stack with nothing in the file: only the request names the connector.
const namedStack = "services:\n  deplo-web: {image: deplo/web:2}\n  " + lateSvc + ": {image: cloudflare/cloudflared:1}\n"

// lateDocker answers whether the app's container runs, whether the connector already does, and which
// services of the project have a container.
func lateDocker(t *testing.T, appRuns bool, connectorRuns bool, services ...string) func() []string {
	t.Helper()
	dir := t.TempDir()
	running, connector := "false", ""
	if appRuns {
		running = "true"
	}
	if connectorRuns {
		connector = "c0ffee"
	}
	mustWrite(t, filepath.Join(dir, "running"), running)
	mustWrite(t, filepath.Join(dir, "connector"), connector)
	mustWrite(t, filepath.Join(dir, "services"), strings.Join(services, "\n"))
	return dockerOnPath(t, `D=`+dir+`
case "$*" in
"inspect -f {{.State.Running}} "*) cat $D/running ;;
"inspect -f {{json .Config.Labels}} "*) echo '{}' ;;
"inspect -f {{len .Mounts}} "*) echo 0 ;;
"image inspect "*) echo null ;;
"ps -q "*"label=com.docker.compose.service="*) cat $D/connector ;;
"ps -q "*) cat $D/running | grep -q true && echo abc ;;
"ps -a "*"com.docker.compose.service"*) cat $D/services ;;
esac
exit 0`)
}

func lateDeploy(t *testing.T, s *Service, req *pb.DeployRequest) *pb.DeployResult {
	t.Helper()
	var res *pb.DeployResult
	s.runDeploy(context.Background(), req, &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return nil
	}})
	if res == nil {
		t.Fatal("the deploy sent no result")
	}
	return res
}

func lateRequest(yaml string, named ...string) *pb.DeployRequest {
	return &pb.DeployRequest{
		DeployId: "dep-late", Slug: "web", ImageRef: "deplo/web:2",
		SourceKind: pb.SourceKind_SOURCE_KIND_IMAGE, BuildKind: pb.BuildKind_BUILD_KIND_NONE,
		ComposeYaml: yaml, Network: testNetwork, ReadyTimeoutMs: 1, StartAfterReady: named,
	}
}

// heldThenStarted: the stack came up without the connector, the app was found running, and only then the connector started.
func heldThenStarted(t *testing.T, got []string) {
	t.Helper()
	first := callAt(got, liveUp, " up -d", "--scale "+lateSvc+"=0")
	after := got[max(first, 0):]
	ready := callAt(after, "inspect -f {{.State.Running}} deplo-web")
	if late := callAt(after, liveUp, " up -d --no-deps "+lateSvc); first < 0 || ready < 0 || late < ready {
		t.Fatalf("want the stack up without the connector, the app ready, then the connector; got:\n%s", strings.Join(got, "\n"))
	}
}

func TestLateServices_areTheOnesTheStackMarksOrTheRequestNames(t *testing.T) {
	if got := lateServices(lateStack, nil); !slices.Equal(got, []string{lateSvc}) {
		t.Fatalf("a marked service waits: %v", got)
	}
	if got := lateServices(namedStack, []string{lateSvc, lateSvc, "bad name", "$(x)", "gone"}); !slices.Equal(got, []string{lateSvc}) {
		t.Fatalf("a named service waits once, and only one the stack has: %v", got)
	}
	listed := "services:\n  a: {image: x, labels: [\"deplo.start-after-ready=true\"]}\n  b: {image: x, labels: {deplo.start-after-ready: \"false\"}}\n"
	if got := lateServices(listed, nil); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("labels written as a list count too, and only when true: %v", got)
	}
	if got := lateServices(namedStack, nil); len(got) != 0 {
		t.Fatalf("an ordinary stack holds nothing back: %v", got)
	}
	if got := lateServices("services: [", []string{lateSvc}); len(got) != 0 {
		t.Fatalf("a stack that cannot be read holds nothing back: %v", got)
	}
}

func TestDeploy_startsTheConnectorOnlyOnceTheAppIsReady(t *testing.T) {
	for name, req := range map[string]*pb.DeployRequest{
		"marked in the stack":  lateRequest(lateStack),
		"named by the request": lateRequest(namedStack, lateSvc),
	} {
		t.Run(name, func(t *testing.T) {
			calls := lateDocker(t, true, false)
			if res := lateDeploy(t, New(t.TempDir(), t.TempDir(), "/", ""), req); !res.GetReady() {
				t.Fatalf("deploy failed: %q", res.GetError())
			}
			heldThenStarted(t, calls())
		})
	}
}

func TestDeploy_neverStartsTheConnectorOfAnAppThatIsNotReady(t *testing.T) {
	calls := lateDocker(t, false, false)
	res := lateDeploy(t, New(t.TempDir(), t.TempDir(), "/", ""), lateRequest(lateStack))
	if res.GetReady() {
		t.Fatal("an app that never runs is not ready")
	}
	if got := calls(); callAt(got, "--no-deps "+lateSvc) >= 0 {
		t.Fatalf("the connector was started for an app that never became ready:\n%s", strings.Join(got, "\n"))
	}
}

// A redeploy must not take the address offline: the connector that runs keeps running through it.
func TestDeploy_leavesARunningConnectorUp(t *testing.T) {
	calls := lateDocker(t, true, true)
	if res := lateDeploy(t, New(t.TempDir(), t.TempDir(), "/", ""), lateRequest(lateStack)); !res.GetReady() {
		t.Fatalf("deploy failed: %q", res.GetError())
	}
	got := calls()
	if callAt(got, "--scale") >= 0 || callAt(got, liveUp, " up -d --remove-orphans") < 0 {
		t.Fatalf("a running connector must not be stopped:\n%s", strings.Join(got, "\n"))
	}
}

func TestDeploy_aConnectorThatWillNotStartFailsIt(t *testing.T) {
	dockerOnPath(t, `case "$*" in
"inspect -f {{.State.Running}} "*) echo true ;;
"image inspect "*) echo null ;;
*"--no-deps `+lateSvc+`") echo boom >&2; exit 1 ;;
esac
exit 0`)
	res := lateDeploy(t, New(t.TempDir(), t.TempDir(), "/", ""), lateRequest(lateStack))
	if res.GetReady() || !strings.Contains(res.GetError(), lateSvc+" did not start: boom") {
		t.Fatalf("result = %v", res)
	}
}

// The stand-in keeps the app online; it never runs a connector of its own.
func TestOverlap_holdsTheConnectorBackInBothStacks(t *testing.T) {
	calls := lateDocker(t, true, false)
	req := lateRequest(lateStack)
	req.StandInComposeYaml = strings.ReplaceAll(lateStack, "deplo-web:", "deplo-web_next:")
	if res := lateDeploy(t, New(t.TempDir(), t.TempDir(), "/", ""), req); !res.GetReady() {
		t.Fatalf("deploy failed: %q", res.GetError())
	}
	got := calls()
	if callAt(got, standInUp, " up -d", "--scale "+lateSvc+"=0") < 0 || callAt(got, standInUp, "--no-deps") >= 0 {
		t.Fatalf("the stand-in must come up without the connector and never start it:\n%s", strings.Join(got, "\n"))
	}
	heldThenStarted(t, got)
}

func TestFailover_aStackStartedFromABundleHoldsItsConnectorBack(t *testing.T) {
	calls := lateDocker(t, true, false)
	err := New(t.TempDir(), t.TempDir(), "/", "").startFromBundle(context.Background(), &lease.Start{
		DeploymentID: "dep-1", Slug: "web", ImageRef: "deplo/web:2", ComposeYAML: namedStack,
		Network: testNetwork, StartAfterReady: []string{lateSvc},
	})
	if err != nil {
		t.Fatalf("start from the bundle: %v", err)
	}
	heldThenStarted(t, calls())
}

func TestReroute_holdsBackAConnectorThatIsNotRunning(t *testing.T) {
	calls := lateDocker(t, true, false)
	res, err := New(t.TempDir(), t.TempDir(), "/", "").Reroute(context.Background(),
		&pb.RerouteRequest{Slug: "web", Network: testNetwork, ComposeYaml: lateStack})
	if err != nil || !res.GetOk() {
		t.Fatalf("reroute: %v %q", err, res.GetError())
	}
	got := calls()
	first := callAt(got, liveUp, " up -d", "--scale "+lateSvc+"=0")
	if late := callAt(got, liveUp, " up -d --no-deps "+lateSvc); first < 0 || late < first {
		t.Fatalf("want the stack up without the connector, then the connector; got:\n%s", strings.Join(got, "\n"))
	}
}

func TestReroute_leavesARunningConnectorUp(t *testing.T) {
	calls := lateDocker(t, true, true)
	res, err := New(t.TempDir(), t.TempDir(), "/", "").Reroute(context.Background(),
		&pb.RerouteRequest{Slug: "web", Network: testNetwork, ComposeYaml: lateStack})
	if err != nil || !res.GetOk() {
		t.Fatalf("reroute: %v %q", err, res.GetError())
	}
	if got := calls(); callAt(got, "--scale") >= 0 || callAt(got, "--no-deps") >= 0 {
		t.Fatalf("a running connector must be left to the one up:\n%s", strings.Join(got, "\n"))
	}
}

func TestStartStack_startsTheConnectorAfterTheRest(t *testing.T) {
	calls := lateDocker(t, true, false, "deplo-web", lateSvc)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	mustWrite(t, s.stackPath("web"), lateStack)
	res, err := s.StartStack(context.Background(), &pb.StackRef{Slug: "web"})
	if err != nil || !res.GetOk() {
		t.Fatalf("start: %v %q", err, res.GetError())
	}
	got := calls()
	rest := callAt(got, liveUp, " start deplo-web")
	if late := callAt(got, liveUp, " start "+lateSvc); rest < 0 || late < rest || strings.Contains(got[rest], lateSvc) {
		t.Fatalf("want the app started, then the connector; got:\n%s", strings.Join(got, "\n"))
	}
}

func TestStartStack_doesNotStartTheConnectorWhenTheRestWillNot(t *testing.T) {
	calls := dockerOnPath(t, `case "$*" in
"ps -a "*) printf 'deplo-web\n`+lateSvc+`\n' ;;
*" start "*|"start "*) echo boom >&2; exit 1 ;;
esac
exit 0`)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	mustWrite(t, s.stackPath("web"), lateStack)
	res, err := s.StartStack(context.Background(), &pb.StackRef{Slug: "web"})
	if err != nil || res.GetOk() {
		t.Fatalf("a stack that will not start is not started: %v %v", err, res)
	}
	if got := calls(); callAt(got, " start "+lateSvc) >= 0 {
		t.Fatalf("the connector was started beside an app that is down:\n%s", strings.Join(got, "\n"))
	}
}

func TestStartStack_ofAnOrdinaryStackIsOneStart(t *testing.T) {
	calls := lateDocker(t, true, false)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	mustWrite(t, s.stackPath("web"), namedStack)
	if res, err := s.StartStack(context.Background(), &pb.StackRef{Slug: "web"}); err != nil || !res.GetOk() {
		t.Fatalf("start: %v %q", err, res.GetError())
	}
	if got := calls(); len(got) != 1 || !strings.HasSuffix(got[0], " start") {
		t.Fatalf("want one compose start, got:\n%s", strings.Join(got, "\n"))
	}
}

func TestStartAfterReadyIsAdvertised(t *testing.T) {
	if !slices.Contains(Capabilities, "deploy.start-after-ready") {
		t.Fatal("Hello does not advertise deploy.start-after-ready")
	}
}
