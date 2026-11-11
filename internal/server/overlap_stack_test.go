package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const (
	stackLive    = "services:\n  web: {image: acme/web:2, deploy: {replicas: 2}}\n  worker: {image: acme/worker:2}\n"
	stackStandIn = "services:\n  web_next: {image: acme/web:2, deploy: {replicas: 2}}\n  worker_next: {image: acme/worker:2}\n"
	stackRoutes  = `{"traefik.enable":"true","traefik.http.routers.shop-web.rule":"Host(` + "`shop.example.com`" + `)"}`
	stackNoRoute = `{"traefik.enable":"false"}`
)

// stackCopy is one container of the fake host: which project and service it belongs to, and how it is doing.
type stackCopy struct {
	id, project, service, status, health, labels string
}

func (c stackCopy) sample() string {
	return fmt.Sprintf(`{"id":%q,"name":"/%s-%s-1","project":%q,"service":%q,"status":%q,"restarts":0,`+
		`"created":"2026-11-11T10:00:00Z","startedAt":"2026-11-11T10:00:00Z","pid":0,"exitCode":0,"oomKilled":false,`+
		`"health":%q,"restartPolicy":"unless-stopped","ip":"10.0.0.2 "}`,
		c.id, c.project, c.service, c.project, c.service, c.status, c.health)
}

// stackDocker is a host running copies: `ps` lists them by project or by slug, `inspect` answers per id.
// Once the live stack is brought up, afterSwap (when set) replaces what the stand-in's copies report.
func stackDocker(t *testing.T, copies []stackCopy, afterSwap string) func() []string {
	t.Helper()
	dir := t.TempDir()
	byProject := map[string][]string{}
	var all []string
	for _, c := range copies {
		byProject[c.project] = append(byProject[c.project], c.id)
		all = append(all, c.id)
		mustWrite(t, filepath.Join(dir, "labels", c.id), c.labels)
		mustWrite(t, filepath.Join(dir, "sample", c.id), c.sample())
		if afterSwap != "" && strings.HasSuffix(c.project, standInSuffix) {
			c.health = afterSwap
			mustWrite(t, filepath.Join(dir, "swapped", c.id), c.sample())
		}
	}
	for p, ids := range byProject {
		mustWrite(t, filepath.Join(dir, "ps", p), strings.Join(ids, "\n")+"\n")
	}
	mustWrite(t, filepath.Join(dir, "slug"), strings.Join(all, "\n")+"\n")
	return dockerOnPath(t, `D=`+dir+`
for last; do :; done
case "$*" in
"ps "*"label=com.docker.compose.project="*)
  for a; do case "$a" in label=com.docker.compose.project=*) p=${a#label=com.docker.compose.project=} ;; esac; done
  cat $D/ps/$p 2>/dev/null ;;
"ps "*"label=deplo.slug="*) cat $D/slug ;;
"inspect -f {{.State.Running}} "*) echo false ;;
"inspect -f {{json .Config.Labels}} "*) shift 3; for id; do cat $D/labels/$id 2>/dev/null || echo '{}'; echo; done ;;
"inspect -f {\"id\""*) shift 3; for id; do
    if [ -f $D/live-up ] && [ -f $D/swapped/$id ]; then cat $D/swapped/$id; else cat $D/sample/$id; fi; echo; done ;;
"inspect -f {{len .Mounts}} "*) echo 0 ;;
"image inspect "*) echo null ;;
*" config --images"*) echo acme/web:2 ;;
*" config --format json"*) echo '{"services":{}}' ;;
"compose -p deplo-shop -f "*" up -d"*) touch $D/live-up ;;
esac
exit 0`)
}

func stackRequest() *pb.DeployRequest {
	return &pb.DeployRequest{
		DeployId: "dep-stack", Slug: "shop",
		SourceKind: pb.SourceKind_SOURCE_KIND_COMPOSE, BuildKind: pb.BuildKind_BUILD_KIND_NONE,
		ComposeYaml: stackLive, StandInComposeYaml: stackStandIn, Network: testNetwork,
		Env: map[string]string{"KEY": "v"}, Ready: &pb.ReadyCheck{}, ReadyTimeoutMs: 1,
	}
}

func liveCopies() []stackCopy {
	return []stackCopy{
		{"w1", "deplo-shop", "web", "running", "healthy", stackRoutes},
		{"k1", "deplo-shop", "worker", "running", "", stackNoRoute},
	}
}

func standInCopies(webHealth string) []stackCopy {
	return []stackCopy{
		{"n1", "deplo-shop_next", "web_next", "running", webHealth, stackRoutes},
		{"n2", "deplo-shop_next", "worker_next", "running", "", stackNoRoute},
	}
}

const stackStandInUp, stackLiveUp = "-p deplo-shop_next ", "-p deplo-shop "

func TestStackOverlap_everyServiceIsReadyBesideTheLiveStackBeforeItIsReplaced(t *testing.T) {
	calls := stackDocker(t, append(liveCopies(), standInCopies("healthy")...), "")
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if res := lateDeploy(t, s, stackRequest()); !res.GetReady() {
		t.Fatalf("deploy failed: %q", res.GetError())
	}
	got := calls()
	standIn := callAt(got, stackStandInUp, " up -d")
	live := callAt(got, stackLiveUp, " up -d")
	removed := callAt(got[max(live, 0):], stackStandInUp, " down")
	if standIn < 0 || live < standIn || removed < 0 {
		t.Fatalf("want stand-in up, live replaced, stand-in removed; got:\n%s", strings.Join(got, "\n"))
	}
	// The stand-in reads the stack's variables like the live stack does.
	env := filepath.Join(s.filesRoot("shop"), ".env")
	if !strings.Contains(got[standIn], "--env-file "+env) || !strings.Contains(got[removed+max(live, 0)], "--env-file "+env) {
		t.Fatalf("the stand-in must start and stop with the stack's env file:\n%s", strings.Join(got, "\n"))
	}
	if _, err := os.Stat(s.stackPath("shop_next")); !os.IsNotExist(err) {
		t.Fatalf("the stand-in's stack file is still there: %v", err)
	}
}

// One copy of one service that never passes its health check: the deploy fails and nothing live is touched.
func TestStackOverlap_aCopyThatNeverGetsReadyFailsTheDeployAndKeepsTheOldRelease(t *testing.T) {
	calls := stackDocker(t, append(liveCopies(), standInCopies("starting")...), "")
	res := lateDeploy(t, New(t.TempDir(), t.TempDir(), "/", ""), stackRequest())
	if res.GetReady() || !strings.Contains(res.GetError(), "still online") ||
		!strings.Contains(res.GetError(), "Service web_next never passed its health check") {
		t.Fatalf("want a failed deploy that names the copy and says the old version serves, got ready=%v %q", res.GetReady(), res.GetError())
	}
	got := calls()
	if callAt(got, stackLiveUp, " up ") >= 0 {
		t.Fatalf("the live stack was touched:\n%s", strings.Join(got, "\n"))
	}
	if callAt(got[max(callAt(got, stackStandInUp, " up -d"), 0):], stackStandInUp, " down") < 0 {
		t.Fatalf("the stand-in was left running:\n%s", strings.Join(got, "\n"))
	}
}

// The routed service is watched under the name it has in the stand-in.
func TestStackOverlap_theRoutedServiceMustListenInTheStandIn(t *testing.T) {
	stackDocker(t, append(liveCopies(), standInCopies("healthy")...), "")
	req := stackRequest()
	req.Ready = &pb.ReadyCheck{Port: 3000, PortRequired: true, Service: "web"}
	res := lateDeploy(t, New(t.TempDir(), t.TempDir(), "/", ""), req)
	if res.GetReady() || !strings.Contains(res.GetError(), "Service web_next is running but never listened on port 3000") {
		t.Fatalf("want the stand-in's own service checked for its port, got ready=%v %q", res.GetReady(), res.GetError())
	}
}

// The live stack counts on its own: what the stand-in's copies do while it is replaced is not its failure.
func TestStackOverlap_theLiveStackIsJudgedWithoutTheStandIn(t *testing.T) {
	stackDocker(t, append(liveCopies(), standInCopies("healthy")...), "unhealthy")
	if res := lateDeploy(t, New(t.TempDir(), t.TempDir(), "/", ""), stackRequest()); !res.GetReady() {
		t.Fatalf("deploy failed: %q", res.GetError())
	}
}

// A stack built from the app's source: the stand-in reads the same working copy, behind the same check.
func TestStackOverlap_aSourceStackChecksItsStandInBeforeStartingIt(t *testing.T) {
	calls := stackDocker(t, append(liveCopies(), standInCopies("healthy")...), "")
	req := stackRequest()
	req.SourceKind, req.BuildKind = pb.SourceKind_SOURCE_KIND_UPLOAD, pb.BuildKind_BUILD_KIND_COMPOSE
	req.ContextTar = composeTarball(t, map[string]string{"compose.yaml": "services: {}\n"})
	if res := lateDeploy(t, New(t.TempDir(), t.TempDir(), "/", ""), req); !res.GetReady() {
		t.Fatalf("deploy failed: %q", res.GetError())
	}
	got := calls()
	checked := callAt(got, stackStandInUp, " config --format json")
	standIn := callAt(got, stackStandInUp, " up -d")
	if live := callAt(got, stackLiveUp, " up -d"); checked < 0 || standIn < checked || live < standIn {
		t.Fatalf("want the stand-in checked, started, then the live stack replaced; got:\n%s", strings.Join(got, "\n"))
	}
	if callAt(got, stackStandInUp, " build") >= 0 {
		t.Fatalf("the stand-in must run the images the stack just built:\n%s", strings.Join(got, "\n"))
	}
}

// Two stacks that route one host differently make the proxy drop the route for both.
func TestStackOverlap_swapsInPlaceWhenTheRoutesChanged(t *testing.T) {
	next := standInCopies("healthy")
	next[0].labels = strings.Replace(stackRoutes, "shop.example.com", "store.example.com", 1)
	calls := stackDocker(t, append(liveCopies(), next...), "")
	var logs []string
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.runDeploy(context.Background(), stackRequest(), &emitter{send: func(ev *pb.DeployEvent) error {
		if l := ev.GetLog(); l != nil {
			logs = append(logs, l.GetText())
		}
		return nil
	}})
	got := calls()
	if callAt(got, stackStandInUp, " up -d") >= 0 || callAt(got, stackLiveUp, " up -d") < 0 {
		t.Fatalf("a stand-in with other routes must never start:\n%s", strings.Join(got, "\n"))
	}
	if !strings.Contains(strings.Join(logs, "\n"), "How this app is reached changed") {
		t.Fatalf("the log should say why it swapped in place:\n%s", strings.Join(logs, "\n"))
	}
}

// Nothing to keep online: the first deploy, or a stack that was stopped.
func TestStackOverlap_startsNoStandInWhenNothingIsRunning(t *testing.T) {
	calls := stackDocker(t, nil, "")
	lateDeploy(t, New(t.TempDir(), t.TempDir(), "/", ""), stackRequest())
	if got := calls(); callAt(got, stackStandInUp) >= 0 || callAt(got, stackLiveUp, " up -d") < 0 {
		t.Fatalf("want only the live stack brought up:\n%s", strings.Join(got, "\n"))
	}
}

func TestWatcherIsSilentWhileAStackStandInRuns(t *testing.T) {
	standIn := readySample{id: "c2", name: "deplo-web_next-web_next-1", project: "deplo-web" + standInSuffix,
		status: "running", startedAt: rt0}
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: []readySample{web(rt0), standIn}}
	w := newWatcherOn(t, h)
	w.release("web", httpRule(), true)
	runTicks(w, h, 10)
	if len(h.restarted) != 0 {
		t.Fatal("restarted while the stack's stand-in ran")
	}
}

func TestCapabilities_advertisesStackOverlap(t *testing.T) {
	if !containsString(Capabilities, "deploy.overlap.stack") {
		t.Error(`Capabilities must advertise "deploy.overlap.stack"`)
	}
}
