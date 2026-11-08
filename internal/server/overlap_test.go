package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const overlapRoutes = `{"traefik.enable":"true","traefik.http.routers.deplo-web-0.rule":"Host(` + "`web.example.com`" + `)"}`

// overlapDocker answers which containers run and what they are labelled with, per container name.
func overlapDocker(t *testing.T, running, labels map[string]string, mounts ...map[string]string) func() []string {
	t.Helper()
	dir := t.TempDir()
	sets := map[string]map[string]string{"running": running, "labels": labels, "mounts": nil}
	if len(mounts) > 0 {
		sets["mounts"] = mounts[0]
	}
	for sub, set := range sets {
		mustWrite(t, filepath.Join(dir, sub, ".keep"), "")
		for name, v := range set {
			mustWrite(t, filepath.Join(dir, sub, name), v)
		}
	}
	return dockerOnPath(t, `D=`+dir+`
for last; do :; done
case "$*" in
"inspect -f {{.State.Running}} "*) cat $D/running/$last 2>/dev/null || echo false ;;
"inspect -f {{json .Config.Labels}} "*) cat $D/labels/$last 2>/dev/null || echo '{}' ;;
"inspect -f {{len .Mounts}} "*) cat $D/mounts/$last 2>/dev/null || echo 0 ;;
"image inspect "*) echo null ;;
esac
exit 0`)
}

func overlapDeploy(t *testing.T) (*pb.DeployResult, []string) {
	t.Helper()
	s := New(t.TempDir(), t.TempDir(), "/", "")
	var res *pb.DeployResult
	var logs []string
	s.runDeploy(context.Background(), &pb.DeployRequest{
		DeployId: "dep-overlap", Slug: "web", ImageRef: "deplo/web:2",
		SourceKind: pb.SourceKind_SOURCE_KIND_IMAGE, BuildKind: pb.BuildKind_BUILD_KIND_NONE,
		ComposeYaml: "services: {deplo-web: {image: deplo/web:2}}\n", Network: testNetwork, ReadyTimeoutMs: 1,
		StandInComposeYaml: "services: {deplo-web_next: {image: deplo/web:2}}\n",
	}, &emitter{send: func(ev *pb.DeployEvent) error {
		if r := ev.GetResult(); r != nil {
			res = r
		}
		if l := ev.GetLog(); l != nil {
			logs = append(logs, l.GetText())
		}
		return nil
	}})
	if res == nil {
		t.Fatal("the deploy sent no result")
	}
	return res, logs
}

// The index of the first call containing every part, or -1.
func callAt(calls []string, parts ...string) int {
	for i, c := range calls {
		ok := true
		for _, p := range parts {
			ok = ok && strings.Contains(c, p)
		}
		if ok {
			return i
		}
	}
	return -1
}

const standInUp, liveUp = "-p deplo-web_next ", "-p deplo-web "

func TestOverlapKeepsTheLiveContainerUntilTheNewOneIsReady(t *testing.T) {
	calls := overlapDocker(t,
		map[string]string{"deplo-web": "true", "deplo-web_next": "true"},
		map[string]string{"deplo-web": overlapRoutes, "deplo-web_next": overlapRoutes})
	res, _ := overlapDeploy(t)
	if !res.GetReady() {
		t.Fatalf("deploy failed: %q", res.GetError())
	}
	got := calls()
	standIn := callAt(got, standInUp, " up -d")
	standInReady := callAt(got, "inspect -f {{.State.Running}} deplo-web_next")
	live := callAt(got, liveUp, " up -d")
	removed := callAt(got[max(live, 0):], standInUp, " down")
	if standIn < 0 || standInReady < standIn || live < standInReady || removed < 0 {
		t.Fatalf("want stand-in up, stand-in ready, live replaced, stand-in removed; got:\n%s", strings.Join(got, "\n"))
	}
}

func TestOverlapLeavesTheLiveContainerAloneWhenTheNewOneNeverStarts(t *testing.T) {
	calls := overlapDocker(t,
		map[string]string{"deplo-web": "true", "deplo-web_next": "false"},
		map[string]string{"deplo-web": overlapRoutes, "deplo-web_next": overlapRoutes})
	res, _ := overlapDeploy(t)
	if res.GetReady() || res.GetError() == "" {
		t.Fatalf("a stand-in that never runs must fail the deploy, got ready=%v %q", res.GetReady(), res.GetError())
	}
	got := calls()
	if callAt(got, liveUp) >= 0 {
		t.Fatalf("the live container was touched:\n%s", strings.Join(got, "\n"))
	}
	if callAt(got[max(callAt(got, standInUp, " up -d"), 0):], standInUp, " down") < 0 {
		t.Fatalf("the stand-in was left running:\n%s", strings.Join(got, "\n"))
	}
	if !strings.Contains(res.GetError(), "still online") {
		t.Fatalf("the error should say the old version still serves: %q", res.GetError())
	}
}

// Storage two copies may not share (a lock file, an embedded database) must not block every deploy.
func TestOverlapSwapsInPlaceWhenAnAppWithStorageCannotRunTwice(t *testing.T) {
	calls := overlapDocker(t,
		map[string]string{"deplo-web": "true", "deplo-web_next": "false"},
		map[string]string{"deplo-web": overlapRoutes, "deplo-web_next": overlapRoutes},
		map[string]string{"deplo-web": "1"})
	res, logs := overlapDeploy(t)
	got := calls()
	removed := callAt(got, standInUp, " down")
	live := callAt(got, liveUp, " up -d")
	if !res.GetReady() || removed < 0 || live < removed {
		t.Fatalf("want the stand-in removed, then the live stack swapped in place; ready=%v %q\n%s",
			res.GetReady(), res.GetError(), strings.Join(got, "\n"))
	}
	if !strings.Contains(strings.Join(logs, "\n"), "goes offline briefly") {
		t.Fatalf("the log should say why it swapped in place:\n%s", strings.Join(logs, "\n"))
	}
}

// Nothing to keep online: the first deploy, or an app that was stopped.
func TestOverlapSwapsInPlaceWhenNothingIsRunning(t *testing.T) {
	calls := overlapDocker(t, map[string]string{"deplo-web": "false"}, nil)
	overlapDeploy(t)
	got := calls()
	if callAt(got, standInUp) >= 0 || callAt(got, liveUp, " up -d") < 0 {
		t.Fatalf("want only the live stack brought up:\n%s", strings.Join(got, "\n"))
	}
}

// Two containers that route the same host differently make the proxy drop the route for both.
func TestOverlapSwapsInPlaceWhenTheRoutesChanged(t *testing.T) {
	moved := strings.Replace(overlapRoutes, "web.example.com", "shop.example.com", 1)
	calls := overlapDocker(t,
		map[string]string{"deplo-web": "true", "deplo-web_next": "true"},
		map[string]string{"deplo-web": overlapRoutes, "deplo-web_next": moved})
	res, logs := overlapDeploy(t)
	if !res.GetReady() {
		t.Fatalf("deploy failed: %q", res.GetError())
	}
	got := calls()
	if callAt(got, standInUp, " up -d") >= 0 || callAt(got, liveUp, " up -d") < 0 {
		t.Fatalf("a stand-in with other routes must never start:\n%s", strings.Join(got, "\n"))
	}
	if !strings.Contains(strings.Join(logs, "\n"), "domains changed") {
		t.Fatalf("the log should say why it swapped in place:\n%s", strings.Join(logs, "\n"))
	}
}

func TestOverlapRemovesItsStackFile(t *testing.T) {
	overlapDocker(t,
		map[string]string{"deplo-web": "true", "deplo-web_next": "true"},
		map[string]string{"deplo-web": overlapRoutes, "deplo-web_next": overlapRoutes})
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.runDeploy(context.Background(), &pb.DeployRequest{
		DeployId: "dep-overlap", Slug: "web", ImageRef: "deplo/web:2",
		SourceKind: pb.SourceKind_SOURCE_KIND_IMAGE, BuildKind: pb.BuildKind_BUILD_KIND_NONE,
		ComposeYaml: "services: {}\n", Network: testNetwork, ReadyTimeoutMs: 1,
		StandInComposeYaml: "services: {}\n",
	}, &emitter{send: func(*pb.DeployEvent) error { return nil }})
	if _, err := os.Stat(s.stackPath("web_next")); !os.IsNotExist(err) {
		t.Fatalf("the stand-in's stack file is still there: %v", err)
	}
}

func TestCapabilities_advertisesOverlap(t *testing.T) {
	if !containsString(Capabilities, "deploy.overlap") {
		t.Error(`Capabilities must advertise "deploy.overlap"`)
	}
}
