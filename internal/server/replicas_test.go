package server

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const replicasYaml = "services: {%s: {image: deplo/web:2, deploy: {replicas: 3}}}\n"

// replicasDocker answers `ps` per compose project (the ids of its running copies) and labels per id.
func replicasDocker(t *testing.T, projects map[string]string, mounts map[string]string) func() []string {
	t.Helper()
	dir := t.TempDir()
	for name, v := range projects {
		mustWrite(t, filepath.Join(dir, "ps", name), v)
	}
	for _, id := range []string{"w1", "w2", "w3", "n1", "n2", "n3"} {
		mustWrite(t, filepath.Join(dir, "labels", id), overlapRoutes)
	}
	for id, v := range mounts {
		mustWrite(t, filepath.Join(dir, "mounts", id), v)
	}
	mustWrite(t, filepath.Join(dir, "mounts", ".keep"), "")
	return dockerOnPath(t, `D=`+dir+`
for last; do :; done
case "$*" in
"ps -q --filter label=com.docker.compose.project="*)
  for a; do case "$a" in label=com.docker.compose.project=*) p=${a#label=com.docker.compose.project=} ;; esac; done
  cat $D/ps/$p 2>/dev/null ;;
"inspect -f {{.State.Running}} "*) echo false ;;
"inspect -f {{json .Config.Labels}} "*) cat $D/labels/$last 2>/dev/null || echo '{}' ;;
"inspect -f {{len .Mounts}} "*) cat $D/mounts/$last 2>/dev/null || echo 0 ;;
"image inspect "*) echo null ;;
esac
exit 0`)
}

func replicasDeploy(t *testing.T) *pb.DeployResult {
	t.Helper()
	s := New(t.TempDir(), t.TempDir(), "/", "")
	var res *pb.DeployResult
	s.runDeploy(context.Background(), &pb.DeployRequest{
		DeployId: "dep-replicas", Slug: "web", ImageRef: "deplo/web:2",
		SourceKind: pb.SourceKind_SOURCE_KIND_IMAGE, BuildKind: pb.BuildKind_BUILD_KIND_NONE,
		ComposeYaml: strings.Replace(replicasYaml, "%s", "deplo-web", 1), Network: testNetwork, ReadyTimeoutMs: 1,
		StandInComposeYaml: strings.Replace(replicasYaml, "%s", "deplo-web_next", 1),
	}, &emitter{send: func(ev *pb.DeployEvent) error {
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

func TestReplicasAllStartBeforeTheOldOnesAreReplaced(t *testing.T) {
	calls := replicasDocker(t, map[string]string{
		"deplo-web": "w1\nw2\nw3\n", "deplo-web_next": "n1\nn2\nn3\n",
	}, nil)
	res := replicasDeploy(t)
	if !res.GetReady() {
		t.Fatalf("deploy failed: %q", res.GetError())
	}
	got := calls()
	standIn := callAt(got, standInUp, " up -d")
	live := callAt(got, liveUp, " up -d")
	removed := callAt(got[max(live, 0):], standInUp, " down")
	if standIn < 0 || live < standIn || removed < 0 {
		t.Fatalf("want stand-in up, live replaced, stand-in removed; got:\n%s", strings.Join(got, "\n"))
	}
}

// One copy that never runs fails the deploy; the old release keeps serving even when it has Files.
func TestReplicasOneNeverReadyFailsTheDeployAndKeepsTheOldRelease(t *testing.T) {
	calls := replicasDocker(t, map[string]string{
		"deplo-web": "w1\nw2\nw3\n", "deplo-web_next": "n1\nn2\n",
	}, map[string]string{"w1": "1"})
	res := replicasDeploy(t)
	if res.GetReady() || !strings.Contains(res.GetError(), "still online") {
		t.Fatalf("want a failed deploy that says the old version serves, got ready=%v %q", res.GetReady(), res.GetError())
	}
	got := calls()
	if callAt(got, liveUp) >= 0 {
		t.Fatalf("the live copies were touched:\n%s", strings.Join(got, "\n"))
	}
	if callAt(got[max(callAt(got, standInUp, " up -d"), 0):], standInUp, " down") < 0 {
		t.Fatalf("the stand-in was left running:\n%s", strings.Join(got, "\n"))
	}
}

func TestReplicaCount(t *testing.T) {
	for in, want := range map[string]int{
		"services: {a: {image: x}}":                             1,
		"services: {a: {image: x, deploy: {replicas: 4}}}":      4,
		"services: {a: {image: x}, b: {deploy: {replicas: 2}}}": 2,
		"not: [yaml": 1,
	} {
		if got := replicaCount(in); got != want {
			t.Errorf("replicaCount(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestCapabilities_advertisesReplicas(t *testing.T) {
	if !containsString(Capabilities, "deploy.replicas") {
		t.Error(`Capabilities must advertise "deploy.replicas"`)
	}
}
