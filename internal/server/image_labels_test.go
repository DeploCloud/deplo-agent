package server

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func TestTraefikLabelKeys(t *testing.T) {
	got := traefikLabelKeys(`{"org.opencontainers.image.source":"x","traefik.enable":"true","Traefik.http.routers.evil.rule":"Host(` + "`victim.com`" + `)"}`)
	want := []string{"Traefik.http.routers.evil.rule", "traefik.enable"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
	if k := traefikLabelKeys(`{"deplo.project":"prj_1"}`); len(k) != 0 {
		t.Fatalf("an ordinary label set must pass: %v", k)
	}
	if k := traefikLabelKeys("null"); len(k) != 0 {
		t.Fatalf("an image with no labels must pass: %v", k)
	}
	if k := traefikLabelKeys("not json"); len(k) != 0 {
		t.Fatalf("unreadable output must not refuse: %v", k)
	}
}

func TestComposeBaseArgs(t *testing.T) {
	got := composeBaseArgs("deplo-shop", "/s/shop.yml", "/s/files/shop/.env", "/s/files/shop")
	want := []string{"compose", "-p", "deplo-shop", "-f", "/s/shop.yml", "--project-directory", "/s/files/shop", "--env-file", "/s/files/shop/.env"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %v, want %v", got, want)
	}
}

// fakeImageDocker puts a docker on PATH: `config --images` lists images, `image inspect` answers from
// present/, a pull moves pullable/ into present/, and every call is logged to calls.
func fakeImageDocker(t *testing.T, images []string, present, pullable map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for sub, set := range map[string]map[string]string{"present": present, "pullable": pullable} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			t.Fatal(err)
		}
		for img, labels := range set {
			name := strings.NewReplacer("/", "_", ":", "_").Replace(img)
			if err := os.WriteFile(filepath.Join(dir, sub, name), []byte(labels), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "images"), []byte(strings.Join(images, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
D=` + dir + `
echo "$*" >> $D/calls
case "$*" in
*" config --images"*) cat $D/images ;;
"pull "*|*" pull "*) for f in $D/pullable/*; do [ -e "$f" ] && mv "$f" $D/present/; done ;;
"image inspect "*)
	for last; do :; done
	f=$D/present/$(echo "$last" | tr '/:' '__')
	[ -f "$f" ] || { echo "No such image: $last" >&2; exit 1; }
	cat "$f" ;;
esac
exit 0
`
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	return filepath.Join(dir, "calls")
}

func rerouteWithFake(t *testing.T) (*pb.StackResult, error) {
	t.Helper()
	s := New(t.TempDir(), t.TempDir(), "/", "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.Reroute(ctx, &pb.RerouteRequest{
		Slug:        "deplo-agent-test-admit",
		Network:     testNetwork,
		ComposeYaml: "services:\n  web:\n    image: ${IMG}\n",
		Env:         map[string]string{"IMG": "deplo-agent-test-img:1"},
	})
}

func readCalls(t *testing.T, path string) string {
	t.Helper()
	b, _ := os.ReadFile(path)
	return string(b)
}

func TestRerouteRefusesAnImageWithTraefikLabels(t *testing.T) {
	calls := fakeImageDocker(t, []string{"deplo-agent-test-img:1"},
		map[string]string{"deplo-agent-test-img:1": `{"traefik.http.routers.x.rule":"Host(` + "`victim.com`" + `)"}`}, nil)
	res, err := rerouteWithFake(t)
	if err != nil {
		t.Fatal(err)
	}
	if res.GetOk() || !strings.Contains(res.GetError(), "Traefik labels") {
		t.Fatalf("a labelled image must be refused, got ok=%v %q", res.GetOk(), res.GetError())
	}
	if strings.Contains(readCalls(t, calls), " up ") {
		t.Fatal("compose up ran for a refused image")
	}
}

func TestRerouteStartsPresentImagesWithoutPulling(t *testing.T) {
	calls := fakeImageDocker(t, []string{"deplo-agent-test-img:1"},
		map[string]string{"deplo-agent-test-img:1": `{"org.opencontainers.image.source":"x"}`}, nil)
	res, err := rerouteWithFake(t)
	if err != nil || !res.GetOk() {
		t.Fatalf("a clean present image must start: %v %q", err, res.GetError())
	}
	got := readCalls(t, calls)
	if strings.Contains(got, " pull ") || !strings.Contains(got, " up -d") {
		t.Fatalf("want up and no pull, got:\n%s", got)
	}
}

func TestReroutePullsAMissingImageThenChecksIt(t *testing.T) {
	calls := fakeImageDocker(t, []string{"deplo-agent-test-img:1"}, nil,
		map[string]string{"deplo-agent-test-img:1": `{"traefik.enable":"true"}`})
	res, err := rerouteWithFake(t)
	if err != nil {
		t.Fatal(err)
	}
	if res.GetOk() || !strings.Contains(res.GetError(), "Traefik labels") {
		t.Fatalf("a pulled labelled image must be refused, got ok=%v %q", res.GetOk(), res.GetError())
	}
	got := readCalls(t, calls)
	if !strings.Contains(got, "pull --ignore-buildable --policy missing") || strings.Contains(got, " up ") {
		t.Fatalf("want a missing-only pull and no up, got:\n%s", got)
	}
}

func TestRerouteRefusesAnImageThatCannotBeInspected(t *testing.T) {
	calls := fakeImageDocker(t, []string{"deplo-agent-test-img:1"}, nil, nil)
	res, err := rerouteWithFake(t)
	if err != nil {
		t.Fatal(err)
	}
	if res.GetOk() || !strings.Contains(res.GetError(), "could not be inspected") {
		t.Fatalf("an image that cannot be inspected must be refused, got ok=%v %q", res.GetOk(), res.GetError())
	}
	if strings.Contains(readCalls(t, calls), " up ") {
		t.Fatal("compose up ran for an unchecked image")
	}
}

// Deploy has already pulled and built, so a still-missing image is refused without another pull.
func TestAdmitImagesFailsClosedWithoutAPull(t *testing.T) {
	fakeImageDocker(t, nil, nil, map[string]string{"deplo-agent-test-img:1": "null"})
	if err := admitImages(context.Background(), []string{"deplo-agent-test-img:1"}, nil); err == nil {
		t.Fatal("an image that cannot be inspected must not be admitted")
	}
}

// A compose stack's builds share the daemon's BuildKit with every other app, so their cache mounts
// get the same per-app namespace a single-image build does.
func TestPrepareStackImagesNamespacesTheBuildCache(t *testing.T) {
	out, bin := t.TempDir(), t.TempDir()
	script := "#!/bin/sh\necho \"$*\" >> " + out + "/calls\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	s := New(t.TempDir(), t.TempDir(), "/", "")
	req := &pb.DeployRequest{Slug: "deplo-agent-test-stack"}
	base := composeBaseArgs("deplo-deplo-agent-test-stack", "/s/x.yml", "", "")
	if _, ok := s.prepareStackImages(context.Background(), req, base, &emitter{send: func(*pb.DeployEvent) error { return nil }}); !ok {
		t.Fatal("prepareStackImages failed")
	}
	calls, err := os.ReadFile(filepath.Join(out, "calls"))
	if err != nil {
		t.Fatal(err)
	}
	want := " build --build-arg BUILDKIT_CACHE_MOUNT_NS=" + s.cacheNamespace(req.GetSlug())
	if !strings.Contains(string(calls), want) {
		t.Fatalf("compose build is not namespaced (want %q):\n%s", want, calls)
	}
}
