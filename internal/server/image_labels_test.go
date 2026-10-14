package server

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

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
