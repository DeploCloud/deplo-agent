package server

import (
	"reflect"
	"testing"
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
