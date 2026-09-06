package server

import (
	"strings"
	"testing"
)

func TestCacheNamespace_isStableAndUnguessable(t *testing.T) {
	s := &Service{agentDir: t.TempDir()}
	a := s.cacheNamespace("shop")
	if a != s.cacheNamespace("shop") {
		t.Fatal("the namespace must be stable for one app")
	}
	if a == s.cacheNamespace("shop-2") {
		t.Fatal("two apps must not share a namespace")
	}
	if len(a) != 24 || strings.Contains(a, "shop") {
		t.Fatalf("namespace %q must be an opaque hex token", a)
	}
	// A second agent on the same slug (another host, another salt) differs.
	other := &Service{agentDir: t.TempDir()}
	if other.cacheNamespace("shop") == a {
		t.Fatal("the salt must be per host")
	}
	// The salt survives a restart on the same host.
	again := &Service{agentDir: s.agentDir}
	if again.cacheNamespace("shop") != a {
		t.Fatal("the salt must be persisted")
	}
}
