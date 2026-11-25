package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// c39: a recreated proxy refused new visitors for the 10 s Docker gives the old one to drain.
func TestBringUpTraefik_theOldProxyDrainsThreeSeconds(t *testing.T) {
	argv := dockerOnPath(t, "")
	dir := t.TempDir()
	path := filepath.Join(dir, "docker-compose.yml")
	if err := os.WriteFile(path, []byte("services: {traefik: {image: traefik}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	_ = s.bringUpTraefik(context.Background(), path, false)
	for _, l := range argv() {
		if strings.HasPrefix(l, "compose -f "+path+" up") {
			if !strings.HasSuffix(l, "--timeout 3") {
				t.Fatalf("the proxy is recreated with %q", l)
			}
			return
		}
	}
	t.Fatalf("no recreate in %v", argv())
}
