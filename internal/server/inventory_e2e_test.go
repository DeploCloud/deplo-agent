package server

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// End-to-end (real docker, real syft download and checksum, real memory cap) on one labelled container.
func TestE2E_InventoryImage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	if os.Geteuid() != 0 || !inventoryCanCap() {
		t.Skip("capping the reader's memory needs root and systemd")
	}
	const img = "alpine:3.20"
	if res, err := dockercli.Run(ctx, 5*time.Minute, "pull", "-q", img); err != nil || res.Code != 0 {
		t.Skipf("cannot pull %s: %v %s", img, err, res.Stderr)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	name := "deplo-e2e-inventory-" + suffix
	res, err := dockercli.Run(ctx, time.Minute, "run", "-d", "--name", name,
		"--label", "deplo.managed=true", "--label", "deplo.project=prj_e2e_"+suffix, img, "sleep", "300")
	if err != nil || res.Code != 0 {
		t.Fatalf("start container: %v %s", err, res.Stderr)
	}
	t.Cleanup(func() { _, _ = dockercli.Run(context.Background(), time.Minute, "rm", "-f", name) })

	id, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", "-f", "{{.Id}}", img)
	if err != nil || id.Code != 0 {
		t.Fatalf("inspect image: %v %s", err, id.Stderr)
	}
	s := New(t.TempDir(), t.TempDir(), "/", e2eToolsBase(t))
	resp, err := s.InventoryImage(ctx, &pb.InventoryImageRequest{ImageId: strings.TrimSpace(id.Stdout)})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != pb.InventoryStatus_INVENTORY_STATUS_OK {
		t.Fatalf("want OK, got %v: %s", resp.Status, resp.Error)
	}
	if resp.DistroId != "alpine" || resp.ScannerVersion != syftVersion {
		t.Fatalf("unexpected distro/scanner: %s %s", resp.DistroId, resp.ScannerVersion)
	}
	found := false
	for _, p := range resp.Packages {
		if p.Type == "apk" && p.Name == "busybox" && strings.HasPrefix(p.Purl, "pkg:apk/alpine/busybox@") {
			found = true
		}
	}
	if !found {
		t.Fatalf("busybox missing from %d packages", len(resp.Packages))
	}
}
