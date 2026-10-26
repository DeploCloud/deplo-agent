package server

import (
	"context"
	"os"
	"path/filepath"
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

// End-to-end: a newer build of a running workload's repository is pulled by digest, read, and gone again.
func TestE2E_InventoryCandidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	if os.Geteuid() != 0 || !inventoryCanCap() {
		t.Skip("capping the reader's memory needs root and systemd")
	}
	const cur, next = "alpine:3.20", "alpine:3.21"
	for _, img := range []string{cur, next} {
		if res, err := dockercli.Run(ctx, 5*time.Minute, "pull", "-q", img); err != nil || res.Code != 0 {
			t.Skipf("cannot pull %s: %v %s", img, err, res.Stderr)
		}
	}
	digests, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", "-f", "{{index .RepoDigests 0}}", next)
	if err != nil || digests.Code != 0 {
		t.Fatalf("read %s's digest: %v %s", next, err, digests.Stderr)
	}
	ref := strings.TrimSpace(digests.Stdout)
	_, _ = dockercli.Run(ctx, time.Minute, "image", "rm", next)
	_, _ = dockercli.Run(ctx, time.Minute, "image", "rm", ref)
	if still, _ := dockercli.Run(ctx, 30*time.Second, "image", "inspect", ref); still.Code == 0 {
		t.Skipf("%s is in use on this host", next)
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	name := "deplo-e2e-candidate-" + suffix
	res, err := dockercli.Run(ctx, time.Minute, "run", "-d", "--name", name,
		"--label", "deplo.managed=true", "--label", "deplo.project=prj_e2e_"+suffix, cur, "sleep", "300")
	if err != nil || res.Code != 0 {
		t.Fatalf("start container: %v %s", err, res.Stderr)
	}
	t.Cleanup(func() { _, _ = dockercli.Run(context.Background(), time.Minute, "rm", "-f", name) })
	id, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", "-f", "{{.Id}}", cur)
	if err != nil || id.Code != 0 {
		t.Fatalf("inspect image: %v %s", err, id.Stderr)
	}

	s := New(t.TempDir(), t.TempDir(), "/", e2eToolsBase(t))
	resp, err := s.InventoryCandidate(ctx, &pb.InventoryCandidateRequest{ImageRef: ref, CurrentImageId: strings.TrimSpace(id.Stdout)})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetResult().GetStatus() != pb.InventoryStatus_INVENTORY_STATUS_OK || resp.GetImageId() == "" {
		t.Fatalf("want OK with an id, got %v %q: %s", resp.GetResult().GetStatus(), resp.GetImageId(), resp.GetResult().GetError())
	}
	if !strings.HasPrefix(resp.GetResult().GetDistroVersion(), "3.21") {
		t.Fatalf("read the wrong build: alpine %s", resp.GetResult().GetDistroVersion())
	}
	if left, _ := dockercli.Run(ctx, 30*time.Second, "image", "inspect", ref); left.Code == 0 {
		t.Fatalf("%s pulled only to be read is still on the host", ref)
	}
}

// End-to-end: a real npm app image; the lockfile tells the build-only package from the one that runs.
func TestE2E_InventoryMarksBuildOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	if os.Geteuid() != 0 || !inventoryCanCap() {
		t.Skip("capping the reader's memory needs root and systemd")
	}
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "package.json"),
		`{"name":"e2e","version":"1.0.0","dependencies":{"express":"4.17.1"},"devDependencies":{"left-pad":"1.3.0"}}`)
	mustWrite(t, filepath.Join(dir, "Dockerfile"),
		"FROM node:22-alpine\nWORKDIR /app\nCOPY package.json .\nRUN npm install --no-audit --no-fund --loglevel=error\n")
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	tag := "deplo-e2e-deps:" + suffix
	if res, err := dockercli.Run(ctx, 10*time.Minute, "build", "-q", "-t", tag, dir); err != nil || res.Code != 0 {
		t.Skipf("cannot build the npm image: %v %s", err, res.Stderr)
	}
	t.Cleanup(func() { _, _ = dockercli.Run(context.Background(), time.Minute, "image", "rm", "-f", tag) })
	name := "deplo-e2e-deps-" + suffix
	res, err := dockercli.Run(ctx, time.Minute, "run", "-d", "--name", name,
		"--label", "deplo.managed=true", "--label", "deplo.project=prj_e2e_"+suffix, tag, "sleep", "300")
	if err != nil || res.Code != 0 {
		t.Fatalf("start container: %v %s", err, res.Stderr)
	}
	t.Cleanup(func() { _, _ = dockercli.Run(context.Background(), time.Minute, "rm", "-f", name) })
	id, _ := dockercli.Run(ctx, 30*time.Second, "image", "inspect", "-f", "{{.Id}}", tag)

	s := New(t.TempDir(), t.TempDir(), "/", e2eToolsBase(t))
	resp, err := s.InventoryImage(ctx, &pb.InventoryImageRequest{ImageId: strings.TrimSpace(id.Stdout)})
	if err != nil || resp.Status != pb.InventoryStatus_INVENTORY_STATUS_OK {
		t.Fatalf("inventory: %v %v %s", err, resp.GetStatus(), resp.GetError())
	}
	marks := map[string]*pb.ImagePackage{}
	for _, p := range resp.Packages {
		if p.Type == "npm" && strings.HasPrefix(p.Location, "/app/") {
			marks[p.Name] = p
		}
	}
	if p := marks["left-pad"]; p == nil || !p.BuildOnly {
		t.Fatalf("left-pad must be build-only: %v", p)
	}
	if p := marks["express"]; p == nil || p.BuildOnly || len(p.Via) != 0 {
		t.Fatalf("express is a direct dependency that runs: %v", p)
	}
	if p := marks["body-parser"]; p == nil || p.BuildOnly || len(p.Via) == 0 || p.Via[len(p.Via)-1] != "express" {
		t.Fatalf("body-parser runs, pulled in by express: %v", p)
	}
}
