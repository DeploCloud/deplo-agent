package server

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const testImageID = "sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10"

const syftFixture = `{"artifacts":[
 {"name":"libssl3","version":"3.0.11-1~deb12u2","type":"deb","purl":"pkg:deb/debian/libssl3@3.0.11-1~deb12u2?distro=debian-12&upstream=openssl","locations":[{"path":"/var/lib/dpkg/status"}]},
 {"name":"minimist","version":"1.2.5","type":"npm","purl":"pkg:npm/minimist@1.2.5","locations":[{"path":"/app/node_modules/minimist/package.json"}]},
 {"name":"mystery","version":"1","type":"binary","purl":"","locations":[]}
],"distro":{"id":"debian","versionID":"12.13"},"descriptor":{"name":"syft","version":"1.54.1"}}`

// workloadDocker answers the docker calls an inventory makes for one running container of project prj_web.
const workloadDocker = `case "$*" in
  ps*) echo '{"ID":"c1","Names":"deplo-web","State":"running"}' ;;
  "inspect -f"*) echo '{"id":"c1","image":"` + testImageID + `","ref":"nginx:1.27-alpine","running":true,"project":"prj_web","app":"","service":"web"}'
     echo '{"id":"c2","image":"sha256:aa","ref":"x","running":false,"project":"","app":"","service":""}' ;;
  *digests*) echo '{"id":"` + testImageID + `","digests":["nginx@` + testImageID + `"]}' ;;
  *size*) echo '{"size":1000,"wd":"/app"}' ;;
  save*) : > "$3" ;;
esac`

func TestCapabilities_advertisesInventory(t *testing.T) {
	for _, c := range []string{"image-inventory", "image-inventory.candidate", "deploy.pull-base", "reroute.pull"} {
		if !containsString(Capabilities, c) {
			t.Errorf("Capabilities must advertise %q", c)
		}
	}
}

func TestListWorkloadImages_skipsUnlabelledContainers(t *testing.T) {
	dockerOnPath(t, workloadDocker)
	got, err := listWorkloadImages(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 workload image, got %d: %v", len(got), got)
	}
	w := got[0]
	if w.ProjectId != "prj_web" || w.Service != "web" || w.ImageId != testImageID || !w.Running || w.ImageRef != "nginx:1.27-alpine" {
		t.Fatalf("unexpected workload image: %v", w)
	}
	if len(w.RepoDigests) != 1 || !strings.HasPrefix(w.RepoDigests[0], "nginx@sha256:") {
		t.Fatalf("repo digests not attached: %v", w.RepoDigests)
	}
}

func TestParseSyftJSON(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sbom.json")
	mustWrite(t, p, syftFixture)
	resp, _, err := parseSyftJSON(p)
	if err != nil {
		t.Fatal(err)
	}
	if resp.DistroId != "debian" || resp.DistroVersion != "12.13" || resp.ScannerVersion != "1.54.1" {
		t.Fatalf("distro/scanner not read: %v", resp)
	}
	if len(resp.Packages) != 2 {
		t.Fatalf("a package without a purl must be dropped, got %d", len(resp.Packages))
	}
	if resp.Packages[1].Location != "/app/node_modules/minimist/package.json" || resp.Packages[1].Type != "npm" {
		t.Fatalf("unexpected package: %v", resp.Packages[1])
	}
}

// inventoryService builds a Service whose host passes every guard, with a fake syft and systemd-run.
func inventoryService(t *testing.T, systemdRun string) *Service {
	t.Helper()
	dockerOnPath(t, workloadDocker)
	bin := filepath.Dir(firstOnPath(t, "docker"))
	mustWrite(t, filepath.Join(bin, "systemd-run"), systemdRun)
	_ = os.Chmod(filepath.Join(bin, "systemd-run"), 0o755)

	base := t.TempDir()
	fixture := filepath.Join(base, "fixture.json")
	mustWrite(t, fixture, syftFixture)
	syft := filepath.Join(base, "tools", "syft-"+syftVersion)
	mustWrite(t, syft, "#!/bin/sh\nfor a in \"$@\"; do case \"$a\" in syft-json=*) cp "+fixture+" \"${a#syft-json=}\";; esac; done\n")
	_ = os.Chmod(syft, 0o755)

	free, mem, canCap := inventoryFree, inventoryMemory, inventoryCanCap
	t.Cleanup(func() { inventoryFree, inventoryMemory, inventoryCanCap = free, mem, canCap })
	inventoryFree = func(string) int64 { return 1 << 40 }
	inventoryMemory = func() (int64, int64) { return 8 << 30, 6 << 30 }
	inventoryCanCap = func() bool { return true }
	return New(t.TempDir(), t.TempDir(), "/", base)
}

const passThroughSystemdRun = "#!/bin/sh\nwhile [ \"$1\" != \"--\" ]; do shift; done\nshift\nexec \"$@\"\n"

func firstOnPath(t *testing.T, name string) string {
	t.Helper()
	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		if p := filepath.Join(dir, name); usableBinary(p) {
			return p
		}
	}
	t.Fatalf("%s not on PATH", name)
	return ""
}

func TestInventoryImage_readsPackagesAndCleansUp(t *testing.T) {
	s := inventoryService(t, passThroughSystemdRun)
	resp, err := s.InventoryImage(context.Background(), &pb.InventoryImageRequest{ImageId: testImageID})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != pb.InventoryStatus_INVENTORY_STATUS_OK {
		t.Fatalf("want OK, got %v: %s", resp.Status, resp.Error)
	}
	if len(resp.Packages) != 2 || resp.WorkingDir != "/app" || resp.DistroId != "debian" {
		t.Fatalf("unexpected response: %v", resp)
	}
	left, _ := filepath.Glob(filepath.Join(s.buildTmpDir, inventoryDirPrefix+"*"))
	if len(left) != 0 {
		t.Fatalf("temp dir left behind: %v", left)
	}
}

func TestInventoryImage_guards(t *testing.T) {
	ctx := context.Background()
	s := inventoryService(t, passThroughSystemdRun)

	if _, err := s.InventoryImage(ctx, &pb.InventoryImageRequest{ImageId: "nginx:latest"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a non-digest id: want InvalidArgument, got %v", err)
	}
	other := "sha256:" + strings.Repeat("b", 64)
	if _, err := s.InventoryImage(ctx, &pb.InventoryImageRequest{ImageId: other}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("an image no workload uses: want InvalidArgument, got %v", err)
	}

	want := func(name string, st pb.InventoryStatus) {
		t.Helper()
		resp, err := s.InventoryImage(ctx, &pb.InventoryImageRequest{ImageId: testImageID})
		if err != nil || resp.Status != st {
			t.Errorf("%s: want %v, got %v (%v)", name, st, resp.GetStatus(), err)
		}
	}

	s.inventoryMu.Lock()
	want("concurrent inventory", pb.InventoryStatus_INVENTORY_STATUS_BUSY)
	s.inventoryMu.Unlock()

	s.deploys["dep"] = newInflight(func() {})
	want("deploy in flight", pb.InventoryStatus_INVENTORY_STATUS_BUSY)
	delete(s.deploys, "dep")

	inventoryFree = func(string) int64 { return 1500 }
	want("low disk", pb.InventoryStatus_INVENTORY_STATUS_SKIPPED_LOW_DISK)
	inventoryFree = func(string) int64 { return 1 << 40 }

	inventoryCanCap = func() bool { return false }
	want("no memory cap", pb.InventoryStatus_INVENTORY_STATUS_UNSUPPORTED)
	inventoryCanCap = func() bool { return true }

	inventoryMemory = func() (int64, int64) { return 8 << 30, 1 << 30 }
	want("low memory", pb.InventoryStatus_INVENTORY_STATUS_SKIPPED_LOW_MEMORY)
}

func TestInventoryImage_killedReaderIsTooLarge(t *testing.T) {
	s := inventoryService(t, "#!/bin/sh\nkill -9 $$\n")
	resp, err := s.InventoryImage(context.Background(), &pb.InventoryImageRequest{ImageId: testImageID})
	if err != nil || resp.Status != pb.InventoryStatus_INVENTORY_STATUS_TOO_LARGE {
		t.Fatalf("want TOO_LARGE, got %v (%v)", resp.GetStatus(), err)
	}
}

func TestSweepInventoryDirs(t *testing.T) {
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, inventoryDirPrefix+"123", "image.tar"), "x")
	mustWrite(t, filepath.Join(dir, "deplo-build-web-1", "keep"), "x")
	sweepInventoryDirs(dir)
	if _, err := os.Stat(filepath.Join(dir, inventoryDirPrefix+"123")); !os.IsNotExist(err) {
		t.Error("a leftover inventory dir must be removed")
	}
	if _, err := os.Stat(filepath.Join(dir, "deplo-build-web-1")); err != nil {
		t.Error("a build dir must be left alone")
	}
}

func TestLowPriority(t *testing.T) {
	bin := t.TempDir()
	t.Setenv("PATH", bin)
	if got := lowPriority("docker", "save"); strings.Join(got, " ") != "docker save" {
		t.Errorf("without nice/ionice: got %v", got)
	}
	for _, n := range []string{"nice", "ionice"} {
		mustWrite(t, filepath.Join(bin, n), "#!/bin/sh\n")
		_ = os.Chmod(filepath.Join(bin, n), 0o755)
	}
	if got := lowPriority("docker", "save"); strings.Join(got, " ") != "nice -n 19 ionice -c 3 docker save" {
		t.Errorf("with nice/ionice: got %v", got)
	}
}

func gzipBytes(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write(raw)
	_ = gz.Close()
	return buf.Bytes()
}

func TestInstallTarBinary_checksum(t *testing.T) {
	archive := gzipBytes(t, tarball(t, []tar.Header{{Name: "syft", Mode: 0o755, Typeflag: tar.TypeReg}},
		map[string]string{"syft": "#!/bin/sh\ntrue\n"}))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(archive) }))
	defer srv.Close()
	sum := sha256.Sum256(archive)

	dir := t.TempDir()
	bad := filepath.Join(dir, "syft-bad")
	if err := installTarBinary(context.Background(), srv.URL, "syft", bad, strings.Repeat("0", 64)); err == nil {
		t.Fatal("a checksum mismatch must refuse the install")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatalf("a refused install must leave nothing behind: %v", entries)
	}

	good := filepath.Join(dir, "syft-good")
	if err := installTarBinary(context.Background(), srv.URL, "syft", good, hex.EncodeToString(sum[:])); err != nil {
		t.Fatalf("matching checksum: %v", err)
	}
	if !usableBinary(good) {
		t.Fatal("the binary is not installed")
	}
}

func TestBuildArgv_pullOnlyWhenAsked(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", t.TempDir())
	if got := s.buildArgv(&pb.DeployRequest{Slug: "web"}); containsString(got, "--pull") {
		t.Errorf("a plain build must not pull: %v", got)
	}
	if got := s.buildArgv(&pb.DeployRequest{Slug: "web", PullBaseImages: true}); !containsString(got, "--pull") {
		t.Errorf("pull_base_images must add --pull: %v", got)
	}
}

func TestAdmitStackImages_pullFetchesEveryImage(t *testing.T) {
	argv := dockerOnPath(t, `case "$*" in
  *"config --images"*) echo nginx:1.27-alpine ;;
  *"image inspect"*) echo '{}' ;;
esac`)
	base := []string{"compose", "-p", "deplo-db", "-f", "/x.yml"}
	if err := admitStackImages(context.Background(), base, false); err != nil {
		t.Fatal(err)
	}
	if err := admitStackImages(context.Background(), base, true); err != nil {
		t.Fatal(err)
	}
	var pulls []string
	for _, l := range argv() {
		if strings.Contains(l, " pull ") || strings.HasSuffix(l, " pull") {
			pulls = append(pulls, l)
		}
	}
	if len(pulls) != 1 || strings.Contains(pulls[0], "--policy") {
		t.Fatalf("want exactly one unconditional pull, got %v", pulls)
	}
}
