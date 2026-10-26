package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/hostmetrics"
)

// The package reader every inventory uses, pinned with the checksum of its release archive.
const syftVersion = "1.54.1"

var syftSHA256 = map[string]string{
	"amd64": "c069905b391cc4c20a5ba65ad5c10be2a7ba074f8ea6ad203e24d14e303dad47",
	"arm64": "dfdf0537610113edbefe1f1fc6548bc957b2d77439636ec824fcf0e10d46d054",
}

const (
	inventoryTimeout = 10 * time.Minute
	inventoryMinCap  = int64(512) << 20
	inventoryMaxCap  = int64(2) << 30
	inventoryHeadway = int64(512) << 20
)

var imageIDPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Seams for tests: the host's memory, its free disk, and whether it can cap a process's memory.
var (
	inventoryMemory = hostmetrics.Memory
	inventoryFree   = func(dir string) int64 {
		if dir == "" {
			dir = os.TempDir()
		}
		free, _ := storeFreeBytes(dir)
		return free
	}
	inventoryCanCap = func() bool {
		_, err := exec.LookPath("systemd-run")
		_, serr := os.Stat("/run/systemd/system")
		return err == nil && serr == nil
	}
)

const workloadInspectTemplate = `{"id":{{json .ID}},` +
	`"image":{{json .Image}},` +
	`"ref":{{json .Config.Image}},` +
	`"running":{{json .State.Running}},` +
	`"project":{{json (index .Config.Labels "deplo.project")}},` +
	`"app":{{json (index .Config.Labels "deplo.app")}},` +
	`"service":{{json (index .Config.Labels "com.docker.compose.service")}}}`

// ListWorkloadImages names the image behind every Deplo-managed container, running or stopped.
func (s *Service) ListWorkloadImages(ctx context.Context, _ *pb.ListWorkloadImagesRequest) (*pb.ListWorkloadImagesResponse, error) {
	images, err := listWorkloadImages(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &pb.ListWorkloadImagesResponse{Images: images}, nil
}

func listWorkloadImages(ctx context.Context) ([]*pb.WorkloadImage, error) {
	rows, err := listManagedContainers(ctx)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	res, err := dockercli.Run(ctx, 30*time.Second, append([]string{"inspect", "-f", workloadInspectTemplate}, ids...)...)
	if err != nil {
		return nil, err
	}
	var out []*pb.WorkloadImage
	imageIDs := map[string]bool{}
	for _, line := range splitLines(res.Stdout) {
		var c struct {
			Image, Ref, Project, App, Service string
			Running                           bool
		}
		if json.Unmarshal([]byte(line), &c) != nil || c.Project == "" || c.Image == "" {
			continue
		}
		imageIDs[c.Image] = true
		out = append(out, &pb.WorkloadImage{
			ProjectId: c.Project, AppId: c.App, Service: c.Service,
			ImageRef: c.Ref, ImageId: c.Image, Running: c.Running,
		})
	}
	if res.Code != 0 && len(out) == 0 {
		return nil, fmt.Errorf("docker inspect exited %d: %s", res.Code, strings.TrimSpace(res.Stderr))
	}
	digests := repoDigests(ctx, imageIDs)
	for _, w := range out {
		w.RepoDigests = digests[w.ImageId]
	}
	return out, nil
}

func repoDigests(ctx context.Context, ids map[string]bool) map[string][]string {
	args := []string{"image", "inspect", "-f", `{"id":{{json .Id}},"digests":{{json .RepoDigests}}}`}
	for id := range ids {
		args = append(args, id)
	}
	out := map[string][]string{}
	res, err := dockercli.Run(ctx, 30*time.Second, args...)
	if err != nil {
		return out
	}
	for _, line := range splitLines(res.Stdout) {
		var d struct {
			ID      string   `json:"id"`
			Digests []string `json:"digests"`
		}
		if json.Unmarshal([]byte(line), &d) == nil && d.ID != "" {
			out[d.ID] = d.Digests
		}
	}
	return out
}

// InventoryImage reads the package list of one workload image with syft.
func (s *Service) InventoryImage(ctx context.Context, req *pb.InventoryImageRequest) (*pb.InventoryImageResponse, error) {
	id := req.GetImageId()
	if !imageIDPattern.MatchString(id) {
		return nil, status.Error(codes.InvalidArgument, "image_id must be sha256:<64 hex>")
	}
	if !s.inventoryMu.TryLock() {
		return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_BUSY, "another image is being checked"), nil
	}
	defer s.inventoryMu.Unlock()
	if s.deployRunning() {
		return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_BUSY, "a deploy is running on this server"), nil
	}

	workloads, err := listWorkloadImages(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if !runsImage(workloads, id) {
		return nil, status.Error(codes.InvalidArgument, "no Deplo workload on this server uses that image")
	}

	start := time.Now()
	resp, err := s.inventory(ctx, id)
	if err != nil {
		return nil, err
	}
	resp.DurationMs = time.Since(start).Milliseconds()
	return resp, nil
}

func (s *Service) inventory(ctx context.Context, id string) (*pb.InventoryImageResponse, error) {
	res, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", "-f", `{"size":{{json .Size}},"wd":{{json .Config.WorkingDir}}}`, id)
	if err != nil || res.Code != 0 {
		return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_FAILED, "inspect the image: "+strings.TrimSpace(res.Stderr)), nil
	}
	var img struct {
		Size int64  `json:"size"`
		WD   string `json:"wd"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(res.Stdout)), &img); err != nil {
		return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_FAILED, "read the image size: "+err.Error()), nil
	}

	if free := inventoryFree(s.buildTmpDir); free < 2*img.Size {
		return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_SKIPPED_LOW_DISK,
			fmt.Sprintf("needs %s free, %s left", humanBytes(2*img.Size), humanBytes(free))), nil
	}
	if !inventoryCanCap() {
		return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_UNSUPPORTED, "this server cannot cap the reader's memory"), nil
	}
	total, avail := inventoryMemory()
	memCap := min(max(total/4, inventoryMinCap), inventoryMaxCap)
	if avail < memCap+inventoryHeadway {
		return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_SKIPPED_LOW_MEMORY,
			fmt.Sprintf("needs %s of free memory, %s left", humanBytes(memCap+inventoryHeadway), humanBytes(avail))), nil
	}

	syft, err := s.ensureSyft(ctx)
	if err != nil {
		return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_FAILED, err.Error()), nil
	}
	tmp, err := os.MkdirTemp(s.buildTmpDir, inventoryDirPrefix)
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)

	cctx, cancel := context.WithTimeout(ctx, inventoryTimeout)
	defer cancel()
	tarPath := filepath.Join(tmp, "image.tar")
	tail := &lineTail{max: 20}
	argv := lowPriority("docker", "save", "-o", tarPath, id)
	if code, err := dockercli.SpawnEnv(cctx, inventoryTimeout, tail.add, nil, argv[0], argv[1:]...); err != nil || code != 0 {
		return inventoryRunFailed(cctx, "save the image", err, code, tail), nil
	}

	outPath := filepath.Join(tmp, "sbom.json")
	env := []string{
		"SYFT_CHECK_FOR_APP_UPDATE=false", "SYFT_PARALLELISM=1", "TMPDIR=" + tmp,
		"GOMEMLIMIT=" + strconv.FormatInt(memCap*3/4, 10),
		"SYFT_FILE_METADATA_SELECTION=none", "SYFT_FILE_METADATA_DIGESTS=",
	}
	tail = &lineTail{max: 20}
	code, err := dockercli.SpawnEnv(cctx, inventoryTimeout, tail.add, env, "systemd-run",
		"--quiet", "--scope", "--collect", "--nice=19",
		"-p", "MemoryMax="+strconv.FormatInt(memCap, 10), "-p", "MemorySwapMax=0", "-p", "IOWeight=10",
		"--", syft, "scan", "docker-archive:"+tarPath, "--select-catalogers", "+"+lockCataloger,
		"-o", "syft-json="+outPath, "-q")
	if err != nil || code != 0 {
		if killedBySignal(err) && cctx.Err() == nil {
			return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_TOO_LARGE, "the image is too large to check on this server"), nil
		}
		return inventoryRunFailed(cctx, "read the packages", err, code, tail), nil
	}

	resp, doc, err := parseSyftJSON(outPath)
	if err != nil {
		return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_FAILED, "read the package list: "+err.Error()), nil
	}
	resp.WorkingDir = img.WD
	applyDepMarks(resp, doc, markDeps(doc, img.WD, readAppManifest(ctx, id, img.WD)))
	return resp, nil
}

const inventoryDirPrefix = "deplo-inventory-"

// sweepInventoryDirs removes what an inventory cut short by a restart left in the build temp dir.
func sweepInventoryDirs(dir string) {
	if dir == "" {
		dir = os.TempDir()
	}
	left, _ := filepath.Glob(filepath.Join(dir, inventoryDirPrefix+"*"))
	for _, p := range left {
		_ = os.RemoveAll(p)
	}
}

func (s *Service) deployRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.deploys {
		f.mu.Lock()
		done := f.done
		f.mu.Unlock()
		if !done {
			return true
		}
	}
	return false
}

func runsImage(workloads []*pb.WorkloadImage, id string) bool {
	for _, w := range workloads {
		if w.GetImageId() == id {
			return true
		}
	}
	return false
}

// lowPriority runs a command under `nice` and idle-class `ionice` when the host has both.
func lowPriority(name string, args ...string) []string {
	_, nerr := exec.LookPath("nice")
	_, ierr := exec.LookPath("ionice")
	if nerr != nil || ierr != nil {
		return append([]string{name}, args...)
	}
	return append([]string{"nice", "-n", "19", "ionice", "-c", "3", name}, args...)
}

func humanBytes(n int64) string {
	return fmt.Sprintf("%.1f GB", float64(n)/(1<<30))
}

func killedBySignal(err error) bool {
	var ee *exec.ExitError
	return errors.As(err, &ee) && ee.ProcessState != nil && !ee.ProcessState.Exited()
}

func inventoryRunFailed(cctx context.Context, what string, err error, code int, tail *lineTail) *pb.InventoryImageResponse {
	if errors.Is(cctx.Err(), context.DeadlineExceeded) {
		return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_TIMED_OUT, what+" took longer than "+inventoryTimeout.String())
	}
	msg := fmt.Sprintf("%s: exit %d", what, code)
	if err != nil {
		msg = what + ": " + err.Error()
	}
	if last := strings.TrimSpace(strings.Join(tail.lines, "\n")); last != "" {
		msg += ": " + last
	}
	return inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_FAILED, msg)
}

func inventoryStatus(st pb.InventoryStatus, msg string) *pb.InventoryImageResponse {
	return &pb.InventoryImageResponse{Status: st, Error: msg}
}

func parseSyftJSON(path string) (*pb.InventoryImageResponse, *syftDoc, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	var doc syftDoc
	if err := json.NewDecoder(f).Decode(&doc); err != nil {
		return nil, nil, err
	}
	resp := &pb.InventoryImageResponse{
		Status:         pb.InventoryStatus_INVENTORY_STATUS_OK,
		DistroId:       doc.Distro.ID,
		DistroVersion:  doc.Distro.VersionID,
		ScannerVersion: doc.Descriptor.Version,
	}
	for _, a := range doc.Artifacts {
		// Lockfile entries only feed the dependency graph; the installed copy is what runs.
		if a.Purl == "" || a.FoundBy == lockCataloger {
			continue
		}
		p := &pb.ImagePackage{Purl: a.Purl, Name: a.Name, Version: a.Version, Type: a.Type}
		if len(a.Locations) > 0 {
			p.Location = a.Locations[0].Path
		}
		resp.Packages = append(resp.Packages, p)
	}
	return resp, &doc, nil
}

// applyDepMarks copies build_only and via onto the packages they belong to.
func applyDepMarks(resp *pb.InventoryImageResponse, doc *syftDoc, marks map[string]depMark) {
	if len(marks) == 0 {
		return
	}
	byPkg := map[string]depMark{}
	for _, a := range doc.Artifacts {
		if m, ok := marks[a.ID]; ok && len(a.Locations) > 0 {
			byPkg[a.Purl+"\x00"+a.Locations[0].Path] = m
		}
	}
	for _, p := range resp.Packages {
		if m, ok := byPkg[p.Purl+"\x00"+p.Location]; ok {
			p.BuildOnly, p.Via = m.buildOnly, m.via
		}
	}
}

func (s *Service) ensureSyft(ctx context.Context) (string, error) {
	dest := filepath.Join(s.dataBase, "tools", "syft-"+syftVersion)
	if usableBinary(dest) {
		return dest, nil
	}
	sum, ok := syftSHA256[runtime.GOARCH]
	if !ok || runtime.GOOS != "linux" {
		return "", fmt.Errorf("no pinned syft for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	url := fmt.Sprintf("https://github.com/anchore/syft/releases/download/v%s/syft_%s_linux_%s.tar.gz",
		syftVersion, syftVersion, runtime.GOARCH)
	if err := installTarBinary(ctx, url, "syft", dest, sum); err != nil {
		return "", fmt.Errorf("install syft: %w", err)
	}
	return dest, nil
}
