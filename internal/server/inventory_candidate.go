package server

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// A newer build is always pinned by digest, so the panel names exactly what gets read.
var candidateRefPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._/:-]*@sha256:[0-9a-f]{64}$`)

// InventoryCandidate pulls a newer build of a running workload's image, reads it, and drops it
// again when it was not on the host before and nothing runs it.
func (s *Service) InventoryCandidate(ctx context.Context, req *pb.InventoryCandidateRequest) (*pb.InventoryCandidateResponse, error) {
	ref, cur := req.GetImageRef(), req.GetCurrentImageId()
	if !candidateRefPattern.MatchString(ref) || strings.HasPrefix(ref, "deplo/") {
		return nil, status.Error(codes.InvalidArgument, "image_ref must be repository@sha256:<64 hex>, outside deplo/")
	}
	if !imageIDPattern.MatchString(cur) {
		return nil, status.Error(codes.InvalidArgument, "current_image_id must be sha256:<64 hex>")
	}
	if !s.inventoryMu.TryLock() {
		return candidate(inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_BUSY, "another image is being checked"), ""), nil
	}
	defer s.inventoryMu.Unlock()
	if s.deployRunning() {
		return candidate(inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_BUSY, "a deploy is running on this server"), ""), nil
	}

	workloads, err := listWorkloadImages(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if !runsRepository(workloads, cur, ref) {
		return nil, status.Error(codes.InvalidArgument, "no Deplo workload runs that image from that repository")
	}

	// The newer build is about the running one's size: pulling, then saving it, takes up to three.
	size, err := imageSize(ctx, cur)
	if err != nil {
		return candidate(inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_FAILED, err.Error()), ""), nil
	}
	if free := inventoryFree(s.buildTmpDir); free < 3*size {
		return candidate(inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_SKIPPED_LOW_DISK,
			fmt.Sprintf("needs %s free, %s left", humanBytes(3*size), humanBytes(free))), ""), nil
	}

	start := time.Now()
	present := imageID(ctx, ref) != ""
	if res, err := dockercli.Run(ctx, inventoryTimeout, "pull", "--quiet", ref); err != nil || res.Code != 0 {
		msg := strings.TrimSpace(res.Stderr)
		if err != nil {
			msg = err.Error()
		}
		return candidate(inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_FAILED, "pull the newer build: "+msg), ""), nil
	}
	id := imageID(ctx, ref)
	if !present {
		defer dropUnusedCandidate(ref, id)
	}
	if id == "" {
		return candidate(inventoryStatus(pb.InventoryStatus_INVENTORY_STATUS_FAILED, "the pulled build has no image id"), ""), nil
	}
	resp, err := s.inventory(ctx, id)
	if err != nil {
		return nil, err
	}
	resp.DurationMs = time.Since(start).Milliseconds()
	return candidate(resp, id), nil
}

func candidate(r *pb.InventoryImageResponse, id string) *pb.InventoryCandidateResponse {
	return &pb.InventoryCandidateResponse{Result: r, ImageId: id}
}

// runsRepository: a workload runs the current image, and the candidate comes from its repository.
func runsRepository(workloads []*pb.WorkloadImage, cur, ref string) bool {
	want := repository(ref)
	for _, w := range workloads {
		if w.GetImageId() == cur && repository(w.GetImageRef()) == want {
			return true
		}
	}
	return false
}

// repository is a reference without its tag or digest, with Docker Hub's implied parts spelled out.
func repository(ref string) string {
	name, _, _ := strings.Cut(ref, "@")
	if i := strings.LastIndex(name, ":"); i > strings.LastIndex(name, "/") {
		name = name[:i]
	}
	first, _, found := strings.Cut(name, "/")
	if !found || (!strings.ContainsAny(first, ".:") && first != "localhost") {
		name = "docker.io/" + name
	}
	if rest, ok := strings.CutPrefix(name, "docker.io/"); ok && !strings.Contains(rest, "/") {
		name = "docker.io/library/" + rest
	}
	return strings.ToLower(name)
}

func imageSize(ctx context.Context, id string) (int64, error) {
	res, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", "-f", `{"size":{{json .Size}}}`, id)
	if err != nil || res.Code != 0 {
		return 0, fmt.Errorf("inspect the running image: %s", strings.TrimSpace(res.Stderr))
	}
	var img struct {
		Size int64 `json:"size"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(res.Stdout)), &img); err != nil {
		return 0, fmt.Errorf("read the running image's size: %w", err)
	}
	return img.Size, nil
}

// imageID is the local id a reference resolves to, or "" when it is not on this host.
func imageID(ctx context.Context, ref string) string {
	res, err := dockercli.Run(ctx, 30*time.Second, "image", "inspect", "-f", "{{.Id}}", ref)
	if err != nil || res.Code != 0 {
		return ""
	}
	return strings.TrimSpace(res.Stdout)
}

// dropUnusedCandidate removes a build pulled only to be read, unless a container took it since.
func dropUnusedCandidate(ref, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if id != "" {
		if workloads, err := listWorkloadImages(ctx); err == nil && runsImage(workloads, id) {
			return
		}
	}
	_, _ = dockercli.Run(ctx, time.Minute, "image", "rm", ref)
}
