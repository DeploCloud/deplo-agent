package server

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// ContainerStats returns live resource usage for a project's containers.
func (s *Service) ContainerStats(ctx context.Context, req *pb.ContainerStatsRequest) (*pb.ContainerStatsResponse, error) {
	projectID := req.GetProjectId()
	if projectID == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}

	cs, err := listProjectContainers(ctx, projectID)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]containerRow, len(cs))
	for _, c := range cs {
		allowed[c.Name] = c
	}

	var names []string
	if requested := req.GetContainers(); len(requested) > 0 {
		for _, n := range requested {
			if _, ok := allowed[n]; ok {
				names = append(names, n)
			}
		}
	} else {
		for _, c := range cs {
			names = append(names, c.Name)
		}
	}
	if len(names) == 0 {
		return &pb.ContainerStatsResponse{}, nil
	}

	running := make([]string, 0, len(names))
	out := make([]*pb.ContainerStat, 0, len(names))
	for _, n := range names {
		if allowed[n].State == "running" {
			running = append(running, n)
		} else {
			out = append(out, &pb.ContainerStat{Name: n, Running: false})
		}
	}

	if len(running) > 0 {
		stats := collectContainerStats(ctx, running)
		for _, n := range running {
			st, ok := stats[n]
			if !ok {
				out = append(out, &pb.ContainerStat{Name: n, Running: false})
				continue
			}
			st.Name = n
			st.Running = true
			out = append(out, st)
		}
	}
	return &pb.ContainerStatsResponse{Stats: out}, nil
}

func collectContainerStats(ctx context.Context, names []string) map[string]*pb.ContainerStat {
	out := map[string]*pb.ContainerStat{}
	if len(names) == 0 {
		return out
	}
	args := append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, names...)
	res, err := dockercli.Run(ctx, 20*time.Second, args...)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		if st, ok := parseStatsLine(line); ok {
			out[st.Name] = st
		}
	}
	return out
}

func parseStatsLine(line string) (*pb.ContainerStat, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, false
	}
	var raw struct {
		Name     string `json:"Name"`
		CPUPerc  string `json:"CPUPerc"`
		MemUsage string `json:"MemUsage"`
		MemPerc  string `json:"MemPerc"`
		NetIO    string `json:"NetIO"`
		BlockIO  string `json:"BlockIO"`
		PIDs     string `json:"PIDs"`
	}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return nil, false
	}
	memUsed, memLimit := splitSizes(raw.MemUsage)
	netRx, netTx := splitSizes(raw.NetIO)
	blkRead, blkWrite := splitSizes(raw.BlockIO)
	return &pb.ContainerStat{
		Name:       raw.Name,
		CpuPct:     parsePercent(raw.CPUPerc),
		MemUsed:    memUsed,
		MemLimit:   memLimit,
		MemPct:     parsePercent(raw.MemPerc),
		NetRx:      netRx,
		NetTx:      netTx,
		BlockRead:  blkRead,
		BlockWrite: blkWrite,
		Pids:       parsePids(raw.PIDs),
	}, true
}

func splitSizes(s string) (int64, int64) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		return parseHumanSize(s), 0
	}
	return parseHumanSize(parts[0]), parseHumanSize(parts[1])
}

func parsePercent(s string) float64 {
	s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), "%"))
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return n
}

func parsePids(s string) int32 {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0
	}
	return int32(n)
}
