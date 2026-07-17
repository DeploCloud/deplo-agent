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

// containerstats.go implements the ContainerStats RPC: a one-shot
// `docker stats --no-stream` snapshot for the named containers of ONE project —
// the agent-side data source for the per-app / per-database Monitoring tab.
//
// It is a sibling of instances.go and reuses listProjectContainers for the
// label scoping: only containers carrying deplo.project=<project_id> are ever
// stat'd, so a container name off the wire can never reach a sibling project's
// container (defence in depth, mirroring the Part C container RPCs).
//
// net_* / block_* come back as CUMULATIVE totals (that is all `docker stats`
// reports); the control plane derives bytes/sec from the delta between
// consecutive samples, which also survives a counter reset on restart.

// ContainerStats returns live resource usage for a project's containers.
func (s *Service) ContainerStats(ctx context.Context, req *pb.ContainerStatsRequest) (*pb.ContainerStatsResponse, error) {
	projectID := req.GetProjectId()
	// An empty project_id would drop the label filter and stat EVERY container on
	// the host (cross-tenant enumeration). Reject it, exactly like ListInstances.
	if projectID == "" {
		return nil, status.Error(codes.InvalidArgument, "project_id is required")
	}

	// Label-scoped set: the only containers this RPC may ever report on.
	cs, err := listProjectContainers(ctx, projectID)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]containerRow, len(cs))
	for _, c := range cs {
		allowed[c.Name] = c
	}

	// Which of the project's containers to report: the requested subset that
	// actually belongs to the project (any foreign name is dropped), or all of
	// them when none was requested.
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

	// `docker stats` only reports RUNNING containers; stat the running ones and
	// return a zeroed, running=false row for the rest so the tab can show
	// "stopped" honestly rather than dropping the container.
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
				// Vanished between the ps and the stats read (or no row emitted).
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

// collectContainerStats runs ONE `docker stats --no-stream` for all the named
// containers and parses each JSON line into a ContainerStat, keyed by name.
// Best-effort: a name missing from the output is simply absent from the map.
func collectContainerStats(ctx context.Context, names []string) map[string]*pb.ContainerStat {
	out := map[string]*pb.ContainerStat{}
	if len(names) == 0 {
		return out
	}
	// --no-stream still samples a CPU window (~1s), and runs all the named
	// containers in one process, so a per-stack snapshot is ~1-2s; give it the
	// same order-of-magnitude headroom as the other container RPCs.
	args := append([]string{"stats", "--no-stream", "--format", "{{json .}}"}, names...)
	res, err := dockercli.Run(ctx, 20*time.Second, args...)
	if err != nil {
		return out
	}
	// A non-zero code (a name that stopped mid-call) still leaves the found rows
	// on stdout — parse whatever came back, like inspectContainers does.
	for _, line := range strings.Split(strings.TrimSpace(res.Stdout), "\n") {
		if st, ok := parseStatsLine(line); ok {
			out[st.Name] = st
		}
	}
	return out
}

// parseStatsLine turns one `docker stats --format {{json .}}` line into a
// ContainerStat. Pure (no docker) so it is unit-testable; returns ok=false for a
// blank line or one that is not the expected JSON object.
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

// splitSizes parses a `docker stats` "A / B" pair (e.g. "10.5MiB / 1.944GiB",
// "1.2kB / 3.4kB", "0B / 8.19kB") into two byte counts, reusing cleanup.go's
// parseHumanSize (which already handles docker's SI + binary units and rounds).
// A missing side is 0.
func splitSizes(s string) (int64, int64) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		return parseHumanSize(s), 0
	}
	return parseHumanSize(parts[0]), parseHumanSize(parts[1])
}

// parsePercent turns "1.23%" into 1.23; 0 for "--" or anything unparseable.
func parsePercent(s string) float64 {
	s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), "%"))
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return n
}

// parsePids turns docker's PIDs column ("5") into an int32; 0 if unparseable.
func parsePids(s string) int32 {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0
	}
	return int32(n)
}
