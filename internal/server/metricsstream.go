package server

import (
	"context"
	"log"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/hostmetrics"
)

const (
	defaultStreamInterval = 5 * time.Second
	minStreamInterval     = 1 * time.Second
	maxStreamInterval     = 60 * time.Second
)

const (
	sourceCgroup2     = "cgroup2"
	sourceDockerStats = "docker-stats"
)

// StreamMetrics streams host + container telemetry until the control plane disconnects, the deadline rotates the stream, or the process exits.
func (s *Service) StreamMetrics(req *pb.MetricsStreamRequest, stream pb.Agent_StreamMetricsServer) error {
	ctx := stream.Context()

	dataDir := req.GetDataDir()
	if dataDir == "" {
		dataDir = s.dataDir
	}
	interval := clampInterval(time.Duration(req.GetIntervalMs()) * time.Millisecond)

	host := hostmetrics.NewSampler(dataDir)

	var containers *containerSampler
	if req.GetIncludeContainers() {
		containers = newContainerSampler(ctx)
		defer containers.Close()
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}

		sample := buildSample(ctx, host, containers)
		if err := stream.Send(sample); err != nil {
			return err
		}
	}
}

func buildSample(
	ctx context.Context,
	host *hostmetrics.Sampler,
	containers *containerSampler,
) (sample *pb.MetricsSample) {
	sample = &pb.MetricsSample{
		SampledAtUnixMs: time.Now().UnixMilli(),
		Source:          sourceDockerStats,
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("deplo-agent: metrics sample panicked, skipping this frame: %v", r)
			sample = &pb.MetricsSample{SampledAtUnixMs: time.Now().UnixMilli()}
		}
	}()

	running := 0
	if containers != nil {
		stats, source := containers.Sample(ctx)
		sample.Containers = stats
		sample.Source = source
		running = containers.RunningCount()
	} else {
		running = dockercli.RunningContainers(ctx)
	}
	sample.Host = hostMetricsPB(host.Sample(), running)
	return sample
}

func clampInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultStreamInterval
	}
	if d < minStreamInterval {
		return minStreamInterval
	}
	if d > maxStreamInterval {
		return maxStreamInterval
	}
	return d
}

type containerSampler struct {
	ros     *roster
	cg      *cgroupSampler
	demoted bool
}

func newContainerSampler(ctx context.Context) *containerSampler {
	cs := &containerSampler{ros: newRoster(ctx)}
	if cgroup2Available() {
		cs.cg = newCgroupSampler()
		if entries, _ := cs.ros.Snapshot(); len(entries) > 0 {
			cs.cg.Sample(entries, time.Now())
		}
	} else {
		log.Printf("deplo-agent: cgroup v2 unavailable, metrics stream using %s", sourceDockerStats)
	}
	return cs
}

// Sample returns this tick's container stats and the backend that produced them.
func (cs *containerSampler) Sample(ctx context.Context) ([]*pb.ContainerStat, string) {
	entries, _ := cs.ros.Snapshot()
	if len(entries) == 0 {
		return nil, cs.sourceName()
	}

	if cs.cg != nil && !cs.demoted {
		if cs.cg.Unhealthy() {
			log.Printf("deplo-agent: cgroup reads failing, demoting metrics stream to %s", sourceDockerStats)
			cs.demoted = true
		} else {
			return cs.cg.Sample(entries, time.Now()), sourceCgroup2
		}
	}
	return cs.dockerStatsSample(ctx, entries), sourceDockerStats
}

func (cs *containerSampler) dockerStatsSample(ctx context.Context, entries []rosterEntry) []*pb.ContainerStat {
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.State == "running" {
			names = append(names, e.Name)
		}
	}

	stats := map[string]*pb.ContainerStat{}
	if len(names) > 0 {
		stats = collectContainerStats(ctx, names)
	}

	out := make([]*pb.ContainerStat, 0, len(entries))
	for _, e := range entries {
		st, ok := stats[e.Name]
		if !ok {
			st = &pb.ContainerStat{}
		}
		applyIdentity(st, e, ok && e.State == "running")
		out = append(out, st)
	}
	return out
}

func applyIdentity(st *pb.ContainerStat, e rosterEntry, running bool) {
	st.Name = e.Name
	st.ProjectId = e.ProjectID
	st.ContainerId = e.ID
	st.State = e.State
	st.Health = e.Health
	st.RestartCount = e.RestartCount
	st.OomKills = e.OomKills
	st.Running = running
}

func (cs *containerSampler) sourceName() string {
	if cs.cg != nil && !cs.demoted {
		return sourceCgroup2
	}
	return sourceDockerStats
}

// RunningCount is the HOST-WIDE running-container count for the frame's host gauge - deliberately not the roster's label-scoped RunningCount().
func (cs *containerSampler) RunningCount() int { return cs.ros.HostRunningCount() }

func (cs *containerSampler) Close() { cs.ros.Close() }
