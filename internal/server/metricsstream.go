package server

// https://deplo.build/docs/guides/monitoring

import (
	"context"
	"log"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/hostmetrics"
)

// metricsstream.go implements StreamMetrics: ONE long-lived stream per host carrying
// this machine's metrics plus every Deplo-managed container's stats, on the agent's own
// ticker.

// Cadence bounds for MetricsStreamRequest.interval_ms. A cadence is a hint from
// the control plane, never a lever for pinning the host: a caller asking for 10ms
// gets 1s, and one asking for an hour gets a minute.
const (
	defaultStreamInterval = 5 * time.Second
	minStreamInterval     = 1 * time.Second
	maxStreamInterval     = 60 * time.Second
)

// Backend names carried in MetricsSample.source.
const (
	sourceCgroup2     = "cgroup2"
	sourceDockerStats = "docker-stats"
)

// StreamMetrics streams host + container telemetry until the control plane
// disconnects, the deadline rotates the stream, or the process exits.
func (s *Service) StreamMetrics(req *pb.MetricsStreamRequest, stream pb.Agent_StreamMetricsServer) error {
	// The stream's context IS the lifetime, exactly as in FollowLogs: a control-plane
	// disconnect cancels it, which stops the ticker, the roster's `docker events` child
	// and every goroutine hanging off it.
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
			// Clean Canceled on disconnect rather than a misleading handler error.
			return ctx.Err()
		case <-t.C:
		}

		sample := buildSample(ctx, host, containers)
		if err := stream.Send(sample); err != nil {
			return err
		}
	}
}

// buildSample takes one tick's measurements. It recovers: without one, a single bad
// container takes the process down every tick and every healthy app on the host
// renders as unhealthy fleet-wide.
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
			// Whatever was assembled before the panic is discarded: a partial
			// frame is a fabricated one, and the honest rendering of a missed
			// tick is a gap. Host stays nil, which the control plane refuses.
			sample = &pb.MetricsSample{SampledAtUnixMs: time.Now().UnixMilli()}
		}
	}()

	running := 0
	if containers != nil {
		stats, source := containers.Sample(ctx)
		sample.Containers = stats
		sample.Source = source
		// The running-container count is the roster's CACHED host-wide figure, not a fresh
		// `docker ps -q` like the unary Metrics RPC does: that call costs ~190ms of dockerd
		// CPU, which on a 5s ticker would cost more than every metric in this frame combined.
		running = containers.RunningCount()
	} else {
		// include_containers=false skips per-container stats, but running_containers is a
		// HOST gauge that must not change meaning with that flag - the unary Metrics RPC
		// always reports it.
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

/* ------------------------------------------------------------------ */
/* Container sampling: roster + a backend                              */
/* ------------------------------------------------------------------ */

// containerSampler pairs the label-scoped roster (who is on this host) with
// whichever backend reads their numbers.
type containerSampler struct {
	ros *roster
	cg  *cgroupSampler
	// Once demoted, STAY demoted for the process lifetime.
	demoted bool
}

func newContainerSampler(ctx context.Context) *containerSampler {
	cs := &containerSampler{ros: newRoster(ctx)}
	if cgroup2Available() {
		cs.cg = newCgroupSampler()
		// Prime the CPU baseline NOW, so the stream's first frame already spans a real
		// window. Without this every container reported 0% on the opening frame, and the
		// 55-minute deadline rotation dropped that zero onto every app's chart hourly.
		if entries, _ := cs.ros.Snapshot(); len(entries) > 0 {
			cs.cg.Sample(entries, time.Now())
		}
	} else {
		// cgroup v1 or a hybrid hierarchy. Deliberately NOT solved - the fallback
		// is code that has been in production since ContainerStats shipped.
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
			// The cgroup reader has failed repeatedly - a delegated-controller gap
			// under rootless, a path shape we did not anticipate. Demote rather
			// than keep emitting a degraded series, and say so in `source`.
			log.Printf("deplo-agent: cgroup reads failing, demoting metrics stream to %s", sourceDockerStats)
			cs.demoted = true
		} else {
			return cs.cg.Sample(entries, time.Now()), sourceCgroup2
		}
	}
	return cs.dockerStatsSample(ctx, entries), sourceDockerStats
}

// dockerStatsSample is the fallback backend: ONE `docker stats --no-stream` for every
// running container on the host (not one per stack, which is what the per-project
// ContainerStats RPC does and what made the old model expensive), re-joined to the
// roster for identity.
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
			// Not running, or it vanished between the roster read and the stats read. Report it
			// with identity and running=false so the tab can say "stopped" rather than dropping
			// the container entirely, but never with synthesised usage numbers.
			st = &pb.ContainerStat{}
		}
		applyIdentity(st, e, ok && e.State == "running")
		out = append(out, st)
	}
	return out
}

// applyIdentity stamps the roster's view of WHO this container is onto a stat the
// backend produced.
func applyIdentity(st *pb.ContainerStat, e rosterEntry, running bool) {
	st.Name = e.Name
	st.ProjectId = e.ProjectID
	st.ContainerId = e.ID
	st.State = e.State
	st.Health = e.Health
	st.RestartCount = e.RestartCount
	st.Running = running
}

func (cs *containerSampler) sourceName() string {
	if cs.cg != nil && !cs.demoted {
		return sourceCgroup2
	}
	return sourceDockerStats
}

// RunningCount is the HOST-WIDE running-container count for the frame's host
// gauge - deliberately not the roster's label-scoped RunningCount(). See the
// roster's hostRunning field.
func (cs *containerSampler) RunningCount() int { return cs.ros.HostRunningCount() }

func (cs *containerSampler) Close() { cs.ros.Close() }
