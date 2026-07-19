package server

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// metricsstream_test.go covers the StreamMetrics handler over the real in-process
// gRPC harness (dialLocal — actual TCP on 127.0.0.1:0, not bufconn), so the
// context-cancellation semantics under test are the ones gRPC really delivers.
//
// Every test runs with include_containers:false, which keeps the whole file
// Docker-free: the container half is the roster's and the two backends' to prove,
// and CI has no daemon. What is left is exactly what belongs to the handler —
// cadence, clamping, and lifetime.
//
// Bounds are generous and no test asserts an exact tick count: CI runners are
// shared and a ticker that slips a beat under load is not a bug.

// The handler must actually emit on the cadence it was asked for. Two frames is
// the minimum that proves a TICKER rather than a single reply.
func TestStreamMetrics_emitsSamplesAtInterval(t *testing.T) {
	client, done := dialLocal(t)
	defer done()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	stream, err := client.StreamMetrics(ctx, &pb.MetricsStreamRequest{
		IntervalMs:        1000,
		IncludeContainers: false,
	})
	if err != nil {
		t.Fatalf("StreamMetrics: %v", err)
	}

	start := time.Now()
	for i := 0; i < 2; i++ {
		sample, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		if sample.GetHost() == nil {
			t.Fatalf("sample %d has no host metrics — the host half is never optional", i)
		}
		if sample.GetHost().GetCpuCores() < 1 {
			t.Errorf("sample %d reports %d cores", i, sample.GetHost().GetCpuCores())
		}
		if sample.GetSampledAtUnixMs() <= 0 {
			t.Errorf("sample %d has no agent timestamp", i)
		}
		// include_containers:false must mean NO container work was done at all,
		// not "the roster ran and found nothing".
		if len(sample.GetContainers()) != 0 {
			t.Errorf("sample %d carried %d containers despite include_containers:false",
				i, len(sample.GetContainers()))
		}
	}
	// Two frames at a 1s cadence cannot arrive in well under a second unless the
	// ticker is being ignored and the loop is spinning.
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("two frames arrived in %v; the 1s cadence is not being honoured", elapsed)
	}
}

// A cadence is a HINT. A control plane asking for 1ms must not be able to pin the
// host, and one asking for an hour must not be able to stall the charts. The
// clamp is asserted through observable timing, since the handler's clamped value
// is not on the wire.
func TestStreamMetrics_clampsIntervalAtBothEnds(t *testing.T) {
	t.Run("below the floor", func(t *testing.T) {
		client, done := dialLocal(t)
		defer done()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		stream, err := client.StreamMetrics(ctx, &pb.MetricsStreamRequest{IntervalMs: 1})
		if err != nil {
			t.Fatalf("StreamMetrics: %v", err)
		}
		start := time.Now()
		for i := 0; i < 2; i++ {
			if _, err := stream.Recv(); err != nil {
				t.Fatalf("Recv %d: %v", i, err)
			}
		}
		// Clamped to the 1s floor, two frames take ~2s. Un-clamped at 1ms they
		// would arrive almost immediately.
		if elapsed := time.Since(start); elapsed < 1500*time.Millisecond {
			t.Errorf("two frames in %v for interval_ms=1; expected the %v floor to apply",
				elapsed, minStreamInterval)
		}
	})

	t.Run("above the ceiling", func(t *testing.T) {
		// A pure unit assertion for the ceiling: waiting out a 60s clamp to
		// observe it would make this file take minutes.
		if got := clampInterval(2 * time.Hour); got != maxStreamInterval {
			t.Errorf("clampInterval(2h) = %v, want %v", got, maxStreamInterval)
		}
		if got := clampInterval(0); got != defaultStreamInterval {
			t.Errorf("clampInterval(0) = %v, want the default %v", got, defaultStreamInterval)
		}
		if got := clampInterval(-5 * time.Second); got != defaultStreamInterval {
			t.Errorf("clampInterval(negative) = %v, want the default %v", got, defaultStreamInterval)
		}
		if got := clampInterval(7 * time.Second); got != 7*time.Second {
			t.Errorf("clampInterval(7s) = %v, want it left alone", got)
		}
	})
}

// THE LEAK TEST. A handler that ignores stream.Context() keeps its ticker — and,
// with containers enabled, a `docker events` child process — alive for every
// client that ever disconnected. Cancellation must propagate and the RPC must
// terminate as Canceled rather than hanging or reporting a spurious error.
func TestStreamMetrics_clientCancelEndsTheStream(t *testing.T) {
	client, done := dialLocal(t)
	defer done()
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := client.StreamMetrics(ctx, &pb.MetricsStreamRequest{IntervalMs: 1000})
	if err != nil {
		t.Fatalf("StreamMetrics: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}

	cancel()

	// The next Recv must fail promptly with Canceled.
	deadline := time.After(10 * time.Second)
	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Recv succeeded after cancel; the handler ignored ctx.Done()")
		}
		if got := status.Code(err); got != codes.Canceled {
			t.Errorf("after cancel got code %v (%v), want Canceled", got, err)
		}
	case <-deadline:
		t.Fatal("Recv did not return within 10s of cancel — the handler is not watching ctx.Done()")
	}
}

// buildSample must survive a panic by losing ONE FRAME, never the stream. A
// handler that propagates the panic kills telemetry for the whole host, and the
// control plane then reconnects straight back into it every few seconds.
//
// Driven directly rather than through gRPC because the panic has to be injected:
// a nil *containerSampler with a non-nil interface would be the realistic
// trigger, and here a deliberately nil host sampler stands in for any nil
// dereference inside the tick.
func TestBuildSample_panicCostsOneFrameNotTheStream(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("buildSample let a panic escape: %v", r)
		}
	}()

	// A nil *hostmetrics.Sampler panics on the method call inside the tick.
	sample := buildSample(context.Background(), nil, nil)

	if sample == nil {
		t.Fatal("buildSample returned nil; callers dereference the frame")
	}
	// The recovered frame must carry NO host metrics. A partial frame is a
	// fabricated one — the control plane refuses a frame with no host half,
	// which renders the honest gap.
	if sample.GetHost() != nil {
		t.Error("a recovered frame carried host metrics; a partial frame must not be emitted")
	}
	if sample.GetSampledAtUnixMs() <= 0 {
		t.Error("a recovered frame should still be timestamped, for diagnosability")
	}
}
