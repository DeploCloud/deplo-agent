package server

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// The handler must actually emit on the cadence it was asked for.
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
			t.Fatalf("sample %d has no host metrics - the host half is never optional", i)
		}
		if sample.GetHost().GetCpuCores() < 1 {
			t.Errorf("sample %d reports %d cores", i, sample.GetHost().GetCpuCores())
		}
		if sample.GetSampledAtUnixMs() <= 0 {
			t.Errorf("sample %d has no agent timestamp", i)
		}
		if len(sample.GetContainers()) != 0 {
			t.Errorf("sample %d carried %d containers despite include_containers:false",
				i, len(sample.GetContainers()))
		}
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("two frames arrived in %v; the 1s cadence is not being honoured", elapsed)
	}
}

// A cadence is a HINT.
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
		if elapsed := time.Since(start); elapsed < 1500*time.Millisecond {
			t.Errorf("two frames in %v for interval_ms=1; expected the %v floor to apply",
				elapsed, minStreamInterval)
		}
	})

	t.Run("above the ceiling", func(t *testing.T) {
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

// THE LEAK TEST.
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
		t.Fatal("Recv did not return within 10s of cancel - the handler is not watching ctx.Done()")
	}
}

// buildSample must survive a panic by losing ONE FRAME, never the stream.
func TestBuildSample_panicCostsOneFrameNotTheStream(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("buildSample let a panic escape: %v", r)
		}
	}()

	sample := buildSample(context.Background(), nil, nil)

	if sample == nil {
		t.Fatal("buildSample returned nil; callers dereference the frame")
	}
	if sample.GetHost() != nil {
		t.Error("a recovered frame carried host metrics; a partial frame must not be emitted")
	}
	if sample.GetSampledAtUnixMs() <= 0 {
		t.Error("a recovered frame should still be timestamped, for diagnosability")
	}
}
