package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func logEvent(text string) *pb.DeployEvent {
	return &pb.DeployEvent{Event: &pb.DeployEvent_Log{Log: &pb.LogLine{Level: "info", Text: text}}}
}
func resultEvent(ready bool) *pb.DeployEvent {
	return &pb.DeployEvent{Event: &pb.DeployEvent_Result{Result: &pb.DeployResult{Ready: ready}}}
}
func phaseEvent(p pb.DeployPhase) *pb.DeployEvent {
	return &pb.DeployEvent{Event: &pb.DeployEvent_Phase{Phase: &pb.PhaseChange{Phase: p}}}
}

// A subscriber from seq 0 receives every event in order, including the terminal
// result, then the subscription returns.
func TestInflight_subscribeFromStart(t *testing.T) {
	f := newInflight(func() {})
	go func() {
		f.append(logEvent("a"))
		f.append(logEvent("b"))
		f.append(resultEvent(true))
	}()

	var got []*pb.DeployEvent
	err := f.subscribe(context.Background(), 0, func(ev *pb.DeployEvent) error {
		got = append(got, ev)
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 events, got %d", len(got))
	}
	for i, ev := range got {
		if ev.GetSeq() != uint64(i+1) {
			t.Fatalf("event %d has seq %d", i, ev.GetSeq())
		}
	}
	if got[2].GetResult() == nil {
		t.Fatalf("last event is not the terminal result")
	}
}

// A reattach with from_seq replays only events past the cursor — the core of D5:
// a control plane that saw seq 1 reconnects with from_seq=1 and gets 2,3 only.
func TestInflight_reattachReplaysPastCursor(t *testing.T) {
	f := newInflight(func() {})
	f.append(logEvent("a")) // seq 1
	f.append(logEvent("b")) // seq 2
	go func() {
		time.Sleep(20 * time.Millisecond)
		f.append(resultEvent(true)) // seq 3
	}()

	var got []*pb.DeployEvent
	err := f.subscribe(context.Background(), 1, func(ev *pb.DeployEvent) error {
		got = append(got, ev)
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if len(got) != 2 || got[0].GetSeq() != 2 || got[1].GetSeq() != 3 {
		t.Fatalf("expected seq [2,3], got %v", seqs(got))
	}
}

// Two concurrent subscribers both see the full stream — a live Deploy reader and
// a reattacher do not steal events from each other.
func TestInflight_twoSubscribers(t *testing.T) {
	f := newInflight(func() {})
	var wg sync.WaitGroup
	collect := func(out *[]*pb.DeployEvent) {
		defer wg.Done()
		_ = f.subscribe(context.Background(), 0, func(ev *pb.DeployEvent) error {
			*out = append(*out, ev)
			return nil
		})
	}
	var a, b []*pb.DeployEvent
	wg.Add(2)
	go collect(&a)
	go collect(&b)
	time.Sleep(10 * time.Millisecond) // let both attach
	f.append(logEvent("x"))
	f.append(resultEvent(false))
	wg.Wait()
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("both subscribers should see 2 events, got %d and %d", len(a), len(b))
	}
}

// Cancelling a subscriber's context detaches it without affecting the deploy or
// other subscribers (the build goes on).
func TestInflight_subscriberCancelDetaches(t *testing.T) {
	f := newInflight(func() {})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- f.subscribe(ctx, 0, func(ev *pb.DeployEvent) error { return nil })
	}()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("cancelled subscriber should return its ctx error")
		}
	case <-time.After(time.Second):
		t.Fatalf("cancelled subscriber did not return")
	}
	// The inflight is still usable: a fresh subscriber still completes.
	go func() { f.append(resultEvent(true)) }()
	if err := f.subscribe(context.Background(), 0, func(*pb.DeployEvent) error { return nil }); err != nil {
		t.Fatalf("post-detach subscribe failed: %v", err)
	}
}

// A verbose/malicious build cannot grow the retained buffer without bound: log
// events are capped by count, the oldest are coalesced into a single note, yet
// the phase and terminal result events are always retained and the newest log
// line survives — so reattach still yields a truncated log + a note + the result.
func TestInflight_logRetentionIsBounded(t *testing.T) {
	f := newInflight(func() {})
	f.append(phaseEvent(pb.DeployPhase_DEPLOY_PHASE_BUILDING)) // structural, must survive
	total := maxRetainedLogEvents + 500
	for i := 0; i < total; i++ {
		f.append(logEvent(fmt.Sprintf("line %d", i)))
	}
	f.append(resultEvent(true))

	f.mu.Lock()
	events := append([]*pb.DeployEvent(nil), f.events...)
	dropped := f.droppedLog
	logCount := f.logCount
	f.mu.Unlock()

	if dropped == 0 {
		t.Fatalf("expected some log events to be trimmed, dropped=0")
	}
	if logCount > maxRetainedLogEvents {
		t.Fatalf("retained real logs %d exceed cap %d", logCount, maxRetainedLogEvents)
	}
	// Buffer bounded: real logs + note + phase + result, never the full flood.
	if len(events) >= total {
		t.Fatalf("buffer not bounded: retained %d of %d events", len(events), total)
	}

	var sawPhase, sawResult, sawNote, sawNewest bool
	newest := fmt.Sprintf("line %d", total-1)
	for _, ev := range events {
		switch {
		case ev.GetPhase() != nil:
			sawPhase = true
		case ev.GetResult() != nil:
			sawResult = true
		case ev.GetLog() != nil:
			if ev.GetLog().GetLevel() == "warn" && strings.Contains(ev.GetLog().GetText(), "trimmed") {
				sawNote = true
			}
			if ev.GetLog().GetText() == newest {
				sawNewest = true
			}
		}
	}
	if !sawPhase {
		t.Fatalf("phase event was evicted; structural events must be retained")
	}
	if !sawResult {
		t.Fatalf("terminal result was evicted; it must always be retained")
	}
	if !sawNote {
		t.Fatalf("no truncation note in the retained buffer")
	}
	if !sawNewest {
		t.Fatalf("newest log line %q was evicted; the most recent line must survive", newest)
	}

	// Seqs remain strictly ascending across the (trimmed) buffer, so the replay
	// cursor stays well-defined.
	var prev uint64
	for _, ev := range events {
		if ev.GetSeq() <= prev {
			t.Fatalf("seq not ascending: %d after %d", ev.GetSeq(), prev)
		}
		prev = ev.GetSeq()
	}

	// A fresh subscriber still terminates and ends on the terminal result.
	var got []*pb.DeployEvent
	if err := f.subscribe(context.Background(), 0, func(ev *pb.DeployEvent) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if len(got) == 0 || got[len(got)-1].GetResult() == nil {
		t.Fatalf("subscriber did not end on the terminal result")
	}
}

func seqs(evs []*pb.DeployEvent) []uint64 {
	out := make([]uint64, len(evs))
	for i, e := range evs {
		out[i] = e.GetSeq()
	}
	return out
}
