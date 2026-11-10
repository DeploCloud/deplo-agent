package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func replicasOf(n int) []readySample {
	var out []readySample
	for i := 1; i <= n; i++ {
		out = append(out, readySample{
			id: "r" + strconv.Itoa(i), name: "deplo-web-deplo-web-" + strconv.Itoa(i),
			service: "deplo-web", status: "running", startedAt: rt0, ip: "10.0.0." + strconv.Itoa(i+1),
		})
	}
	return out
}

// One replica stops answering: only that copy restarts and the event names it.
func TestWatcherRestartsOnlyTheHungReplica(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: replicasOf(3)}
	w := newWatcherOn(t, h)
	w.probe = func(_ context.Context, _ *pb.RestartRule, c readySample) bool { return c.id != "r2" }
	w.release("web", httpRule(), true)
	runTicks(w, h, 3)
	if len(h.restarted) != 1 || h.restarted[0] != "r2" {
		t.Fatalf("restarted = %v, want only r2", h.restarted)
	}
	evs := w.drain()
	if len(evs) != 1 || evs[0].Kind != pb.RestartEvent_KIND_RESTARTED || evs[0].Container != "deplo-web-deplo-web-2" {
		t.Fatalf("events = %v", evs)
	}
}

// A copy that is paused for a volume backup is not a hung copy.
func TestWatcherSkipsAReplicaHeldForBackup(t *testing.T) {
	h := &fakeHost{now: rt0.Add(10 * time.Minute), containers: replicasOf(2)}
	w := newWatcherOn(t, h)
	w.probe = func(context.Context, *pb.RestartRule, readySample) bool { return false }
	w.held = func(id string) bool { return id == "r1" }
	w.release("web", httpRule(), true)
	runTicks(w, h, 3)
	if len(h.restarted) != 1 || h.restarted[0] != "r2" {
		t.Fatalf("restarted = %v, want only r2", h.restarted)
	}
}

func TestServiceWiresTheBackupHoldIntoTheWatcher(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	s.markHeld("abc123def456789", true)
	w := s.restartWatch()
	if w.held == nil || !w.held("abc123def456") || w.held("zzz") {
		t.Fatal("the watcher does not ask the backup hold")
	}
}
