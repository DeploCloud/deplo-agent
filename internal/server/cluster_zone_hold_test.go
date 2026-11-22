package server

import (
	"testing"
	"time"
)

func holdCluster() *zoneCluster {
	return &zoneCluster{Self: "fp-a", Members: []zoneMember{{Fingerprint: "fp-a"}, {Fingerprint: "fp-b"}, {Fingerprint: "fp-c"}}}
}

// A server back from a power cut is named once it has been heard for the removal time: before, its proxy may still
// answer with its default certificate (found on the ADR-0049 lab).
func TestZoneHold_aServerBackIsNamedOnlyOnceHeardForTheRemovalTime(t *testing.T) {
	t0 := time.Now()
	z := &clusterZones{since: t0.Add(-time.Hour)}
	c, drop := holdCluster(), 10*time.Second
	heard := map[string]time.Time{"fp-b": t0, "fp-c": t0}
	if h := z.holdLocked(t0, heard, drop, c); len(h) != 0 {
		t.Fatalf("held %v while every server answers", h)
	}
	// fp-c falls quiet for 40 s while fp-b goes on speaking every 2 s, then fp-c speaks again.
	back := t0.Add(40 * time.Second)
	for at := t0; at.Before(back); at = at.Add(2 * time.Second) {
		if h := z.holdLocked(at, map[string]time.Time{"fp-b": at, "fp-c": t0}, drop, c); len(h) != 0 {
			t.Fatalf("held %v while fp-c is away", h)
		}
	}
	heard = map[string]time.Time{"fp-b": back, "fp-c": back}
	if h := z.holdLocked(back, heard, drop, c); !h["fp-c"] || h["fp-b"] {
		t.Fatalf("just back: held %v", h)
	}
	heard = map[string]time.Time{"fp-b": back.Add(9 * time.Second), "fp-c": back.Add(9 * time.Second)}
	if h := z.holdLocked(back.Add(9*time.Second), heard, drop, c); !h["fp-c"] {
		t.Fatalf("9 s back: held %v", h)
	}
	heard = map[string]time.Time{"fp-b": back.Add(11 * time.Second), "fp-c": back.Add(11 * time.Second)}
	if h := z.holdLocked(back.Add(11*time.Second), heard, drop, c); len(h) != 0 {
		t.Fatalf("11 s back: held %v", h)
	}
	// A gap shorter than the removal time never left the answers, so it is not held either.
	heard = map[string]time.Time{"fp-b": back.Add(20 * time.Second), "fp-c": back.Add(17 * time.Second)}
	if h := z.holdLocked(back.Add(20*time.Second), heard, drop, c); len(h) != 0 {
		t.Fatalf("after a short gap: held %v", h)
	}
}

func TestZoneHold_thisServerNamesItselfOnlyAfterTheRemovalTime(t *testing.T) {
	t0 := time.Now()
	z := &clusterZones{since: t0}
	c, drop := holdCluster(), 10*time.Second
	if h := z.holdLocked(t0.Add(9*time.Second), nil, drop, c); !h["fp-a"] {
		t.Fatalf("just started: held %v", h)
	}
	if h := z.holdLocked(t0.Add(11*time.Second), nil, drop, c); h["fp-a"] {
		t.Fatal("held past the removal time from its start")
	}
}

// Frozen for longer than the removal time and thawed: it answers as one just started (the last agreed set, itself held),
// never SERVFAIL from what it heard before nor only itself (found on cluster eu, 22 Nov 2026).
func TestZoneHold_aServerThawedAfterTheRemovalTimeIsOneJustStarted(t *testing.T) {
	t0 := time.Now()
	z := &clusterZones{since: t0.Add(-time.Hour)}
	c, drop := holdCluster(), 10*time.Second
	for at := t0; at.Before(t0.Add(4 * time.Second)); at = at.Add(time.Second) {
		z.wakeLocked(at)
	}
	if !z.since.Equal(t0.Add(-time.Hour)) {
		t.Fatal("a push every second started it again")
	}
	thaw := t0.Add(90 * time.Second)
	z.wakeLocked(thaw)
	if !z.since.Equal(thaw) {
		t.Fatalf("since %v after a 87 s gap", z.since)
	}
	if h := z.holdLocked(thaw, map[string]time.Time{"fp-b": thaw, "fp-c": thaw}, drop, c); !h["fp-a"] {
		t.Fatalf("thawed: held %v", h)
	}
}
