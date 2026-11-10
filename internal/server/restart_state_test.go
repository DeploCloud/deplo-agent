package server

import (
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

var rt0 = time.Date(2026, 11, 10, 3, 0, 0, 0, time.UTC)

func httpRule() *pb.RestartRule {
	return &pb.RestartRule{Probe: pb.RestartRule_PROBE_HTTP, Port: 3000, Path: "/", IntervalS: 30, TimeoutS: 10, StartPeriodS: 60}
}

func fail(st *restartState, at time.Time, n int) restartVerdict {
	var v restartVerdict
	for i := 0; i < n; i++ {
		v = st.record(at.Add(time.Duration(i)*30*time.Second), "c1", rt0, false)
	}
	return v
}

func TestRestartAfterThreeFailuresInARow(t *testing.T) {
	st := newRestartState(httpRule())
	at := rt0.Add(5 * time.Minute)
	if v := fail(st, at, 2); v.restart {
		t.Fatal("restarted after two failures")
	}
	if v := st.record(at.Add(time.Minute), "c1", rt0, true); v.restart || v.recovered {
		t.Fatalf("a pass is not news: %+v", v)
	}
	if v := fail(st, at.Add(2*time.Minute), 2); v.restart {
		t.Fatal("a pass must reset the count")
	}
	st.record(at.Add(5*time.Minute), "c1", rt0, true)
	v := fail(st, at.Add(10*time.Minute), 3)
	if !v.restart || v.unanswered != time.Minute || v.inWindowNow != 1 {
		t.Fatalf("verdict = %+v", v)
	}
	if v := st.record(at.Add(12*time.Minute), "c1", rt0, true); !v.recovered {
		t.Fatal("the restarted container answering is a recovery")
	}
	if v := st.record(at.Add(13*time.Minute), "c1", rt0, true); v.recovered {
		t.Fatal("recovery is reported once")
	}
}

func TestAppsOwnRetriesReplaceThree(t *testing.T) {
	r := httpRule()
	r.Retries = 5
	st := newRestartState(r)
	if v := fail(st, rt0.Add(5*time.Minute), 4); v.restart {
		t.Fatal("restarted before its own retries")
	}
	if v := st.record(rt0.Add(9*time.Minute), "c1", rt0, false); !v.restart {
		t.Fatal("fifth failure should restart")
	}
}

func TestDockerHealthCountsAtOnce(t *testing.T) {
	st := newRestartState(&pb.RestartRule{Probe: pb.RestartRule_PROBE_DOCKER_HEALTH, Retries: 9})
	if v := st.record(rt0.Add(time.Minute), "c1", rt0, false); !v.restart {
		t.Fatal("Docker already counted its retries")
	}
}

func TestCountsArePerContainer(t *testing.T) {
	st := newRestartState(httpRule())
	at := rt0.Add(5 * time.Minute)
	st.record(at, "a", rt0, false)
	st.record(at, "b", rt0, false)
	st.record(at, "a", rt0, false)
	st.record(at, "b", rt0, true)
	if v := st.record(at, "a", rt0, false); !v.restart {
		t.Fatal("a's third failure restarts a")
	}
	if v := st.record(at, "b", rt0, false); v.restart {
		t.Fatal("b passed in between")
	}
}

func TestNothingCountsDuringTheStartPeriod(t *testing.T) {
	st := newRestartState(httpRule())
	for i := 0; i < 10; i++ {
		if v := st.record(rt0.Add(time.Duration(i)*5*time.Second), "c1", rt0, false); v.restart {
			t.Fatal("restarted inside the start period")
		}
	}
	if st.fails["c1"] != 0 {
		t.Fatal("start-period failures must not accumulate")
	}
	if v := fail(st, rt0.Add(61*time.Second), 3); !v.restart {
		t.Fatal("counts again once the period is over")
	}
}

func TestPausesAfterFiveRestartsInThirtyMinutes(t *testing.T) {
	st := newRestartState(httpRule())
	at := rt0.Add(2 * time.Minute)
	for i := 0; i < 5; i++ {
		if v := fail(st, at.Add(time.Duration(i)*5*time.Minute), 3); !v.restart {
			t.Fatalf("restart %d missing: %+v", i+1, v)
		}
	}
	v := fail(st, at.Add(26*time.Minute), 3)
	if v.restart || !v.pause || !st.paused {
		t.Fatalf("sixth should pause: %+v", v)
	}
	if v := fail(st, at.Add(27*time.Minute), 3); v.restart || v.pause {
		t.Fatal("paused: no more restarts, and the pause is announced once")
	}
}

func TestRestartsOlderThanTheWindowDoNotCount(t *testing.T) {
	st := newRestartState(httpRule())
	at := rt0.Add(2 * time.Minute)
	for i := 0; i < 5; i++ {
		fail(st, at.Add(time.Duration(i)*10*time.Minute), 3)
	}
	if v := fail(st, at.Add(120*time.Minute), 3); !v.restart {
		t.Fatalf("old restarts expired: %+v", v)
	}
}

func pauseIt(st *restartState) time.Time {
	at := rt0.Add(2 * time.Minute)
	for i := 0; i < 6; i++ {
		fail(st, at.Add(time.Duration(i)*time.Minute), 3)
	}
	return at.Add(6*time.Minute + 60*time.Second)
}

func TestResumesAfterThirtyQuietMinutes(t *testing.T) {
	st := newRestartState(httpRule())
	last := pauseIt(st)
	if !st.paused {
		t.Fatal("setup: not paused")
	}
	if st.quiet(last.Add(10 * time.Minute)) {
		t.Fatal("not quiet yet")
	}
	if !st.quiet(last.Add(31 * time.Minute)) {
		t.Fatal("30 minutes without a failed check resumes")
	}
}

func TestAFailureWhilePausedRestartsTheQuietClock(t *testing.T) {
	st := newRestartState(httpRule())
	last := pauseIt(st)
	st.record(last.Add(20*time.Minute), "c1", rt0, false)
	if st.quiet(last.Add(31 * time.Minute)) {
		t.Fatal("a failed check is not quiet")
	}
}

func TestResumeClearsEverything(t *testing.T) {
	st := newRestartState(httpRule())
	pauseIt(st)
	if !st.resume() || st.paused || len(st.restarts) != 0 {
		t.Fatal("resume must clear the pause and the window")
	}
	if st.resume() {
		t.Fatal("resume reports whether it was paused")
	}
	if v := fail(st, rt0.Add(time.Hour), 3); !v.restart {
		t.Fatal("restarting is back on")
	}
}
