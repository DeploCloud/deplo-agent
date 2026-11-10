package server

import (
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const (
	restartWindow      = 30 * time.Minute
	restartMax         = 5
	restartQuiet       = 30 * time.Minute
	restartDefaultFail = 3
)

// restartState is one app's counter. https://deplo.build/docs/guides/health-checks-and-automatic-restarts
type restartState struct {
	rule     *pb.RestartRule
	fails    map[string]int
	since    map[string]time.Time
	restarts []time.Time
	paused   bool
	lastFail time.Time
	// A restarted container has not answered yet.
	recovering bool
}

type restartVerdict struct {
	restart     bool
	pause       bool
	recovered   bool
	unanswered  time.Duration
	inWindowNow int
}

func newRestartState(rule *pb.RestartRule) *restartState {
	return &restartState{rule: rule, fails: map[string]int{}, since: map[string]time.Time{}}
}

func (st *restartState) threshold() int {
	if st.rule.GetProbe() == pb.RestartRule_PROBE_DOCKER_HEALTH {
		return 1
	}
	if n := int(st.rule.GetRetries()); n > 0 {
		return n
	}
	return restartDefaultFail
}

// inStartPeriod: a container that just started is given the rule's start period before anything counts.
func (st *restartState) inStartPeriod(now, startedAt time.Time) bool {
	return !startedAt.IsZero() && now.Sub(startedAt) < time.Duration(st.rule.GetStartPeriodS())*time.Second
}

// record folds in one probe of one container and says what to do about it.
func (st *restartState) record(now time.Time, id string, startedAt time.Time, ok bool) restartVerdict {
	if st.inStartPeriod(now, startedAt) {
		delete(st.fails, id)
		delete(st.since, id)
		return restartVerdict{}
	}
	if ok {
		delete(st.fails, id)
		delete(st.since, id)
		if st.recovering {
			st.recovering = false
			return restartVerdict{recovered: true}
		}
		return restartVerdict{}
	}
	st.lastFail = now
	if st.fails[id] == 0 {
		st.since[id] = now
	}
	st.fails[id]++
	if st.fails[id] < st.threshold() {
		return restartVerdict{}
	}
	if st.paused {
		return restartVerdict{}
	}
	st.prune(now)
	if len(st.restarts) >= restartMax {
		st.paused = true
		return restartVerdict{pause: true, inWindowNow: len(st.restarts)}
	}
	unanswered := now.Sub(st.since[id])
	st.restarts = append(st.restarts, now)
	delete(st.fails, id)
	delete(st.since, id)
	st.recovering = true
	return restartVerdict{restart: true, unanswered: unanswered, inWindowNow: len(st.restarts)}
}

func (st *restartState) prune(now time.Time) {
	keep := st.restarts[:0]
	for _, t := range st.restarts {
		if now.Sub(t) < restartWindow {
			keep = append(keep, t)
		}
	}
	st.restarts = keep
}

// quiet reports a paused app that has gone restartQuiet without a failed check.
func (st *restartState) quiet(now time.Time) bool {
	return st.paused && now.Sub(st.lastFail) >= restartQuiet
}

// resume turns restarting back on and forgets every count; it reports whether it was paused.
func (st *restartState) resume() bool {
	was := st.paused
	st.paused = false
	st.restarts = nil
	st.recovering = false
	st.fails = map[string]int{}
	st.since = map[string]time.Time{}
	return was
}
