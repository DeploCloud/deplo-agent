package server

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const (
	maxRetainedLogBytes  = 4 << 20
	maxFinishedLogBytes  = 32 << 20
	maxRetainedLogEvents = 20000
	logEventOverhead     = 64
)

type inflight struct {
	startedAt time.Time

	mu       sync.Mutex
	cond     *sync.Cond
	events   []*pb.DeployEvent
	lastSeq  uint64
	phase    pb.DeployPhase
	done     bool
	finished time.Time
	cancel   context.CancelFunc

	logBytes   int
	logCount   int
	droppedLog uint64
	noteIdx    int
	noteSeq    uint64
}

func newInflight(cancel context.CancelFunc) *inflight {
	f := &inflight{startedAt: time.Now(), cancel: cancel, noteIdx: -1}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *inflight) append(ev *pb.DeployEvent) *pb.DeployEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastSeq++
	ev.Seq = f.lastSeq
	f.events = append(f.events, ev)
	if ev.GetLog() != nil {
		f.logCount++
		f.logBytes += logEventSize(ev)
		f.enforceLogBudget()
	}
	if p := ev.GetPhase(); p != nil {
		f.phase = p.GetPhase()
	}
	if ev.GetResult() != nil {
		f.done = true
		f.finished = time.Now()
	}
	f.cond.Broadcast()
	return ev
}

// enforceLogBudget trims to 7/8 of the budget in one pass, so the O(n) compaction runs once per
// many lines rather than on every line past the cap.
func (f *inflight) enforceLogBudget() {
	if f.logBytes <= maxRetainedLogBytes && f.logCount <= maxRetainedLogEvents {
		return
	}
	wantBytes, wantCount := maxRetainedLogBytes*7/8, maxRetainedLogEvents*7/8
	kept, notePos := f.events[:0], -1
	for i, ev := range f.events {
		if i != f.noteIdx && ev.GetLog() != nil && f.logCount > 1 &&
			(f.logBytes > wantBytes || f.logCount > wantCount) {
			f.logBytes -= logEventSize(ev)
			f.logCount--
			if f.droppedLog == 0 {
				f.noteSeq = ev.GetSeq()
			}
			f.droppedLog++
			if f.noteIdx < 0 && notePos < 0 {
				notePos = len(kept)
				kept = append(kept, nil)
			}
			continue
		}
		if i == f.noteIdx {
			notePos = len(kept)
		}
		kept = append(kept, ev)
	}
	clear(f.events[len(kept):])
	f.events = kept
	if notePos >= 0 {
		f.noteIdx = notePos
		f.events[notePos] = f.newNote()
	}
}

func (f *inflight) newNote() *pb.DeployEvent {
	return &pb.DeployEvent{
		Seq: f.noteSeq,
		Event: &pb.DeployEvent_Log{Log: &pb.LogLine{
			Level: "warn",
			Text:  fmt.Sprintf("[deplo] %d earlier log line(s) trimmed to bound agent memory", f.droppedLog),
		}},
	}
}

func logEventSize(ev *pb.DeployEvent) int {
	l := ev.GetLog()
	if l == nil {
		return 0
	}
	return len(l.GetLevel()) + len(l.GetText()) + logEventOverhead
}

func (f *inflight) subscribe(ctx context.Context, fromSeq uint64, send func(*pb.DeployEvent) error) error {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			f.mu.Lock()
			f.cond.Broadcast()
			f.mu.Unlock()
		case <-stop:
		}
	}()

	cursor := fromSeq
	for {
		f.mu.Lock()
		for f.lastSeq <= cursor && !f.done && ctx.Err() == nil {
			f.cond.Wait()
		}
		if ctx.Err() != nil {
			f.mu.Unlock()
			return ctx.Err()
		}
		first := sort.Search(len(f.events), func(i int) bool { return f.events[i].GetSeq() > cursor })
		batch := append([]*pb.DeployEvent(nil), f.events[first:]...)
		if f.lastSeq > cursor {
			cursor = f.lastSeq
		}
		finished := f.done && cursor >= f.lastSeq
		f.mu.Unlock()

		for _, ev := range batch {
			if err := send(ev); err != nil {
				return err
			}
		}
		if finished {
			return nil
		}
	}
}

// trimFinished forgets the oldest finished deploys once their retained logs together pass
// maxFinishedLogBytes, so a burst of previews cannot hold N x 4 MiB. A running deploy is never dropped.
func (s *Service) trimFinished() {
	s.mu.Lock()
	defer s.mu.Unlock()
	type rec struct {
		id    string
		at    time.Time
		bytes int
	}
	var done []rec
	total := 0
	for id, f := range s.deploys {
		f.mu.Lock()
		if f.done {
			done = append(done, rec{id, f.finished, f.logBytes})
			total += f.logBytes
		}
		f.mu.Unlock()
	}
	sort.Slice(done, func(i, j int) bool { return done[i].at.Before(done[j].at) })
	for _, r := range done {
		if total <= maxFinishedLogBytes {
			return
		}
		delete(s.deploys, r.id)
		total -= r.bytes
	}
}
