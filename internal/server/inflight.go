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

func (f *inflight) enforceLogBudget() {
	for (f.logBytes > maxRetainedLogBytes || f.logCount > maxRetainedLogEvents) && f.logCount > 1 {
		idx := f.oldestEvictableLogIndex()
		if idx < 0 {
			return
		}
		victim := f.events[idx]
		f.logBytes -= logEventSize(victim)
		f.logCount--
		if f.droppedLog == 0 {
			f.noteSeq = victim.GetSeq()
		}
		f.droppedLog++
		f.events = append(f.events[:idx], f.events[idx+1:]...)
		if f.noteIdx < 0 {
			f.events = append(f.events, nil)
			copy(f.events[idx+1:], f.events[idx:])
			f.events[idx] = f.newNote()
			f.noteIdx = idx
		} else {
			f.events[f.noteIdx] = f.newNote()
		}
	}
}

func (f *inflight) oldestEvictableLogIndex() int {
	for i := range f.events {
		if i == f.noteIdx {
			continue
		}
		if f.events[i].GetLog() != nil {
			return i
		}
	}
	return -1
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
