package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// Retention budget for buffered LOG events. A single deploy can emit an
// unbounded, attacker-controlled log stream (e.g. `RUN yes | head -c 2G`), and
// this agent is a root-privileged process shared by every app on the host, so
// the in-flight buffer MUST NOT grow with the build's output. Phase and terminal
// result events are always retained (they are few and structural); only the LOG
// events are capped — by total bytes AND by count — after which the oldest log
// lines are coalesced into a single truncation note.
const (
	maxRetainedLogBytes  = 4 << 20 // ~4 MiB of retained log text
	maxRetainedLogEvents = 20000   // guards against a flood of tiny lines
	// Per-event fixed overhead charged on top of the text so that even empty
	// lines cost something toward the count/byte budget.
	logEventOverhead = 64
)

// inflight tracks one deploy the agent is running (or recently finished), keyed
// by its stable deploy id. It is the heart of reconnection/replay (PLAN D5,
// Part-B half): the deploy runs in a background goroutine on a DEPLOY-scoped
// context (not the gRPC stream's), so a control-plane disconnect does NOT kill
// the build. Every event the deploy emits is stamped with a monotonic seq
// (`lastSeq`) and appended to `events`; a connecting (Deploy) or reconnecting
// (ReattachDeploy) stream replays retained events past a cursor, then receives
// live events until the terminal result. Finished deploys are retained briefly
// so a control plane that dropped right before the result can still fetch it.
//
// `events` is a BOUNDED buffer: log events are capped by byte + count budget and
// the oldest are coalesced into a single truncation note, so a verbose or
// malicious build cannot OOM the shared host. Because trimming breaks any
// seq==index+1 relationship, seq is assigned from `lastSeq` (never the slice
// length) and subscribers advance their cursor by seq, not by index.
type inflight struct {
	startedAt time.Time

	mu       sync.Mutex
	cond     *sync.Cond
	events   []*pb.DeployEvent // retained events in ascending seq order (bounded)
	lastSeq  uint64            // seq of the most recently appended event
	phase    pb.DeployPhase
	done     bool               // terminal result has been appended
	finished time.Time          // when done flipped true (for retention/eviction)
	cancel   context.CancelFunc // cancels the deploy's background context

	// Log-retention bookkeeping (see the budget constants above).
	logBytes   int // retained real-log bytes (excludes the note)
	logCount   int // retained real-log events (excludes the note)
	droppedLog uint64
	noteIdx    int    // index of the truncation note in events, or -1 if none
	noteSeq    uint64 // seq the note occupies (the first-dropped log's seq)
}

func newInflight(cancel context.CancelFunc) *inflight {
	f := &inflight{startedAt: time.Now(), cancel: cancel, noteIdx: -1}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// append stamps an event with the next seq, records phase/terminal transitions,
// enforces the log-retention budget, and wakes every subscriber. Returns the
// stamped event (so the live path can also forward it without re-deriving seq).
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

// enforceLogBudget evicts the oldest retained LOG events until the byte and
// count budgets are satisfied, coalescing what it drops into a single note.
// Phase/result events are never touched, and the single most recent log line is
// always kept (so `logCount` never falls below 1 here). Must hold f.mu.
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
		// Remove the victim from the buffer.
		f.events = append(f.events[:idx], f.events[idx+1:]...)
		if f.noteIdx < 0 {
			// First eviction: drop a truncation note into the freed slot.
			f.events = append(f.events, nil)
			copy(f.events[idx+1:], f.events[idx:])
			f.events[idx] = f.newNote()
			f.noteIdx = idx
		} else {
			// Subsequent evictions only ever target logs AFTER the note (higher
			// seq), so the note's index is stable; refresh its text with a NEW
			// event value rather than mutating the one subscribers may be sending.
			f.events[f.noteIdx] = f.newNote()
		}
	}
}

// oldestEvictableLogIndex returns the index of the oldest retained real LOG
// event (skipping the truncation note and any phase/result events), or -1 if
// there is none. Must hold f.mu.
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

// newNote builds a fresh truncation-note event reflecting the current dropped
// count. It occupies noteSeq (the first-dropped log's seq) so it replays in
// order for a reattacher whose cursor predates the drop. Must hold f.mu.
func (f *inflight) newNote() *pb.DeployEvent {
	return &pb.DeployEvent{
		Seq: f.noteSeq,
		Event: &pb.DeployEvent_Log{Log: &pb.LogLine{
			Level: "warn",
			Text:  fmt.Sprintf("[deplo] %d earlier log line(s) trimmed to bound agent memory", f.droppedLog),
		}},
	}
}

// logEventSize estimates an event's retained cost: its log text plus a fixed
// per-event overhead. Non-log events are not budgeted and return 0.
func logEventSize(ev *pb.DeployEvent) int {
	l := ev.GetLog()
	if l == nil {
		return 0
	}
	return len(l.GetLevel()) + len(l.GetText()) + logEventOverhead
}

// subscribe replays buffered events with seq > fromSeq, then streams live events
// until the deploy is done or ctx is cancelled (the SUBSCRIBER's context — i.e.
// the gRPC stream; cancelling it detaches this reader without affecting the
// deploy or other readers). send is the per-event sink; a send error detaches.
//
// The cursor advances by seq (not slice index): each pass drains every retained
// event with seq > cursor, then advances the cursor to the highest seq seen so
// far. Events that were trimmed from the buffer are simply skipped (their gap is
// summarized by the truncation note), never re-scanned or blocked on.
func (f *inflight) subscribe(ctx context.Context, fromSeq uint64, send func(*pb.DeployEvent) error) error {
	// A goroutine to wake the cond when the subscriber's context is cancelled,
	// so a detached reader doesn't block forever on cond.Wait().
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
		// Wait until there is an event past the cursor, or the deploy is done, or
		// the subscriber went away.
		for f.lastSeq <= cursor && !f.done && ctx.Err() == nil {
			f.cond.Wait()
		}
		if ctx.Err() != nil {
			f.mu.Unlock()
			return ctx.Err()
		}
		// Collect every retained event newer than the cursor, in order, then send
		// outside the lock (send may block on the network). Advance the cursor to
		// the latest seq: anything in (cursor, lastSeq] not retained was trimmed
		// and must not be waited on again.
		var batch []*pb.DeployEvent
		for _, ev := range f.events {
			if ev.GetSeq() > cursor {
				batch = append(batch, ev)
			}
		}
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

func (f *inflight) isDone() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.done
}
