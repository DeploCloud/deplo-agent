package server

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// logLines records what a guard logs.
type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *logLines) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *logLines) with(sub string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, s := range l.lines {
		if strings.Contains(s, sub) {
			out = append(out, s)
		}
	}
	return out
}

// deplo-agent#59: a lease that ran out logs each stop with its reason and the age of the lease it read.
func TestStopLog_aLeaseThatRanOutSaysWhyAndHowOldTheLeaseWas(t *testing.T) {
	f := newGuard(t)
	var log logLines
	f.g.logf = log.logf
	f.g.Renew(orderFor(3))
	f.clk.Add(61 * time.Second)
	f.g.Check(context.Background())
	got := log.with("stopping db-main here")
	if len(got) != 1 || !strings.Contains(got[0], "its lease ran out") || !strings.Contains(got[0], "epoch 3, renewed 1m1s ago, lives 1m0s, renewed in this run of the agent") {
		t.Fatalf("logged %q", log.lines)
	}
	if len(log.with("stopping web here")) != 0 {
		t.Fatalf("logged a stop of web, which the fence leaves alone: %q", log.lines)
	}
}

// After a restart, the lease the decision read came from disk, and the log says so.
func TestStopLog_aLeaseReadFromDiskSaysSo(t *testing.T) {
	f := newGuard(t)
	f.g.Renew(orderFor(3))
	f.clk.Add(2 * time.Minute)
	var log logLines
	f.reopen().logf = log.logf
	f.g.Check(context.Background())
	got := log.with("stopping files here")
	if len(got) != 1 || !strings.Contains(got[0], "renewed 2m0s ago") || !strings.Contains(got[0], "read from disk at start") {
		t.Fatalf("logged %q", log.lines)
	}
}

// A renewal that lands while the fence stops the stacks starts them again, and the log says it did once it did.
func TestStopLog_aRenewalDuringTheStopIsLogged(t *testing.T) {
	f := newReturn(t)
	var log logLines
	f.g.logf = log.logf
	f.renew(orderFor(3))
	f.clk.Add(61 * time.Second)
	f.g.fence = func(context.Context, []lease.Entry) error {
		f.g.Renew(orderFor(4))
		return nil
	}
	f.g.Check(context.Background())
	f.g.work.Wait()
	if got := log.with("again"); len(got) != 1 || !strings.Contains(got[0], "started db-main") || len(f.started) == 0 {
		t.Fatalf("logged %q, started %v", log.lines, f.started)
	}
}

// c42: at a thaw the log said "starting lab-c, lab-a again" while the renewal had placed both elsewhere and nothing
// started. A stack that does not start is never said to.
func TestStopLog_aRenewalThatPlacesTheStacksElsewhereSaysNoStart(t *testing.T) {
	f := newReturn(t)
	var log logLines
	f.g.logf = log.logf
	f.renew(orderFor(3))
	f.clk.Add(61 * time.Second)
	f.g.fence = func(context.Context, []lease.Entry) error {
		o := movedAway(4)
		o.Placement[0].Member = "srv_b"
		f.g.Renew(o)
		return nil
	}
	f.g.Check(context.Background())
	f.g.work.Wait()
	if got := log.with("again"); len(got) != 0 || len(f.started) != 0 {
		t.Fatalf("logged %q, started %v", got, f.started)
	}
}

// A live lease that places a writer elsewhere logs that stop too, with the lease it read.
func TestStopLog_aWriterPlacedElsewhereIsLogged(t *testing.T) {
	f := newRelinquish(t)
	var log logLines
	f.g.logf = log.logf
	f.renew(orderFor(1))
	f.clk.Add(time.Second)
	f.renew(movedAway(2))
	if got := log.with("stopping files here: the lease places it on srv_b"); len(got) != 1 || !strings.Contains(got[0], "epoch 2, renewed 0s ago") {
		t.Fatalf("logged %q", log.lines)
	}
}

// c43: after an agent restart its visitors waited 5 s for the first renewal, though the lease it had kept on disk
// still ran. In the same boot that lease answers them; after a reboot, or once it ran out, it does not.
func TestVisitorHolders_aLeaseReadBackInTheSameBootAnswersVisitors(t *testing.T) {
	f := newGuard(t)
	f.g.Renew(orderFor(3))
	f.clk.Add(2 * time.Second)
	f.reopen()
	if here, _ := f.g.visitorHolders("clu_1", "prj_2"); !here {
		t.Fatal("an agent that restarted does not answer for what its running lease places here")
	}
	old := bootID
	bootID = func() string { return old() + "-next" }
	if here, _ := f.g.visitorHolders("clu_1", "prj_2"); here {
		t.Fatal("a lease from before a reboot answers visitors")
	}
	bootID = old
	f.clk.Add(time.Minute)
	if here, _ := f.g.visitorHolders("clu_1", "prj_2"); here {
		t.Fatal("a lease that ran out answers visitors")
	}
}
