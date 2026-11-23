package server

import (
	"context"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// eu, 23 Nov 2026: a new App's data was copied to every member of one host's disk, a sync of the lease file took
// seconds under the lease's lock, and every member fenced though its leader renewed it. A write that hangs now holds
// no plain renewal up, and the fence never fires on a lease that was renewed.
func TestLease_aLeaseFileThatHangsHoldsNoRenewalUp(t *testing.T) {
	leaseWritesInline = false
	defer func() { leaseWritesInline = true }()
	f := newGuard(t)
	f.g.Renew(orderFor(1))
	release := make(chan struct{})
	writeLease = func(path string, data []byte, perm os.FileMode) error {
		<-release
		return writeFileAtomic(path, data, perm)
	}
	defer func() {
		close(release)
		f.g.work.Wait()
		writeLease = writeFileAtomic
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10; i++ {
			f.clk.Add(time.Second)
			if r := f.g.Renew(orderFor(1)); !r.OK {
				t.Errorf("a renewal got %+v", r)
			}
			f.g.Check(context.Background())
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a renewal waits on the lease file")
	}
	if len(f.fenced) != 0 {
		t.Fatalf("fenced %v while the leader renewed every second", f.fenced)
	}
}

// A new epoch is on disk before the member answers for it: across a restart it still refuses an older one.
func TestLease_aNewEpochIsOnDiskBeforeTheAnswer(t *testing.T) {
	leaseWritesInline = false
	defer func() { leaseWritesInline = true }()
	f := newGuard(t)
	f.g.Renew(orderFor(4))
	raw, err := os.ReadFile(f.file)
	if err != nil || !strings.Contains(string(raw), `"epoch":4`) {
		t.Fatalf("the lease file after the answer: %s (%v)", raw, err)
	}
}

// The next leader goes on from what the last renewal said: who is gone, and when each member was last heard.
func TestSuccession_anAskCarriesTheGoneSetAndWhenEachMemberWasHeard(t *testing.T) {
	f := newGuard(t)
	got := &asks{}
	f.g.succeed = got.ask
	o := orderFor(3)
	o.From, o.Line, o.Gone = "srv_lead", []string{"srv_a"}, []string{"fp-srv_c"}
	o.Heard = map[string]int64{"srv_lead": 0, "srv_c": 125_000}
	f.g.Renew(o)
	f.clk.Add(lease.PreVoteFor)
	f.g.Check(context.Background())
	f.g.work.Wait()
	got.mu.Lock()
	defer got.mu.Unlock()
	if len(got.got) != 1 {
		t.Fatalf("asks %+v", got.got)
	}
	a := got.got[0]
	if !slices.Equal(a.Gone, []string{"fp-srv_c"}) || a.Heard["srv_lead"] != lease.PreVoteFor.Milliseconds() || a.Heard["srv_c"] != 125_000+lease.PreVoteFor.Milliseconds() {
		t.Fatalf("the ask carries gone %v and heard %v", a.Gone, a.Heard)
	}
}

// Tests write the lease file under the lock, as the agent did before: none lands after its test's directory is gone.
func init() { leaseWritesInline = true }

type lines struct {
	mu  sync.Mutex
	got []string
}

func (l *lines) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.got = append(l.got, format)
}

func (l *lines) count(part string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.got {
		if strings.Contains(s, part) {
			n++
		}
	}
	return n
}

// eu, 23 Nov 2026: "stopping lab-pg here: the lease places it on eu-2" every second while its copy waited. Said once
// per member it runs on, however many renewals ask again.
func TestRelinquish_theStopIsSaidOncePerPlaceNotPerRenewal(t *testing.T) {
	f := newRelinquish(t)
	said := &lines{}
	f.g.logf = said.logf
	f.renew(orderFor(1))
	for i := 0; i < 5; i++ {
		f.clk.Add(time.Second)
		f.renew(movedAway(2))
		f.g.work.Wait()
	}
	if n := said.count("stopping %s here: the lease places it on"); n != 1 {
		t.Fatalf("said the stop %d times", n)
	}
}

// A fence whose stop fails is tried every check, and said once.
func TestLease_aStopTriedAgainIsSaidOnce(t *testing.T) {
	f := newGuard(t)
	said := &lines{}
	f.g.logf = said.logf
	f.g.Renew(orderFor(1))
	f.failing = true
	for i := 0; i < 70; i++ {
		f.clk.Add(time.Second)
		f.g.Check(context.Background())
	}
	if n := said.count("stopping %s here: %s"); n != 2 {
		t.Fatalf("said %d stops, want one for each of db-main and files", n)
	}
}
