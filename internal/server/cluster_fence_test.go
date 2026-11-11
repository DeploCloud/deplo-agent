package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

const (
	fenceA = "deplo-agent-test-fence-a"
	fenceB = "deplo-agent-test-fence-b"
	fenceC = "deplo-agent-test-fence-c"
)

func fenceEntries(slugs ...string) []lease.Entry {
	var out []lease.Entry
	for _, s := range slugs {
		out = append(out, lease.Entry{Kind: "database", ID: s, Member: "srv_a", Slug: s, SingleWriter: true})
	}
	return out
}

func count(lines []string, part string) int {
	n := 0
	for _, l := range lines {
		if strings.Contains(l, part) {
			n++
		}
	}
	return n
}

// The leader starts the work elsewhere 21 s after the fence begins: three slow databases stop together, and a
// deploy that holds one stack's lock does not hold the fence up.
func TestFence_stopsEveryStackAtOnceWithoutWaitingForAStacksLock(t *testing.T) {
	argv := dockerOnPath(t, `case " $* " in *" stop "*) sleep 0.5;; esac`)
	s := newTestService(t)
	unlock := s.lockStack(fenceA)
	defer unlock()
	done := make(chan error, 1)
	began := time.Now()
	go func() { done <- s.fenceStacks(context.Background(), fenceEntries(fenceA, fenceB, fenceC)) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the fence waited for a stack's lock")
	}
	if took := time.Since(began); took > 1200*time.Millisecond {
		t.Fatalf("three stops of 0.5 s took %s: they ran one after another", took)
	}
	if n := count(argv(), " stop -t "); n != 3 {
		t.Fatalf("%d stacks were stopped with a bounded grace, want 3:\n%s", n, strings.Join(argv(), "\n"))
	}
}

func hurryFence(t *testing.T, grace time.Duration) {
	t.Helper()
	old := fenceGrace
	fenceGrace = grace
	t.Cleanup(func() { fenceGrace = old })
}

func TestFence_killsAStackThatDoesNotStopInTime(t *testing.T) {
	hurryFence(t, 200*time.Millisecond)
	argv := dockerOnPath(t, `case " $* " in *" stop "*) exec sleep 30;; esac`)
	s := newTestService(t)
	began := time.Now()
	if err := s.fenceStacks(context.Background(), fenceEntries(fenceA)); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Fatalf("the fence took %s for a stack that would not stop", took)
	}
	if count(argv(), " kill") == 0 {
		t.Fatalf("nothing was killed:\n%s", strings.Join(argv(), "\n"))
	}
}

func TestFence_aStackThatStillRunsIsAnErrorSoTheLeaseIsNotCalledFenced(t *testing.T) {
	hurryFence(t, 200*time.Millisecond)
	// docker answers every ps with a container id: the stack is still up whatever was tried.
	dockerOnPath(t, `case "$1" in ps) echo abc123;; esac`)
	s := newTestService(t)
	if err := s.fenceStacks(context.Background(), fenceEntries(fenceA)); err == nil {
		t.Fatal("a stack that still runs was reported as stopped")
	}
}

func writeStack(t *testing.T, s *Service, slug string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(s.stackPath(slug)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(s.stackPath(slug), []byte("services: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// fencedMember is a member whose lease placed one database here and has now run out.
func fencedMember(t *testing.T) (*Service, *fakeClock) {
	t.Helper()
	s := newTestService(t)
	s.SetAgentDir(t.TempDir())
	clk := &fakeClock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	s.leaseOnce.Do(func() { s.lease = newLeaseGuard(s.leaseFile(), clk.Now, s.fenceStacks) })
	s.lease.Renew(lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 1, Placement: fenceEntries(fenceA)})
	return s, clk
}

func TestStartStack_aStartThatWaitedOnTheLockPastTheFenceStartsNothing(t *testing.T) {
	argv := dockerOnPath(t, "")
	s, clk := fencedMember(t)
	writeStack(t, s, fenceA)
	unlock := s.lockStack(fenceA)
	done := make(chan error, 1)
	go func() {
		_, err := s.StartStack(context.Background(), &pb.StackRef{Slug: fenceA})
		done <- err
	}()
	// The start is admitted while the lease lives and waits for the lock; the lease runs out meanwhile.
	time.Sleep(150 * time.Millisecond)
	clk.Add(lease.FenceAfter + time.Second)
	unlock()
	if err := <-done; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("the start answered %v", err)
	}
	if count(argv(), " start") != 0 {
		t.Fatalf("the stack was started after the fence:\n%s", strings.Join(argv(), "\n"))
	}
}

func TestStartStack_aLeaseThatRunsOutDuringTheStartStopsTheStackAgain(t *testing.T) {
	mark := filepath.Join(t.TempDir(), "starting")
	argv := dockerOnPath(t, `case " $* " in *" start "*) : > `+mark+`; sleep 0.5;; esac`)
	s, clk := fencedMember(t)
	writeStack(t, s, fenceA)
	done := make(chan error, 1)
	go func() {
		_, err := s.StartStack(context.Background(), &pb.StackRef{Slug: fenceA})
		done <- err
	}()
	eventually(t, "docker to be starting the stack", func() bool { return isFile(mark) })
	clk.Add(lease.FenceAfter + time.Second)
	if err := <-done; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("the start answered %v", err)
	}
	lines := argv()
	if last := lines[len(lines)-1]; count(lines, " stop -t ") == 0 || strings.Contains(last, " start") {
		t.Fatalf("the stack was left up after its lease ran out:\n%s", strings.Join(lines, "\n"))
	}
}

// A lease file that will not open says nothing about what may run here. It is read as a lease that ran out, never
// as no lease: every stack on this host stops until the leader speaks, and nothing of them starts meanwhile.
func TestLease_aLeaseFileThatCannotBeReadIsADeadLeaseNotNoLease(t *testing.T) {
	argv := dockerOnPath(t, "")
	s := newTestService(t)
	s.SetAgentDir(t.TempDir())
	// The agent starts again as the cluster member it was.
	if err := os.WriteFile(s.memberFile(), []byte(`{"member":true,"cluster":"clu_1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	writeStack(t, s, fenceA)
	if err := os.WriteFile(s.leaseFile(), []byte(`{"clu_1": {"epoch": 7, "plac`), 0o600); err != nil {
		t.Fatal(err)
	}
	g := s.leaseGuard()
	if !g.Holds(fenceA) || !g.RanOut() {
		t.Fatalf("an unreadable lease reads as none: holds %v, ran out %v", g.Holds(fenceA), g.RanOut())
	}
	g.Check(context.Background())
	if count(argv(), "deplo-"+fenceA+" ") == 0 || count(argv(), " stop -t ") == 0 {
		t.Fatalf("the stack on this host was not stopped:\n%s", strings.Join(argv(), "\n"))
	}
	// The leader speaks: the lease is its word again.
	if r := g.Renew(lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 7, Placement: fenceEntries(fenceA)}); !r.OK {
		t.Fatalf("renewal after an unreadable lease: %+v", r)
	}
	if g.Holds(fenceA) {
		t.Fatal("the stack is still held under a live lease")
	}
}

func TestWriteFileAtomic_leavesTheOldFileWhenTheNewOneCannotBeWritten(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "state.json")
	if err := writeFileAtomic(file, []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A directory in the temporary file's place: the write fails, and says so.
	if err := os.Mkdir(file+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(file, []byte("two"), 0o600); err == nil {
		t.Fatal("a write that could not happen reported nothing")
	}
	if raw, _ := os.ReadFile(file); string(raw) != "one" {
		t.Fatalf("the file holds %q", raw)
	}
}
