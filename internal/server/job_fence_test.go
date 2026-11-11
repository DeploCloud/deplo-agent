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

// memberWithJobs is a cluster member on a fake clock whose docker runs every job until it is killed;
// running reports whether a job's command has started.
func memberWithJobs(t *testing.T) (s *Service, clk *fakeClock, running func() bool) {
	t.Helper()
	bin := t.TempDir()
	mark := filepath.Join(bin, "started")
	script := "#!/bin/sh\n[ \"$1\" = inspect ] && { echo prj_deplo-agent-test; exit 0; }\n" +
		"case \" $* \" in *\" -e " + jobMarkerEnv + " \"*) : > " + mark + "; exec sleep 60;; esac\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	s = newTestService(t)
	s.SetAgentDir(t.TempDir())
	clk = &fakeClock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	s.leaseOnce.Do(func() { s.lease = newLeaseGuard(s.leaseFile(), clk.Now, s.fenceStacks) })
	return s, clk, func() bool {
		_, err := os.Stat(mark)
		return err == nil
	}
}

func startSleepJob(s *Service) (*pb.StartJobResponse, error) {
	return s.StartJob(context.Background(), &pb.StartJobRequest{
		ProjectId: "prj_deplo-agent-test", Container: "deplo-agent-test-cron", Shell: "sh", Command: "sleep 60",
	})
}

func settledJob(t *testing.T, s *Service, id string) *pb.PollJobResponse {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		p, _ := s.PollJob(context.Background(), &pb.PollJobRequest{JobId: id})
		if !p.GetRunning() {
			return p
		}
		if time.Now().After(deadline) {
			t.Fatal("job never settled")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Only a stateless App is placed here, so the fence has no stack to stop: the job is all there is.
func statelessOrder() lease.Order {
	return lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: 1, Placement: []lease.Entry{
		{Kind: "app", ID: "prj_1", Member: "srv_a", Slug: "web"},
	}}
}

func TestLease_stopsARunningScheduledJobAtSixtySeconds(t *testing.T) {
	s, clk, running := memberWithJobs(t)
	s.leaseGuard().Renew(statelessOrder())
	started, err := startSleepJob(s)
	if err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); !running(); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the job's command never started")
		}
	}

	clk.Add(59 * time.Second)
	s.leaseGuard().Check(context.Background())
	if p, _ := s.PollJob(context.Background(), &pb.PollJobRequest{JobId: started.GetJobId()}); !p.GetRunning() {
		t.Fatalf("stopped inside a live lease: %+v", p)
	}

	clk.Add(2 * time.Second)
	s.leaseGuard().Check(context.Background())
	p := settledJob(t, s, started.GetJobId())
	if !p.GetFenced() || p.GetExitCode() != -1 {
		t.Fatalf("want a job stopped by the fence, got %+v", p)
	}
	if !strings.Contains(p.GetStderr(), "lost touch with its cluster") {
		t.Fatalf("stderr = %q, want the reason", p.GetStderr())
	}
}

func TestLease_runsNoScheduledJobUntilItIsRenewed(t *testing.T) {
	s, clk, _ := memberWithJobs(t)
	s.leaseGuard().Renew(statelessOrder())
	clk.Add(61 * time.Second)

	if _, err := startSleepJob(s); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a member whose lease ran out started a job: %v", err)
	}

	s.leaseGuard().Renew(statelessOrder())
	started, err := startSleepJob(s)
	if err != nil {
		t.Fatalf("a renewed member refused a job: %v", err)
	}
	// A job a person stops is not one the fence stopped.
	if _, err := s.KillJob(context.Background(), &pb.KillJobRequest{JobId: started.GetJobId()}); err != nil {
		t.Fatal(err)
	}
	if p := settledJob(t, s, started.GetJobId()); p.GetFenced() {
		t.Fatalf("a stopped job reported as fenced: %+v", p)
	}
}

func TestStartJob_outsideAClusterIsNeverRefusedByALease(t *testing.T) {
	s, _, _ := memberWithJobs(t)
	started, err := startSleepJob(s)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.KillJob(context.Background(), &pb.KillJobRequest{JobId: started.GetJobId()})
	settledJob(t, s, started.GetJobId())
}
