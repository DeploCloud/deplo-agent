package server

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func gateWait(t *testing.T, d time.Duration) {
	t.Helper()
	was := appGateWait
	appGateWait = d
	t.Cleanup(func() { appGateWait = was })
}

func TestAppGate_aDeployWaitsForTheHoldAndAHoldForEveryDeploy(t *testing.T) {
	var g appGate
	ctx := context.Background()
	leaveHold, _, err := g.enter(ctx, "web", true, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if leave, _, err := g.enter(ctx, "other", false, time.Second, nil); err != nil {
		t.Fatal("another App's deploy waited on this one's hold")
	} else {
		leave()
	}
	var said atomic.Int32
	got := make(chan time.Duration, 1)
	go func() {
		leave, waited, err := g.enter(ctx, "web", false, 5*time.Second, func() { said.Add(1) })
		if err != nil {
			t.Error(err)
		}
		got <- waited
		leave()
	}()
	time.Sleep(150 * time.Millisecond)
	select {
	case <-got:
		t.Fatal("the deploy started while the hold was open")
	default:
	}
	leaveHold()
	leaveHold()
	if w := <-got; w < 100*time.Millisecond || said.Load() != 1 {
		t.Fatalf("waited %s, said it %d times", w, said.Load())
	}

	a, _, _ := g.enter(ctx, "web", false, time.Second, nil)
	b, waited, _ := g.enter(ctx, "web", false, time.Second, nil)
	if waited != 0 {
		t.Fatal("two deploys of one App waited on each other")
	}
	held := make(chan struct{})
	go func() {
		leave, _, err := g.enter(ctx, "web", true, 5*time.Second, nil)
		if err != nil {
			t.Error(err)
		}
		close(held)
		leave()
	}()
	a()
	time.Sleep(100 * time.Millisecond)
	select {
	case <-held:
		t.Fatal("the hold began while a deploy was still under way")
	default:
	}
	b()
	<-held
}

func TestAppGate_theWaitIsCapped(t *testing.T) {
	var g appGate
	leave, _, _ := g.enter(context.Background(), "web", true, time.Second, nil)
	defer leave()
	start := time.Now()
	if _, _, err := g.enter(context.Background(), "web", false, 80*time.Millisecond, nil); !errors.Is(err, errGateWait) {
		t.Fatalf("err = %v", err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("a capped wait took %s", took)
	}
}

func deployLog(s *Service, slug string) (*emitter, func() ([]string, *pb.DeployResult)) {
	var logs []string
	var res *pb.DeployResult
	e := &emitter{send: func(ev *pb.DeployEvent) error {
		if l := ev.GetLog(); l != nil {
			logs = append(logs, l.GetText())
		}
		if r := ev.GetResult(); r != nil {
			res = r
		}
		return nil
	}}
	return e, func() ([]string, *pb.DeployResult) { return logs, res }
}

func TestWaitForBackup_saysItWaitedAndForHowLong(t *testing.T) {
	gateWait(t, 5*time.Second)
	s := &Service{}
	leaveHold, _, _ := s.gate.enter(context.Background(), "web", true, time.Second, nil)
	time.AfterFunc(200*time.Millisecond, leaveHold)
	e, got := deployLog(s, "web")
	leave, ok := s.waitForBackup(context.Background(), "web", e)
	if !ok {
		t.Fatal("the deploy stopped waiting")
	}
	leave()
	logs, _ := got()
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "A backup of this app has paused it to copy its data, so the deploy waits until it runs again (5s at most).") ||
		!strings.Contains(joined, "Waited ") || !strings.Contains(joined, "for the backup to finish.") {
		t.Fatalf("log:\n%s", joined)
	}
}

func TestWaitForBackup_saysTheCapWhenItStopsWaiting(t *testing.T) {
	gateWait(t, 100*time.Millisecond)
	s := &Service{}
	leaveHold, _, _ := s.gate.enter(context.Background(), "web", true, time.Second, nil)
	defer leaveHold()
	e, got := deployLog(s, "web")
	if _, ok := s.waitForBackup(context.Background(), "web", e); ok {
		t.Fatal("the deploy went ahead past the cap")
	}
	if _, res := got(); res == nil || res.GetReady() || !strings.Contains(res.GetError(), "was still paused by a backup after 100ms") {
		t.Fatalf("result = %v", res)
	}
}

func TestWaitForBackup_aDeployWithNoBackupSaysNothing(t *testing.T) {
	s := &Service{}
	e, got := deployLog(s, "web")
	leave, ok := s.waitForBackup(context.Background(), "web", e)
	if !ok {
		t.Fatal("refused")
	}
	leave()
	if logs, _ := got(); len(logs) != 0 {
		t.Fatalf("logs = %v", logs)
	}
}

func TestVolumeBackup_waitsForADeployInProgressBeforePausing(t *testing.T) {
	gateWait(t, 5*time.Second)
	argv := dockerOnPath(t, holdDocker("0", "0"))
	s := &Service{dataBase: t.TempDir()}
	leaveDeploy, _, _ := s.gate.enter(context.Background(), "agent-test-web", false, time.Second, nil)
	done := make(chan []string, 1)
	go func() {
		err, logs := archiveWith(t, s)
		if err != nil {
			t.Error(err)
		}
		done <- logs
	}()
	time.Sleep(300 * time.Millisecond)
	if strings.Contains(argvVerbs(argv()), "pause") {
		t.Fatal("the backup paused the app in the middle of its deploy")
	}
	leaveDeploy()
	logs := strings.Join(<-done, "\n")
	if !strings.Contains(logs, "A deploy of this app is under way, so the backup waits for it to finish before pausing the app (5s at most).") ||
		!strings.Contains(logs, "for the deploy to finish.") {
		t.Fatalf("log:\n%s", logs)
	}
	if !strings.Contains(argvVerbs(argv()), "pause") {
		t.Fatal("the backup never paused the app once the deploy ended")
	}
}

func TestVolumeBackup_aDeployThatOutlastsTheCapFailsTheBackupWithoutPausing(t *testing.T) {
	gateWait(t, 100*time.Millisecond)
	argv := dockerOnPath(t, holdDocker("0", "0"))
	s := &Service{dataBase: t.TempDir()}
	leaveDeploy, _, _ := s.gate.enter(context.Background(), "agent-test-web", false, time.Second, nil)
	defer leaveDeploy()
	err, _ := archiveWith(t, s)
	if err == nil || !strings.Contains(err.Error(), "a deploy of this app was still under way after 100ms") {
		t.Fatalf("err = %v", err)
	}
	if strings.Contains(argvVerbs(argv()), "pause") {
		t.Fatal("the app was paused anyway")
	}
}

// A paused container fails its health check: the hold lets a deploy in only once it passes again.
func TestVolumeBackup_aDeployWaitsUntilAHealthCheckPassesAgain(t *testing.T) {
	gateWait(t, 10*time.Second)
	state := filepath.Join(t.TempDir(), "inspects")
	dockerOnPath(t, `case "$1" in
ps) echo abc123def456 ;;
inspect) n=$(cat `+state+` 2>/dev/null || echo 0); n=$((n+1)); echo $n > `+state+`
  if [ $n -eq 1 ]; then echo "abc123def456 healthy"; elif [ $n -lt 4 ]; then echo "abc123def456 unhealthy"; else echo "abc123def456 healthy"; fi ;;
esac`)
	s := &Service{dataBase: t.TempDir()}
	if err, _ := archiveWith(t, s); err != nil {
		t.Fatal(err)
	}
	if !s.HeldForBackup("abc123def456") {
		t.Fatal("a container that has not passed its health check again already counts as up")
	}
	start := time.Now()
	e, _ := deployLog(s, "agent-test-web")
	leave, ok := s.waitForBackup(context.Background(), "agent-test-web", e)
	if !ok {
		t.Fatal("the deploy stopped waiting")
	}
	leave()
	if took := time.Since(start); took < 500*time.Millisecond {
		t.Fatalf("the deploy went in %s after the pause, while the health check still failed", took)
	}
	if s.HeldForBackup("abc123def456") {
		t.Fatal("still held once healthy")
	}
}
