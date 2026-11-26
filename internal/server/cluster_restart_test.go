package server

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// c47: back from a kill -9, the agent answered the visitors other members carried to it only once docker answered
// the rest of its start, 4 s later. It reads what it answers for first, alone; the rest waits for its first renewal.
func TestRestart_whatAServerAnswersForIsReadBeforeTheRestOfItsStart(t *testing.T) {
	was := agentStarted
	agentStarted = time.Now()
	t.Cleanup(func() { agentStarted = was })
	a, b, _, _, members := answerPair(t)
	bothAnswer(t, a, b, members)
	placeOn(t, b, "srvB", appEntry("srvB"), dbEntry("srvB"))
	eventually(t, "the lease on disk", func() bool {
		raw, _ := os.ReadFile(b.leaseFile())
		return strings.Contains(string(raw), "db-shop")
	})
	_ = os.Remove(filepath.Join(b.agentDir, handoverFile))

	next := New(t.TempDir(), t.TempDir(), "/", "")
	next.SetAgentDir(b.agentDir)
	var mu sync.Mutex
	var calls []string
	asked := func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(calls) }
	release := make(chan struct{})
	ops := newFakeFwdHost().ops()
	ready := ops.ready
	ops.ready = func(ctx context.Context, id string) appReadiness {
		mu.Lock()
		calls = append(calls, "ready "+id)
		mu.Unlock()
		<-release
		return ready(ctx, id)
	}
	next.fwdOps = ops
	next.leaseGuard().has = func(string) bool { return true }
	next.supervision().list = func(context.Context, string) ([]writerContainer, error) {
		mu.Lock()
		calls = append(calls, "list")
		mu.Unlock()
		return nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go next.forwarders().Resume()
	go next.superviseWriters(ctx)
	eventually(t, "a first docker call", func() bool { return len(asked()) > 0 })
	// A visitor carried here meanwhile waits for that look: it asks docker nothing of its own.
	got := make(chan bool, 1)
	go func() { got <- next.forwarders().ready(ctx, "prj1") }()
	time.Sleep(200 * time.Millisecond)
	close(release)
	if !<-got {
		t.Fatal("the App the start read is not ready")
	}
	time.Sleep(200 * time.Millisecond)
	if got := asked(); !slices.Equal(got, []string{"ready prj1"}) {
		t.Fatalf("before its first renewal the start asked docker %q", got)
	}
	placeOn(t, next, "srvB", appEntry("srvB"), dbEntry("srvB"))
	eventually(t, "the supervisor's first round once renewed", func() bool { return slices.Contains(asked(), "list") })
}

// The start's one inspect of every App's containers is read back App by App, as appReady reads one.
func TestReadinessByApp_readsEachAppOfOneInspect(t *testing.T) {
	at := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	running := `{"id":"a1","name":"/deplo-shop-1","status":"running","started":"` + at + `","health":null}`
	exited := `{"id":"b1","name":"/deplo-blog-1","status":"exited","started":"` + at + `","health":null}`
	got := readinessByApp("prj1 "+running+"\nprj2 "+exited+"\n", []string{"prj1", "prj2", "prj3"})
	if !got["prj1"].OK || got["prj2"].OK || got["prj3"].OK {
		t.Fatalf("read %+v", got)
	}
	if want := readinessOf([]string{running}); got["prj1"] != want {
		t.Fatalf("prj1 is %+v, one look of its own says %+v", got["prj1"], want)
	}
}
