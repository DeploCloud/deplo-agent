package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// Two operations on ONE stack must not interleave: `docker compose` on the same `-p` from two goroutines races its own create/remove steps.
func TestLockStackSerializesOneSlug(t *testing.T) {
	s := &Service{}
	var mu sync.Mutex
	inside, peak := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.lockStack("shop")()
			mu.Lock()
			inside++
			if inside > peak {
				peak = inside
			}
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			mu.Lock()
			inside--
			mu.Unlock()
		}()
	}
	wg.Wait()
	if peak != 1 {
		t.Fatalf("two operations ran on one stack at once (peak %d)", peak)
	}
}

// Different stacks are independent: one slow stop must not hold up another app.
func TestLockStackDoesNotSerializeAcrossSlugs(t *testing.T) {
	s := &Service{}
	held := make(chan struct{})
	release := make(chan struct{})
	go func() {
		defer s.lockStack("shop")()
		close(held)
		<-release
	}()
	<-held
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer s.lockStack("blog")()
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a lock on one stack blocked another")
	}
	close(release)
}

func TestLockStackForgetsAReleasedSlug(t *testing.T) {
	s := &Service{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.lockStack("preview-42")() }()
	}
	wg.Wait()
	if n := len(s.stackLocks); n != 0 {
		t.Fatalf("%d stack locks left after every holder released", n)
	}
}

// Deploy brings a stack up only while it holds that stack's lock, like Reroute and Destroy do.
func TestDeployWaitsForTheStackLockBeforeUp(t *testing.T) {
	calls := fakeImageDocker(t, nil, map[string]string{"deplo-agent-test-img:1": "null"}, nil)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	const slug = "deplo-agent-test-lock"
	unlock := s.lockStack(slug)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runDeploy(context.Background(), &pb.DeployRequest{
			DeployId: "deplo-agent-test-lock", Slug: slug, ImageRef: "deplo-agent-test-img:1",
			SourceKind: pb.SourceKind_SOURCE_KIND_IMAGE, BuildKind: pb.BuildKind_BUILD_KIND_NONE,
			ComposeYaml: "services: {}\n", Network: testNetwork, ReadyTimeoutMs: 1,
		}, &emitter{send: func(*pb.DeployEvent) error { return nil }})
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(readCalls(t, calls), "image inspect") {
		if time.Now().After(deadline) {
			t.Fatal("the deploy never reached its image check")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if strings.Contains(readCalls(t, calls), " up ") {
		unlock()
		t.Fatal("compose up ran while another operation held the stack")
	}
	unlock()
	<-done
	if !strings.Contains(readCalls(t, calls), " up -d") {
		t.Fatalf("compose up never ran once the lock was free:\n%s", readCalls(t, calls))
	}
}
