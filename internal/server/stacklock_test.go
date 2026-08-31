package server

import (
	"sync"
	"testing"
	"time"
)

// Two operations on ONE stack must not interleave: `docker compose` on the same
// `-p` from two goroutines races its own create/remove steps.
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
