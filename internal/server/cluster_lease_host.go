package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

func (s *Service) leaseFile() string {
	if s.agentDir == "" {
		return ""
	}
	return filepath.Join(s.agentDir, "cluster-lease.json")
}

// leaseGuard is created with the member, so a lease that ran out while the agent was down is still honoured.
func (s *Service) leaseGuard() *leaseGuard {
	s.leaseOnce.Do(func() {
		s.lease = newLeaseGuard(s.leaseFile(), bootClock, s.fenceStacks)
		s.lease.start, s.lease.leader = startLane(startLanes(), s.startFromBundle), s.leaderPins
		s.lease.remove, s.lease.resume, s.lease.trim = s.removeMoved, s.resumeHeld, s.trimReplicas
		s.lease.keep, s.lease.purge, s.lease.stop = s.keepLeft, s.purgeLeft, s.stopMoved
		s.lease.relinquish = s.relinquishStack
		s.lease.aside, s.lease.dropAside = s.asideStack, s.dropAside
		s.lease.dropFailed = s.dropFailedStart
		s.lease.succeed = s.askLeaderToLead
		s.lease.has = func(slug string) bool { return isFile(s.stackPath(slug)) }
		s.lease.running, s.lease.runningAll = stackRuns, stacksRun
		if s.lease.unreadable && s.loadMember().Member {
			s.lease.presumeDead(s.stackSlugs())
		}
	})
	return s.lease
}

// stackSlugs is every stack this host holds, a deploy's stand-in aside.
func (s *Service) stackSlugs() []string {
	files, _ := filepath.Glob(filepath.Join(s.stackDir, "*.yml"))
	var out []string
	for _, f := range files {
		slug := strings.TrimSuffix(filepath.Base(f), ".yml")
		if validateSlug(slug) == nil {
			out = append(out, slug)
		}
	}
	return out
}

// fenceGrace is what a fenced stack gets to stop cleanly before it is killed; a var so tests can hurry it.
var fenceGrace = 5 * time.Second

// fenceTakes is the longest from a lease running out to its stacks being down: the check, the stop, the kills
// fenceStack runs together, plus a second. A leader waits 13 s for an agent Deplo knows has it (quickStop).
func fenceTakes() time.Duration {
	return leaseCheckEvery + fenceGrace + fenceStopSlack + fenceKillFor + time.Second
}

// fenceStopSlack is what compose takes past the grace to say the stop is done; fenceKillFor bounds the kills.
const (
	fenceStopSlack = time.Second
	fenceKillFor   = 3 * time.Second
)

// fenceStacks stops every stack at once, and every running scheduled job. It never waits for a stack's lock: a
// deploy that holds one is cancelled, and whatever it started is stopped by the deploy itself on its way out.
func (s *Service) fenceStacks(ctx context.Context, entries []lease.Entry) error {
	s.fenceJobs()
	slugs := map[string]bool{}
	for _, e := range entries {
		slugs[e.Slug] = true
	}
	s.cancelDeploysOf(slugs)
	ctx, cancel := context.WithTimeout(ctx, fenceGrace+10*time.Second)
	defer cancel()
	errs := make(chan error, len(slugs))
	for slug := range slugs {
		go func() {
			began := time.Now()
			err := s.fenceStack(ctx, slug)
			// Read after the stop, so the fence is never late: what exited since it began is what it stopped.
			listed, done := context.WithTimeout(context.Background(), 5*time.Second)
			s.supervision().fencedSince(listed, slug, began)
			done()
			errs <- err
		}()
	}
	var first error
	for range slugs {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// fenceStack stops one stack within fenceGrace, kills what is left, and fails when something of it still runs.
func (s *Service) fenceStack(ctx context.Context, slug string) error {
	if err := validateSlug(slug); err != nil {
		return err
	}
	grace := strconv.Itoa(max(int(fenceGrace/time.Second), 1))
	res, err := dockercli.Run(ctx, fenceGrace+fenceStopSlack, s.composeCtl(slug, "stop", "-t", grace)...)
	kills := [][]string{}
	if err != nil || res.Code != 0 || stackRuns(ctx, slug) {
		kills = append(kills, s.composeCtl(slug, "kill"), []string{"kill", "deplo-" + slug})
	}
	// A deploy's stand-in mounts the same volume: it goes with the stack.
	if isFile(s.stackPath(slug + standInSuffix)) {
		kills = append(kills, append(s.standInCompose(slug), "kill"))
	}
	var wg sync.WaitGroup
	for _, argv := range kills {
		wg.Go(func() { _, _ = dockercli.Run(ctx, fenceKillFor, argv...) })
	}
	wg.Wait()
	if stackRuns(ctx, slug) {
		return fmt.Errorf("%s: still running", slug)
	}
	return nil
}

// relinquishStack stops a stack whose lease moved to another member: once at once, as the fence does, then again
// under the stack's lock, so a start that was under way when the lease moved does not outlive it. Nothing is removed.
func (s *Service) relinquishStack(ctx context.Context, slug string) (bool, error) {
	s.supervision().unfenced(slug)
	stopped := false
	var err error
	if stackRuns(ctx, slug) {
		stopped, err = true, s.fenceStack(ctx, slug)
	}
	defer s.lockStack(slug)()
	if stackRuns(ctx, slug) {
		stopped, err = true, s.fenceStack(ctx, slug)
	}
	return stopped, err
}

// lockToStart takes a stack's lock for something that brings it up, refused while this member has no live lease.
// done releases the lock and says whether the lease ran out meanwhile: the stack is then stopped again.
func (s *Service) lockToStart(slug string) (done func() bool, ok bool) {
	unlock := s.lockStack(slug)
	if s.leaseGuard().Holds(slug) {
		unlock()
		return nil, false
	}
	return func() bool {
		defer unlock()
		if !s.leaseGuard().mustStayDown(slug) {
			return false
		}
		ctx, cancel := context.WithTimeout(context.Background(), fenceGrace+10*time.Second)
		defer cancel()
		if err := s.fenceStack(ctx, slug); err != nil {
			fmt.Fprintf(os.Stderr, "deplo-agent: the lease ran out while %s started, and it could not be stopped again: %v\n", slug, err)
		} else {
			fmt.Fprintf(os.Stderr, "deplo-agent: stopped %s again: its lease ran out while it started\n", slug)
		}
		return true
	}, true
}
