package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

type dependencyJob struct {
	done chan struct{}
	err  error
}
type dependencyPreparation struct {
	spec     *dependencySpec
	miseDone chan struct{}
	done     chan struct{}
	miseErr  error
	err      error
	attempts int
	seed     string
}
type dependencyState struct {
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	binaries     map[string]bool
	preparations map[string]*dependencyPreparation
	jobs         map[string]*dependencyJob
}

func (s *Service) preparedDependencyImages() map[string]bool {
	images := map[string]bool{"docker": true, "docker.io/library/docker": true}
	s.dependencies.mu.Lock()
	defer s.dependencies.mu.Unlock()
	for _, prep := range s.dependencies.preparations {
		for _, dep := range prep.spec.Dependencies {
			if dep.Kind == "docker-image" && dep.Target == "host" {
				images[repoOf(dep.Image)] = true
			}
		}
	}
	return images
}

// The service owns preparation; an RPC cancellation only stops its waiter.
func (s *Service) StartDeplopackPreparation(parent context.Context) {
	if runtime.GOOS != "linux" || (runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64") {
		return
	}
	s.dependencies.mu.Lock()
	s.dependencies.ctx, s.dependencies.cancel = context.WithCancel(parent)
	s.dependencies.binaries = map[string]bool{}
	s.dependencies.preparations = map[string]*dependencyPreparation{}
	s.dependencies.jobs = map[string]*dependencyJob{}
	s.dependencies.wg.Add(1)
	s.dependencies.mu.Unlock()
	go func() {
		defer s.dependencies.wg.Done()
		ctx, cancel := context.WithTimeout(s.dependencies.ctx, 20*time.Minute)
		defer cancel()
		binary, version, _, err := s.ensureDetector(ctx)
		if err != nil {
			log.Printf("DeploPack background installation failed: %v", err)
			return
		}
		if _, err := s.ensureBuilder(ctx, version); err != nil {
			log.Printf("DeploPack background builder installation failed: %v", err)
		}
		s.queueDeplopackPreparation(binary)
	}()
}

func (s *Service) StopDeplopackPreparation() {
	s.dependencies.mu.Lock()
	cancel := s.dependencies.cancel
	s.dependencies.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	done := make(chan struct{})
	go func() { s.dependencies.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		log.Print("DeploPack preparation cleanup timed out")
	}
}

func (s *Service) queueDeplopackPreparation(binary string) {
	info, err := os.Stat(binary)
	if err != nil {
		return
	}
	key := fmt.Sprintf("%s/%d/%d", binary, info.ModTime().UnixNano(), info.Size())
	state := &s.dependencies
	state.mu.Lock()
	if state.ctx == nil || state.ctx.Err() != nil || state.binaries[key] {
		state.mu.Unlock()
		return
	}
	state.binaries[key] = true
	state.wg.Add(1)
	state.mu.Unlock()
	go func() {
		defer state.wg.Done()
		spec, err := readDependencySpec(state.ctx, binary)
		if err != nil {
			log.Printf("DeploPack dependency catalog failed: %v", err)
			return
		}
		if spec == nil {
			log.Print("DeploPack dependency catalog unavailable or unsupported; using legacy preparation")
			return
		}
		s.prepareDeplopack(spec, false)
	}()
}

func (s *Service) prepareDeplopack(spec *dependencySpec, retry bool) *dependencyPreparation {
	state := &s.dependencies
	state.mu.Lock()
	defer state.mu.Unlock()
	previous := state.preparations[spec.fingerprint]
	attempts := 1
	if previous != nil {
		select {
		case <-previous.done:
			if previous.err != nil && (!retry || previous.attempts >= 2) {
				return previous
			}
			if previous.err != nil {
				attempts = previous.attempts + 1
			}
		default:
			return previous
		}
	}
	preparation := &dependencyPreparation{spec: spec, miseDone: make(chan struct{}), done: make(chan struct{}), attempts: attempts, seed: "deplo-deplopack-images-" + spec.fingerprint[:20]}
	state.preparations[spec.fingerprint] = preparation
	state.wg.Add(1)
	go func() {
		defer state.wg.Done()
		defer close(preparation.done)
		ctx, cancel := context.WithTimeout(state.ctx, 20*time.Minute)
		defer cancel()
		log.Printf("DeploPack preparation started catalog=%s attempt=%d", spec.fingerprint[:20], attempts)
		preparation.miseErr = s.dependencyResource(ctx, "mise/"+spec.Platform+"/"+spec.resource("mise").URL, retry, func() error { return s.installDependencyMise(ctx, *spec.resource("mise")) })
		close(preparation.miseDone)
		preparation.err = preparation.miseErr
		if preparation.err == nil {
			for _, dep := range spec.Dependencies {
				if dep.Kind != "docker-image" || dep.Target != "host" {
					continue
				}
				dep := dep
				preparation.err = s.dependencyResource(ctx, "docker/"+spec.Platform+"/"+dep.Image, retry, func() error { return dependencyPullImage(ctx, spec.Platform, dep.Image) })
				if preparation.err != nil {
					break
				}
			}
		}
		if preparation.err == nil {
			preparation.err = s.dependencyResource(ctx, "seed/"+spec.fingerprint, retry, func() error { return s.prepareDependencySeed(ctx, preparation) })
		}
		if preparation.err != nil {
			log.Printf("DeploPack preparation failed catalog=%s: %v", spec.fingerprint[:20], preparation.err)
		} else {
			log.Printf("DeploPack preparation ready catalog=%s seed=%s", spec.fingerprint[:20], preparation.seed)
		}
	}()
	return preparation
}

func (s *Service) dependencyResource(ctx context.Context, key string, retry bool, work func() error) error {
	state := &s.dependencies
	state.mu.Lock()
	job := state.jobs[key]
	if job != nil {
		select {
		case <-job.done:
			// Successful work must revalidate disk state after cleanup or external removal.
			if job.err == nil || retry {
				job = nil
			}
		default:
		}
	}
	if job == nil {
		job = &dependencyJob{done: make(chan struct{})}
		state.jobs[key] = job
		state.mu.Unlock()
		log.Printf("DeploPack dependency start: %s", key)
		job.err = work()
		close(job.done)
		if job.err == nil {
			log.Printf("DeploPack dependency ready: %s", key)
		} else {
			log.Printf("DeploPack dependency failed: %s: %v", key, job.err)
		}
		return job.err
	}
	state.mu.Unlock()
	log.Printf("DeploPack dependency reused or awaited: %s", key)
	select {
	case <-job.done:
		return job.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) awaitDeplopackPreparation(ctx context.Context, spec *dependencySpec, miseOnly bool, e *emitter) (*dependencyPreparation, error) {
	state := &s.dependencies
	state.mu.Lock()
	started := state.ctx != nil
	state.mu.Unlock()
	if !started {
		return nil, fmt.Errorf("DeploPack preparation service is not running")
	}
	preparation := s.prepareDeplopack(spec, true)
	done := preparation.done
	if miseOnly {
		done = preparation.miseDone
	}
	select {
	case <-done:
	default:
		if e != nil {
			e.log("info", "Waiting for DeploPack dependencies already being prepared in background")
		}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
		var err error
		if miseOnly {
			err = preparation.miseErr
		} else {
			err = preparation.err
		}
		if err != nil && preparation.attempts < 2 {
			// Retry only after the owned preparation has ended, never while it still runs.
			select {
			case <-preparation.done:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			preparation = s.prepareDeplopack(spec, true)
			done = preparation.done
			if miseOnly {
				done = preparation.miseDone
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
			}
			if miseOnly {
				err = preparation.miseErr
			} else {
				err = preparation.err
			}
		}
		return preparation, err
	}
}
