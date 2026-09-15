package server

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

var dockerLevelStderr = regexp.MustCompile(
	`(?m)(?:OCI runtime|unable to start container process|executable file not found in \$PATH|Error response from daemon|No such container|is not running|is paused|Cannot connect to the Docker daemon|cannot exec in a stopped|container .* is (?:not running|paused|restarting)|chdir to cwd .* set in config\.json failed)`,
)

func isDockerLevelStderr(s string) bool {
	return dockerLevelStderr.MatchString(s)
}

type shellPlan struct {
	run []string
}

func (p shellPlan) raw() bool { return p.run == nil }

type shellCandidate struct {
	probe []string
	run   []string
}

var shellCandidates = []shellCandidate{
	{probe: []string{"sh", "-c", ":"}, run: []string{"sh", "-lc"}},
	{probe: []string{"bash", "-c", ":"}, run: []string{"bash", "-lc"}},
	{probe: []string{"ash", "-c", ":"}, run: []string{"ash", "-lc"}},
	{probe: []string{"busybox", "sh", "-c", ":"}, run: []string{"busybox", "sh", "-c"}},
}

const shellTTL = 5 * time.Minute

const shellCacheMax = 1024

type shellCacheEntry struct {
	plan  shellPlan
	image string
	at    time.Time
}

var (
	shellCacheMu sync.Mutex
	shellCache   = map[string]shellCacheEntry{}
)

func evictShellCacheLocked(now time.Time) {
	for k, v := range shellCache {
		if now.Sub(v.at) >= shellTTL {
			delete(shellCache, k)
		}
	}
	for len(shellCache) >= shellCacheMax {
		var oldestKey string
		var oldestAt time.Time
		found := false
		for k, v := range shellCache {
			if !found || v.at.Before(oldestAt) {
				oldestKey, oldestAt, found = k, v.at, true
			}
		}
		if !found {
			break
		}
		delete(shellCache, oldestKey)
	}
}

func resolveShellPlan(ctx context.Context, name, image string) shellPlan {
	shellCacheMu.Lock()
	hit, ok := shellCache[name]
	shellCacheMu.Unlock()
	if ok && hit.image == image && time.Since(hit.at) < shellTTL {
		return hit.plan
	}

	plan := shellPlan{run: nil}
	for _, c := range shellCandidates {
		args := append([]string{"exec", name}, c.probe...)
		res, err := dockercli.Run(ctx, 5*time.Second, args...)
		if err != nil {
			return shellPlan{run: nil}
		}
		if res.Code == 0 {
			plan = shellPlan{run: c.run}
			break
		}
		if isDockerLevelStderr(res.Stderr) {
			return shellPlan{run: nil}
		}
	}
	shellCacheMu.Lock()
	now := time.Now()
	if _, exists := shellCache[name]; !exists && len(shellCache) >= shellCacheMax {
		evictShellCacheLocked(now)
	}
	shellCache[name] = shellCacheEntry{plan: plan, image: image, at: now}
	shellCacheMu.Unlock()
	return plan
}

func shellLabelFor(ctx context.Context, name, image string) string {
	plan := resolveShellPlan(ctx, name, image)
	if plan.raw() {
		return "raw exec (no shell)"
	}
	if plan.run[0] == "bash" {
		return "/bin/bash"
	}
	return "/bin/sh"
}

func splitArgv(s string) []string {
	out := []string{}
	var cur strings.Builder
	var quote rune
	has := false
	for _, ch := range s {
		switch {
		case quote != 0:
			if ch == quote {
				quote = 0
			} else {
				cur.WriteRune(ch)
			}
			has = true
		case ch == '"' || ch == '\'':
			quote = ch
			has = true
		case ch == ' ' || ch == '\t':
			if has {
				out = append(out, cur.String())
				cur.Reset()
				has = false
			}
		default:
			cur.WriteRune(ch)
			has = true
		}
	}
	if has {
		out = append(out, cur.String())
	}
	return out
}

// Exec runs a command in a container (docker exec), mirroring lib/infra/docker.ts execInContainer + lib/data/console.ts execInContainer's shell/raw dispatch.
func (s *Service) Exec(ctx context.Context, req *pb.ExecRequest) (*pb.ExecResponse, error) {
	if err := assertOwned(ctx, req.GetContainer(), req.GetProjectId()); err != nil {
		return nil, err
	}
	name := req.GetContainer()
	command := strings.TrimSpace(req.GetCommand())
	if command == "" {
		return &pb.ExecResponse{Code: 0, RawMode: false}, nil
	}

	plan := resolveShellPlan(ctx, name, req.GetImage())
	if !plan.raw() {
		args := append(append([]string{"exec", name}, plan.run...), command)
		res, err := dockercli.Run(ctx, 30*time.Second, args...)
		if err != nil {
			return nil, err
		}
		return &pb.ExecResponse{
			Code:    int32(res.Code),
			Stdout:  res.Stdout,
			Stderr:  res.Stderr,
			RawMode: false,
		}, nil
	}

	argv := splitArgv(command)
	if len(argv) == 0 {
		return &pb.ExecResponse{Code: 0, RawMode: true}, nil
	}
	args := append([]string{"exec", name}, argv...)
	res, err := dockercli.Run(ctx, 30*time.Second, args...)
	if err != nil {
		return nil, err
	}
	return &pb.ExecResponse{
		Code:    int32(res.Code),
		Stdout:  res.Stdout,
		Stderr:  res.Stderr,
		RawMode: true,
	}, nil
}

// ShellLabel returns the default/chosen container's shell label for the banner.
func (s *Service) ShellLabel(ctx context.Context, req *pb.ShellLabelRequest) (*pb.ShellLabelResponse, error) {
	if err := assertOwned(ctx, req.GetContainer(), req.GetProjectId()); err != nil {
		return nil, err
	}
	return &pb.ShellLabelResponse{
		Label: shellLabelFor(ctx, req.GetContainer(), req.GetImage()),
	}, nil
}
