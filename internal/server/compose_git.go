package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type composeRepository struct {
	Root string `json:"root"`
	Path string `json:"path"`
}

// ReadRepoCompose reads one file without exposing repository environment files.
func (s *Service) ReadRepoCompose(ctx context.Context, req *pb.ReadRepoComposeRequest) (*pb.ReadRepoComposeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if _, err := validateAnalysisSource(req.GetSource()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	select {
	case s.analysisSlots <- struct{}{}:
		defer func() { <-s.analysisSlots }()
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	discard := &emitter{send: func(*pb.DeployEvent) error { return nil }}
	root, sha, cleanup, err := s.materializeGit(ctx, req.GetSource(), "compose-analysis", discard)
	if err != nil {
		return nil, analysisError(ctx, codes.InvalidArgument, err)
	}
	defer cleanup()
	contents, err := readRepositoryCompose(root, req.GetComposePath())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &pb.ReadRepoComposeResponse{CommitSha: sha, ComposeYaml: contents}, nil
}

func repositoryPath(root, base, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || strings.ContainsAny(relative, "$\x00\\") {
		return "", fmt.Errorf("repository file paths must be literal relative paths")
	}
	path := filepath.Join(base, relative)
	inside := func(candidate string) bool {
		rel, err := filepath.Rel(root, candidate)
		return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
	}
	if !inside(path) {
		return "", fmt.Errorf("repository path escapes the app root")
	}
	// Missing bind directories are allowed, but existing ancestors must remain inside the checkout.
	for ancestor := path; ; ancestor = filepath.Dir(ancestor) {
		if info, err := os.Lstat(ancestor); err == nil && info.Mode()&os.ModeSymlink != 0 {
			if _, err := filepath.EvalSymlinks(ancestor); err != nil {
				return "", fmt.Errorf("repository symlink cannot be resolved")
			}
		}
		real, err := filepath.EvalSymlinks(ancestor)
		if err == nil {
			if !inside(real) {
				return "", fmt.Errorf("repository symlink escapes the app root")
			}
			return path, nil
		}
		if !os.IsNotExist(err) || ancestor == root {
			return "", fmt.Errorf("cannot resolve repository path")
		}
	}
}

func readRepositoryCompose(root, relative string) (string, error) {
	path, err := repositoryPath(root, root, relative)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return "", fmt.Errorf("Compose file must be a regular file of at most 1 MiB")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read repository Compose file: %w", err)
	}
	if err := validateRepositoryCompose(root, filepath.Dir(path), data); err != nil {
		return "", err
	}
	return string(data), nil
}

func validateRepositoryCompose(root, base string, data []byte) error {
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("invalid repository Compose YAML")
	}
	if doc["include"] != nil {
		return fmt.Errorf("Compose include is not supported")
	}
	check := func(value any, directory string) error {
		path, ok := value.(string)
		if !ok {
			return fmt.Errorf("invalid repository file reference")
		}
		// Explicit host paths are authorized by the control plane's Compose privilege checks.
		if filepath.IsAbs(path) && !strings.ContainsAny(path, "$\x00") {
			return nil
		}
		_, err := repositoryPath(root, directory, path)
		return err
	}
	services, ok := doc["services"].(map[string]any)
	if !ok || len(services) == 0 {
		return fmt.Errorf("Compose file has no services")
	}
	for _, entry := range services {
		svc, ok := entry.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid Compose service")
		}
		if svc["extends"] != nil {
			return fmt.Errorf("Compose extends is not supported")
		}
		if build := svc["build"]; build != nil {
			contextPath := base
			var dockerfile any
			switch b := build.(type) {
			case string:
				if err := check(b, base); err != nil {
					return err
				}
				contextPath = b
				if !filepath.IsAbs(contextPath) {
					contextPath = filepath.Join(base, contextPath)
				}
			case map[string]any:
				if context, exists := b["context"]; exists {
					if err := check(context, base); err != nil {
						return err
					}
					contextPath = fmt.Sprint(context)
					if !filepath.IsAbs(contextPath) {
						contextPath = filepath.Join(base, contextPath)
					}
				}
				dockerfile = b["dockerfile"]
				var contexts []any
				switch extra := b["additional_contexts"].(type) {
				case map[string]any:
					for _, value := range extra {
						contexts = append(contexts, value)
					}
				case []any:
					for _, value := range extra {
						text, ok := value.(string)
						_, path, found := strings.Cut(text, "=")
						if !ok || !found {
							return fmt.Errorf("invalid additional build context")
						}
						contexts = append(contexts, path)
					}
				case nil:
				default:
					return fmt.Errorf("invalid additional build contexts")
				}
				for _, value := range contexts {
					path, ok := value.(string)
					if ok && (strings.HasPrefix(path, "service:") || strings.HasPrefix(path, "docker-image://")) {
						continue
					}
					if err := check(value, base); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("invalid Compose build")
			}
			if dockerfile != nil {
				if err := check(dockerfile, contextPath); err != nil {
					return err
				}
			}
			if strings.HasPrefix(contextPath, root+string(filepath.Separator)) || contextPath == root {
				for _, name := range []string{".dockerignore", "Dockerfile", "dockerfile"} {
					if _, err := repositoryPath(root, contextPath, name); err != nil {
						return err
					}
				}
			}
		}
		files := svc["env_file"]
		if files != nil {
			list, ok := files.([]any)
			if !ok {
				list = []any{files}
			}
			for _, file := range list {
				if record, ok := file.(map[string]any); ok {
					file = record["path"]
				}
				if err := check(file, base); err != nil {
					return err
				}
			}
		}
		volumes, _ := svc["volumes"].([]any)
		for _, volume := range volumes {
			var source string
			if text, ok := volume.(string); ok {
				parts := strings.SplitN(text, ":", 2)
				if len(parts) == 2 {
					source = parts[0]
				}
			} else if record, ok := volume.(map[string]any); ok && record["type"] == "bind" {
				source, _ = record["source"].(string)
			}
			if source != "" && (strings.ContainsAny(source, "/.$\\") || filepath.IsAbs(source)) {
				if err := check(source, base); err != nil {
					return err
				}
			}
		}
	}
	for _, kind := range []string{"configs", "secrets"} {
		entries, _ := doc[kind].(map[string]any)
		for _, entry := range entries {
			if record, ok := entry.(map[string]any); ok && record["file"] != nil {
				if err := check(record["file"], base); err != nil {
					return err
				}
			}
		}
	}
	_, err := repositoryPath(root, base, ".env")
	return err
}

func (s *Service) resolveRepositoryCompose(ctx context.Context, slug, contents, envFile string, repo composeRepository) (string, error) {
	if err := validateRepositoryCompose(repo.Root, filepath.Dir(filepath.Join(repo.Root, repo.Path)), []byte(contents)); err != nil {
		return "", err
	}
	file, err := os.CreateTemp(s.filesRoot(slug), ".repo-compose-*.yml")
	if err != nil {
		return "", err
	}
	defer os.Remove(file.Name())
	if _, err := file.WriteString(contents); err != nil {
		file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	base := filepath.Dir(filepath.Join(repo.Root, repo.Path))
	args := composeBaseArgs("deplo-"+slug, file.Name(), "", base)
	if local := filepath.Join(base, ".env"); isFile(local) {
		args = append(args, "--env-file", local)
	}
	args = append(args, "--env-file", envFile, "config", "--no-env-resolution")
	result, err := dockercli.Run(ctx, 2*time.Minute, args...)
	if err != nil || result.Code != 0 {
		return "", fmt.Errorf("cannot resolve repository Compose configuration; check variables and file references")
	}
	return result.Stdout, nil
}

func (s *Service) repositoryStatePath(slug string) string {
	return filepath.Join(s.filesRoot(slug), ".compose-repository.json")
}

func (s *Service) loadComposeRepository(slug string) (*composeRepository, error) {
	data, err := os.ReadFile(s.repositoryStatePath(slug))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var repo composeRepository
	if err := json.Unmarshal(data, &repo); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(repo.Root, filepath.Join(s.filesRoot(slug), "repositories")+string(filepath.Separator)) {
		return nil, fmt.Errorf("invalid repository checkout state")
	}
	return &repo, nil
}

func (s *Service) checkoutRepositoryCompose(ctx context.Context, req *pb.DeployRequest, e *emitter) (*composeRepository, string, error) {
	if _, err := validateAnalysisSource(req.GetGit()); err != nil {
		return nil, "", err
	}
	if req.GetGit() == nil || !fullCommitSha.MatchString(req.GetGit().GetCommit()) {
		return nil, "", fmt.Errorf("repository Compose requires a pinned Git revision")
	}
	parent := filepath.Join(s.filesRoot(req.GetSlug()), "repositories")
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, "", err
	}
	root, sha, cleanup, err := s.materializeGitIn(ctx, req.GetGit(), req.GetSlug(), e, parent)
	if err != nil {
		return nil, "", err
	}
	if sha != req.GetGit().GetCommit() {
		cleanup()
		return nil, "", fmt.Errorf("repository revision changed")
	}
	if _, err := readRepositoryCompose(root, req.GetComposePath()); err != nil {
		cleanup()
		return nil, "", err
	}
	return &composeRepository{Root: root, Path: req.GetComposePath()}, sha, nil
}

func (s *Service) cleanComposeRepositories(ctx context.Context, slug string) {
	state, err := s.loadComposeRepository(slug)
	if err != nil {
		return
	}
	keep := []string{}
	if state != nil {
		keep = append(keep, state.Root)
	}
	listed, err := dockercli.Run(ctx, 30*time.Second, "ps", "-aq", "--filter", "label=com.docker.compose.project=deplo-"+slug)
	if err != nil || listed.Code != 0 {
		return
	}
	if ids := strings.Fields(listed.Stdout); len(ids) > 0 {
		result, err := dockercli.Run(ctx, time.Minute, append([]string{"inspect"}, ids...)...)
		if err != nil || result.Code != 0 {
			return
		}
		var containers []struct{ Mounts []struct{ Source string } }
		if json.Unmarshal([]byte(result.Stdout), &containers) != nil {
			return
		}
		for _, container := range containers {
			for _, mount := range container.Mounts {
				keep = append(keep, mount.Source)
			}
		}
	}
	parent := filepath.Join(s.filesRoot(slug), "repositories")
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	stack, _ := os.ReadFile(s.stackPath(slug))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(parent, entry.Name())
		used := strings.Contains(string(stack), dir+string(filepath.Separator))
		for _, path := range keep {
			if path == dir || strings.HasPrefix(path, dir+string(filepath.Separator)) {
				used = true
				break
			}
		}
		if !used {
			_ = os.RemoveAll(dir)
		}
	}
}
