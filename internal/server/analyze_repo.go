package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const maxAnalysisOutput = 4 << 20

var analysisEnvName = regexp.MustCompile(`^DEPLOPACK_[A-Z][A-Z0-9_]*$`)

// AnalyzeRepo runs the detector against a disposable checkout of one revision.
func (s *Service) AnalyzeRepo(ctx context.Context, req *pb.AnalyzeRepoRequest) (*pb.AnalyzeRepoResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	select {
	case s.analysisSlots <- struct{}{}:
		defer func() { <-s.analysisSlots }()
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if _, err := validateAnalysisSource(req.GetSource()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	for name, value := range req.GetEnvironment() {
		if !analysisEnvName.MatchString(name) || name == "DEPLOPACK_PROVIDER" || strings.ContainsRune(value, 0) {
			return nil, status.Error(codes.InvalidArgument, "invalid detector configuration environment")
		}
		if (name == "DEPLOPACK_CONFIG_FILE" || name == "DEPLOPACK_SHELL_SCRIPT") && !analysisRelative(value) {
			return nil, status.Error(codes.InvalidArgument, "configuration path must be inside the project")
		}

	}
	binary, version, warning, err := s.ensureDetector(ctx)
	if err != nil {
		return nil, analysisError(ctx, codes.Unavailable, err)
	}
	dir, sha, cleanup, err := s.analysisCheckout(ctx, req.GetSource(), req.GetEnvironment())
	if err != nil {
		return nil, analysisError(ctx, codes.InvalidArgument, err)
	}
	defer cleanup()
	result, err := executeDetector(ctx, binary, dir, req.GetEnvironment())
	if err != nil {
		return nil, analysisError(ctx, codes.DataLoss, err)
	}
	if warning != "" {
		logs, _ := result["logs"].([]any)
		result["logs"] = append(logs, map[string]any{"Level": "info", "Msg": warning, "DocsPath": ""})
	}
	structured, err := structpb.NewStruct(result)
	if err != nil {
		return nil, status.Error(codes.DataLoss, "invalid detector result")
	}
	root := filepath.ToSlash(filepath.Clean(req.GetSource().GetSubdir()))
	if root == "." || root == "" {
		root = "."
	}
	return &pb.AnalyzeRepoResponse{CommitSha: sha, RootDirectory: root, DetectorVersion: version, Result: structured}, nil
}

func analysisError(ctx context.Context, code codes.Code, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(code, err.Error())
}

// boundedOutput cancels the process group as soon as captured output exceeds its budget.
type boundedOutput struct {
	buffer   bytes.Buffer
	limit    int
	cancel   context.CancelFunc
	exceeded bool
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.buffer.Len() {
		b.exceeded = true
		b.cancel()
		return 0, fmt.Errorf("process output exceeds limit")
	}
	return b.buffer.Write(p)
}

func executeDetector(ctx context.Context, binary, dir string, env map[string]string) (map[string]any, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := dockercli.Command(ctx, binary)
	cmd.Dir = dir
	cmd.WaitDelay = dockercli.WaitDelay
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.TempDir()}
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		cmd.Env = append(cmd.Env, name+"="+env[name])
	}
	output := &boundedOutput{limit: maxAnalysisOutput, cancel: cancel}
	diagnostic := &boundedOutput{limit: 64 << 10, cancel: cancel}
	cmd.Stdout, cmd.Stderr = output, diagnostic
	err := cmd.Run()
	if output.exceeded || diagnostic.exceeded {
		return nil, status.Error(codes.ResourceExhausted, "detector output exceeds limit")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	var result map[string]any
	if json.Unmarshal(output.buffer.Bytes(), &result) != nil || result == nil {
		return nil, fmt.Errorf("detector returned invalid JSON")
	}
	success, ok := result["success"].(bool)
	if !ok {
		return nil, fmt.Errorf("detector result has no boolean success")
	}
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		return nil, fmt.Errorf("could not execute detector")
	}
	if (err == nil) != success {
		return nil, fmt.Errorf("detector exit status disagrees with result")
	}
	if detections, ok := result["detections"].([]any); !ok || detections == nil {
		return nil, fmt.Errorf("detector result has no detections array")
	}
	return result, nil
}

// analysisGit keeps credentials in the environment and does not return Git's raw diagnostics.
func analysisGit(ctx context.Context, dir, auth, input string, args ...string) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	args = append([]string{"-c", "core.hooksPath=/dev/null", "-c", "protocol.ext.allow=never", "-c", "protocol.file.allow=never"}, args...)
	cmd := dockercli.Command(ctx, "git", args...)
	cmd.Dir = dir
	cmd.WaitDelay = dockercli.WaitDelay
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "GIT_TERMINAL_PROMPT=0", "GIT_LFS_SKIP_SMUDGE=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1"}
	if ca := os.Getenv("GIT_SSL_CAINFO"); ca != "" {
		cmd.Env = append(cmd.Env, "GIT_SSL_CAINFO="+ca)
	}
	if auth != "" {
		cmd.Env = append(cmd.Env, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=http.extraHeader", "GIT_CONFIG_VALUE_0="+auth)
	}
	cmd.Stdin = strings.NewReader(input)
	output := &boundedOutput{limit: 32 << 20, cancel: cancel}
	diagnostic := &boundedOutput{limit: 64 << 10, cancel: cancel}
	cmd.Stdout, cmd.Stderr = output, diagnostic
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("Git operation failed")
	}
	if output.exceeded || diagnostic.exceeded {
		return "", fmt.Errorf("Git output exceeds limit")
	}
	return output.buffer.String(), nil
}
