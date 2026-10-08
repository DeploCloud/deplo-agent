package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const detectorReleaseAPI = "https://api.github.com/repos/DeploCloud/deplopack/releases/latest"

var detectorTag = regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]+$`)
var detectorDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

type detectorInstall struct {
	Version string `json:"version"`
	SHA256  string `json:"sha256"`
}

func (s *Service) ensureDetector(ctx context.Context) (string, string, string, error) {
	return s.ensureLatestDetector(ctx)
}

func (s *Service) ensureLatestDetector(ctx context.Context) (string, string, string, error) {
	select {
	case s.detectorMu <- struct{}{}:
		defer func() { <-s.detectorMu }()
	case <-ctx.Done():
		return "", "", "", ctx.Err()
	}
	if runtime.GOOS != "linux" || (runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64") {
		return "", "", "", fmt.Errorf("detector installation supports Linux amd64 and arm64")
	}
	if s.detectorPath != "" && time.Since(s.detectorChecked) < 24*time.Hour {
		return s.detectorPath, s.detectorVersion, s.detectorWarning, nil
	}
	tools := filepath.Join(s.dataBase, "tools", "deplopack")
	if err := os.MkdirAll(tools, 0700); err != nil {
		return "", "", "", err
	}
	marker := filepath.Join(tools, "detector.json")
	if s.detectorPath == "" {
		var installed detectorInstall
		if data, err := os.ReadFile(marker); err == nil && json.Unmarshal(data, &installed) == nil && detectorTag.MatchString(installed.Version) && detectorDigest.MatchString(installed.SHA256) {
			path := filepath.Join(tools, "detector-"+installed.Version)
			if verifyDeplopack(ctx, path, installed) == nil {
				s.detectorPath, s.detectorVersion = path, installed.Version
			}
		}
	}
	path, version, err := s.updateDetector(ctx, tools, marker)
	if err != nil {
		if ctx.Err() != nil {
			return "", "", "", ctx.Err()
		}
		if s.detectorPath != "" {
			s.detectorChecked = time.Now()
			s.detectorWarning = "Detector update unavailable; using the last verified version"
			return s.detectorPath, s.detectorVersion, s.detectorWarning, nil
		}
		return "", "", "", err
	}
	s.detectorWarning = ""
	s.detectorChecked = time.Now()
	s.detectorPath, s.detectorVersion = path, version
	return path, version, "", nil
}

func releaseDocument(ctx context.Context, url string, limit int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "deplo-agent")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not read detector release")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("detector release HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || int64(len(body)) > limit {
		return nil, fmt.Errorf("invalid or oversized detector release document")
	}
	return body, nil
}

func (s *Service) updateDetector(ctx context.Context, tools, marker string) (string, string, error) {
	document, err := releaseDocument(ctx, detectorReleaseAPI, 1<<20)
	if err != nil {
		return "", "", err
	}
	var release struct {
		Tag        string `json:"tag_name"`
		Draft      bool   `json:"draft"`
		Prerelease bool   `json:"prerelease"`
	}
	if json.Unmarshal(document, &release) != nil || release.Draft || release.Prerelease || !detectorTag.MatchString(release.Tag) {
		return "", "", fmt.Errorf("no compatible stable detector release")
	}
	version := strings.TrimPrefix(release.Tag, "v")
	return s.installDeplopack(ctx, tools, marker, "detector", version, release.Tag)
}

func (s *Service) ensureBuilder(ctx context.Context, version string) (string, error) {
	if !detectorTag.MatchString(version) || strings.HasPrefix(version, "v") {
		return "", fmt.Errorf("invalid DeploPack version")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	select {
	case s.detectorMu <- struct{}{}:
		defer func() { <-s.detectorMu }()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if runtime.GOOS != "linux" || (runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64") {
		return "", fmt.Errorf("DeploPack supports Linux amd64 and arm64")
	}
	tools := filepath.Join(s.dataBase, "tools", "deplopack")
	if err := os.MkdirAll(tools, 0700); err != nil {
		return "", err
	}
	marker := filepath.Join(tools, "builder-"+version+".json")
	var installed detectorInstall
	if data, err := os.ReadFile(marker); err == nil && json.Unmarshal(data, &installed) == nil && installed.Version == version && detectorDigest.MatchString(installed.SHA256) {
		binary := filepath.Join(tools, "builder-"+version)
		if verifyDeplopack(ctx, binary, installed) == nil {
			return binary, nil
		}
	}
	binary, _, err := s.installDeplopack(ctx, tools, marker, "builder", version, "v"+version)
	return binary, err
}

func (s *Service) installDeplopack(ctx context.Context, tools, marker, role, version, tag string) (string, string, error) {
	base := "https://github.com/DeploCloud/deplopack/releases/download/" + tag + "/"
	checksums, err := releaseDocument(ctx, base+"checksums.txt", 1<<20)
	if err != nil {
		return "", "", err
	}
	asset := "deplopack-" + role + "-linux-" + runtime.GOARCH
	digest := ""
	for _, line := range strings.Split(string(checksums), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && strings.TrimPrefix(fields[1], "*") == asset && detectorDigest.MatchString(fields[0]) {
			if digest != "" {
				return "", "", fmt.Errorf("duplicate DeploPack checksum")
			}
			digest = fields[0]
		}
	}
	if digest == "" {
		return "", "", fmt.Errorf("release has no DeploPack checksum for this architecture")
	}
	installed := detectorInstall{Version: version, SHA256: digest}
	dest := filepath.Join(tools, role+"-"+version)
	if verifyDeplopack(ctx, dest, installed) != nil {
		staged, err := s.stageVerifiedBinary(ctx, dest, base+asset, digest)
		if err != nil {
			return "", "", err
		}
		defer os.Remove(staged)
		if err := verifyDeplopack(ctx, staged, installed); err != nil {
			return "", "", err
		}
		if err := os.Rename(staged, dest); err != nil {
			return "", "", err
		}
	}
	data, err := json.Marshal(installed)
	if err != nil {
		return "", "", err
	}
	tmp, err := os.CreateTemp(tools, ".detector-state-*")
	if err != nil {
		return "", "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return "", "", err
	}
	if err := tmp.Close(); err != nil {
		return "", "", err
	}
	if err := os.Rename(tmp.Name(), marker); err != nil {
		return "", "", err
	}
	return dest, version, nil
}

func verifyDeplopack(ctx context.Context, path string, installed detectorInstall) error {
	if !usableBinary(path) {
		return fmt.Errorf("DeploPack binary is missing")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	digest := sha256.New()
	n, err := io.Copy(digest, io.LimitReader(file, maxAgentBinary+1))
	if err != nil || n > maxAgentBinary || hex.EncodeToString(digest.Sum(nil)) != installed.SHA256 {
		return fmt.Errorf("DeploPack checksum mismatch")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := dockercli.Command(ctx, path, "--version")
	cmd.WaitDelay = dockercli.WaitDelay
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	output := &boundedOutput{limit: 1024, cancel: cancel}
	cmd.Stdout = output
	cmd.Stderr = io.Discard
	if cmd.Run() != nil || strings.TrimSpace(output.buffer.String()) != installed.Version {
		return fmt.Errorf("DeploPack version mismatch")
	}
	return nil
}
