package server

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

const dependencyMiseDir = "/tmp/railpack/mise"

type dependencyMiseMarker struct {
	Resource      string `json:"resource"`
	ArchiveSHA256 string `json:"archiveSha256"`
	BinarySHA256  string `json:"binarySha256"`
}

func (s *Service) installDependencyMise(ctx context.Context, dep packDependency) error {
	if err := os.MkdirAll(dependencyMiseDir, 0700); err != nil {
		return err
	}
	destination := filepath.Join(dependencyMiseDir, dep.Executable)
	marker := filepath.Join(dependencyMiseDir, ".deplopack-"+dep.Executable+".json")
	encoded, _ := json.Marshal(dep)
	resource := fmt.Sprintf("%x", sha256.Sum256(encoded))
	var existing dependencyMiseMarker
	if data, err := os.ReadFile(marker); err == nil && json.Unmarshal(data, &existing) == nil && existing.Resource == resource && detectorDigest.MatchString(existing.ArchiveSHA256) && detectorDigest.MatchString(existing.BinarySHA256) {
		if digest, err := dependencyFileDigest(destination); err == nil && digest == existing.BinarySHA256 && dependencyMiseVersion(ctx, destination, dep.Version) == nil {
			log.Printf("DeploPack verified Mise reused: %s", dep.Version)
			return nil
		}
	}
	sums, err := releaseDocument(ctx, dep.ChecksumsURL, 1<<20)
	if err != nil {
		return err
	}
	want := ""
	for _, line := range strings.Split(string(sums), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && strings.TrimPrefix(fields[1], "*") == dep.Asset {
			if want != "" || !detectorDigest.MatchString(fields[0]) {
				return fmt.Errorf("invalid Mise archive checksum")
			}
			want = fields[0]
		}
	}
	if want == "" {
		return fmt.Errorf("Mise archive checksum missing")
	}
	archive, err := os.CreateTemp(dependencyMiseDir, ".deplopack-mise-archive-*")
	if err != nil {
		return err
	}
	defer os.Remove(archive.Name())
	defer archive.Close()
	body, err := downloadFile(ctx, dep.URL)
	if err != nil {
		return err
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(archive, hash), io.LimitReader(body, maxAgentBinary+1))
	body.Close()
	if copyErr != nil {
		return copyErr
	}
	if n > maxAgentBinary || hex.EncodeToString(hash.Sum(nil)) != want {
		return fmt.Errorf("Mise archive checksum mismatch or size exceeded")
	}
	if _, err := archive.Seek(0, 0); err != nil {
		return err
	}
	staged, err := os.CreateTemp(dependencyMiseDir, ".deplopack-mise-binary-*")
	if err != nil {
		return err
	}
	defer os.Remove(staged.Name())
	defer staged.Close()
	if err := extractDependencyMise(ctx, archive, staged, dep.Entry); err != nil {
		return err
	}
	if err := staged.Chmod(0755); err != nil {
		return err
	}
	if err := staged.Close(); err != nil {
		return err
	}
	if err := dependencyMiseVersion(ctx, staged.Name(), dep.Version); err != nil {
		return err
	}
	digest, err := dependencyFileDigest(staged.Name())
	if err != nil {
		return err
	}
	if err := os.Rename(staged.Name(), destination); err != nil {
		return err
	}
	return writeDependencyMarker(marker, dependencyMiseMarker{Resource: resource, ArchiveSHA256: want, BinarySHA256: digest})
}

func extractDependencyMise(ctx context.Context, source io.Reader, destination io.Writer, entry string) error {
	gz, err := gzip.NewReader(dependencyContextReader{ctx: ctx, source: source})
	if err != nil {
		return err
	}
	defer gz.Close()
	decompressed := &io.LimitedReader{R: dependencyContextReader{ctx: ctx, source: gz}, N: 2*maxAgentBinary + 1}
	reader := tar.NewReader(decompressed)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := reader.Next()
		if err == io.EOF {
			return fmt.Errorf("Mise executable missing from archive")
		}
		if err != nil {
			return err
		}
		if header.Name != entry {
			continue
		}
		if header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > maxAgentBinary {
			return fmt.Errorf("invalid Mise executable archive entry")
		}
		if _, err = io.Copy(destination, reader); err != nil {
			return err
		}
		// Read through the gzip trailer so a truncated or corrupt archive is rejected.
		_, err = io.Copy(io.Discard, decompressed)
		if decompressed.N == 0 {
			return fmt.Errorf("Mise archive exceeds decompressed size limit")
		}
		return err
	}
}

func dependencyFileDigest(file string) (string, error) {
	info, err := os.Lstat(file)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() > maxAgentBinary {
		return "", fmt.Errorf("invalid dependency executable")
	}
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(f, maxAgentBinary+1)); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func dependencyMiseVersion(ctx context.Context, binary, version string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := dockercli.Command(ctx, binary, "--version")
	cmd.WaitDelay = dockercli.WaitDelay
	cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
	output := &boundedOutput{limit: 4096, cancel: cancel}
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("Mise executable version check failed")
	}
	for _, word := range strings.Fields(output.buffer.String()) {
		if word == version {
			return nil
		}
	}
	return fmt.Errorf("Mise executable version mismatch")
}

func writeDependencyMarker(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	staged, err := os.CreateTemp(filepath.Dir(path), ".deplopack-state-*")
	if err != nil {
		return err
	}
	defer os.Remove(staged.Name())
	defer staged.Close()
	if _, err := staged.Write(data); err != nil {
		return err
	}
	if err := staged.Close(); err != nil {
		return err
	}
	return os.Rename(staged.Name(), path)
}

type dependencyContextReader struct {
	ctx    context.Context
	source io.Reader
}

func (r dependencyContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.source.Read(p)
}
