package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func TestRepositoryComposePathsStayInsideApp(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "deploy")
	if err := os.Mkdir(base, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := repositoryPath(root, base, "../config.env"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../../outside", "/etc/passwd", "${HOST_PATH}", `..\outside`} {
		if _, err := repositoryPath(root, base, path); err == nil {
			t.Errorf("accepted %q", path)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := repositoryPath(root, base, "../escape/missing"); err == nil {
		t.Fatal("accepted external symlink")
	}
	if err := os.Symlink(filepath.Join(outside, "missing"), filepath.Join(root, "dangling")); err != nil {
		t.Fatal(err)
	}
	if _, err := repositoryPath(root, root, "dangling/file"); err == nil {
		t.Fatal("accepted dangling external symlink")
	}
}

func TestRepositoryComposeValidatesEveryLocalReference(t *testing.T) {
	root := t.TempDir()
	for _, service := range []string{
		"build: ../../outside",
		"build: {context: ., dockerfile: ../../Dockerfile}",
		"env_file: ../../outside.env",
		"volumes: ['../../outside:/data']",
		"volumes: [{type: bind, source: ../../outside, target: /data}]",
		"extends: {file: other.yml, service: app}",
	} {
		if err := validateRepositoryCompose(root, root, []byte("services:\n  app:\n    "+service+"\n")); err == nil {
			t.Errorf("accepted %s", service)
		}
	}
	for _, kind := range []string{"configs", "secrets"} {
		contents := "services:\n  app: {image: busybox}\n" + kind + ":\n  file: {file: ../escape}\n"
		if err := validateRepositoryCompose(root, root, []byte(contents)); err == nil {
			t.Errorf("accepted external %s", kind)
		}
	}
	if err := validateRepositoryCompose(root, root, []byte("services:\n  app:\n    build: .\n    env_file: {path: ./optional.env, required: false}\n    volumes: ['./data:/data']\n")); err != nil {
		t.Fatal(err)
	}
}

func TestReadRepositoryComposePreservesContentAndRejectsOversizedFiles(t *testing.T) {
	root := t.TempDir()
	contents := "services:\n  app: {image: busybox}\n"
	path := filepath.Join(root, "compose.yml")
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := readRepositoryCompose(root, "compose.yml"); err != nil || got != contents {
		t.Fatalf("read: %q %v", got, err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", (1<<20)+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRepositoryCompose(root, "compose.yml"); err == nil {
		t.Fatal("accepted oversized file")
	}
}

func TestRepositoryComposeRequiresPinnedRevision(t *testing.T) {
	s := &Service{}
	req := &pb.DeployRequest{Git: &pb.GitSource{Url: "https://example.com/repo.git"}}
	e := &emitter{send: func(*pb.DeployEvent) error { return nil }}
	if _, _, err := s.checkoutRepositoryCompose(context.Background(), req, e); err == nil {
		t.Fatal("accepted unpinned source")
	}
}
