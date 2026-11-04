package server

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func TestSameImageRepository(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want bool
	}{
		{"deplo/web:dpl_a", "deplo/web:dpl_b", true},
		{"deplo/api:dpl_a", "deplo/web:dpl_b", false},
		{"nginx:latest", "deplo/web:dpl_b", false},
		{"deplo/web:dpl_a --rm", "deplo/web:dpl_b", false},
	} {
		if got := sameImageRepository(c.a, c.b); got != c.want {
			t.Errorf("sameImageRepository(%q, %q) = %v", c.a, c.b, got)
		}
	}
}

// The image is reused only while the branch still points at the commit it was built from.
func TestReuseBuiltImageFollowsTheBranch(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	allowFileGit(t)
	origin := t.TempDir()
	for _, args := range [][]string{{"init", "-q", "-b", "main"}, {"-c", "user.email=a@acme.com", "-c", "user.name=a",
		"commit", "-q", "--allow-empty", "-m", "one"}} {
		if out, err := exec.Command("git", append([]string{"-C", origin}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	sha, err := gitOutput(context.Background(), origin, "rev-parse", "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	sha = strings.TrimSpace(sha)
	url := "file://" + origin
	if got := remoteHead(context.Background(), url, "", "main"); got != sha {
		t.Fatalf("remoteHead(main) = %q, want %q", got, sha)
	}
	if got := remoteHead(context.Background(), url, "", ""); got != sha {
		t.Fatalf("remoteHead(HEAD) = %q, want %q", got, sha)
	}
	if got := remoteHead(context.Background(), url, "", "nope"); got != "" {
		t.Fatalf("a missing branch read %q", got)
	}

	bin, calls := t.TempDir(), filepath.Join(t.TempDir(), "calls")
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/bin/sh\necho \"$@\" >> "+calls+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	req := func(commit, ref string) *pb.DeployRequest {
		return &pb.DeployRequest{ImageRef: "deplo/web:dpl_new", SourceKind: pb.SourceKind_SOURCE_KIND_GIT,
			BuildKind: pb.BuildKind_BUILD_KIND_RAILPACK, Git: &pb.GitSource{Url: url, Branch: "main"},
			ReuseImage: &pb.ReuseImage{ImageRef: ref, CommitSha: commit}}
	}
	e := &emitter{send: func(*pb.DeployEvent) error { return nil }}
	for _, r := range []*pb.DeployRequest{req("0123456789012345678901234567890123456789", "deplo/web:dpl_old"),
		req(sha, "deplo/api:dpl_old")} {
		if _, ok := reuseBuiltImage(context.Background(), r, e); ok {
			t.Fatalf("reused %v", r.GetReuseImage())
		}
	}
	if got, ok := reuseBuiltImage(context.Background(), req(sha, "deplo/web:dpl_old"), e); !ok || got != sha {
		t.Fatalf("same commit: reused=%v sha=%q", ok, got)
	}
	if b, _ := os.ReadFile(calls); strings.TrimSpace(string(b)) != "tag deplo/web:dpl_old deplo/web:dpl_new" {
		t.Fatalf("docker calls = %q", b)
	}
}

func TestCapabilities_advertisesReuseImage(t *testing.T) {
	if !containsString(Capabilities, "deploy.reuse-image") {
		t.Error(`Capabilities must advertise "deploy.reuse-image"`)
	}
}
