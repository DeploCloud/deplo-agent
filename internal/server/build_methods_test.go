package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func TestMajorVersion(t *testing.T) {
	cases := []struct{ in, def, want string }{
		{"20.11.0", "20", "20"},
		{"v18", "20", "18"},
		{"", "20", "20"},
		{"latest", "20", "20"},
		{"3.12", "3", "3"},
	}
	for _, c := range cases {
		if got := majorVersion(c.in, c.def); got != c.want {
			t.Errorf("majorVersion(%q,%q) = %q, want %q", c.in, c.def, got, c.want)
		}
	}
}

func TestImageTag(t *testing.T) {
	if got := imageTag("deplo/app:abc123"); got != "abc123" {
		t.Errorf("imageTag tagged = %q, want abc123", got)
	}
	if got := imageTag("deplo/app"); got != "deplo/app" {
		t.Errorf("imageTag untagged = %q, want the whole ref", got)
	}
}

// A file the agent writes into a checkout may already be a symlink to a host path: the root
// agent must replace the link, never write through it.
func TestWriteBuildFileNeverWritesThroughASymlink(t *testing.T) {
	host, dir := t.TempDir(), t.TempDir()
	created, existing := filepath.Join(host, "authorized_keys"), filepath.Join(host, "passwd")
	mustWrite(t, existing, "keep\n")
	for name, target := range map[string]string{"Dockerfile": created, "Staticfile": existing} {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
		if err := writeBuildFile(filepath.Join(dir, name), []byte("ssh-ed25519 AAAA evil\n")); err != nil {
			t.Fatal(err)
		}
		if li, err := os.Lstat(filepath.Join(dir, name)); err != nil || !li.Mode().IsRegular() {
			t.Fatalf("%s should now be a regular file in the checkout: %v %v", name, li, err)
		}
	}
	if b, _ := os.ReadFile(existing); string(b) != "keep\n" {
		t.Fatalf("the symlinked file overwrote its host target: %q", b)
	}
	if _, err := os.Lstat(created); !os.IsNotExist(err) {
		t.Fatalf("a dangling symlink let the agent create a host file: %v", err)
	}
}

// railpack reads the repo as root on the host, so a link that resolves outside the checkout
// (absolute, via .., or through another link) is gone before it runs.
func TestHostToolsNeverSeeALinkOutOfTheCheckout(t *testing.T) {
	out, bin := t.TempDir(), t.TempDir()
	probe := "#!/bin/sh\n[ \"$1\" = --version ] && { echo railpack v" + railpackVersion + "; exit 0; }\n" +
		"for f in Procfile package.json inside; do if [ -L \"$2/$f\" ]; then echo \"$f link\"; " +
		"elif [ -e \"$2/$f\" ]; then echo \"$f file\"; else echo \"$f gone\"; fi; done > " + out + "/seen\nexit 1\n"
	if err := os.WriteFile(filepath.Join(bin, "railpack"), []byte(probe), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	s := New(t.TempDir(), t.TempDir(), "/", "")
	base := t.TempDir()
	dir := filepath.Join(base, "repo")
	mustWrite(t, filepath.Join(base, "host-package.json"), `{"secret":true}`)
	mustWrite(t, filepath.Join(dir, "sub", "real.txt"), "mine\n")
	for name, target := range map[string]string{
		"Procfile": filepath.Join(base, "host-package.json"), "package.json": "../host-package.json", "inside": "sub/real.txt",
	} {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	req := &pb.DeployRequest{Slug: "deplo-agent-test-links", ProjectId: "prj", ImageRef: "deplo/deplo-agent-test-links:t",
		BuildKind: pb.BuildKind_BUILD_KIND_RAILPACK, BuildSpec: &pb.BuildSpec{}}
	_ = s.buildImage(context.Background(), req, dir, &emitter{send: func(*pb.DeployEvent) error { return nil }})
	seen, err := os.ReadFile(filepath.Join(out, "seen"))
	if err != nil {
		t.Fatalf("railpack never ran: %v", err)
	}
	if want := "Procfile gone\npackage.json gone\ninside link\n"; string(seen) != want {
		t.Fatalf("railpack saw\n%s\nwant\n%s", seen, want)
	}
}

// A relative link that climbs out through another link is caught by resolving it, not by its text.
func TestStripEscapingSymlinksResolvesChains(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "repo")
	mustWrite(t, filepath.Join(dir, "a", "keep.txt"), "x")
	for link, target := range map[string]string{"s": ".", "a/z": "../s/..", "a/ok": "../s/a/keep.txt", "dangling": "nope"} {
		if err := os.Symlink(target, filepath.Join(dir, link)); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := stripEscapingSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(removed, ",") != "a/z" {
		t.Fatalf("removed %v, want only a/z", removed)
	}
}
