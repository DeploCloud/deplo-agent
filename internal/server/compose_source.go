package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/safepath"
)

// The compose file names compose itself looks for, in its order.
var composeFileNames = []string{"compose.yaml", "compose.yml", "docker-compose.yaml", "docker-compose.yml"}

const maxSourceCompose = 512 << 10

// readSourceCompose reads the app's compose file from buildDir: the named one, else the first of
// compose's own names. nil means none is there. It must be a regular file inside buildDir.
func readSourceCompose(buildDir, named string) (*pb.SourceCompose, error) {
	names := composeFileNames
	if named = strings.TrimSpace(named); named != "" {
		names = []string{named}
	}
	realDir, err := filepath.EvalSymlinks(buildDir)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		p, ok := safepath.Join(buildDir, name)
		if !ok {
			return nil, fmt.Errorf("the compose file %q is outside the build folder", named)
		}
		real, err := filepath.EvalSymlinks(p)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		rel, _ := filepath.Rel(buildDir, p)
		if real != realDir && !strings.HasPrefix(real, realDir+string(os.PathSeparator)) {
			return nil, fmt.Errorf("%s links outside the build folder, so it is not read", rel)
		}
		f, err := os.Open(real)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if fi, err := f.Stat(); err != nil || !fi.Mode().IsRegular() {
			return nil, fmt.Errorf("%s is not a regular file", rel)
		}
		b, err := io.ReadAll(io.LimitReader(f, maxSourceCompose+1))
		switch {
		case err != nil:
			return nil, err
		case len(b) > maxSourceCompose:
			return nil, fmt.Errorf("%s is larger than %d KiB", rel, maxSourceCompose>>10)
		case !utf8.Valid(b):
			return nil, fmt.Errorf("%s is not UTF-8 text", rel)
		}
		return &pb.SourceCompose{Path: filepath.ToSlash(rel), Content: string(b)}, nil
	}
	if named != "" {
		return nil, fmt.Errorf("there is no %s in the build folder", named)
	}
	return nil, nil
}

// redirectToCompose answers a compose deploy without a render: the compose file at this commit,
// for the control plane to render and deploy pinned to it. Nothing is built or started.
func redirectToCompose(req *pb.DeployRequest, buildDir, commitSha string, e *emitter) {
	sc, err := readSourceCompose(buildDir, req.GetBuildSpec().GetComposeFile())
	if err == nil && sc == nil {
		err = fmt.Errorf("there is no compose file in the build folder (looked for %s)", strings.Join(composeFileNames, ", "))
	}
	if err != nil {
		e.result(false, err.Error(), commitSha)
		return
	}
	e.log("info", "Found "+sc.Path+": deploying the stack it describes")
	e.finish(&pb.DeployResult{CommitSha: commitSha, SourceCompose: sc})
}

// srcDir is the app's working copy for a stack built from its source: updated in place, so the
// folders its services write (./data binds) outlive a deploy. DestroyStack removes it.
func (s *Service) srcDir(slug string) string { return filepath.Join(s.stackDir, slug+".src") }

func (s *Service) srcListPath(slug string) string { return filepath.Join(s.stackDir, slug+".src.list") }

// sourceProjectDir is the stack's compose project directory inside the working copy.
func (s *Service) sourceProjectDir(req *pb.DeployRequest) (string, bool) {
	sub := ""
	if req.GetSourceKind() == pb.SourceKind_SOURCE_KIND_GIT {
		sub = strings.TrimSpace(req.GetGit().GetSubdir())
	}
	return safepath.Join(s.srcDir(req.GetSlug()), sub)
}

// updateWorkingCopy moves the app's working copy to the fresh checkout in from (the repository
// root for git) and checks the result before compose reads it.
func (s *Service) updateWorkingCopy(req *pb.DeployRequest, from string, e *emitter) bool {
	slug := req.GetSlug()
	defer s.lockStack(slug)()
	if err := s.syncWorkingCopy(slug, from, req.GetSourceKind() == pb.SourceKind_SOURCE_KIND_GIT); err != nil {
		e.result(false, "update the app's source on this server: "+err.Error(), "")
		return false
	}
	if !s.stripWorkingCopyLinks(slug, e) {
		return false
	}
	dir, ok := s.sourceProjectDir(req)
	if fi, err := os.Stat(dir); !ok || err != nil || !fi.IsDir() {
		e.result(false, fmt.Sprintf("git subdir %q was not found in the repository", req.GetGit().GetSubdir()), "")
		return false
	}
	return true
}

func (s *Service) stripWorkingCopyLinks(slug string, e *emitter) bool {
	removed, err := stripEscapingSymlinks(s.srcDir(slug))
	if err != nil {
		e.result(false, "check the links in the app's source: "+err.Error(), "")
		return false
	}
	if len(removed) > 0 {
		e.log("warn", "Removed links that point outside the app's source: "+strings.Join(removed, ", "))
	}
	return true
}

// stripSourceLinks is the same check before a Start or a Reroute brings a source-built stack up:
// its services can write the working copy, and Docker follows a link they planted when it mounts.
func (s *Service) stripSourceLinks(slug string) error {
	if _, err := os.Lstat(s.srcDir(slug)); err != nil {
		return nil
	}
	if _, err := stripEscapingSymlinks(s.srcDir(slug)); err != nil {
		return fmt.Errorf("check the links in the app's source: %w", err)
	}
	return nil
}

// syncWorkingCopy writes every file of from into the working copy, through an os.Root so a link
// a container planted cannot redirect a write. Files it never wrote are kept; with prune, one it
// wrote last time that from no longer has is removed, as `git checkout` would.
func (s *Service) syncWorkingCopy(slug, from string, prune bool) error {
	if err := os.MkdirAll(s.srcDir(slug), 0o700); err != nil {
		return err
	}
	root, err := os.OpenRoot(s.srcDir(slug))
	if err != nil {
		return err
	}
	defer root.Close()
	var synced []string
	err = filepath.WalkDir(from, func(p string, d fs.DirEntry, err error) error {
		if err != nil || p == from {
			return err
		}
		rel, _ := filepath.Rel(from, p)
		if d.IsDir() && rel == ".git" {
			return filepath.SkipDir
		}
		switch {
		case d.IsDir():
			return syncDir(root, rel)
		case d.Type().IsRegular():
			synced = append(synced, rel)
			return syncFile(root, rel, p)
		case d.Type()&fs.ModeSymlink != 0:
			synced = append(synced, rel)
			return syncLink(root, rel, p)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if prune {
		kept := map[string]bool{}
		for _, rel := range synced {
			kept[rel] = true
		}
		for _, old := range readLines(s.srcListPath(slug)) {
			if fi, err := root.Lstat(old); err == nil && !kept[old] && !fi.IsDir() {
				_ = root.Remove(old)
			}
		}
	}
	return writeFileAtomic(s.srcListPath(slug), []byte(strings.Join(synced, "\n")+"\n"), 0o600)
}

func syncDir(root *os.Root, rel string) error {
	fi, err := root.Lstat(rel)
	if err == nil && fi.IsDir() {
		return nil
	}
	if err == nil {
		if err := root.Remove(rel); err != nil {
			return err
		}
	}
	return root.Mkdir(rel, 0o755)
}

func syncFile(root *os.Root, rel, src string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	si, err := in.Stat()
	if err != nil {
		return err
	}
	perm := si.Mode().Perm()
	if fi, err := root.Lstat(rel); err == nil {
		switch {
		case fi.IsDir():
			return fmt.Errorf("%s is a folder here but a file in the source", rel)
		case !fi.Mode().IsRegular():
			if err := root.Remove(rel); err != nil {
				return err
			}
		case fi.Size() == si.Size() && sameContent(root, rel, in):
			if fi.Mode().Perm() != perm {
				return root.Chmod(rel, perm)
			}
			return nil
		}
		if _, err := in.Seek(0, io.SeekStart); err != nil {
			return err
		}
	}
	// In place, keeping the inode: a running service that binds this one file sees the new content.
	out, err := root.OpenFile(rel, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = root.Chmod(rel, perm)
	}
	return err
}

func sameContent(root *os.Root, rel string, src io.Reader) bool {
	f, err := root.Open(rel)
	if err != nil {
		return false
	}
	defer f.Close()
	a, b := make([]byte, 32<<10), make([]byte, 32<<10)
	for {
		n, errA := io.ReadFull(src, a)
		m, errB := io.ReadFull(f, b)
		if n != m || !bytes.Equal(a[:n], b[:m]) {
			return false
		}
		if errA != nil || errB != nil {
			return endOfFile(errA) && endOfFile(errB)
		}
	}
}

func endOfFile(err error) bool { return err == io.EOF || err == io.ErrUnexpectedEOF }

func syncLink(root *os.Root, rel, src string) error {
	target, err := os.Readlink(src)
	if err != nil {
		return err
	}
	if fi, err := root.Lstat(rel); err == nil {
		if fi.Mode()&fs.ModeSymlink != 0 {
			if cur, err := root.Readlink(rel); err == nil && cur == target {
				return nil
			}
		}
		if err := root.Remove(rel); err != nil {
			return fmt.Errorf("%s: %w", rel, err)
		}
	}
	return root.Symlink(target, rel)
}

func readLines(path string) []string {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return splitLines(string(b))
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// removeWorkingCopy deletes a source-built stack's working copy, data folders included.
func (s *Service) removeWorkingCopy(slug string) {
	_ = os.RemoveAll(s.srcDir(slug))
	_ = os.Remove(s.srcListPath(slug))
}
