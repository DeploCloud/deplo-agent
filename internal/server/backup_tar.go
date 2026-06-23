package server

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// volumeNamePattern is the shape a Docker named volume always has. The volume
// name arrives off the wire and is interpolated into `-v <name>:/v` for a helper
// container; an unvalidated name like "/" or "/etc" would bind-mount a HOST PATH
// instead of a managed volume, turning wipeVolume's `rm -rf` and the restore
// untar loose on the host filesystem. So — exactly like validateSlug and
// normalizeRel for the other off-the-wire identifiers — the name is re-validated
// where the I/O runs and never trusted (defence in depth behind the control
// plane's own naming). The pattern forbids '/', '..', and a leading '.', so a
// path can never masquerade as a volume name.
var volumeNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// hasDotDot reports whether a POSIX-ish relative path contains a ".." segment
// (or is one), the traversal vector for a tar entry written into a helper
// container's `tar -x`. Used by the volume-restore demux, mirroring the
// extractToDir guard the files/ arm already applies.
func hasDotDot(p string) bool {
	for _, seg := range strings.Split(filepath.ToSlash(p), "/") {
		if seg == ".." {
			return true
		}
	}
	return false
}

// validateVolumeName rejects any name that is not a safe Docker named volume
// (anything containing a path separator, or otherwise outside the pattern), so a
// wire-supplied "/" / "/etc" can never become a host bind mount.
func validateVolumeName(name string) error {
	if !volumeNamePattern.MatchString(name) || strings.Contains(name, "..") {
		return fmt.Errorf("unsafe volume name %q (must be a docker named volume, not a path)", name)
	}
	return nil
}

// backup_tar.go holds the tar/volume plumbing for project backup+restore: adding
// a host dir or raw bytes to the archive, wiping + repopulating named volumes via
// throwaway helper containers, extracting into the files dir (anti-traversal),
// and the env-file round-trip used by the snapshot.

// addDirToTar walks `root` and writes every regular file + dir into `tw` under
// `prefix/<relpath>`. Symlinks are SKIPPED (the files dir is operator-editable;
// a symlink in the archive is an escape vector on restore — we never restore one,
// matching the build-context's link rejection).
func addDirToTar(tw *tar.Writer, root, prefix string) error {
	return filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil // don't emit the root itself
		}
		name := prefix + "/" + filepath.ToSlash(rel)
		switch {
		case info.IsDir():
			return tw.WriteHeader(&tar.Header{
				Name:     name + "/",
				Mode:     0o755,
				Typeflag: tar.TypeDir,
				ModTime:  info.ModTime(),
			})
		case info.Mode().IsRegular():
			hdr := &tar.Header{
				Name:     name,
				Mode:     int64(info.Mode().Perm()),
				Size:     info.Size(),
				Typeflag: tar.TypeReg,
				ModTime:  info.ModTime(),
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			f, err := os.Open(p)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = io.Copy(tw, f)
			return err
		default:
			// Symlink / device / socket — skip (not part of a config-files backup).
			return nil
		}
	})
}

// addBytesToTar writes a single in-memory file into the archive.
func addBytesToTar(tw *tar.Writer, name string, content []byte) error {
	if err := tw.WriteHeader(&tar.Header{
		Name:     name,
		Mode:     0o600,
		Size:     int64(len(content)),
		Typeflag: tar.TypeReg,
		ModTime:  time.Unix(0, 0),
	}); err != nil {
		return err
	}
	_, err := tw.Write(content)
	return err
}

// extractToDir writes one tar entry (relative path `rel` under `root`) to disk,
// re-validating the path against `root` (the entry name arrived from an S3 object
// — never trusted). A ".." escape or absolute path is rejected; symlinks/links
// are skipped (never restored). Mirrors materializeUpload's threat model.
func extractToDir(root, rel string, hdr *tar.Header, r io.Reader) error {
	// Reject any ".." segment OUTRIGHT (not merely anchor it away): the entry name
	// came from an S3 object, so a traversal attempt is a clear signal the archive
	// is hostile/corrupt and the restore must abort rather than silently relocate
	// the file. Mirrors normalizeRel (files.go) rather than the build-context's
	// anchoring, since here we'd rather fail loud than quietly drop the ".."s.
	for _, seg := range strings.Split(filepath.ToSlash(rel), "/") {
		if seg == ".." {
			return fmt.Errorf("archive entry %q escapes the target dir", rel)
		}
	}
	target := filepath.Join(root, filepath.Clean("/"+rel))
	if target != root && !strings.HasPrefix(target, root+string(os.PathSeparator)) {
		return fmt.Errorf("archive entry %q escapes the target dir", rel)
	}
	switch hdr.Typeflag {
	case tar.TypeDir:
		return os.MkdirAll(target, 0o755)
	case tar.TypeReg:
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode&0o777))
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(f, r)
		return err
	default:
		return nil // skip links/devices/etc.
	}
}

// wipeVolume empties a named volume's contents WITHOUT removing the volume
// itself (the volume stays attached to the stopped stack; we just clear it so a
// restore overwrites rather than merges). A helper container mounts the volume
// and `rm -rf /v/* /v/.[!.]*`.
func wipeVolume(ctx context.Context, vol string) error {
	if err := validateVolumeName(vol); err != nil {
		return err
	}
	// `sh -c` with a glob that also catches dotfiles; `|| true` so an empty volume
	// (nothing to remove) is not a non-zero exit.
	code, err := dockercli.Stream(ctx, 5*time.Minute, func(string) {}, "",
		"run", "--rm", "-v", vol+":/v", volumeHelperImage,
		"sh", "-c", "rm -rf /v/..?* /v/.[!.]* /v/* 2>/dev/null || true")
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("wipe exited %d", code)
	}
	return nil
}

// parseEnvFile parses KEY=VALUE lines (renderEnvFile's output) back into a map.
// The inverse of renderEnvFile (deploy.go). Blank lines are skipped; a line with
// no '=' is ignored.
func parseEnvFile(s string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[k] = v
	}
	return out
}

// volumeStreams demultiplexes the restore archive into one running helper
// container per target volume, each fed a tar stream it extracts into the volume.
// One container per volume keeps each `tar -x` rooted at its own volume mount.
type volumeStreams struct {
	ctx     context.Context
	writers map[string]*volumeWriter
}

type volumeWriter struct {
	pw   *io.PipeWriter
	tw   *tar.Writer
	done chan error
}

// newVolumeStreams starts a helper container per volume that reads a tar from
// stdin and extracts it into the volume (mounted rw at /v). Each container's
// stdin is a pipe we wrap in a tar.Writer.
func newVolumeStreams(ctx context.Context, vols []string) *volumeStreams {
	vs := &volumeStreams{ctx: ctx, writers: map[string]*volumeWriter{}}
	for _, vol := range vols {
		if vol == "" || vs.writers[vol] != nil {
			continue
		}
		pr, pw := io.Pipe()
		w := &volumeWriter{pw: pw, tw: tar.NewWriter(pw), done: make(chan error, 1)}
		go func(v string, reader *io.PipeReader) {
			// `tar -C /v -xf -` reads the tar we feed on stdin into the volume.
			code, err := dockercli.PipeIn(ctx, 10*time.Minute, reader, nil,
				"run", "--rm", "-i", "-v", v+":/v", volumeHelperImage,
				"tar", "-C", "/v", "-xf", "-")
			if err == nil && code != 0 {
				err = fmt.Errorf("volume extract exited %d", code)
			}
			_ = reader.CloseWithError(err)
			w.done <- err
		}(vol, pr)
		vs.writers[vol] = w
	}
	return vs
}

func (vs *volumeStreams) writerFor(vol string) (*tar.Writer, bool) {
	w, ok := vs.writers[vol]
	if !ok {
		return nil, false
	}
	return w.tw, true
}

// finish closes each volume's tar + pipe writer (flushing the trailer so the
// helper's `tar -x` sees clean EOF) and waits for every helper to exit, returning
// the first extraction error.
func (vs *volumeStreams) finish(e *rsEmitter) error {
	var firstErr error
	for vol, w := range vs.writers {
		if err := w.tw.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := w.pw.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := <-w.done; err != nil {
			e.log("warn", fmt.Sprintf("volume %q: %v", vol, err))
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// closeAll is a defensive cleanup (deferred): abort any still-open writers so a
// mid-restore error doesn't leak helper containers blocked on stdin.
func (vs *volumeStreams) closeAll() {
	for _, w := range vs.writers {
		_ = w.pw.CloseWithError(io.ErrClosedPipe)
	}
}
