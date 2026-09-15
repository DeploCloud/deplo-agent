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

var volumeNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

func hasDotDot(p string) bool {
	for _, seg := range strings.Split(filepath.ToSlash(p), "/") {
		if seg == ".." {
			return true
		}
	}
	return false
}

func validateVolumeName(name string) error {
	if !volumeNamePattern.MatchString(name) || strings.Contains(name, "..") {
		return fmt.Errorf("unsafe volume name %q (must be a docker named volume, not a path)", name)
	}
	return nil
}

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
			return nil
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
			return nil
		}
	})
}

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

func extractToDir(root, rel string, hdr *tar.Header, r io.Reader) error {
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
		return nil
	}
}

func wipeVolume(ctx context.Context, vol string) error {
	if err := validateVolumeName(vol); err != nil {
		return err
	}
	code, err := dockercli.Stream(ctx, 5*time.Minute, func(string) {}, "",
		volumeHelperRun(ctx, "-v", vol+":/v", volumeHelperImage,
			"sh", "-c", "rm -rf /v/..?* /v/.[!.]* /v/* 2>/dev/null || true")...)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("wipe exited %d", code)
	}
	return nil
}

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

type volumeStreams struct {
	ctx     context.Context
	writers map[string]*volumeWriter
}

type volumeWriter struct {
	pw   *io.PipeWriter
	tw   *tar.Writer
	done chan error
}

func newVolumeStreams(ctx context.Context, vols []string) *volumeStreams {
	vs := &volumeStreams{ctx: ctx, writers: map[string]*volumeWriter{}}
	for _, vol := range vols {
		if vol == "" || vs.writers[vol] != nil {
			continue
		}
		pr, pw := io.Pipe()
		w := &volumeWriter{pw: pw, tw: tar.NewWriter(pw), done: make(chan error, 1)}
		go func(v string, reader *io.PipeReader) {
			code, err := dockercli.PipeIn(ctx, 10*time.Minute, reader, nil,
				volumeHelperRun(ctx, "-i", "-v", v+":/v", volumeHelperImage,
					"tar", "-C", "/v", "-xf", "-")...)
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

func (vs *volumeStreams) closeAll() {
	for _, w := range vs.writers {
		_ = w.pw.CloseWithError(io.ErrClosedPipe)
	}
}
