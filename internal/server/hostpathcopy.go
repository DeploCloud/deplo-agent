package server

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

var deniedHostRoots = []string{
	"/", "/bin", "/boot", "/dev", "/etc", "/home", "/lib", "/lib32", "/lib64",
	"/media", "/mnt", "/opt", "/proc", "/root", "/run", "/sbin", "/srv", "/sys",
	"/tmp", "/usr", "/var", "/var/lib", "/var/lib/docker", "/var/run",
}

var deniedHostSubtrees = []string{
	"/proc", "/sys", "/dev", "/var/lib/docker", "/var/lib/containerd",
	"/root", "/home", "/etc/ssh", "/etc/ssl/private", "/etc/cron.d", "/etc/cron.daily",
	"/etc/systemd", "/etc/sudoers.d", "/var/lib/deplo-agent",
	"/data/stacks", "/data/backups",
}

var allowedHostSubtrees = []string{"/data/stacks/files"}

func validateHostPath(p string) (string, error) {
	if strings.TrimSpace(p) == "" {
		return "", status.Error(codes.InvalidArgument, "no path given")
	}
	if !filepath.IsAbs(p) {
		return "", status.Errorf(codes.InvalidArgument, "host path %q is not absolute", p)
	}
	clean := filepath.Clean(p)
	if strings.Contains(clean, "..") {
		return "", status.Errorf(codes.InvalidArgument, "host path %q climbs out of itself", p)
	}
	if err := refuseSystemPath(clean); err != nil {
		return "", err
	}
	real, err := resolveHostPath(clean)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "host path %q cannot be resolved: %v", p, err)
	}
	if real != clean {
		if err := refuseSystemPath(real); err != nil {
			return "", err
		}
	}
	return real, nil
}

func refuseSystemPath(clean string) error {
	for _, root := range allowedHostSubtrees {
		if strings.HasPrefix(clean, root+"/") {
			return nil
		}
	}
	for _, root := range deniedHostRoots {
		if clean == root {
			return status.Errorf(
				codes.PermissionDenied,
				"%q is a system directory and is not something Deplo will copy", clean,
			)
		}
	}
	for _, root := range deniedHostSubtrees {
		if clean == root || strings.HasPrefix(clean, root+"/") {
			return status.Errorf(
				codes.PermissionDenied,
				"%q is under %s, which is never a service's own data", clean, root,
			)
		}
	}
	return nil
}

func resolveHostPath(p string) (string, error) {
	cur, rest := p, ""
	for {
		real, err := filepath.EvalSymlinks(cur)
		if err == nil {
			return filepath.Join(real, rest), nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return p, nil
		}
		rest = filepath.Join(filepath.Base(cur), rest)
		cur = parent
	}
}

// ExportHostPath tars a host directory out of this machine, gzipped, as raw byte chunks - the same producer as ExportVolume, with a bind mount in place of the named volume.
func (s *Service) ExportHostPath(
	req *pb.ExportHostPathRequest,
	stream pb.Agent_ExportHostPathServer,
) error {
	path, err := validateHostPath(req.GetPath())
	if err != nil {
		return err
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		return status.Errorf(codes.NotFound, "no such directory on this host: %s", path)
	}
	mount, entry := path, "."
	if !info.IsDir() {
		if !req.GetAllowFile() {
			return status.Errorf(codes.InvalidArgument, "%s is a file, not a directory", path)
		}
		mount, entry = filepath.Dir(path), filepath.Base(path)
		if err := refuseSystemPath(mount); err != nil {
			return err
		}
	}

	ctx := stream.Context()
	cw := &chunkWriter{send: func(b []byte) error {
		return stream.Send(&pb.VolumeChunk{Frame: &pb.VolumeChunk_Data{Data: b}})
	}}
	gz, _ := gzip.NewWriterLevel(cw, gzip.BestSpeed)

	code, runErr := dockercli.PipeOut(ctx, volumeCopyTimeout, gz, nil,
		volumeHelperRun(ctx, "-v", mount+":/v:ro", volumeHelperImage,
			"tar", "-C", "/v", "-cf", "-", entry)...)
	if cerr := gz.Close(); cerr != nil && runErr == nil {
		return fmt.Errorf("export host path %q: finish gzip: %w", path, cerr)
	}
	if cw.err != nil {
		return cw.err
	}
	if runErr != nil {
		return fmt.Errorf("export host path %q: %w", path, runErr)
	}
	if code != 0 {
		return fmt.Errorf("export host path %q: tar exited %d", path, code)
	}
	return nil
}

// ImportHostPath is the destination half: header first (target dir + wipe flag), then the gzipped tar.
func (s *Service) ImportHostPath(stream pb.Agent_ImportHostPathServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("import host path: read header: %w", err)
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return sendHostPathResult(stream, false, 0, "", "first message must carry a header (path)")
	}
	path, verr := validateHostPath(hdr.GetPath())
	if verr != nil {
		return sendHostPathResult(stream, false, 0, "", verr.Error())
	}
	isFile := hdr.GetFile()
	extractInto := path
	if isFile {
		dir := filepath.Dir(path)
		if err := refuseSystemPath(dir); err != nil {
			return sendHostPathResult(stream, false, 0, "", err.Error())
		}
		if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
			return sendHostPathResult(stream, false, 0, "",
				fmt.Sprintf("create %s: %v", dir, mkErr))
		}
		tmp, tmpErr := os.MkdirTemp(dir, ".deplo-import-")
		if tmpErr != nil {
			return sendHostPathResult(stream, false, 0, "",
				fmt.Sprintf("create a staging directory beside %s: %v", path, tmpErr))
		}
		defer os.RemoveAll(tmp)
		extractInto = tmp
	} else if mkErr := os.MkdirAll(path, 0o755); mkErr != nil {
		return sendHostPathResult(stream, false, 0, "",
			fmt.Sprintf("create %s: %v", path, mkErr))
	}

	pr, pw := io.Pipe()
	done := make(chan error, 1)
	helper := importHelperName()
	go func() {
		code, perr := dockercli.PipeIn(ctx, volumeCopyTimeout, pr, nil,
			volumeHelperRun(ctx, "-i", "--name", helper, "-v", extractInto+":/v", volumeHelperImage,
				"tar", "-C", "/v", "-xf", "-")...)
		if perr == nil && code != 0 {
			perr = fmt.Errorf("host path extract exited %d", code)
		}
		_ = pr.CloseWithError(perr)
		done <- perr
	}()

	gz, gzErr := newSanitizingGunzipPump(pw)
	if gzErr != nil {
		_ = pw.CloseWithError(gzErr)
		<-done
		return sendHostPathResult(stream, false, 0, "",
			fmt.Sprintf("import host path %q: %v", path, gzErr))
	}

	var recvErr, wipeErr error
	var received int64
	digest := sha256.New()
	wiped := false
	for {
		msg, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			recvErr = rerr
			break
		}
		if data := msg.GetData(); len(data) > 0 {
			if hdr.GetWipeFirst() && !isFile && !wiped {
				if werr := wipeHostPath(path); werr != nil {
					wipeErr = werr
					break
				}
				wiped = true
			}
			received += int64(len(data))
			digest.Write(data)
			if _, werr := gz.Write(data); werr != nil {
				recvErr = werr
				break
			}
		}
	}

	gzCloseErr := gz.Close()
	_ = pw.Close()
	extractErr := <-done

	failure := ""
	switch {
	case wipeErr != nil:
		failure = fmt.Sprintf("empty target %q: %v", path, wipeErr)
	case recvErr != nil:
		failure = fmt.Sprintf("import host path %q: receive: %v", path, recvErr)
	case gzCloseErr != nil:
		failure = fmt.Sprintf("import host path %q: decompress: %v", path, gzCloseErr)
	case extractErr != nil:
		failure = fmt.Sprintf("import host path %q: extract: %v", path, extractErr)
	case received == 0:
		failure = fmt.Sprintf("import host path %q: the source sent no data", path)
	}
	if failure != "" {
		if wiped || wipeErr != nil {
			failure += abandonImport(helper, func(context.Context) error {
				return wipeHostPath(path)
			})
		}
		return stream.SendAndClose(importResult(false, 0, "", failure, &gz.drops))
	}
	if isFile {
		if mvErr := moveStagedFile(extractInto, path); mvErr != nil {
			return sendHostPathResult(stream, false, 0, "",
				fmt.Sprintf("import host path %q: %v", path, mvErr))
		}
	}
	return stream.SendAndClose(
		importResult(true, received, hex.EncodeToString(digest.Sum(nil)), "", &gz.drops))
}

func moveStagedFile(staging, target string) error {
	entries, err := os.ReadDir(staging)
	if err != nil {
		return err
	}
	if len(entries) != 1 {
		return fmt.Errorf("expected one file in the archive, got %d", len(entries))
	}
	from := filepath.Join(staging, entries[0].Name())
	if entries[0].IsDir() {
		return fmt.Errorf("%s is a directory, and %s names a file", entries[0].Name(), target)
	}
	if err := os.RemoveAll(target); err != nil {
		return err
	}
	return os.Rename(from, target)
}

func wipeHostPath(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if rerr := os.RemoveAll(filepath.Join(path, e.Name())); rerr != nil {
			return rerr
		}
	}
	return nil
}

func sendHostPathResult(
	stream pb.Agent_ImportHostPathServer,
	ok bool,
	bytesWritten int64,
	sha256Hex string,
	errMsg string,
) error {
	return stream.SendAndClose(importResult(ok, bytesWritten, sha256Hex, errMsg, nil))
}
