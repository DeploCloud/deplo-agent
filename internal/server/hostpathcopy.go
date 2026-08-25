package server

// https://deplo.build/docs/guides/move-from-dokploy

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

// hostpathcopy.go copies a plain HOST DIRECTORY across hosts — the bind-mount half of a
// migration from another platform, where a service's data may live in a directory
// rather than in a Docker volume (Dokploy mounts a `type: bind` source straight off the
// host).

// deniedHostRoots are refused as a copy source or target, exactly (a path EQUAL to one
// of them) — a deeper path under most of them is legitimate, and refusing those
// wholesale would refuse the actual use case (the other platform keeps its service data
// under /etc/<platform>/...).
var deniedHostRoots = []string{
	"/", "/bin", "/boot", "/dev", "/etc", "/home", "/lib", "/lib32", "/lib64",
	"/media", "/mnt", "/opt", "/proc", "/root", "/run", "/sbin", "/srv", "/sys",
	"/tmp", "/usr", "/var", "/var/lib", "/var/lib/docker", "/var/run",
}

// deniedHostSubtrees are refused along with everything under them: kernel and
// device filesystems are never a service's data, and Docker's own state directory
// is where every OTHER tenant's volumes live.
var deniedHostSubtrees = []string{"/proc", "/sys", "/dev", "/var/lib/docker"}

// validateHostPath cleans and vets a wire-supplied host directory.
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
	for _, root := range deniedHostRoots {
		if clean == root {
			return "", status.Errorf(
				codes.PermissionDenied,
				"%q is a system directory and is not something Deplo will copy", clean,
			)
		}
	}
	for _, root := range deniedHostSubtrees {
		if clean == root || strings.HasPrefix(clean, root+"/") {
			return "", status.Errorf(
				codes.PermissionDenied,
				"%q is under %s, which is never a service's own data", clean, root,
			)
		}
	}
	return clean, nil
}

// ExportHostPath tars a host directory out of this machine, gzipped, as raw byte
// chunks — the same producer as ExportVolume, with a bind mount in place of the
// named volume. The caller is expected to have QUIESCED whatever writes there.
func (s *Service) ExportHostPath(
	req *pb.ExportHostPathRequest,
	stream pb.Agent_ExportHostPathServer,
) error {
	path, err := validateHostPath(req.GetPath())
	if err != nil {
		return err
	}
	// It must ALREADY be here, and be a directory. Docker would happily create it
	// on the mount and export an empty archive — the exact shape that made a wrong
	// source host look like a successful copy of nothing.
	info, statErr := os.Stat(path)
	if statErr != nil {
		return status.Errorf(codes.NotFound, "no such directory on this host: %s", path)
	}
	if !info.IsDir() {
		return status.Errorf(codes.InvalidArgument, "%s is a file, not a directory", path)
	}

	ctx := stream.Context()
	cw := &chunkWriter{send: func(b []byte) error {
		return stream.Send(&pb.VolumeChunk{Frame: &pb.VolumeChunk_Data{Data: b}})
	}}
	gz := gzip.NewWriter(cw)

	code, runErr := dockercli.PipeOut(ctx, volumeCopyTimeout, gz, nil,
		"run", "--rm", "-v", path+":/v:ro", volumeHelperImage,
		"tar", "-C", "/v", "-cf", "-", ".")
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

// ImportHostPath is the destination half: header first (target dir + wipe flag),
// then the gzipped tar. Mirrors ImportVolume, including the deferred wipe.
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
	// The whole path is materialised, parents included. The deny-list above is what keeps
	// a wrong path from being a dangerous one; a missing parent only ever meant "this host
	// has not run that platform".
	if mkErr := os.MkdirAll(path, 0o755); mkErr != nil {
		return sendHostPathResult(stream, false, 0, "",
			fmt.Sprintf("create %s: %v", path, mkErr))
	}

	pr, pw := io.Pipe()
	done := make(chan error, 1)
	// Named so a failure can remove it before emptying the directory it is
	// writing into - see abandonImport.
	helper := importHelperName()
	go func() {
		code, perr := dockercli.PipeIn(ctx, volumeCopyTimeout, pr, nil,
			"run", "--rm", "-i", "--name", helper, "-v", path+":/v", volumeHelperImage,
			"tar", "-C", "/v", "-xf", "-")
		if perr == nil && code != 0 {
			perr = fmt.Errorf("host path extract exited %d", code)
		}
		_ = pr.CloseWithError(perr)
		done <- perr
	}()

	gz, gzErr := newGunzipPump(pw)
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
			if hdr.GetWipeFirst() && !wiped {
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

	// One failure path: whatever this import emptied is emptied again, so a
	// half-written directory never survives a copy that did not finish. See
	// abandonImport.
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
		return sendHostPathResult(stream, false, 0, "", failure)
	}
	return sendHostPathResult(stream, true, received, hex.EncodeToString(digest.Sum(nil)), "")
}

// wipeHostPath empties a directory in place, keeping the directory itself (it may
// already be bind-mounted into a stopped container). Removes the ENTRIES rather
// than the tree, so the mount stays valid.
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
	return stream.SendAndClose(&pb.StackResult{
		Ok:           ok,
		Error:        errMsg,
		BytesWritten: bytesWritten,
		Sha256:       sha256Hex,
	})
}
