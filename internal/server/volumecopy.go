package server

// https://deplo.build/docs/guides/move-from-dokploy

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// volumecopy.go implements the cross-host named-volume copy that backs a server MOVE (a
// database or project relocating to another server).

// volumeCopyTimeout bounds a single export/import. A move of a large DB volume can
// take a while; this matches the generous per-step budget the project backup path
// uses for the same helper-container tar of a volume.
const volumeCopyTimeout = 30 * time.Minute

// chunkBytes is the payload size of one VolumeChunk data frame. Comfortably under
// the gRPC max message size (the control plane dials with a 256 MiB cap), while big
// enough that framing overhead is negligible on a multi-GB volume.
const chunkBytes = 1 << 20 // 1 MiB

// volumeExistsTimeout bounds the one `docker volume inspect` that gates an export.
// A local daemon query: short on purpose, so a wedged daemon fails the copy rather
// than holding the stream open for the copy's own 30 minutes.
const volumeExistsTimeout = 15 * time.Second

// importCleanupTimeout bounds the tidy-up after a failed import.
const importCleanupTimeout = 6 * time.Minute

// abandonImport re-empties a destination that this import had ALREADY emptied, and
// reports what it did in the same sentence as the failure. `helper` names the extract
// container, which must go first.
func abandonImport(helper string, empty func(context.Context) error) string {
	ctx, cancel := context.WithTimeout(context.Background(), importCleanupTimeout)
	defer cancel()
	if helper != "" {
		_, _ = dockercli.Run(ctx, 30*time.Second, "rm", "-f", helper)
	}
	if err := empty(ctx); err != nil {
		return fmt.Sprintf(" (the destination is INCOMPLETE: it could not be emptied either: %v)", err)
	}
	return " (the destination was emptied: nothing of this copy was kept)"
}

// importHelperName is a unique name for one extract container, so a failed import
// can take it down before emptying what it was writing into.
func importHelperName() string {
	return fmt.Sprintf("deplo-import-%d", time.Now().UnixNano())
}

// ExportVolume tars a named volume from a read-only helper container, gzips it, and
// streams it out as raw byte chunks.
func (s *Service) ExportVolume(req *pb.ExportVolumeRequest, stream pb.Agent_ExportVolumeServer) error {
	vol := req.GetVolumeName()
	// Re-validate off-the-wire (defence in depth behind the control plane's naming):
	// an unsafe name like "/" or "/etc" would bind-mount a host path into the helper.
	if err := validateVolumeName(vol); err != nil {
		return fmt.Errorf("export volume: %w", err)
	}
	ctx := stream.Context()

	// The volume must ALREADY be here. NotFound, so the caller can say which host was
	// asked and for what.
	if err := assertVolumeExists(ctx, vol); err != nil {
		return err
	}

	// gzip the tar as it is produced, writing compressed bytes straight into the
	// stream via chunkWriter - no temp file, no full-archive buffering.
	cw := &chunkWriter{send: func(b []byte) error {
		return stream.Send(&pb.VolumeChunk{Frame: &pb.VolumeChunk_Data{Data: b}})
	}}
	gz := gzip.NewWriter(cw)

	// Producer: the helper container tars the volume's contents to stdout; PipeOut
	// copies that into gz (→ chunkWriter → stream.Send).
	code, err := dockercli.PipeOut(ctx, volumeCopyTimeout, gz, nil,
		volumeHelperRun(ctx, "-v", vol+":/v:ro", volumeHelperImage,
			"tar", "-C", "/v", "-cf", "-", ".")...)
	// Flush + finish the gzip trailer BEFORE reporting, so the destination sees a
	// complete stream. A Close error trumps a benign producer exit.
	if cerr := gz.Close(); cerr != nil && err == nil {
		return fmt.Errorf("export volume %q: finish gzip: %w", vol, cerr)
	}
	if cw.err != nil {
		// stream.Send failed (the control plane relay went away), nothing more to do.
		return cw.err
	}
	if err != nil {
		// busybox `tar -cf -` legitimately exits 1 for a benign "file changed as we read it"
		// on a LIVE volume while STILL emitting a complete archive (same case archiveVolume
		// tolerates).
		return fmt.Errorf("export volume %q: %w", vol, err)
	}
	if code != 0 {
		return fmt.Errorf("export volume %q: tar exited %d", vol, code)
	}
	return nil
}

// assertVolumeExists answers NotFound when the named volume is not on this host.
func assertVolumeExists(ctx context.Context, vol string) error {
	res, err := dockercli.Run(ctx, volumeExistsTimeout, "volume", "inspect", vol)
	if err != nil {
		return status.Errorf(codes.Unavailable, "docker is not reachable on this host: %v", err)
	}
	if res.Code != 0 {
		return status.Errorf(
			codes.NotFound,
			"no volume named %q on this host - nothing to export (docker: %s)",
			vol, strings.TrimSpace(res.Stderr),
		)
	}
	return nil
}

// chunkWriter is an io.Writer that frames whatever is written to it into ~1 MiB `data`
// messages on an export stream via `send`. gzip writes here; the concrete stream
// (VolumeChunk vs FilesChunk) is captured by the send closure, so this is shared by
// ExportVolume and ExportFiles.
type chunkWriter struct {
	send func([]byte) error
	err  error
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > chunkBytes {
			n = chunkBytes
		}
		// Copy the slice - gRPC may retain the message past this call, and gzip reuses
		// its buffer across Writes.
		buf := make([]byte, n)
		copy(buf, p[:n])
		if err := w.send(buf); err != nil {
			w.err = err
			return total, err
		}
		total += n
		p = p[n:]
	}
	return total, nil
}

// ImportVolume is the destination half: the FIRST client message carries the target
// volume name + wipe flag; every following message carries a slice of the gzipped tar.
func (s *Service) ImportVolume(stream pb.Agent_ImportVolumeServer) error {
	ctx := stream.Context()

	// 1. The header frame names the target + whether to wipe. It must come first.
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("import volume: read header: %w", err)
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return sendImportResult(stream, false, 0, "", "first message must carry a header (volume name)")
	}
	vol := hdr.GetVolumeName()
	if err := validateVolumeName(vol); err != nil {
		return sendImportResult(stream, false, 0, "", fmt.Sprintf("import volume: %v", err))
	}

	// 2. The wipe is DEFERRED to the first data frame (below).

	// 3. Reassemble the data frames into a byte stream (a pipe the untar reads),
	//    gunzip it, and feed it to `tar -C /v -xf -` in a helper container. The
	//    recv loop runs in this goroutine writing into the pipe; PipeIn drains it.
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	// Named so a failure can remove it before emptying the volume it is writing
	// into - see abandonImport.
	helper := importHelperName()
	go func() {
		// `tar -C /v -xf -` reads the tar we feed on stdin and extracts into the
		// volume (created on demand if absent). -i for interactive stdin.
		code, perr := dockercli.PipeIn(ctx, volumeCopyTimeout, pr, nil,
			volumeHelperRun(ctx, "-i", "--name", helper, "-v", vol+":/v", volumeHelperImage,
				"tar", "-C", "/v", "-xf", "-")...)
		if perr == nil && code != 0 {
			perr = fmt.Errorf("volume extract exited %d", code)
		}
		// Unblock the writer if the extractor died early.
		_ = pr.CloseWithError(perr)
		done <- perr
	}()

	// Gunzip the incoming compressed frames into the pipe the untar reads.
	gz, gzErr := newSanitizingGunzipPump(pw)
	if gzErr != nil {
		_ = pw.CloseWithError(gzErr)
		<-done
		return sendImportResult(stream, false, 0, "", fmt.Sprintf("import volume %q: %v", vol, gzErr))
	}

	var recvErr error
	var wipeErr error
	var received int64
	digest := sha256.New()
	wiped := false
	for {
		msg, rerr := stream.Recv()
		if rerr == io.EOF {
			break // client finished sending; close the gzip + pipe below
		}
		if rerr != nil {
			recvErr = rerr
			break
		}
		// A stray header mid-stream is a protocol violation; ignore non-data frames.
		if data := msg.GetData(); len(data) > 0 {
			// The first real byte is what earns the wipe: a stream that ends here
			// having sent nothing leaves the destination exactly as it was.
			if hdr.GetWipeFirst() && !wiped {
				if err := wipeVolume(ctx, vol); err != nil {
					wipeErr = err
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

	// Flush the gunzip (writes the last decompressed bytes) then close the pipe so
	// the untar sees a clean EOF, and wait for the extractor.
	gzCloseErr := gz.Close()
	_ = pw.Close()
	extractErr := <-done

	// One failure path, because every one of them ends the same way: whatever this import
	// emptied has to be emptied again.
	failure := ""
	switch {
	case wipeErr != nil:
		failure = fmt.Sprintf("wipe target volume %q: %v", vol, wipeErr)
	case recvErr != nil:
		failure = fmt.Sprintf("import volume %q: receive: %v", vol, recvErr)
	case gzCloseErr != nil:
		failure = fmt.Sprintf("import volume %q: decompress: %v", vol, gzCloseErr)
	case extractErr != nil:
		failure = fmt.Sprintf("import volume %q: extract: %v", vol, extractErr)
	case received == 0:
		failure = fmt.Sprintf("import volume %q: the source sent no data", vol)
	}
	if failure != "" {
		// A wipe that failed PART WAY through counts as wiped: `rm -rf` is not
		// transactional, so a non-zero exit means some of it went.
		if wiped || wipeErr != nil {
			failure += abandonImport(helper, func(c context.Context) error {
				return wipeVolume(c, vol)
			})
		}
		return sendImportResult(stream, false, 0, "", failure)
	}
	return sendImportResult(stream, true, received, hex.EncodeToString(digest.Sum(nil)), "")
}

// newGunzipPump returns a WriteCloser that decompresses everything written to it and
// forwards the plaintext to `dst`. gzip.NewReader needs a Reader, but our data arrives
// as Writes off the gRPC stream, so we bridge with an internal pipe: caller Writes
// compressed bytes → gzip.Reader pulls from the pipe → decompressed bytes go to dst.
func newGunzipPump(dst io.Writer) (io.WriteCloser, error) {
	return newTarPump(dst, false)
}

// newSanitizingGunzipPump is newGunzipPump for an archive that came off ANOTHER
// platform's host: the tar is rewritten by sanitizeTar on the way through, because
// what it feeds is `tar -x` running as root.
func newSanitizingGunzipPump(dst io.Writer) (io.WriteCloser, error) {
	return newTarPump(dst, true)
}

func newTarPump(dst io.Writer, sanitize bool) (io.WriteCloser, error) {
	pr, pw := io.Pipe()
	gp := &gunzipPump{pw: pw, done: make(chan error, 1)}
	go func() {
		zr, err := gzip.NewReader(pr)
		if err != nil {
			// A malformed/empty stream: drain so the writer's Write unblocks with err.
			_ = pr.CloseWithError(err)
			gp.done <- err
			return
		}
		var cerr error
		if sanitize {
			cerr = sanitizeTar(dst, zr)
		} else {
			_, cerr = io.Copy(dst, zr)
		}
		if zerr := zr.Close(); zerr != nil && cerr == nil {
			cerr = zerr
		}
		_ = pr.CloseWithError(cerr)
		gp.done <- cerr
	}()
	return gp, nil
}

// sanitizeTar copies a tar stream, dropping what a foreign archive has no business
// putting where a root helper extracts it: the setuid/setgid/sticky bits, every
// device/fifo/socket entry, and any link pointing outside the archive. Same rule the
// backup restore already applies (backup.go), which this half was missing.
func sanitizeTar(dst io.Writer, src io.Reader) error {
	tw := tar.NewWriter(dst)
	tr := tar.NewReader(src)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if hasDotDot(hdr.Name) {
			return fmt.Errorf("archive entry %q contains a path traversal", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeReg, tar.TypeDir:
		case tar.TypeSymlink, tar.TypeLink:
			// A relative link inside the tree is ordinary data (a certbot `live/`
			// dir is nothing else); one that leaves it is not.
			if !linkStaysInside(hdr.Name, hdr.Linkname, hdr.Typeflag == tar.TypeLink) {
				continue
			}
		default:
			continue // char/block/fifo/socket: never a service's own data
		}
		// Ownership is kept (a database volume must stay owned by its engine's uid);
		// only the bits above 0777 go.
		hdr.Mode &= 0o777
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if hdr.Typeflag == tar.TypeReg {
			if _, err := io.Copy(tw, tr); err != nil {
				return err
			}
		}
	}
	return tw.Close()
}

// linkStaysInside answers whether a link's target is still under the archive root.
// A hardlink names another archive entry directly; a symlink resolves against the
// directory the link itself sits in.
func linkStaysInside(name, link string, hard bool) bool {
	if link == "" || strings.HasPrefix(link, "/") {
		return false
	}
	target := link
	if !hard {
		// Relative on purpose: anchoring at "/" would let Clean swallow a leading
		// ".." and turn an escape into a pass.
		rel := strings.TrimPrefix(path.Clean(filepath.ToSlash(name)), "/")
		target = path.Join(path.Dir(rel), link)
	}
	return !hasDotDot(path.Clean(target))
}

// gunzipPump bridges Writes of compressed bytes to a gzip.Reader draining into the
// destination writer (see newGunzipPump).
type gunzipPump struct {
	pw   *io.PipeWriter
	done chan error
}

func (g *gunzipPump) Write(p []byte) (int, error) { return g.pw.Write(p) }

func (g *gunzipPump) Close() error {
	// Signal EOF to the gzip.Reader, then wait for the decompress copy to finish and
	// report the first error (a corrupt archive surfaces here).
	_ = g.pw.Close()
	return <-g.done
}

// sendImportResult closes the client-streaming RPC with a terminal StackResult.
// ImportVolume reports business failures in the body (ok=false + message), matching
// StopStack/DestroyStack, rather than as a gRPC error.
func sendImportResult(
	stream pb.Agent_ImportVolumeServer,
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
