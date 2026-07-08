package server

import (
	"compress/gzip"
	"fmt"
	"io"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// volumecopy.go implements the cross-host named-volume copy that backs a server
// MOVE (a database or project relocating to another server). Docker named volumes
// are host-local and the agent trust model is strictly star — an agent can neither
// dial nor trust a peer agent — so the volume can't travel host-to-host directly.
// The control plane RELAYS it: ExportVolume streams the source volume's gzipped tar
// out of the OLD host, and the control plane feeds those chunks into ImportVolume on
// the NEW host, which untars them into the target volume. No S3 hop, no agent↔agent
// link.
//
// Both directions reuse the exact volume plumbing Backup/Restore already trust: a
// throwaway busybox helper container that mounts the named volume and tar's it
// in/out (see archiveVolume / newVolumeStreams in backup.go / backup_tar.go), the
// volumeHelperImage constant, and validateVolumeName (which rejects a wire-supplied
// path masquerading as a volume name before it reaches a `-v <name>:/v` mount).

// volumeCopyTimeout bounds a single export/import. A move of a large DB volume can
// take a while; this matches the generous per-step budget the project backup path
// uses for the same helper-container tar of a volume.
const volumeCopyTimeout = 30 * time.Minute

// chunkBytes is the payload size of one VolumeChunk data frame. Comfortably under
// the gRPC max message size (the control plane dials with a 256 MiB cap), while big
// enough that framing overhead is negligible on a multi-GB volume.
const chunkBytes = 1 << 20 // 1 MiB

// ExportVolume tars a named volume from a read-only helper container, gzips it, and
// streams it out as raw byte chunks. The caller is expected to have QUIESCED the
// source first (stopped the owning stack) so the on-disk files can't change under
// the read — this handler only reads, never stops anything, so it stays a pure,
// reusable primitive. Mirrors archiveVolume's producer, but the sink is the gRPC
// stream instead of the backup tar.
func (s *Service) ExportVolume(req *pb.ExportVolumeRequest, stream pb.Agent_ExportVolumeServer) error {
	vol := req.GetVolumeName()
	// Re-validate off-the-wire (defence in depth behind the control plane's naming):
	// an unsafe name like "/" or "/etc" would bind-mount a host path into the helper.
	if err := validateVolumeName(vol); err != nil {
		return fmt.Errorf("export volume: %w", err)
	}
	ctx := stream.Context()

	// gzip the tar as it is produced, writing compressed bytes straight into the
	// stream via chunkWriter — no temp file, no full-archive buffering.
	cw := &chunkWriter{send: func(b []byte) error {
		return stream.Send(&pb.VolumeChunk{Frame: &pb.VolumeChunk_Data{Data: b}})
	}}
	gz := gzip.NewWriter(cw)

	// Producer: the helper container tars the volume's contents to stdout; PipeOut
	// copies that into gz (→ chunkWriter → stream.Send).
	code, err := dockercli.PipeOut(ctx, volumeCopyTimeout, gz, nil,
		"run", "--rm", "-v", vol+":/v:ro", volumeHelperImage,
		"tar", "-C", "/v", "-cf", "-", ".")
	// Flush + finish the gzip trailer BEFORE reporting, so the destination sees a
	// complete stream. A Close error trumps a benign producer exit.
	if cerr := gz.Close(); cerr != nil && err == nil {
		return fmt.Errorf("export volume %q: finish gzip: %w", vol, cerr)
	}
	if cw.err != nil {
		// stream.Send failed (the control plane relay went away) — nothing more to do.
		return cw.err
	}
	if err != nil {
		// busybox `tar -cf -` legitimately exits 1 for a benign "file changed as we
		// read it" on a LIVE volume while STILL emitting a complete archive (same case
		// archiveVolume tolerates). But ExportVolume's caller stops the source stack
		// FIRST, so the volume is quiesced and a non-zero exit here is a real failure —
		// surface it rather than shipping a possibly-truncated archive.
		return fmt.Errorf("export volume %q: %w", vol, err)
	}
	if code != 0 {
		return fmt.Errorf("export volume %q: tar exited %d", vol, code)
	}
	return nil
}

// chunkWriter is an io.Writer that frames whatever is written to it into ~1 MiB
// `data` messages on an export stream via `send`. gzip writes here; the concrete
// stream (VolumeChunk vs FilesChunk) is captured by the send closure, so this is
// shared by ExportVolume and ExportFiles.
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
		// Copy the slice — gRPC may retain the message past this call, and gzip reuses
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
// volume name + wipe flag; every following message carries a slice of the gzipped
// tar. The agent (optionally) wipes the target volume, then gunzips + untars the
// reassembled stream into it via a helper container. The caller MUST have stopped
// the destination stack first so nothing writes the volume under the untar. Reuses
// the same `tar -C /v -xf -` extract that Restore's newVolumeStreams runs.
func (s *Service) ImportVolume(stream pb.Agent_ImportVolumeServer) error {
	ctx := stream.Context()

	// 1. The header frame names the target + whether to wipe. It must come first.
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("import volume: read header: %w", err)
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return sendImportResult(stream, false, "first message must carry a header (volume name)")
	}
	vol := hdr.GetVolumeName()
	if err := validateVolumeName(vol); err != nil {
		return sendImportResult(stream, false, fmt.Sprintf("import volume: %v", err))
	}

	// 2. Optionally empty the target so the import overwrites rather than merges into
	//    whatever the freshly-provisioned stack initialised. wipeVolume keeps the
	//    volume itself (it stays attached to the stopped stack), just clears it.
	if hdr.GetWipeFirst() {
		if err := wipeVolume(ctx, vol); err != nil {
			return sendImportResult(stream, false, fmt.Sprintf("wipe target volume %q: %v", vol, err))
		}
	}

	// 3. Reassemble the data frames into a byte stream (a pipe the untar reads),
	//    gunzip it, and feed it to `tar -C /v -xf -` in a helper container. The
	//    recv loop runs in this goroutine writing into the pipe; PipeIn drains it.
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		// `tar -C /v -xf -` reads the tar we feed on stdin and extracts into the
		// volume (created on demand if absent). -i for interactive stdin.
		code, perr := dockercli.PipeIn(ctx, volumeCopyTimeout, pr, nil,
			"run", "--rm", "-i", "-v", vol+":/v", volumeHelperImage,
			"tar", "-C", "/v", "-xf", "-")
		if perr == nil && code != 0 {
			perr = fmt.Errorf("volume extract exited %d", code)
		}
		// Unblock the writer if the extractor died early.
		_ = pr.CloseWithError(perr)
		done <- perr
	}()

	// Gunzip the incoming compressed frames into the pipe the untar reads.
	gz, gzErr := newGunzipPump(pw)
	if gzErr != nil {
		_ = pw.CloseWithError(gzErr)
		<-done
		return sendImportResult(stream, false, fmt.Sprintf("import volume %q: %v", vol, gzErr))
	}

	var recvErr error
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

	if recvErr != nil {
		return sendImportResult(stream, false, fmt.Sprintf("import volume %q: receive: %v", vol, recvErr))
	}
	if gzCloseErr != nil {
		return sendImportResult(stream, false, fmt.Sprintf("import volume %q: decompress: %v", vol, gzCloseErr))
	}
	if extractErr != nil {
		return sendImportResult(stream, false, fmt.Sprintf("import volume %q: extract: %v", vol, extractErr))
	}
	return sendImportResult(stream, true, "")
}

// newGunzipPump returns a WriteCloser that decompresses everything written to it
// and forwards the plaintext to `dst`. gzip.NewReader needs a Reader, but our data
// arrives as Writes off the gRPC stream, so we bridge with an internal pipe: caller
// Writes compressed bytes → gzip.Reader pulls from the pipe → decompressed bytes go
// to dst. Close flushes and tears the bridge down.
func newGunzipPump(dst io.Writer) (io.WriteCloser, error) {
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
		_, cerr := io.Copy(dst, zr)
		if zerr := zr.Close(); zerr != nil && cerr == nil {
			cerr = zerr
		}
		_ = pr.CloseWithError(cerr)
		gp.done <- cerr
	}()
	return gp, nil
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
func sendImportResult(stream pb.Agent_ImportVolumeServer, ok bool, errMsg string) error {
	return stream.SendAndClose(&pb.StackResult{Ok: ok, Error: errMsg})
}
