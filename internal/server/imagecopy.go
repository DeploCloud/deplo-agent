package server

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"regexp"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// imagecopy.go implements the cross-host BUILT IMAGE copy that backs a build
// server: a host that compiles for machines it does not run on. It is the third
// sibling of the volume (volumecopy.go) and files-dir relays and shares their
// reasoning exactly - the agent trust model is strictly star, so an agent can
// neither dial nor trust a peer, and the control plane RELAYS the bytes: it calls
// ExportImage on the BUILDER and feeds the chunks into ImportImage on the host
// that will run the container. No registry, no agent↔agent link.
//
// The two halves are deliberately asymmetric about compression. ExportImage gzips
// at BestSpeed, which is a real win on a classic-graphdriver daemon (`docker save`
// writes UNCOMPRESSED layer tars there) and close to a no-op on a containerd store
// (whose blobs are already compressed). ImportImage does NOT gunzip: `docker load`
// detects and decompresses gzip itself, so pumping the stream straight at it is one
// less moving part than the volume path needs - and a truncated or corrupt stream
// still fails loudly, on the gzip CRC and the tar structure, inside the load.

// imageCopyTimeout bounds a single export/import. A multi-GB image over a slow
// link is the case to survive; this matches the volume relay's budget.
const imageCopyTimeout = 30 * time.Minute

// imageRemoveTimeout bounds the `docker rmi` that follows a successful export.
const imageRemoveTimeout = 2 * time.Minute

// imageRefPattern is the shape of every ref this relay handles: the control plane's
// own `deplo/<deploy key>:<deployment id[:12]>`, where a deploy key is a slug or a
// preview's `<slug>__pr-<n>`. Anchored, and a leading `-` can never match, so a ref
// off the wire cannot arrive at argv looking like a docker flag.
var imageRefPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._/-]*:[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// validateImageRef rejects anything that is not a plain local image tag. Defence in
// depth behind the control plane's naming (deployImageRef is the only minter), and
// the reason neither half of this relay needs to think about argv.
func validateImageRef(ref string) error {
	if !imageRefPattern.MatchString(ref) || strings.Contains(ref, "..") {
		return fmt.Errorf("unsafe image ref %q (must be a plain <name>:<tag>)", ref)
	}
	return nil
}

// ExportImage streams a locally built image out as a gzipped `docker save` archive,
// then optionally deletes it here. The image is a COURIER on a build server, not an
// artifact: what is worth keeping across builds is the BuildKit cache, which this
// never touches. Mirrors ExportVolume's producer, reusing the same chunkWriter.
func (s *Service) ExportImage(req *pb.ExportImageRequest, stream pb.Agent_ExportImageServer) error {
	ref := req.GetImageRef()
	if err := validateImageRef(ref); err != nil {
		return fmt.Errorf("export image: %w", err)
	}
	ctx := stream.Context()

	cw := &chunkWriter{send: func(b []byte) error {
		return stream.Send(&pb.ImageChunk{Frame: &pb.ImageChunk_Data{Data: b}})
	}}
	// BestSpeed, not the default: see the compression note in the file header. The
	// error is impossible for a constant level, but Go makes us read it.
	gz, gzErr := gzip.NewWriterLevel(cw, gzip.BestSpeed)
	if gzErr != nil {
		return fmt.Errorf("export image %q: gzip: %w", ref, gzErr)
	}

	code, err := dockercli.PipeOut(ctx, imageCopyTimeout, gz, nil, "save", ref)
	// Flush + finish the gzip trailer BEFORE reporting, so the destination sees a
	// complete stream. A Close error trumps a benign producer exit.
	if cerr := gz.Close(); cerr != nil && err == nil {
		return fmt.Errorf("export image %q: finish gzip: %w", ref, cerr)
	}
	if cw.err != nil {
		// stream.Send failed (the control plane relay went away) - nothing more to do,
		// and in particular nothing to remove: the image never made it anywhere.
		return cw.err
	}
	if err != nil {
		return fmt.Errorf("export image %q: %w", ref, err)
	}
	if code != 0 {
		return fmt.Errorf("export image %q: docker save exited %d", ref, code)
	}

	if req.GetRemoveAfter() {
		// Best-effort on PURPOSE. The bytes are already on the destination, so a
		// failure to reclaim disk here must not turn a completed transfer into a
		// failed deploy. An image this leaves behind is labelled like any other and
		// the existing Docker cleanup sweep reaps it on its next pass.
		_, _ = dockercli.Run(ctx, imageRemoveTimeout, "rmi", ref)
	}
	return nil
}

// ImportImage is the destination half: the FIRST client message carries the ref the
// stream is expected to hold, every following message a slice of the gzipped `docker
// save` archive. The reassembled stream is piped into `docker image load`, which
// decompresses it itself, and the loaded tag is then verified to actually exist -
// the one check that catches a relay wired to the wrong image, which no amount of
// byte counting would.
func (s *Service) ImportImage(stream pb.Agent_ImportImageServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("import image: read header: %w", err)
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return sendImageResult(stream, false, "first message must carry a header (image ref)", 0, "")
	}
	ref := hdr.GetImageRef()
	if err := validateImageRef(ref); err != nil {
		return sendImageResult(stream, false, fmt.Sprintf("import image: %v", err), 0, "")
	}

	// Bridge the client stream to an io.Reader so the load stays a plain PipeIn, the
	// same shape WriteStoreFile uses. The counter/hash sits on the WRITE side so what
	// is reported is what was actually fed to docker, not what was framed.
	pr, pw := io.Pipe()
	tally := &countingHasher{h: sha256.New()}
	done := make(chan error, 1)
	go func() {
		code, perr := dockercli.PipeIn(ctx, imageCopyTimeout, pr, nil, "image", "load")
		if perr == nil && code != 0 {
			perr = fmt.Errorf("docker image load exited %d", code)
		}
		// Unblock the writer if the load died early.
		_ = pr.CloseWithError(perr)
		done <- perr
	}()

	var recvErr error
	for {
		msg, rerr := stream.Recv()
		if rerr == io.EOF {
			break // client finished sending; close the pipe below
		}
		if rerr != nil {
			recvErr = rerr
			break
		}
		// A stray header mid-stream is a protocol violation; ignore non-data frames.
		if data := msg.GetData(); len(data) > 0 {
			if _, werr := tally.WriteTo(pw, data); werr != nil {
				recvErr = werr
				break
			}
		}
	}

	// Close the pipe so the load sees a clean EOF, then wait for it.
	_ = pw.Close()
	loadErr := <-done

	if recvErr != nil {
		return sendImageResult(stream, false, fmt.Sprintf("import image %q: receive: %v", ref, recvErr), 0, "")
	}
	if loadErr != nil {
		return sendImageResult(stream, false, fmt.Sprintf("import image %q: load: %v", ref, loadErr), 0, "")
	}

	// `docker load` restores whatever tags the archive names. If that is not the tag
	// the caller declared, the compose file about to reference it would fail at run
	// time with an unhelpful "image not found"; saying so here names the real fault.
	if res, ierr := dockercli.Run(ctx, imageRemoveTimeout, "image", "inspect", ref); ierr != nil || res.Code != 0 {
		return sendImageResult(stream, false,
			fmt.Sprintf("import image %q: the archive loaded but that tag is not present on this host", ref),
			tally.n, tally.sum())
	}
	return sendImageResult(stream, true, "", tally.n, tally.sum())
}

// countingHasher tallies the bytes and sha256 of a relayed stream as it passes
// through. Diagnostic, not a verification protocol: the export half reports no
// digest to compare against, and corruption is already fatal inside `docker load`
// (gzip CRC, then tar structure). This is what "bytes_written" honestly means for
// a load - what this host consumed.
type countingHasher struct {
	h hash.Hash
	n int64
}

func (c *countingHasher) WriteTo(w io.Writer, p []byte) (int, error) {
	n, err := w.Write(p)
	if n > 0 {
		_, _ = c.h.Write(p[:n])
		c.n += int64(n)
	}
	return n, err
}

func (c *countingHasher) sum() string { return hex.EncodeToString(c.h.Sum(nil)) }

// sendImageResult closes the client-streaming RPC with a terminal StoreResult.
// ImportImage reports business failures in the body (ok=false + message) rather than
// as a gRPC error, matching ImportVolume and WriteStoreFile - a gRPC error code is
// reserved for "this agent cannot do that at all", which is what an older binary's
// UNIMPLEMENTED means and what the control plane turns into "update the agent".
func sendImageResult(stream pb.Agent_ImportImageServer, ok bool, errMsg string, n int64, sum string) error {
	return stream.SendAndClose(&pb.StoreResult{Ok: ok, Error: errMsg, BytesWritten: n, Sha256: sum})
}
