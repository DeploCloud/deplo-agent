package server

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// imagecopy.go implements the cross-host BUILT IMAGE copy that backs a build server: a
// host that compiles for machines it does not run on.

// imageCopyTimeout bounds a single export/import. A multi-GB image over a slow
// link is the case to survive; this matches the volume relay's budget.
const imageCopyTimeout = 30 * time.Minute

// imageRemoveTimeout bounds the `docker rmi` that follows a successful export.
const imageRemoveTimeout = 2 * time.Minute

// imageRefPattern is the shape of every ref this relay handles: the control plane's own
// `deplo/<deploy key>:<deployment id[:12]>`, where a deploy key is a slug or a
// preview's `<slug>__pr-<n>`.
var imageRefPattern = regexp.MustCompile(`^deplo/[a-zA-Z0-9][a-zA-Z0-9._-]*:[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// validateImageRef rejects anything that is not one of OUR local image tags.
// Defence in depth behind the control plane's naming (deployImageRef is the only
// minter), and the reason neither half of this relay needs to think about argv.
func validateImageRef(ref string) error {
	if !imageRefPattern.MatchString(ref) || strings.Contains(ref, "..") {
		return fmt.Errorf("unsafe image ref %q (must be deplo/<name>:<tag>)", ref)
	}
	return nil
}

// ExportImage streams a locally built image out as a gzipped `docker save` archive,
// then optionally deletes it here.
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
		// Best-effort on PURPOSE. The bytes are already on the destination, so a failure to
		// reclaim disk here must not turn a completed transfer into a failed deploy.
		_, _ = dockercli.Run(ctx, imageRemoveTimeout, "rmi", ref)
	}
	return nil
}

// ImportImage is the destination half: the FIRST client message carries the ref the
// stream is expected to hold, every following message a slice of the gzipped `docker
// save` archive.
func (s *Service) ImportImage(stream pb.Agent_ImportImageServer) error {
	ctx := stream.Context()

	imageLoadMu.Lock()
	defer imageLoadMu.Unlock()

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

	// The tag list BEFORE the load. A failure to read it is fatal rather than degraded:
	// without the baseline there is no way to tell what the archive added, and silently
	// skipping the check is precisely the outcome it exists to prevent.
	before, err := listImageTags(ctx)
	if err != nil {
		return sendImageResult(stream, false, fmt.Sprintf("import image %q: %v", ref, err), 0, "")
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

	// What the archive actually brought. Two independent failures live here:
	// the declared tag missing (the compose about to reference it would fail at run
	// time with an unhelpful "image not found"), and any OTHER tag appearing.
	after, err := listImageTags(ctx)
	if err != nil {
		return sendImageResult(stream, false, fmt.Sprintf("import image %q: %v", ref, err), tally.n, tally.sum())
	}
	unexpected := unexpectedTags(before, after, ref)
	if len(unexpected) > 0 {
		// Undo them. Best-effort per tag: one stubborn image (in use by a running
		// container, say) must not stop the others being reclaimed, and the RPC
		// fails either way so the caller never treats this as a delivery.
		for _, tag := range unexpected {
			_, _ = dockercli.Run(ctx, imageRemoveTimeout, "rmi", tag)
		}
		return sendImageResult(stream, false,
			fmt.Sprintf("import image %q: the archive also carried %s - refused and removed",
				ref, strings.Join(unexpected, ", ")),
			tally.n, tally.sum())
	}
	if !after[ref] {
		return sendImageResult(stream, false,
			fmt.Sprintf("import image %q: the archive loaded but that tag is not present on this host", ref),
			tally.n, tally.sum())
	}
	return sendImageResult(stream, true, "", tally.n, tally.sum())
}

// unexpectedTags is what the archive brought that nobody asked for: every
// `deplo/` tag present after the load that was neither there before nor the one
// declared. Sorted, so the message an operator reads is stable.
//
// Split out of ImportImage because it is the security decision of this RPC and a
// daemon is a poor place to test one.
//
// SCOPED TO `deplo/` DELIBERATELY, and the reason is a false positive that would
// bite harder than the attack. `docker image ls` sees the whole host, and other
// deploys mutate it concurrently - a `docker-image` source pulling `nginx:latest`
// on this box while an import runs would otherwise be flagged as smuggled and
// REMOVED, breaking a perfectly good deploy. Outside our namespace the agent has
// no standing to delete anything.
//
// Inside it, the attack this closes is the one that matters: a compromised builder
// replacing `deplo/<other-app>:<its deployment id>`, which that app's next
// rollback would then run. Base images are re-pulled by a build that needs them,
// so leaving those alone costs nothing.
//
// ponytail: a build finishing on THIS host during the import window mints a
// `deplo/` tag too and would be misattributed - cost is one rebuild of that app.
// The race-free fix is reading the archive's own manifest before loading, which
// needs the whole stream buffered or a tar parser inline; revisit if a target that
// also builds turns out to be the common case.
func unexpectedTags(before, after map[string]bool, declared string) []string {
	var out []string
	for tag := range after {
		if tag == declared || before[tag] {
			continue
		}
		if !strings.HasPrefix(tag, "deplo/") {
			continue
		}
		out = append(out, tag)
	}
	sort.Strings(out)
	return out
}

// imageLoadMu serializes ImportImage on this host so the before/after tag diff is
// exact. See the note on ImportImage.
var imageLoadMu sync.Mutex

// listImageTags is the set of `<repo>:<tag>` on this host, untagged images skipped
// (`<none>:<none>` is not a name anything can collide with).
func listImageTags(ctx context.Context) (map[string]bool, error) {
	res, err := dockercli.Run(ctx, imageRemoveTimeout, "image", "ls", "--format", "{{.Repository}}:{{.Tag}}")
	if err != nil {
		return nil, fmt.Errorf("list images: %w", err)
	}
	if res.Code != 0 {
		return nil, fmt.Errorf("list images: docker image ls exited %d", res.Code)
	}
	tags := make(map[string]bool)
	for _, line := range strings.Split(res.Stdout, "\n") {
		tag := strings.TrimSpace(line)
		if tag == "" || strings.Contains(tag, "<none>") {
			continue
		}
		tags[tag] = true
	}
	return tags, nil
}

// countingHasher tallies the bytes and sha256 of a relayed stream as it passes through.
// This is what "bytes_written" honestly means for a load - what this host consumed.
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
func sendImageResult(stream pb.Agent_ImportImageServer, ok bool, errMsg string, n int64, sum string) error {
	return stream.SendAndClose(&pb.StoreResult{Ok: ok, Error: errMsg, BytesWritten: n, Sha256: sum})
}
