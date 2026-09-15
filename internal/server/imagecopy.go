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

const imageCopyTimeout = 30 * time.Minute

const imageRemoveTimeout = 2 * time.Minute

var imageRefPattern = regexp.MustCompile(`^deplo/[a-zA-Z0-9][a-zA-Z0-9._-]*:[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

func validateImageRef(ref string) error {
	if !imageRefPattern.MatchString(ref) || strings.Contains(ref, "..") {
		return fmt.Errorf("unsafe image ref %q (must be deplo/<name>:<tag>)", ref)
	}
	return nil
}

// ExportImage streams a locally built image out as a gzipped `docker save` archive, then optionally deletes it here.
func (s *Service) ExportImage(req *pb.ExportImageRequest, stream pb.Agent_ExportImageServer) error {
	ref := req.GetImageRef()
	if err := validateImageRef(ref); err != nil {
		return fmt.Errorf("export image: %w", err)
	}
	ctx := stream.Context()

	cw := &chunkWriter{send: func(b []byte) error {
		return stream.Send(&pb.ImageChunk{Frame: &pb.ImageChunk_Data{Data: b}})
	}}
	gz, gzErr := gzip.NewWriterLevel(cw, gzip.BestSpeed)
	if gzErr != nil {
		return fmt.Errorf("export image %q: gzip: %w", ref, gzErr)
	}

	code, err := dockercli.PipeOut(ctx, imageCopyTimeout, gz, nil, "save", ref)
	if cerr := gz.Close(); cerr != nil && err == nil {
		return fmt.Errorf("export image %q: finish gzip: %w", ref, cerr)
	}
	if cw.err != nil {
		return cw.err
	}
	if err != nil {
		return fmt.Errorf("export image %q: %w", ref, err)
	}
	if code != 0 {
		return fmt.Errorf("export image %q: docker save exited %d", ref, code)
	}

	if req.GetRemoveAfter() {
		_, _ = dockercli.Run(ctx, imageRemoveTimeout, "rmi", ref)
	}
	return nil
}

// ImportImage is the destination half: the FIRST client message carries the ref the stream is expected to hold, every following message a slice of the gzipped `docker save` archive.
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

	before, err := listImageTags(ctx)
	if err != nil {
		return sendImageResult(stream, false, fmt.Sprintf("import image %q: %v", ref, err), 0, "")
	}

	pr, pw := io.Pipe()
	tally := &countingHasher{h: sha256.New()}
	done := make(chan error, 1)
	go func() {
		code, perr := dockercli.PipeIn(ctx, imageCopyTimeout, pr, nil, "image", "load")
		if perr == nil && code != 0 {
			perr = fmt.Errorf("docker image load exited %d", code)
		}
		_ = pr.CloseWithError(perr)
		done <- perr
	}()

	var recvErr error
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
			if _, werr := tally.WriteTo(pw, data); werr != nil {
				recvErr = werr
				break
			}
		}
	}

	_ = pw.Close()
	loadErr := <-done

	if recvErr != nil {
		return sendImageResult(stream, false, fmt.Sprintf("import image %q: receive: %v", ref, recvErr), 0, "")
	}
	if loadErr != nil {
		return sendImageResult(stream, false, fmt.Sprintf("import image %q: load: %v", ref, loadErr), 0, "")
	}

	after, err := listImageTags(ctx)
	if err != nil {
		return sendImageResult(stream, false, fmt.Sprintf("import image %q: %v", ref, err), tally.n, tally.sum())
	}
	unexpected := unexpectedTags(before, after, ref)
	if len(unexpected) > 0 {
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

var imageLoadMu sync.Mutex

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

func sendImageResult(stream pb.Agent_ImportImageServer, ok bool, errMsg string, n int64, sum string) error {
	return stream.SendAndClose(&pb.StoreResult{Ok: ok, Error: errMsg, BytesWritten: n, Sha256: sum})
}
