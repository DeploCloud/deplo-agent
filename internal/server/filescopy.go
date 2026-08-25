package server

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// filescopy.go is the files-dir sibling of volumecopy.go: it copies a service's
// host-side files dir (<stack_dir>/files/<slug>) across hosts for a server move.

// ExportFiles tars a service's files dir out as a gzipped stream. The caller is
// expected to have quiesced the source (stopped the stack) first.
func (s *Service) ExportFiles(req *pb.ExportFilesRequest, stream pb.Agent_ExportFilesServer) error {
	slug := req.GetSlug()
	if err := validateSlug(slug); err != nil {
		return fmt.Errorf("export files: %w", err)
	}
	root := s.filesRoot(slug)

	cw := &chunkWriter{send: func(b []byte) error {
		return stream.Send(&pb.FilesChunk{Frame: &pb.FilesChunk_Data{Data: b}})
	}}
	gz := gzip.NewWriter(cw)
	tw := tar.NewWriter(gz)

	// A missing dir (or a non-directory at that path) yields an empty (header-only) tar —
	// valid, and the destination handles it as "nothing to restore".
	st, statErr := os.Stat(root)
	switch {
	case statErr == nil && st.IsDir():
		if err := addDirToTar(tw, root, "files"); err != nil {
			// Best-effort close the writers before returning the walk error.
			_ = tw.Close()
			_ = gz.Close()
			return fmt.Errorf("export files %q: %w", slug, err)
		}
	case statErr == nil || os.IsNotExist(statErr):
		// Benign empty case: no dir, or a non-directory at that path. Nothing to walk.
	default:
		_ = tw.Close()
		_ = gz.Close()
		return fmt.Errorf("export files %q: stat: %w", slug, statErr)
	}

	// Finish the tar + gzip trailers BEFORE reporting, so the destination sees a
	// complete stream. Order matters: close tar (flushes into gzip), then gzip.
	if err := tw.Close(); err != nil {
		_ = gz.Close()
		return fmt.Errorf("export files %q: finish tar: %w", slug, err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("export files %q: finish gzip: %w", slug, err)
	}
	if cw.err != nil {
		return cw.err // stream.Send failed (the relay went away)
	}
	return nil
}

// ImportFiles is the destination half: the FIRST client message carries the target slug
// + wipe flag; every following message carries a slice of the gzipped tar. The caller
// MUST have stopped the destination stack first.
func (s *Service) ImportFiles(stream pb.Agent_ImportFilesServer) error {
	// 1. Header frame first (slug + wipe).
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("import files: read header: %w", err)
	}
	hdr := first.GetHeader()
	if hdr == nil {
		return sendFilesResult(stream, false, "first message must carry a header (slug)")
	}
	slug := hdr.GetSlug()
	if err := validateSlug(slug); err != nil {
		return sendFilesResult(stream, false, fmt.Sprintf("import files: %v", err))
	}
	root := s.filesRoot(slug)

	// 2. The wipe is DEFERRED to the first data frame (below), like ImportVolume's.

	// 3. Reassemble the data frames, gunzip, and untar each entry (framed as
	//    files/<rel>) into the dir via extractToDir. Drive the recv loop as the
	//    producer feeding a gunzip pump whose plaintext we tar-read here.
	pr, pw := io.Pipe()
	gz, gzErr := newGunzipPump(pw)
	if gzErr != nil {
		_ = pw.CloseWithError(gzErr)
		return sendFilesResult(stream, false, fmt.Sprintf("import files %q: %v", slug, gzErr))
	}

	// Consume the client stream into the gunzip pump in a goroutine; the tar reader
	// below pulls the decompressed bytes out of the pipe.
	recvDone := make(chan error, 1)
	// Written only by the goroutine below and read only after `<-recvDone`, which
	// is what makes the plain bool safe here.
	wiped := false
	go func() {
		var rerr error
		for {
			msg, e := stream.Recv()
			if e == io.EOF {
				break
			}
			if e != nil {
				rerr = e
				break
			}
			if data := msg.GetData(); len(data) > 0 {
				// The first real byte earns the wipe. RemoveAll of a non-existent
				// dir is a no-op (nil), matching restoreProject.
				if hdr.GetWipeFirst() && !wiped {
					if werr := os.RemoveAll(root); werr != nil {
						rerr = fmt.Errorf("wipe files dir %q: %w", slug, werr)
						break
					}
					wiped = true
				}
				if _, werr := gz.Write(data); werr != nil {
					rerr = werr
					break
				}
			}
		}
		// Flush the gunzip (last plaintext) then close the pipe so the tar reader
		// sees EOF. A gunzip close error (corrupt archive) trumps a nil recv error.
		if cerr := gz.Close(); cerr != nil && rerr == nil {
			rerr = cerr
		}
		_ = pw.CloseWithError(rerr)
		recvDone <- rerr
	}()

	// Extract each entry into the files dir. extractToDir strips nothing, so pass the
	// entry name minus the "files/" framing.
	tr := tar.NewReader(pr)
	var extractErr error
	for {
		th, terr := tr.Next()
		if terr == io.EOF {
			break
		}
		if terr != nil {
			extractErr = terr
			break
		}
		name := th.Name
		// Only entries under files/ are ours; skip anything else defensively.
		const prefix = "files/"
		if len(name) < len(prefix) || name[:len(prefix)] != prefix {
			continue
		}
		rel := name[len(prefix):]
		if rel == "" {
			continue
		}
		if eerr := extractToDir(root, rel, th, tr); eerr != nil {
			extractErr = eerr
			break
		}
	}
	// Drain the pipe so the producer goroutine can finish, then collect its error.
	_, _ = io.Copy(io.Discard, pr)
	recvErr := <-recvDone

	// One failure path, and it takes the half-written directory with it: a config
	// file that arrived truncated is read by whatever mounts it, and reads clean.
	// No helper container here - this extractor runs in-process. See abandonImport.
	failure := ""
	switch {
	case extractErr != nil:
		failure = fmt.Sprintf("import files %q: extract: %v", slug, extractErr)
	case recvErr != nil:
		failure = fmt.Sprintf("import files %q: receive: %v", slug, recvErr)
	}
	if failure != "" {
		if wiped {
			failure += abandonImport("", func(context.Context) error {
				return os.RemoveAll(root)
			})
		}
		return sendFilesResult(stream, false, failure)
	}
	return sendFilesResult(stream, true, "")
}

// sendFilesResult closes the client-streaming ImportFiles RPC with a terminal
// StackResult (business failures in the body, like ImportVolume).
func sendFilesResult(stream pb.Agent_ImportFilesServer, ok bool, errMsg string) error {
	return stream.SendAndClose(&pb.StackResult{Ok: ok, Error: errMsg})
}
