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

// ExportFiles tars a service's files dir out as a gzipped stream.
func (s *Service) ExportFiles(req *pb.ExportFilesRequest, stream pb.Agent_ExportFilesServer) error {
	slug := req.GetSlug()
	if err := validateSlug(slug); err != nil {
		return fmt.Errorf("export files: %w", err)
	}
	root := s.filesRoot(slug)

	cw := &chunkWriter{send: func(b []byte) error {
		return stream.Send(&pb.FilesChunk{Frame: &pb.FilesChunk_Data{Data: b}})
	}}
	gz, _ := gzip.NewWriterLevel(cw, gzip.BestSpeed)
	tw := tar.NewWriter(gz)

	st, statErr := os.Stat(root)
	switch {
	case statErr == nil && st.IsDir():
		if err := addDirToTar(tw, root, "files"); err != nil {
			_ = tw.Close()
			_ = gz.Close()
			return fmt.Errorf("export files %q: %w", slug, err)
		}
	case statErr == nil || os.IsNotExist(statErr):
	default:
		_ = tw.Close()
		_ = gz.Close()
		return fmt.Errorf("export files %q: stat: %w", slug, statErr)
	}

	if err := tw.Close(); err != nil {
		_ = gz.Close()
		return fmt.Errorf("export files %q: finish tar: %w", slug, err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("export files %q: finish gzip: %w", slug, err)
	}
	if cw.err != nil {
		return cw.err
	}
	return nil
}

// ImportFiles is the destination half: the FIRST client message carries the target slug + wipe flag; every following message carries a slice of the gzipped tar.
func (s *Service) ImportFiles(stream pb.Agent_ImportFilesServer) error {
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

	pr, pw := io.Pipe()
	gz, gzErr := newGunzipPump(pw)
	if gzErr != nil {
		_ = pw.CloseWithError(gzErr)
		return sendFilesResult(stream, false, fmt.Sprintf("import files %q: %v", slug, gzErr))
	}

	recvDone := make(chan error, 1)
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
		if cerr := gz.Close(); cerr != nil && rerr == nil {
			rerr = cerr
		}
		_ = pw.CloseWithError(rerr)
		recvDone <- rerr
	}()

	tr := tar.NewReader(&budgetReader{r: pr, budget: maxProjectRestoreBytes})
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
	_, _ = io.Copy(io.Discard, pr)
	recvErr := <-recvDone

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

func sendFilesResult(stream pb.Agent_ImportFilesServer, ok bool, errMsg string) error {
	return stream.SendAndClose(&pb.StackResult{Ok: ok, Error: errMsg})
}
