package server

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

const volumeCopyTimeout = 6 * time.Hour

const chunkBytes = 1 << 20

const volumeExistsTimeout = 15 * time.Second

const importCleanupTimeout = 6 * time.Minute

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

func importHelperName() string {
	return fmt.Sprintf("deplo-import-%d", time.Now().UnixNano())
}

// ExportVolume tars a named volume from a read-only helper container, gzips it, and streams it out as raw byte chunks.
func (s *Service) ExportVolume(req *pb.ExportVolumeRequest, stream pb.Agent_ExportVolumeServer) error {
	vol := req.GetVolumeName()
	if err := validateVolumeName(vol); err != nil {
		return fmt.Errorf("export volume: %w", err)
	}
	ctx := stream.Context()

	if err := assertVolumeExists(ctx, vol); err != nil {
		return err
	}

	cw := &chunkWriter{send: func(b []byte) error {
		return stream.Send(&pb.VolumeChunk{Frame: &pb.VolumeChunk_Data{Data: b}})
	}}
	gz, _ := gzip.NewWriterLevel(cw, gzip.BestSpeed)

	code, err := dockercli.PipeOut(ctx, volumeCopyTimeout, gz, nil,
		volumeHelperRun(ctx, "-v", vol+":/v:ro", volumeHelperImage,
			"tar", "-C", "/v", "-cf", "-", ".")...)
	if cerr := gz.Close(); cerr != nil && err == nil {
		return fmt.Errorf("export volume %q: finish gzip: %w", vol, cerr)
	}
	if cw.err != nil {
		return cw.err
	}
	if err != nil {
		return fmt.Errorf("export volume %q: %w", vol, err)
	}
	if code != 0 {
		return fmt.Errorf("export volume %q: tar exited %d", vol, code)
	}
	return nil
}

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

// ImportVolume is the destination half: the FIRST client message carries the target volume name + wipe flag; every following message carries a slice of the gzipped tar.
func (s *Service) ImportVolume(stream pb.Agent_ImportVolumeServer) error {
	ctx := stream.Context()

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

	pr, pw := io.Pipe()
	done := make(chan error, 1)
	helper := importHelperName()
	go func() {
		code, perr := dockercli.PipeIn(ctx, volumeCopyTimeout, pr, nil,
			volumeHelperRun(ctx, "-i", "--name", helper, "-v", vol+":/v", volumeHelperImage,
				"tar", "-C", "/v", "-xf", "-")...)
		if perr == nil && code != 0 {
			perr = fmt.Errorf("volume extract exited %d", code)
		}
		_ = pr.CloseWithError(perr)
		done <- perr
	}()

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
			break
		}
		if rerr != nil {
			recvErr = rerr
			break
		}
		if data := msg.GetData(); len(data) > 0 {
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

	gzCloseErr := gz.Close()
	_ = pw.Close()
	extractErr := <-done

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
		if wiped || wipeErr != nil {
			failure += abandonImport(helper, func(c context.Context) error {
				return wipeVolume(c, vol)
			})
		}
		return stream.SendAndClose(importResult(false, 0, "", failure, &gz.drops))
	}
	return stream.SendAndClose(
		importResult(true, received, hex.EncodeToString(digest.Sum(nil)), "", &gz.drops))
}

func newGunzipPump(dst io.Writer) (*gunzipPump, error) {
	return newTarPump(dst, false)
}

func newSanitizingGunzipPump(dst io.Writer) (*gunzipPump, error) {
	return newTarPump(dst, true)
}

func newTarPump(dst io.Writer, sanitize bool) (*gunzipPump, error) {
	pr, pw := io.Pipe()
	gp := &gunzipPump{pw: pw, done: make(chan error, 1)}
	go func() {
		zr, err := gzip.NewReader(pr)
		if err != nil {
			_ = pr.CloseWithError(err)
			gp.done <- err
			return
		}
		var cerr error
		if sanitize {
			cerr = sanitizeTar(dst, zr, &gp.drops)
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

type tarDrops struct {
	links   int32
	special int32
	names   []string
}

const (
	droppedNamesMax = 5
	droppedNameMax  = 80
)

func (d *tarDrops) add(name string, link bool) {
	if link {
		d.links++
	} else {
		d.special++
	}
	if len(d.names) < droppedNamesMax {
		if len(name) > droppedNameMax {
			name = name[:droppedNameMax] + "~"
		}
		d.names = append(d.names, name)
	}
}

func sanitizeTar(dst io.Writer, src io.Reader, drops *tarDrops) error {
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
			if !linkStaysInside(hdr.Name, hdr.Linkname, hdr.Typeflag == tar.TypeLink) {
				drops.add(hdr.Name, true)
				continue
			}
		default:
			drops.add(hdr.Name, false)
			continue
		}
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
	if _, err := io.Copy(io.Discard, src); err != nil {
		return err
	}
	return tw.Close()
}

func linkStaysInside(name, link string, hard bool) bool {
	if link == "" || strings.HasPrefix(link, "/") {
		return false
	}
	target := link
	if !hard {
		rel := strings.TrimPrefix(path.Clean(filepath.ToSlash(name)), "/")
		target = path.Join(path.Dir(rel), link)
	}
	return !hasDotDot(path.Clean(target))
}

type gunzipPump struct {
	pw    *io.PipeWriter
	done  chan error
	drops tarDrops
}

func (g *gunzipPump) Write(p []byte) (int, error) { return g.pw.Write(p) }

func (g *gunzipPump) Close() error {
	_ = g.pw.Close()
	return <-g.done
}

func sendImportResult(
	stream pb.Agent_ImportVolumeServer,
	ok bool,
	bytesWritten int64,
	sha256Hex string,
	errMsg string,
) error {
	return stream.SendAndClose(importResult(ok, bytesWritten, sha256Hex, errMsg, nil))
}

func importResult(ok bool, bytesWritten int64, sha256Hex, errMsg string, drops *tarDrops) *pb.StackResult {
	res := &pb.StackResult{
		Ok:           ok,
		Error:        errMsg,
		BytesWritten: bytesWritten,
		Sha256:       sha256Hex,
	}
	if drops != nil {
		res.DroppedLinks = drops.links
		res.DroppedSpecial = drops.special
		res.DroppedNames = drops.names
	}
	return res
}
