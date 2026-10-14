package server

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"

	"filippo.io/age"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/s3client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type artifactWriter struct {
	gz    io.WriteCloser
	age   io.WriteCloser
	gzOut *countingWriter
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func newArtifactWriter(sink io.Writer, recipient string) (*artifactWriter, error) {
	a := &artifactWriter{}
	w := sink
	if recipient != "" {
		r, err := age.ParseX25519Recipient(recipient)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid backup encryption key: %v", err)
		}
		enc, err := age.Encrypt(sink, r)
		if err != nil {
			return nil, fmt.Errorf("start encryption: %w", err)
		}
		a.age = enc
		w = enc
	}
	a.gzOut = &countingWriter{w: w}
	a.gz = gzip.NewWriter(a.gzOut)
	return a, nil
}

// Writer is what the dump producer writes into.
func (a *artifactWriter) Writer() io.Writer { return a.gz }

// DecryptedSize is how many bytes the artifact holds once its age layer is removed.
func (a *artifactWriter) DecryptedSize() int64 { return a.gzOut.n }

// Close finishes the chain in the ONE order that produces a readable artifact: gzip's trailer first, then age's final-chunk marker.
func (a *artifactWriter) Close() error {
	err := a.gz.Close()
	if a.age != nil {
		if cerr := a.age.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

func openArtifactReader(src io.Reader, identity string) (io.ReadCloser, error) {
	r := src
	if identity != "" {
		id, err := age.ParseX25519Identity(identity)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid backup decryption key: %v", err)
		}
		dec, err := age.Decrypt(src, id)
		if err != nil {
			return nil, fmt.Errorf("decrypt backup (is this the right recovery key?): %w", err)
		}
		r = dec
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open gzip stream (is this a Deplo backup?): %w", err)
	}
	return gz, nil
}

func digestMismatch(got, expected string) error {
	if strings.EqualFold(got, expected) {
		return nil
	}
	return status.Errorf(codes.DataLoss,
		"this backup is not the one Deplo wrote: expected sha256 %s, the artifact is %s. "+
			"Refusing to restore it - the file has been changed or replaced since the backup ran.",
		expected, got)
}

type verifyingReader struct {
	r        io.Reader
	sum      hash.Hash
	expected string
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	if n > 0 {
		v.sum.Write(p[:n])
	}
	return n, err
}

func (v *verifyingReader) finish() error {
	if _, err := io.Copy(v.sum, v.r); err != nil {
		return fmt.Errorf("read the rest of the artifact to verify it: %w", err)
	}
	return digestMismatch(hex.EncodeToString(v.sum.Sum(nil)), v.expected)
}

type artifactSource struct {
	s3              *pb.S3Target
	store           *pb.StoreTarget
	identity        string
	expectedSha256  string
	verifier        *verifyingReader
	integrityProven bool
	configUntrusted bool
	stream          io.Reader
	spooled         string
	label           string
}

func sourceFromRestore(s *Service, req *pb.RestoreRequest) (*artifactSource, error) {
	switch {
	case req.GetStore() != nil:
		if req.GetAgeIdentity() == "" {
			return nil, status.Error(codes.InvalidArgument,
				"restoring from a server store needs its recovery key")
		}
		root, err := s.resolveStoreRoot(req.GetStore().GetRoot(), false)
		if err != nil {
			return nil, err
		}
		if verr := verifyStoreDigest(root, req.GetStore().GetObjectKey(), req.GetExpectedSha256()); verr != nil {
			return nil, verr
		}
		return &artifactSource{
			store:           &pb.StoreTarget{Root: root, ObjectKey: req.GetStore().GetObjectKey()},
			identity:        req.GetAgeIdentity(),
			integrityProven: req.GetExpectedSha256() != "",
			label:           filepath.Join(root, req.GetStore().GetObjectKey()),
		}, nil
	case req.GetS3() != nil && req.GetS3().GetObjectKey() != "":
		return &artifactSource{
			s3:             req.GetS3(),
			identity:       req.GetAgeIdentity(),
			expectedSha256: req.GetExpectedSha256(),
			label:          req.GetS3().GetObjectKey(),
		}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "restore request names no artifact to restore from")
	}
}

func (a *artifactSource) openRaw(ctx context.Context) (io.Reader, func(), error) {
	switch {
	case a.spooled != "":
		f, err := os.Open(a.spooled)
		if err != nil {
			return nil, nil, err
		}
		return f, func() { _ = f.Close() }, nil
	case a.stream != nil:
		return a.stream, func() {}, nil
	case a.store != nil:
		f, err := storeOpen(a.store.GetRoot(), a.store.GetObjectKey())
		if err != nil {
			return nil, nil, err
		}
		return f, func() { _ = f.Close() }, nil
	default:
		obj, err := s3client.Download(ctx, s3cfg(a.s3), a.s3.GetObjectKey())
		if err != nil {
			return nil, nil, fmt.Errorf("open S3 object: %w", err)
		}
		return obj, func() { _ = obj.Close() }, nil
	}
}

func (a *artifactSource) open(ctx context.Context) (io.Reader, func(), error) {
	raw, closeRaw, err := a.openRaw(ctx)
	if err != nil {
		return nil, nil, err
	}
	if a.expectedSha256 != "" {
		a.verifier = &verifyingReader{r: raw, sum: sha256.New(), expected: a.expectedSha256}
		raw = a.verifier
	}
	rc, err := openArtifactReader(raw, a.identity)
	if err != nil {
		closeRaw()
		return nil, nil, err
	}
	return rc, func() {
		_ = rc.Close()
		closeRaw()
	}, nil
}

func (a *artifactSource) verify() error {
	if a.verifier == nil {
		return nil
	}
	if err := a.verifier.finish(); err != nil {
		return err
	}
	a.integrityProven = true
	return nil
}

type artifactDestination struct {
	s3        *pb.S3Target
	store     *pb.StoreTarget
	stream    func([]byte) error
	recipient string
	key       string
	label     string
}

func destinationFromBackup(s *Service, req *pb.BackupRequest, send func([]byte) error) (*artifactDestination, error) {
	recipient := req.GetAgeRecipient()
	switch {
	case req.GetStreamOut():
		if recipient == "" {
			return nil, status.Error(codes.InvalidArgument,
				"a relayed backup must be encrypted, but no encryption key was sent")
		}
		key := req.GetStore().GetObjectKey()
		return &artifactDestination{stream: send, recipient: recipient, key: key, label: "the control plane"}, nil
	case req.GetStore() != nil:
		if recipient == "" {
			return nil, status.Error(codes.InvalidArgument,
				"a backup written to a server must be encrypted, but no encryption key was sent")
		}
		root, err := s.resolveStoreRoot(req.GetStore().GetRoot(), false)
		if err != nil {
			return nil, err
		}
		key := req.GetStore().GetObjectKey()
		if key == "" {
			return nil, status.Error(codes.InvalidArgument, "backup request missing object key")
		}
		return &artifactDestination{
			store:     &pb.StoreTarget{Root: root, ObjectKey: key},
			recipient: recipient,
			key:       key,
			label:     storeObjectLabel(root, key),
		}, nil
	case req.GetS3() != nil && req.GetS3().GetObjectKey() != "":
		return &artifactDestination{
			s3:        req.GetS3(),
			recipient: recipient,
			key:       req.GetS3().GetObjectKey(),
			label:     req.GetS3().GetObjectKey(),
		}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "backup request names no destination")
	}
}

type artifactWritten struct {
	size          int64
	decryptedSize int64
	digest        string
}

func (s *Service) writeArtifact(ctx context.Context, dest *artifactDestination, produce func(io.Writer) error) (out artifactWritten, err error) {
	pr, pw := io.Pipe()
	var decrypted int64
	go func() {
		aw, aerr := newArtifactWriter(pw, dest.recipient)
		if aerr != nil {
			pw.CloseWithError(aerr)
			return
		}
		perr := produce(aw.Writer())
		if cerr := aw.Close(); perr == nil {
			perr = cerr
		}
		decrypted = aw.DecryptedSize()
		pw.CloseWithError(perr)
	}()
	defer func() {
		if err != nil {
			_ = pr.CloseWithError(err)
		}
	}()

	switch {
	case dest.stream != nil:
		sum := sha256.New()
		buf := make([]byte, storeChunkBytes)
		var size int64
		for {
			if cerr := ctx.Err(); cerr != nil {
				return artifactWritten{}, cerr
			}
			n, rerr := pr.Read(buf)
			if n > 0 {
				sum.Write(buf[:n])
				size += int64(n)
				frame := make([]byte, n)
				copy(frame, buf[:n])
				if serr := dest.stream(frame); serr != nil {
					return artifactWritten{}, serr
				}
			}
			if rerr == io.EOF {
				return artifactWritten{size: size, decryptedSize: decrypted, digest: hex.EncodeToString(sum.Sum(nil))}, nil
			}
			if rerr != nil {
				return artifactWritten{}, rerr
			}
		}
	case dest.store != nil:
		n, digest, werr := storeWrite(dest.store.GetRoot(), dest.store.GetObjectKey(), pr, false)
		if werr != nil {
			return artifactWritten{}, werr
		}
		return artifactWritten{size: n, decryptedSize: decrypted, digest: digest}, nil
	default:
		sum := sha256.New()
		n, uerr := s3client.Upload(ctx, s3cfg(dest.s3), dest.key, io.TeeReader(pr, sum))
		if uerr != nil {
			return artifactWritten{}, fmt.Errorf("upload to S3: %w", uerr)
		}
		return artifactWritten{size: n, decryptedSize: decrypted, digest: hex.EncodeToString(sum.Sum(nil))}, nil
	}
}
