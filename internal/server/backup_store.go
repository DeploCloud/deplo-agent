package server

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"filippo.io/age"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/s3client"
)

const (
	storeSentinel                      = ".deplo-backups"
	storePartialSuffix                 = ".partial"
	storePartialStaleAfter             = time.Hour
	storeChunkBytes                    = 1 << 20
	storeDirPerm           os.FileMode = 0o700
	storeFilePerm          os.FileMode = 0o600
)

func (s *Service) managedStoreRoot() string {
	return filepath.Join(s.dataBase, "backups")
}

func (s *Service) resolveStoreRoot(root string, create bool) (string, error) {
	managed := s.managedStoreRoot()
	if strings.TrimSpace(root) == "" {
		if err := os.MkdirAll(managed, storeDirPerm); err != nil {
			return "", status.Errorf(codes.Internal, "create backup store %q: %v", managed, err)
		}
		if err := writeStoreSentinel(managed); err != nil {
			return "", err
		}
		return managed, nil
	}

	root = filepath.Clean(root)
	if !filepath.IsAbs(root) {
		return "", status.Errorf(codes.InvalidArgument, "backup store path %q must be absolute", root)
	}
	if root == managed {
		return s.resolveStoreRoot("", create)
	}
	st, err := os.Stat(root)
	if err != nil {
		return "", status.Errorf(codes.FailedPrecondition, "backup store path %q does not exist on this server", root)
	}
	if !st.IsDir() {
		return "", status.Errorf(codes.InvalidArgument, "backup store path %q is not a directory", root)
	}
	if _, err := os.Stat(filepath.Join(root, storeSentinel)); err != nil {
		if !create {
			return "", status.Errorf(codes.FailedPrecondition,
				"backup store path %q is not initialized for Deplo; test the destination to set it up", root)
		}
		empty, eerr := dirIsEmpty(root)
		if eerr != nil {
			return "", status.Errorf(codes.Internal, "read backup store path %q: %v", root, eerr)
		}
		if !empty {
			return "", status.Errorf(codes.FailedPrecondition,
				"backup store path %q is not empty and holds no Deplo backups; point it at an empty directory", root)
		}
		if err := writeStoreSentinel(root); err != nil {
			return "", err
		}
	}
	return root, nil
}

func writeStoreSentinel(root string) error {
	p := filepath.Join(root, storeSentinel)
	if _, err := os.Stat(p); err == nil {
		return nil
	}
	body := "This directory holds Deplo backup artifacts. Deplo will create, read and\n" +
		"delete files under it. Do not point a Deplo backup destination at a\n" +
		"directory whose contents you want to keep.\n"
	if err := os.WriteFile(p, []byte(body), storeFilePerm); err != nil {
		return status.Errorf(codes.Internal, "mark backup store %q: %v", root, err)
	}
	return nil
}

func dirIsEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()
	names, err := f.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return len(names) == 0, nil
}

func storeKeyPath(root, key string) (string, error) {
	if strings.TrimSpace(key) == "" {
		return "", status.Error(codes.InvalidArgument, "a backup object key is required")
	}
	abs, _, err := resolveParentInside(root, key)
	if err != nil {
		return "", err
	}
	return abs, nil
}

func storeWrite(root, key string, r io.Reader, overwrite bool) (int64, string, error) {
	dst, err := storeKeyPath(root, key)
	if err != nil {
		return 0, "", err
	}
	if !overwrite {
		if _, serr := os.Stat(dst); serr == nil {
			return 0, "", status.Errorf(codes.AlreadyExists, "a backup artifact already exists at %q", key)
		}
	}
	if err := os.MkdirAll(filepath.Dir(dst), storeDirPerm); err != nil {
		return 0, "", fmt.Errorf("create backup directory: %w", err)
	}
	tmp := dst + storePartialSuffix
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|syscall.O_NOFOLLOW, storeFilePerm)
	if err != nil {
		return 0, "", fmt.Errorf("open backup artifact: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	sum := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, sum), r)
	if err != nil {
		return 0, "", fmt.Errorf("write backup artifact: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, "", fmt.Errorf("flush backup artifact: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, "", fmt.Errorf("close backup artifact: %w", err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return 0, "", fmt.Errorf("commit backup artifact: %w", err)
	}
	committed = true
	return n, hex.EncodeToString(sum.Sum(nil)), nil
}

func storeOpen(root, key string) (io.ReadCloser, error) {
	src, err := storeKeyPath(root, key)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(src, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "no backup artifact at %q on this server", key)
		}
		return nil, fmt.Errorf("open backup artifact: %w", err)
	}
	return f, nil
}

func storeDeleteOne(root, key string) (int64, error) {
	p, err := storeKeyPath(root, key)
	if err != nil {
		return 0, err
	}
	if err := os.Remove(p); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("delete backup artifact: %w", err)
	}
	pruneEmptyDirs(root, filepath.Dir(p))
	return 1, nil
}

func storeDeletePrefix(root, prefix string) (int64, error) {
	norm, err := normalizeRel(prefix)
	if err != nil {
		return 0, err
	}
	if norm == "" {
		return 0, status.Error(codes.InvalidArgument, "refusing to delete the whole backup store")
	}
	dir, err := resolveInside(root, norm)
	if err != nil {
		return 0, err
	}
	if dir == canonicalRoot(root) {
		return 0, status.Error(codes.InvalidArgument, "refusing to delete the whole backup store")
	}
	st, serr := os.Stat(dir)
	if serr != nil {
		if os.IsNotExist(serr) {
			return 0, nil
		}
		return 0, fmt.Errorf("read backup prefix: %w", serr)
	}
	if !st.IsDir() {
		if err := os.Remove(dir); err != nil {
			return 0, fmt.Errorf("delete backup artifact: %w", err)
		}
		pruneEmptyDirs(root, filepath.Dir(dir))
		return 1, nil
	}
	var n int64
	err = filepath.WalkDir(dir, func(_ string, d os.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("count backup artifacts: %w", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		return 0, fmt.Errorf("delete backup artifacts: %w", err)
	}
	pruneEmptyDirs(root, filepath.Dir(dir))
	return n, nil
}

func pruneEmptyDirs(root, dir string) {
	base := canonicalRoot(root)
	for dir != base && strings.HasPrefix(dir, base+string(os.PathSeparator)) {
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

func sweepPartials(root string) int {
	n := 0
	cutoff := time.Now().Add(-storePartialStaleAfter)
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(p, storePartialSuffix) {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil || info.ModTime().After(cutoff) {
			return nil
		}
		if os.Remove(p) == nil {
			n++
		}
		return nil
	})
	return n
}

func storeFreeBytes(root string) (free, total int64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		return 0, 0
	}
	bsize := int64(st.Bsize)
	return int64(st.Bavail) * bsize, int64(st.Blocks) * bsize
}

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

func verifyStoreDigest(root, key, expected string) error {
	if expected == "" {
		return nil
	}
	f, err := storeOpen(root, key)
	if err != nil {
		return err
	}
	defer f.Close()
	sum := sha256.New()
	if _, cerr := io.Copy(sum, f); cerr != nil {
		return fmt.Errorf("read backup artifact to verify it: %w", cerr)
	}
	return digestMismatch(hex.EncodeToString(sum.Sum(nil)), expected)
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

func (a *artifactSource) open(ctx context.Context) (io.Reader, func(), error) {
	var (
		raw    io.Reader
		closes []func()
	)
	switch {
	case a.stream != nil:
		raw = a.stream
	case a.store != nil:
		f, err := storeOpen(a.store.GetRoot(), a.store.GetObjectKey())
		if err != nil {
			return nil, nil, err
		}
		raw = f
		closes = append(closes, func() { _ = f.Close() })
	default:
		obj, err := s3client.Download(ctx, s3cfg(a.s3), a.s3.GetObjectKey())
		if err != nil {
			return nil, nil, fmt.Errorf("open S3 object: %w", err)
		}
		raw = obj
		closes = append(closes, func() { _ = obj.Close() })
	}
	if a.expectedSha256 != "" {
		a.verifier = &verifyingReader{r: raw, sum: sha256.New(), expected: a.expectedSha256}
		raw = a.verifier
	}
	rc, err := openArtifactReader(raw, a.identity)
	if err != nil {
		for _, c := range closes {
			c()
		}
		return nil, nil, err
	}
	closes = append([]func(){func() { _ = rc.Close() }}, closes...)
	return rc, func() {
		for _, c := range closes {
			c()
		}
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

func (s *Service) readSourceFor(
	ctx context.Context,
	req *pb.ReadStoreFileRequest,
) (io.Reader, func(), *verifyingReader, error) {
	switch {
	case req.GetStore() != nil:
		t := req.GetStore()
		root, err := s.resolveStoreRoot(t.GetRoot(), false)
		if err != nil {
			return nil, nil, nil, err
		}
		if verr := verifyStoreDigest(root, t.GetObjectKey(), req.GetExpectedSha256()); verr != nil {
			return nil, nil, nil, verr
		}
		f, err := storeOpen(root, t.GetObjectKey())
		if err != nil {
			return nil, nil, nil, err
		}
		return f, func() { _ = f.Close() }, nil, nil

	case req.GetS3() != nil:
		t := req.GetS3()
		obj, err := s3client.Download(ctx, s3cfg(t), t.GetObjectKey())
		if err != nil {
			return nil, nil, nil, fmt.Errorf("open S3 object: %w", err)
		}
		if req.GetExpectedSha256() == "" {
			return obj, func() { _ = obj.Close() }, nil, nil
		}
		v := &verifyingReader{r: obj, sum: sha256.New(), expected: req.GetExpectedSha256()}
		return v, func() { _ = obj.Close() }, v, nil

	default:
		return nil, nil, nil, status.Error(
			codes.InvalidArgument, "read store request names no artifact to read")
	}
}

// ReadStoreFile streams an artifact out, from this host's store or from a bucket it can dial.
func (s *Service) ReadStoreFile(req *pb.ReadStoreFileRequest, stream pb.Agent_ReadStoreFileServer) error {
	raw, closeSrc, verifier, err := s.readSourceFor(stream.Context(), req)
	if err != nil {
		return err
	}
	defer closeSrc()

	src := raw
	if id := req.GetAgeIdentity(); id != "" {
		identity, perr := age.ParseX25519Identity(id)
		if perr != nil {
			return status.Errorf(codes.InvalidArgument, "invalid backup decryption key: %v", perr)
		}
		dec, derr := age.Decrypt(raw, identity)
		if derr != nil {
			return fmt.Errorf("decrypt backup (is this the right recovery key?): %w", derr)
		}
		src = dec
	}

	buf := make([]byte, storeChunkBytes)
	for {
		if err := stream.Context().Err(); err != nil {
			return err
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			frame := make([]byte, n)
			copy(frame, buf[:n])
			if serr := stream.Send(&pb.StoreChunk{
				Frame: &pb.StoreChunk_Data{Data: frame},
			}); serr != nil {
				return serr
			}
		}
		if rerr == io.EOF {
			if verifier != nil {
				return verifier.finish()
			}
			return nil
		}
		if rerr != nil {
			return fmt.Errorf("read backup artifact: %w", rerr)
		}
	}
}

// WriteStoreFile receives an artifact into this host's store.
func (s *Service) WriteStoreFile(stream pb.Agent_WriteStoreFileServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "write store: no header received: %v", err)
	}
	h := first.GetHeader()
	if h == nil || h.GetStore() == nil {
		return status.Error(codes.InvalidArgument, "write store: first message must carry the header")
	}
	root, err := s.resolveStoreRoot(h.GetStore().GetRoot(), false)
	if err != nil {
		return err
	}

	pr, pw := io.Pipe()
	go func() {
		var perr error
		for {
			msg, rerr := stream.Recv()
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				perr = rerr
				break
			}
			if d := msg.GetData(); len(d) > 0 {
				if _, werr := pw.Write(d); werr != nil {
					perr = werr
					break
				}
			}
		}
		pw.CloseWithError(perr)
	}()

	n, sum, werr := storeWrite(root, h.GetStore().GetObjectKey(), pr, h.GetOverwrite())
	if werr != nil {
		_ = pr.CloseWithError(werr)
		return stream.SendAndClose(&pb.StoreResult{Ok: false, Error: werr.Error()})
	}
	return stream.SendAndClose(&pb.StoreResult{Ok: true, BytesWritten: n, Sha256: sum})
}

// RestoreFrom is the cross-host half of Restore: the artifact lives on another server's disk, so the control plane streams it in here rather than asking this host to fetch it (agents cannot dial each other).
func (s *Service) RestoreFrom(stream pb.Agent_RestoreFromServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "restore from: no header received: %v", err)
	}
	h := first.GetHeader()
	if h == nil {
		return status.Error(codes.InvalidArgument, "restore from: first message must carry the header")
	}
	if h.GetAgeIdentity() == "" {
		return status.Error(codes.InvalidArgument, "restoring from a server store needs its recovery key")
	}

	e := &rsEmitter{send: stream.Send}
	ctx := stream.Context()

	pr, pw := io.Pipe()
	go func() {
		var perr error
		for {
			msg, rerr := stream.Recv()
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				perr = rerr
				break
			}
			if d := msg.GetData(); len(d) > 0 {
				if _, werr := pw.Write(d); werr != nil {
					perr = werr
					break
				}
			}
		}
		pw.CloseWithError(perr)
	}()
	defer pr.Close()

	src := &artifactSource{
		stream:          pr,
		identity:        h.GetAgeIdentity(),
		expectedSha256:  h.GetExpectedSha256(),
		configUntrusted: h.GetUntrustedConfig(),
		label:           "the control plane",
	}
	switch h.GetKind() {
	case pb.BackupKind_BACKUP_KIND_DATABASE:
		s.restoreDatabase(ctx, h.GetDatabase(), src, e)
	case pb.BackupKind_BACKUP_KIND_PROJECT:
		s.restoreProject(ctx, h.GetProject(), src, e)
	default:
		e.result(false, "unknown restore kind")
	}
	return nil
}

func (s *Service) storeCheck(t *pb.StoreTarget) *pb.S3CheckResponse {
	root, err := s.resolveStoreRoot(t.GetRoot(), true)
	if err != nil {
		return &pb.S3CheckResponse{Ok: false, Error: statusMessage(err)}
	}
	probe := filepath.Join(root, ".deplo-store-check")
	if werr := os.WriteFile(probe, []byte("ok"), storeFilePerm); werr != nil {
		return &pb.S3CheckResponse{
			Ok:    false,
			Error: fmt.Sprintf("cannot write to %s: %v", root, werr),
			Root:  root,
		}
	}
	_ = os.Remove(probe)
	sweepPartials(root)
	free, total := storeFreeBytes(root)
	return &pb.S3CheckResponse{Ok: true, FreeBytes: free, TotalBytes: total, Root: root}
}

func statusMessage(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Message()
	}
	return err.Error()
}

func storeObjectLabel(root, key string) string { return path.Join(root, key) }
