package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"filippo.io/age"
	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/s3client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func storeObjectLabel(root, key string) string { return path.Join(root, key) }
