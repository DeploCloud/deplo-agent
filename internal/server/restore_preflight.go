package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

const spoolCheckEvery = 64 << 20

// spoolReserveBytes is the free space a restore's local copy leaves for the apps on this disk.
var spoolReserveBytes int64 = 1 << 30

var errSpoolNoRoom = status.Error(codes.ResourceExhausted,
	"not enough free disk space on this server to check the backup before restoring it - free some space and try again")

// restoreArtifact proves the artifact is the one Deplo wrote BEFORE anything is wiped or flushed.
func (s *Service) restoreArtifact(ctx context.Context, kind pb.BackupKind, db *pb.DatabaseDescriptor, p *pb.ProjectDescriptor, src *artifactSource, e *rsEmitter) {
	if kind != pb.BackupKind_BACKUP_KIND_DATABASE && kind != pb.BackupKind_BACKUP_KIND_PROJECT {
		e.result(false, "unknown restore kind")
		return
	}
	if src.expectedSha256 != "" && src.store == nil {
		e.log("info", "Checking the backup before restoring it")
	}
	drop, err := src.spoolToVerify(ctx, s.managedStoreRoot())
	if err != nil {
		e.result(false, statusMessage(err))
		return
	}
	defer drop()
	if kind == pb.BackupKind_BACKUP_KIND_DATABASE {
		s.restoreDatabase(ctx, db, src, e)
		return
	}
	s.restoreProject(ctx, p, src, e)
}

// spoolToVerify copies an S3 or relayed artifact to local disk and checks its sha256 there; the
// restore then reads that copy. A store artifact was already hashed in place.
func (a *artifactSource) spoolToVerify(ctx context.Context, dir string) (func(), error) {
	if a.expectedSha256 == "" || a.store != nil {
		return func() {}, nil
	}
	if err := os.MkdirAll(dir, storeDirPerm); err != nil {
		return nil, fmt.Errorf("create %s: %w", dir, err)
	}
	raw, closeRaw, err := a.openRaw(ctx)
	if err != nil {
		return nil, err
	}
	defer closeRaw()
	f, err := os.CreateTemp(dir, ".deplo-restore-*"+storePartialSuffix)
	if err != nil {
		return nil, err
	}
	drop := func() { _ = os.Remove(f.Name()) }
	sum := sha256.New()
	_, err = io.Copy(io.MultiWriter(&roomGuard{w: f, dir: dir}, sum), raw)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = digestMismatch(hex.EncodeToString(sum.Sum(nil)), a.expectedSha256)
	}
	if err != nil {
		drop()
		return nil, err
	}
	a.spooled, a.expectedSha256, a.integrityProven = f.Name(), "", true
	return drop, nil
}

type roomGuard struct {
	w       io.Writer
	dir     string
	n, next int64
}

func (g *roomGuard) Write(p []byte) (int, error) {
	if g.n >= g.next {
		if free, total := storeFreeBytes(g.dir); total > 0 && free < spoolReserveBytes {
			return 0, errSpoolNoRoom
		}
		g.next = g.n + spoolCheckEvery
	}
	n, err := g.w.Write(p)
	g.n += int64(n)
	if errors.Is(err, syscall.ENOSPC) {
		err = errSpoolNoRoom
	}
	return n, err
}
