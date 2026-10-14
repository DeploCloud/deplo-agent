package server

import (
	"context"
	"io"
	"strings"
	"sync"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/s3client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	backupStepTimeout      = 30 * time.Minute
	volumeHelperImage      = "busybox:1.36"
	maxSnapshotBytes       = 16 << 20
	maxProjectRestoreBytes = 64 << 30
)

type bkEmitter struct {
	mu   sync.Mutex
	send func(*pb.BackupEvent) error
}

func (e *bkEmitter) emit(ev *pb.BackupEvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.send(ev)
}

func (e *bkEmitter) log(level, text string) {
	_ = e.emit(&pb.BackupEvent{Event: &pb.BackupEvent_Log{Log: &pb.LogLine{Level: level, Text: text}}})
}

func (e *bkEmitter) result(ok bool, errMsg, objectKey string, size int64) {
	e.emitResult(ok, errMsg, objectKey, artifactWritten{size: size})
}

func (e *bkEmitter) emitResult(ok bool, errMsg, objectKey string, w artifactWritten) {
	_ = e.emit(&pb.BackupEvent{Event: &pb.BackupEvent_Result{
		Result: &pb.BackupResult{
			Ok:                 ok,
			Error:              errMsg,
			ObjectKey:          objectKey,
			SizeBytes:          w.size,
			Sha256:             w.digest,
			DecryptedSizeBytes: w.decryptedSize,
		},
	}})
}

func (e *bkEmitter) data(b []byte) error {
	return e.emit(&pb.BackupEvent{Event: &pb.BackupEvent_Data{Data: b}})
}

type rsEmitter struct {
	send func(*pb.RestoreEvent) error
}

func (e *rsEmitter) log(level, text string) {
	_ = e.send(&pb.RestoreEvent{Event: &pb.RestoreEvent_Log{Log: &pb.LogLine{Level: level, Text: text}}})
}

func (e *rsEmitter) result(ok bool, errMsg string) {
	_ = e.send(&pb.RestoreEvent{Event: &pb.RestoreEvent_Result{
		Result: &pb.RestoreResult{Ok: ok, Error: errMsg},
	}})
}

func s3cfg(t *pb.S3Target) s3client.Config {
	return s3client.Config{
		Endpoint:             t.GetEndpoint(),
		Region:               t.GetRegion(),
		Bucket:               t.GetBucket(),
		AccessKey:            t.GetAccessKey(),
		SecretKey:            t.GetSecretKey(),
		PathStyle:            t.GetPathStyle(),
		AllowPrivateEndpoint: t.GetAllowPrivateEndpoint(),
		ExtraArgs:            t.GetExtraArgs(),
	}
}

// Backup dumps a database or project to S3, streaming progress.
func (s *Service) Backup(req *pb.BackupRequest, stream pb.Agent_BackupServer) error {
	e := &bkEmitter{send: stream.Send}
	ctx := stream.Context()

	dest, err := destinationFromBackup(s, req, e.data)
	if err != nil {
		e.result(false, statusMessage(err), "", 0)
		return nil
	}
	switch req.GetKind() {
	case pb.BackupKind_BACKUP_KIND_DATABASE:
		s.backupDatabase(ctx, req.GetDatabase(), dest, e)
	case pb.BackupKind_BACKUP_KIND_PROJECT:
		s.backupProject(ctx, req.GetProject(), dest, e)
	default:
		e.result(false, "unknown backup kind", "", 0)
	}
	return nil
}

// Restore restores a database or project from an artifact this host can reach (an S3 object or a local store), in place.
func (s *Service) Restore(req *pb.RestoreRequest, stream pb.Agent_RestoreServer) error {
	e := &rsEmitter{send: stream.Send}
	ctx := stream.Context()

	src, err := sourceFromRestore(s, req)
	if err != nil {
		e.result(false, statusMessage(err))
		return nil
	}
	s.restoreArtifact(ctx, req.GetKind(), req.GetDatabase(), req.GetProject(), src, e)
	return nil
}

func combineErr(err error, stderr string) string {
	if err != nil {
		return err.Error()
	}
	if s := strings.TrimSpace(stderr); s != "" {
		return s
	}
	return "command failed"
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// S3Check verifies the bucket is reachable + writable for the "Test connection" button.
func (s *Service) S3Check(ctx context.Context, req *pb.S3CheckRequest) (*pb.S3CheckResponse, error) {
	if req.GetStore() != nil {
		return s.storeCheck(req.GetStore()), nil
	}
	if req.GetS3() == nil {
		return nil, status.Error(codes.InvalidArgument, "check request missing destination")
	}
	if err := s3client.Check(ctx, s3cfg(req.GetS3())); err != nil {
		return &pb.S3CheckResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.S3CheckResponse{Ok: true}, nil
}

// S3Delete deletes a single artifact (or, with prefix=true, a whole target folder) - backs retention + delete-with-artifacts.
func (s *Service) S3Delete(ctx context.Context, req *pb.S3DeleteRequest) (*pb.S3DeleteResponse, error) {
	if t := req.GetStore(); t != nil {
		root, err := s.resolveStoreRoot(t.GetRoot(), false)
		if err != nil {
			return &pb.S3DeleteResponse{Ok: false, Error: statusMessage(err)}, nil
		}
		var n int64
		if req.GetPrefix() {
			n, err = storeDeletePrefix(root, t.GetObjectKey())
		} else {
			n, err = storeDeleteOne(root, t.GetObjectKey())
		}
		if err != nil {
			return &pb.S3DeleteResponse{Ok: false, Error: statusMessage(err)}, nil
		}
		return &pb.S3DeleteResponse{Ok: true, Deleted: n}, nil
	}
	if req.GetS3() == nil || req.GetS3().GetObjectKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "delete request missing key/prefix")
	}
	cfg := s3cfg(req.GetS3())
	var (
		n   int64
		err error
	)
	if req.GetPrefix() {
		n, err = s3client.DeletePrefix(ctx, cfg, req.GetS3().GetObjectKey())
	} else {
		n, err = s3client.DeleteOne(ctx, cfg, req.GetS3().GetObjectKey())
	}
	if err != nil {
		return &pb.S3DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.S3DeleteResponse{Ok: true, Deleted: n}, nil
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
	s.restoreArtifact(ctx, h.GetKind(), h.GetDatabase(), h.GetProject(), src, e)
	return nil
}

func statusMessage(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Message()
	}
	return err.Error()
}
