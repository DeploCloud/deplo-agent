package server

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

type fakeUploadStream struct {
	grpc.ServerStream
	frames []*pb.DeployUpload
	end    error
}

func (f *fakeUploadStream) Recv() (*pb.DeployUpload, error) {
	if len(f.frames) == 0 {
		return nil, f.end
	}
	next := f.frames[0]
	f.frames = f.frames[1:]
	return next, nil
}

func (f *fakeUploadStream) Send(*pb.DeployEvent) error { return nil }
func (f *fakeUploadStream) Context() context.Context   { return context.Background() }

func chunk(b string) *pb.DeployUpload {
	return &pb.DeployUpload{Frame: &pb.DeployUpload_ContextChunk{ContextChunk: []byte(b)}}
}

func TestSpoolContextWritesTheChunksToDisk(t *testing.T) {
	s := &Service{buildTmpDir: t.TempDir()}
	path, err := s.spoolContext(&fakeUploadStream{frames: []*pb.DeployUpload{chunk("ab"), chunk(""), chunk("cd")}, end: io.EOF})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != "abcd" {
		t.Fatalf("spooled %q", got)
	}
}

func TestSpoolContextLeavesNothingWhenTheClientGoesAway(t *testing.T) {
	dir := t.TempDir()
	s := &Service{buildTmpDir: dir}
	if _, err := s.spoolContext(&fakeUploadStream{frames: []*pb.DeployUpload{chunk("ab")}, end: errors.New("gone")}); err == nil {
		t.Fatal("expected the stream error")
	}
	if left, _ := filepath.Glob(filepath.Join(dir, "*")); len(left) != 0 {
		t.Fatalf("spool left behind: %v", left)
	}
}

func TestDeployStreamNeedsTheRequestFirst(t *testing.T) {
	s := &Service{buildTmpDir: t.TempDir(), deploys: map[string]*inflight{}}
	if err := s.DeployStream(&fakeUploadStream{frames: []*pb.DeployUpload{chunk("ab")}, end: io.EOF}); err == nil {
		t.Fatal("a chunk before the request was accepted")
	}
}

func TestCapabilities_advertisesContextStream(t *testing.T) {
	if !containsString(Capabilities, "deploy.context_stream") {
		t.Error("Capabilities must advertise \"deploy.context_stream\"")
	}
}
