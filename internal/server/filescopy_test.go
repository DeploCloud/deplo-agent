package server

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// fakeExportFilesStream satisfies grpc.ServerStreamingServer[FilesChunk].
type fakeExportFilesStream struct {
	chunks []*pb.FilesChunk
}

func (f *fakeExportFilesStream) Send(c *pb.FilesChunk) error {
	f.chunks = append(f.chunks, c)
	return nil
}
func (f *fakeExportFilesStream) Context() context.Context     { return context.Background() }
func (f *fakeExportFilesStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeExportFilesStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeExportFilesStream) SetTrailer(metadata.MD)       {}
func (f *fakeExportFilesStream) SendMsg(any) error            { return nil }
func (f *fakeExportFilesStream) RecvMsg(any) error            { return nil }

// fakeImportFilesStream satisfies grpc.ClientStreamingServer[FilesChunk, StackResult].
type fakeImportFilesStream struct {
	in     []*pb.FilesChunk
	i      int
	result *pb.StackResult
}

func (f *fakeImportFilesStream) Recv() (*pb.FilesChunk, error) {
	if f.i >= len(f.in) {
		return nil, io.EOF
	}
	c := f.in[f.i]
	f.i++
	return c, nil
}
func (f *fakeImportFilesStream) SendAndClose(r *pb.StackResult) error {
	f.result = r
	return nil
}
func (f *fakeImportFilesStream) Context() context.Context     { return context.Background() }
func (f *fakeImportFilesStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeImportFilesStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeImportFilesStream) SetTrailer(metadata.MD)       {}
func (f *fakeImportFilesStream) SendMsg(any) error            { return nil }
func (f *fakeImportFilesStream) RecvMsg(any) error            { return nil }

// TestImportFiles_headerRequired: first message must be a header.
func TestImportFiles_headerRequired(t *testing.T) {
	svc := New(t.TempDir(), t.TempDir(), "/", "")
	st := &fakeImportFilesStream{in: []*pb.FilesChunk{
		{Frame: &pb.FilesChunk_Data{Data: []byte("nope")}},
	}}
	if err := svc.ImportFiles(st); err != nil {
		t.Fatalf("ImportFiles transport error: %v", err)
	}
	if st.result == nil || st.result.Ok {
		t.Fatalf("expected ok=false result, got %+v", st.result)
	}
	if !strings.Contains(st.result.Error, "header") {
		t.Errorf("error should mention the missing header: %q", st.result.Error)
	}
}

// TestExportFiles_missingDir: a service with no files dir exports a valid empty
// archive (not an error) — a common case (plain single-image project).
func TestExportFiles_missingDir(t *testing.T) {
	svc := New(t.TempDir(), t.TempDir(), "/", "")
	st := &fakeExportFilesStream{}
	if err := svc.ExportFiles(&pb.ExportFilesRequest{Slug: "no-such-service"}, st); err != nil {
		t.Fatalf("ExportFiles of a missing dir should not error: %v", err)
	}
	// An empty gzip+tar still produces a few framing bytes, so importing it into a
	// fresh dir must succeed and leave the dir empty.
	in := []*pb.FilesChunk{
		{Frame: &pb.FilesChunk_Header_{Header: &pb.FilesChunk_Header{Slug: "no-such-service", WipeFirst: true}}},
	}
	for _, c := range st.chunks {
		in = append(in, c)
	}
	im := &fakeImportFilesStream{in: in}
	if err := svc.ImportFiles(im); err != nil {
		t.Fatalf("ImportFiles of an empty archive transport error: %v", err)
	}
	if im.result == nil || !im.result.Ok {
		t.Fatalf("empty-archive import should succeed, got %+v", im.result)
	}
}

// TestFilesCopyRoundTrip drives ExportFiles -> (relay) -> ImportFiles against real
// host directories (no docker needed — the files dir is a plain host dir), proving
// the tree copies across AND that wipe-first overwrites stale destination content.
func TestFilesCopyRoundTrip(t *testing.T) {
	stackDir := t.TempDir()
	svc := New(stackDir, t.TempDir(), "/", "")
	slug := "my-service"

	// Seed the SOURCE files dir with a nested tree.
	srcRoot := svc.filesRoot(slug)
	mustWrite(t, filepath.Join(srcRoot, "config.yml"), "key: value\n")
	mustWrite(t, filepath.Join(srcRoot, "sub", "nested.txt"), "nested-data\n")

	// Export.
	ex := &fakeExportFilesStream{}
	if err := svc.ExportFiles(&pb.ExportFilesRequest{Slug: slug}, ex); err != nil {
		t.Fatalf("ExportFiles: %v", err)
	}
	if len(ex.chunks) == 0 {
		t.Fatal("ExportFiles produced no chunks for a non-empty dir")
	}

	// Simulate a DIFFERENT destination host: a fresh stackDir with a DIFFERENT
	// service instance, pre-seeded with stale junk that wipe-first must remove.
	destStackDir := t.TempDir()
	destSvc := New(destStackDir, t.TempDir(), "/", "")
	destRoot := destSvc.filesRoot(slug)
	mustWrite(t, filepath.Join(destRoot, "config.yml"), "STALE\n")
	mustWrite(t, filepath.Join(destRoot, "leftover.txt"), "junk\n")

	// Relay: header (wipe-first) then every data chunk.
	in := []*pb.FilesChunk{
		{Frame: &pb.FilesChunk_Header_{Header: &pb.FilesChunk_Header{Slug: slug, WipeFirst: true}}},
	}
	for _, c := range ex.chunks {
		in = append(in, c)
	}
	im := &fakeImportFilesStream{in: in}
	if err := destSvc.ImportFiles(im); err != nil {
		t.Fatalf("ImportFiles transport error: %v", err)
	}
	if im.result == nil || !im.result.Ok {
		t.Fatalf("ImportFiles failed: %+v", im.result)
	}

	// The destination now mirrors the source tree; the stale junk is gone.
	assertFile(t, filepath.Join(destRoot, "config.yml"), "key: value\n")
	assertFile(t, filepath.Join(destRoot, "sub", "nested.txt"), "nested-data\n")
	if _, err := os.Stat(filepath.Join(destRoot, "leftover.txt")); !os.IsNotExist(err) {
		t.Errorf("wipe-first should have removed leftover.txt (err=%v)", err)
	}
}

func assertFile(t *testing.T, path, want string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(b) != want {
		t.Errorf("%s = %q, want %q", path, string(b), want)
	}
}
