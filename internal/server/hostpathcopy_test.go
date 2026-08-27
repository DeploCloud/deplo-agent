package server

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// fakeHostPathImportStream replays queued inbound chunks and captures the result.
type fakeHostPathImportStream struct {
	in     []*pb.HostPathChunk
	i      int
	result *pb.StackResult
	ctx    context.Context
}

func (f *fakeHostPathImportStream) Recv() (*pb.HostPathChunk, error) {
	if f.i >= len(f.in) {
		return nil, io.EOF
	}
	c := f.in[f.i]
	f.i++
	return c, nil
}
func (f *fakeHostPathImportStream) SendAndClose(r *pb.StackResult) error {
	f.result = r
	return nil
}
func (f *fakeHostPathImportStream) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}
func (f *fakeHostPathImportStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeHostPathImportStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeHostPathImportStream) SetTrailer(metadata.MD)       {}
func (f *fakeHostPathImportStream) SendMsg(any) error            { return nil }
func (f *fakeHostPathImportStream) RecvMsg(any) error            { return nil }

// TestValidateHostPath_refusesTheSystem pins the guardrail: the roots a migration never
// legitimately names are refused, while a real service directory UNDER one of them is
// allowed - the other platform keeps its data at /etc/<platform>/..., so a blanket
// subtree ban would refuse the actual use case.
func TestValidateHostPath_refusesTheSystem(t *testing.T) {
	for _, bad := range []string{
		"", "relative/path", "/", "/etc", "/root", "/var/lib/docker",
		"/var/lib/docker/volumes/someone-elses", "/proc/1", "/sys/kernel",
		"/data/../etc",
	} {
		if _, err := validateHostPath(bad); err == nil {
			t.Errorf("validateHostPath(%q) should have been refused", bad)
		}
	}
	for _, good := range []string{
		"/etc/dokploy/applications/app/files", "/data/myapp", "/srv/app/uploads",
	} {
		got, err := validateHostPath(good)
		if err != nil {
			t.Errorf("validateHostPath(%q) should be allowed: %v", good, err)
		}
		if got != filepath.Clean(good) {
			t.Errorf("validateHostPath(%q) = %q", good, got)
		}
	}
}

// TestExportHostPath_missingIsNotFound is the host-path twin of the volume guard:
// `docker run -v /missing:/v` CREATES the directory and exports nothing, so the
// existence check has to happen before anything is mounted.
func TestExportHostPath_missingIsNotFound(t *testing.T) {
	svc := New(t.TempDir(), t.TempDir(), "/", "")
	missing := filepath.Join(t.TempDir(), "not-here")

	st := &fakeExportStream{}
	err := svc.ExportHostPath(&pb.ExportHostPathRequest{Path: missing}, st)
	if err == nil {
		t.Fatal("exporting a directory that is not here must fail")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("want NotFound, got %v: %v", status.Code(err), err)
	}
	if _, statErr := os.Stat(missing); statErr == nil {
		t.Error("the export created the directory it was supposed to refuse")
	}
	if len(st.chunks) != 0 {
		t.Errorf("a refused export must send no chunks, got %d", len(st.chunks))
	}
}

// TestImportHostPath_headerOnlyDoesNotWipe: no data, no wipe - the whole reason
// this family of RPCs was rewritten.
func TestImportHostPath_headerOnlyDoesNotWipe(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "payload")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(dir, "precious.txt")
	if err := os.WriteFile(keep, []byte("precious"), 0o644); err != nil {
		t.Fatal(err)
	}

	svc := New(t.TempDir(), t.TempDir(), "/", "")
	st := &fakeHostPathImportStream{in: []*pb.HostPathChunk{
		{Frame: &pb.HostPathChunk_Header_{Header: &pb.HostPathChunk_Header{Path: dir, WipeFirst: true}}},
	}}
	if err := svc.ImportHostPath(st); err != nil {
		t.Fatalf("ImportHostPath transport error: %v", err)
	}
	if st.result == nil || st.result.Ok {
		t.Fatalf("an import that received no data must not report success: %+v", st.result)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Error("the target was emptied for a copy that never sent a byte")
	}
}

// TestImportHostPath_createsTheWholePath is the case this RPC exists for: the
// destination has never run the platform being migrated away from, so nothing of that
// platform's directory tree is there.
func TestImportHostPath_createsTheWholePath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	src := filepath.Join(t.TempDir(), "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("carried"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Two levels of parent that do not exist on this host.
	dst := filepath.Join(t.TempDir(), "never", "seen", "before")

	ex := &fakeExportStream{ctx: ctx}
	if err := svc.ExportHostPath(&pb.ExportHostPathRequest{Path: src}, ex); err != nil {
		t.Fatalf("ExportHostPath: %v", err)
	}
	in := []*pb.HostPathChunk{
		{Frame: &pb.HostPathChunk_Header_{Header: &pb.HostPathChunk_Header{Path: dst, WipeFirst: true}}},
	}
	for _, c := range ex.chunks {
		in = append(in, &pb.HostPathChunk{Frame: &pb.HostPathChunk_Data{Data: c.GetData()}})
	}
	im := &fakeHostPathImportStream{in: in, ctx: ctx}
	if err := svc.ImportHostPath(im); err != nil {
		t.Fatalf("ImportHostPath transport error: %v", err)
	}
	if im.result == nil || !im.result.Ok {
		t.Fatalf("a destination whose parents do not exist yet must still be written: %+v", im.result)
	}
	got, err := os.ReadFile(filepath.Join(dst, "f.txt"))
	if err != nil || string(got) != "carried" {
		t.Errorf("the file did not arrive: %q / %v", got, err)
	}
}

// TestE2E_HostPathCopyRoundTrip moves a real directory through both halves.
func TestE2E_HostPathCopyRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	src := filepath.Join(t.TempDir(), "src")
	dst := filepath.Join(t.TempDir(), "dst")
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "file.txt"), []byte("sentinel-data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "n.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "leftover.txt"), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	ex := &fakeExportStream{ctx: ctx}
	if err := svc.ExportHostPath(&pb.ExportHostPathRequest{Path: src}, ex); err != nil {
		t.Fatalf("ExportHostPath: %v", err)
	}
	if len(ex.chunks) == 0 {
		t.Fatal("ExportHostPath produced no chunks")
	}

	in := []*pb.HostPathChunk{
		{Frame: &pb.HostPathChunk_Header_{Header: &pb.HostPathChunk_Header{Path: dst, WipeFirst: true}}},
	}
	for _, c := range ex.chunks {
		in = append(in, &pb.HostPathChunk{Frame: &pb.HostPathChunk_Data{Data: c.GetData()}})
	}
	im := &fakeHostPathImportStream{in: in, ctx: ctx}
	if err := svc.ImportHostPath(im); err != nil {
		t.Fatalf("ImportHostPath transport error: %v", err)
	}
	if im.result == nil || !im.result.Ok {
		t.Fatalf("ImportHostPath failed: %+v", im.result)
	}

	got, err := os.ReadFile(filepath.Join(dst, "file.txt"))
	if err != nil || !strings.Contains(string(got), "sentinel-data") {
		t.Errorf("copy did not bring file.txt across: %q / %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(dst, "sub", "n.txt")); err != nil {
		t.Errorf("copy did not bring the nested file across: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "leftover.txt")); err == nil {
		t.Error("wipe-first should have removed leftover.txt")
	}
	if im.result.BytesWritten == 0 || im.result.Sha256 == "" {
		t.Errorf("the copy must report what it consumed: %+v", im.result)
	}
}

// TestValidateHostPath_judgesTheResolvedPath pins the symlink half of the guardrail:
// the mount and the wipe both follow links, so the deny-list has to judge where the
// path lands, not how it is spelled.
func TestValidateHostPath_judgesTheResolvedPath(t *testing.T) {
	tmp := t.TempDir()
	real := filepath.Join(tmp, "real")
	if err := os.MkdirAll(real, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, filepath.Join(tmp, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/", filepath.Join(tmp, "poison")); err != nil {
		t.Fatal(err)
	}

	got, err := validateHostPath(filepath.Join(tmp, "link"))
	if err != nil {
		t.Fatalf("a symlink to a legitimate directory must be copyable: %v", err)
	}
	if want, _ := filepath.EvalSymlinks(real); got != want {
		t.Errorf("validateHostPath returned %q, want the resolved %q", got, want)
	}

	if _, err := validateHostPath(filepath.Join(tmp, "poison")); err == nil {
		t.Error("a symlink to / passed the deny-list - that path wipes the host")
	} else if status.Code(err) != codes.PermissionDenied {
		t.Errorf("want PermissionDenied, got %v", err)
	}

	// A target that does not exist yet still resolves the parents it lands under.
	if _, err := validateHostPath(filepath.Join(tmp, "poison", "proc", "new")); err == nil {
		t.Error("a not-yet-created path under a poisoned parent passed the deny-list")
	}
}
