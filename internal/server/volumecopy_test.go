package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// fakeExportStream satisfies grpc.ServerStreamingServer[VolumeChunk] for
// ExportVolume: it just collects every chunk sent.
type fakeExportStream struct {
	chunks []*pb.VolumeChunk
	ctx    context.Context
}

func (f *fakeExportStream) Send(c *pb.VolumeChunk) error {
	f.chunks = append(f.chunks, c)
	return nil
}
func (f *fakeExportStream) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}
func (f *fakeExportStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeExportStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeExportStream) SetTrailer(metadata.MD)       {}
func (f *fakeExportStream) SendMsg(any) error            { return nil }
func (f *fakeExportStream) RecvMsg(any) error            { return nil }

// fakeImportStream satisfies grpc.ClientStreamingServer[VolumeChunk, StackResult]
// for ImportVolume: it replays a queued list of inbound chunks (header first) and
// captures the terminal result from SendAndClose.
type fakeImportStream struct {
	in     []*pb.VolumeChunk
	i      int
	result *pb.StackResult
	ctx    context.Context
}

func (f *fakeImportStream) Recv() (*pb.VolumeChunk, error) {
	if f.i >= len(f.in) {
		return nil, io.EOF
	}
	c := f.in[f.i]
	f.i++
	return c, nil
}
func (f *fakeImportStream) SendAndClose(r *pb.StackResult) error {
	f.result = r
	return nil
}
func (f *fakeImportStream) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}
func (f *fakeImportStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeImportStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeImportStream) SetTrailer(metadata.MD)       {}
func (f *fakeImportStream) SendMsg(any) error            { return nil }
func (f *fakeImportStream) RecvMsg(any) error            { return nil }

// TestImportVolume_headerRequired proves the protocol guard: the first message
// must be a header, else the RPC reports a business failure (not a panic). No
// docker needed.
func TestImportVolume_headerRequired(t *testing.T) {
	svc := New(t.TempDir(), t.TempDir(), "/", "")
	// First (only) message is a data frame, not a header.
	st := &fakeImportStream{in: []*pb.VolumeChunk{
		{Frame: &pb.VolumeChunk_Data{Data: []byte("nope")}},
	}}
	if err := svc.ImportVolume(st); err != nil {
		t.Fatalf("ImportVolume returned a transport error: %v", err)
	}
	if st.result == nil || st.result.Ok {
		t.Fatalf("expected ok=false result, got %+v", st.result)
	}
	if !strings.Contains(st.result.Error, "header") {
		t.Errorf("error should mention the missing header: %q", st.result.Error)
	}
}

// TestImportVolume_unsafeName proves a wire-supplied path masquerading as a volume
// name is rejected before any helper container runs.
func TestImportVolume_unsafeName(t *testing.T) {
	svc := New(t.TempDir(), t.TempDir(), "/", "")
	st := &fakeImportStream{in: []*pb.VolumeChunk{
		{Frame: &pb.VolumeChunk_Header_{Header: &pb.VolumeChunk_Header{VolumeName: "/etc"}}},
	}}
	if err := svc.ImportVolume(st); err != nil {
		t.Fatalf("ImportVolume returned a transport error: %v", err)
	}
	if st.result == nil || st.result.Ok {
		t.Fatalf("expected ok=false for an unsafe name, got %+v", st.result)
	}
}

// TestExportVolume_unsafeName proves ExportVolume rejects a path-as-volume-name.
func TestExportVolume_unsafeName(t *testing.T) {
	svc := New(t.TempDir(), t.TempDir(), "/", "")
	st := &fakeExportStream{}
	err := svc.ExportVolume(&pb.ExportVolumeRequest{VolumeName: "../escape"}, st)
	if err == nil {
		t.Fatal("expected an error for an unsafe volume name")
	}
	if len(st.chunks) != 0 {
		t.Errorf("no chunks should be sent for a rejected name, got %d", len(st.chunks))
	}
}

// TestE2E_VolumeCopyRoundTrip drives ExportVolume → (relay the chunks) →
// ImportVolume against REAL docker volumes, proving the cross-host copy machinery
// moves the bytes and that the import OVERWRITES the destination (wipe-first). It
// isolates the volume tar/untar the way TestE2E_VolumeArchiveRoundTrip does for
// backup — no real stack, just the two new RPCs relayed in-process.
func TestE2E_VolumeCopyRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	src := "deplo-e2e-copy-src"
	dst := "deplo-e2e-copy-dst"
	for _, v := range []string{src, dst} {
		_, _ = dockercli.Run(ctx, 10*time.Second, "volume", "rm", "-f", v)
		if res, err := dockercli.Run(ctx, 20*time.Second, "volume", "create", v); err != nil || res.Code != 0 {
			t.Skipf("cannot create volume %q (%v / %s)", v, err, res.Stderr)
		}
		defer dockercli.Run(context.Background(), 15*time.Second, "volume", "rm", "-f", v)
	}

	// Seed the SOURCE with a sentinel tree.
	if res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", src+":/v", volumeHelperImage,
		"sh", "-c", "echo sentinel-data > /v/file.txt && mkdir -p /v/sub && echo nested > /v/sub/n.txt"); err != nil || res.Code != 0 {
		t.Fatalf("seed source: %v / %s", err, res.Stderr)
	}
	// Seed the DESTINATION with junk that a correct copy (wipe-first) must remove.
	if res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", dst+":/v", volumeHelperImage,
		"sh", "-c", "echo STALE > /v/file.txt && echo junk > /v/leftover.txt"); err != nil || res.Code != 0 {
		t.Fatalf("seed dest: %v / %s", err, res.Stderr)
	}

	// 1. Export the source volume; collect the gzipped-tar chunks.
	ex := &fakeExportStream{ctx: ctx}
	if err := svc.ExportVolume(&pb.ExportVolumeRequest{VolumeName: src}, ex); err != nil {
		t.Fatalf("ExportVolume: %v", err)
	}
	if len(ex.chunks) == 0 {
		t.Fatal("ExportVolume produced no chunks")
	}

	// 2. Relay: build the ImportVolume inbound sequence = header, then every data
	//    chunk verbatim. This is exactly what the control plane's relay does.
	in := []*pb.VolumeChunk{
		{Frame: &pb.VolumeChunk_Header_{Header: &pb.VolumeChunk_Header{VolumeName: dst, WipeFirst: true}}},
	}
	in = append(in, ex.chunks...)
	im := &fakeImportStream{in: in, ctx: ctx}
	if err := svc.ImportVolume(im); err != nil {
		t.Fatalf("ImportVolume transport error: %v", err)
	}
	if im.result == nil || !im.result.Ok {
		t.Fatalf("ImportVolume failed: %+v", im.result)
	}

	// 3. The destination now holds the SOURCE's tree, and the stale junk is gone.
	res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", dst+":/v", volumeHelperImage,
		"sh", "-c", "cat /v/file.txt; echo ---; cat /v/sub/n.txt; echo ---; ls /v/leftover.txt 2>/dev/null || echo GONE")
	if err != nil || res.Code != 0 {
		t.Fatalf("inspect dest: %v / %s", err, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "sentinel-data") {
		t.Errorf("copy did not bring file.txt across: %q", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "nested") {
		t.Errorf("copy did not bring sub/n.txt across: %q", res.Stdout)
	}
	if strings.Contains(res.Stdout, "STALE") {
		t.Errorf("copy should have overwritten the stale file.txt: %q", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "GONE") {
		t.Errorf("wipe-first should have removed leftover.txt: %q", res.Stdout)
	}
	t.Log("volume export→import cross-copy OVERWRITE verified")
}

// TestExportVolume_missingVolumeIsNotFound is the guard that had to exist and did
// not: `docker run -v <name>:/v` CREATES a missing named volume, so an export of a
// volume that is not on this host used to exit 0 with a complete EMPTY archive —
// and the caller wiped a real destination for it. The refusal must be NotFound, and
// it must leave no volume behind.
func TestExportVolume_missingVolumeIsNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	missing := "deplo-e2e-copy-absent"
	_, _ = dockercli.Run(ctx, 10*time.Second, "volume", "rm", "-f", missing)

	st := &fakeExportStream{ctx: ctx}
	err := svc.ExportVolume(&pb.ExportVolumeRequest{VolumeName: missing}, st)
	if err == nil {
		t.Fatal("exporting a volume that is not on this host must fail")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("want NotFound so the caller can say which host was asked, got %v: %v", status.Code(err), err)
	}
	if len(st.chunks) != 0 {
		t.Errorf("a refused export must send no chunks, got %d", len(st.chunks))
	}
	// And it must not have CREATED it on the way past.
	if res, err := dockercli.Run(ctx, 10*time.Second, "volume", "inspect", missing); err == nil && res.Code == 0 {
		dockercli.Run(context.Background(), 10*time.Second, "volume", "rm", "-f", missing)
		t.Error("the export created the volume it was supposed to refuse")
	}
}

// TestImportVolume_headerOnlyDoesNotWipe pins the other half: a header asking to
// wipe, followed by no data at all, is exactly what a failed or empty export looks
// like. The destination must survive it untouched.
func TestImportVolume_headerOnlyDoesNotWipe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	dst := "deplo-e2e-copy-keepme"
	_, _ = dockercli.Run(ctx, 10*time.Second, "volume", "rm", "-f", dst)
	if res, err := dockercli.Run(ctx, 20*time.Second, "volume", "create", dst); err != nil || res.Code != 0 {
		t.Skipf("cannot create volume %q (%v / %s)", dst, err, res.Stderr)
	}
	defer dockercli.Run(context.Background(), 15*time.Second, "volume", "rm", "-f", dst)

	if res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", dst+":/v", volumeHelperImage,
		"sh", "-c", "echo precious > /v/data.txt"); err != nil || res.Code != 0 {
		t.Fatalf("seed dest: %v / %s", err, res.Stderr)
	}

	im := &fakeImportStream{ctx: ctx, in: []*pb.VolumeChunk{
		{Frame: &pb.VolumeChunk_Header_{Header: &pb.VolumeChunk_Header{VolumeName: dst, WipeFirst: true}}},
	}}
	if err := svc.ImportVolume(im); err != nil {
		t.Fatalf("ImportVolume transport error: %v", err)
	}
	if im.result == nil || im.result.Ok {
		t.Fatalf("an import that received no data must not report success: %+v", im.result)
	}

	res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", dst+":/v", volumeHelperImage,
		"sh", "-c", "cat /v/data.txt 2>/dev/null || echo DESTROYED")
	if err != nil || res.Code != 0 {
		t.Fatalf("inspect dest: %v / %s", err, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "precious") {
		t.Errorf("the destination was wiped for a copy that never sent a byte: %q", res.Stdout)
	}
}

// TestImportVolume_reportsBytesAndDigest proves the cross-check the control plane
// makes is actually answerable: a real copy comes back with the compressed byte
// count and the sha256 of what arrived.
func TestImportVolume_reportsBytesAndDigest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	src := "deplo-e2e-copy-digest-src"
	dst := "deplo-e2e-copy-digest-dst"
	for _, v := range []string{src, dst} {
		_, _ = dockercli.Run(ctx, 10*time.Second, "volume", "rm", "-f", v)
		if res, err := dockercli.Run(ctx, 20*time.Second, "volume", "create", v); err != nil || res.Code != 0 {
			t.Skipf("cannot create volume %q (%v / %s)", v, err, res.Stderr)
		}
		defer dockercli.Run(context.Background(), 15*time.Second, "volume", "rm", "-f", v)
	}
	if res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", src+":/v", volumeHelperImage,
		"sh", "-c", "head -c 100000 /dev/urandom > /v/blob.bin"); err != nil || res.Code != 0 {
		t.Fatalf("seed source: %v / %s", err, res.Stderr)
	}

	ex := &fakeExportStream{ctx: ctx}
	if err := svc.ExportVolume(&pb.ExportVolumeRequest{VolumeName: src}, ex); err != nil {
		t.Fatalf("ExportVolume: %v", err)
	}
	sent := 0
	relay := sha256.New()
	in := []*pb.VolumeChunk{
		{Frame: &pb.VolumeChunk_Header_{Header: &pb.VolumeChunk_Header{VolumeName: dst, WipeFirst: true}}},
	}
	for _, c := range ex.chunks {
		in = append(in, c)
		sent += len(c.GetData())
		relay.Write(c.GetData())
	}
	im := &fakeImportStream{in: in, ctx: ctx}
	if err := svc.ImportVolume(im); err != nil {
		t.Fatalf("ImportVolume transport error: %v", err)
	}
	if im.result == nil || !im.result.Ok {
		t.Fatalf("ImportVolume failed: %+v", im.result)
	}
	if im.result.BytesWritten != int64(sent) {
		t.Errorf("byte count: want %d, got %d", sent, im.result.BytesWritten)
	}
	if im.result.Sha256 != hex.EncodeToString(relay.Sum(nil)) {
		t.Errorf("digest: want %s, got %s", hex.EncodeToString(relay.Sum(nil)), im.result.Sha256)
	}
}
