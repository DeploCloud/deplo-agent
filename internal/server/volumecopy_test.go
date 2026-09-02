package server

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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

// TestE2E_VolumeCopyRoundTrip drives ExportVolume → (relay the chunks) → ImportVolume
// against REAL docker volumes, proving the cross-host copy machinery moves the bytes
// and that the import OVERWRITES the destination (wipe-first).
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

// TestExportVolume_missingVolumeIsNotFound is the guard that had to exist and did not:
// `docker run -v <name>:/v` CREATES a missing named volume, so an export of a volume
// that is not on this host used to exit 0 with a complete EMPTY archive, and the
// caller wiped a real destination for it.
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

// TestImportVolume_truncatedStreamLeavesNothing proves all-or-nothing survives a stream
// that dies MID-transfer, not only one that never starts.
func TestImportVolume_truncatedStreamLeavesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	src := "deplo-e2e-copy-trunc-src"
	dst := "deplo-e2e-copy-trunc-dst"
	for _, v := range []string{src, dst} {
		_, _ = dockercli.Run(ctx, 10*time.Second, "volume", "rm", "-f", v)
		if res, err := dockercli.Run(ctx, 20*time.Second, "volume", "create", v); err != nil || res.Code != 0 {
			t.Skipf("cannot create volume %q (%v / %s)", v, err, res.Stderr)
		}
		defer dockercli.Run(context.Background(), 15*time.Second, "volume", "rm", "-f", v)
	}

	// Big and incompressible, so the truncated stream still carries most of the
	// file: the point is that the extract gets FAR, not that it never starts.
	if res, err := dockercli.Run(ctx, 60*time.Second, "run", "--rm", "-v", src+":/v", volumeHelperImage,
		"sh", "-c", "dd if=/dev/urandom of=/v/big.bin bs=1024 count=4096 2>/dev/null"); err != nil || res.Code != 0 {
		t.Fatalf("seed source: %v / %s", err, res.Stderr)
	}
	if res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", dst+":/v", volumeHelperImage,
		"sh", "-c", "echo precious > /v/old.txt"); err != nil || res.Code != 0 {
		t.Fatalf("seed dest: %v / %s", err, res.Stderr)
	}

	ex := &fakeExportStream{ctx: ctx}
	if err := svc.ExportVolume(&pb.ExportVolumeRequest{VolumeName: src}, ex); err != nil {
		t.Fatalf("ExportVolume: %v", err)
	}
	var whole []byte
	for _, c := range ex.chunks {
		whole = append(whole, c.GetData()...)
	}
	if len(whole) < 64*1024 {
		t.Fatalf("export produced too little to truncate meaningfully (%d bytes)", len(whole))
	}

	// The relay dies with the last 32 KiB still in flight: the gzip trailer never
	// arrives, which is how a truncation announces itself.
	im := &fakeImportStream{ctx: ctx, in: []*pb.VolumeChunk{
		{Frame: &pb.VolumeChunk_Header_{Header: &pb.VolumeChunk_Header{VolumeName: dst, WipeFirst: true}}},
		{Frame: &pb.VolumeChunk_Data{Data: whole[:len(whole)-32*1024]}},
	}}
	if err := svc.ImportVolume(im); err != nil {
		t.Fatalf("ImportVolume transport error: %v", err)
	}
	if im.result == nil || im.result.Ok {
		t.Fatalf("a truncated stream must not report success: %+v", im.result)
	}
	if !strings.Contains(im.result.Error, "emptied") {
		t.Errorf("the failure must say what it did with the destination: %q", im.result.Error)
	}

	res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", dst+":/v", volumeHelperImage,
		"sh", "-c", "ls -A /v | wc -l")
	if err != nil || res.Code != 0 {
		t.Fatalf("inspect dest: %v / %s", err, res.Stderr)
	}
	if strings.TrimSpace(res.Stdout) != "0" {
		t.Errorf("a failed import must leave the destination empty, it holds %s entries", strings.TrimSpace(res.Stdout))
	}
}

// TestSanitizeTar keeps the import half as strict as the restore half: the archive
// comes off another platform's host and is extracted by a helper running as root.
func TestSanitizeTar(t *testing.T) {
	var in bytes.Buffer
	tw := tar.NewWriter(&in)
	write := func(h *tar.Header, body string) {
		h.Size = int64(len(body))
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if body != "" {
			if _, err := tw.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(&tar.Header{Name: "./data", Typeflag: tar.TypeDir, Mode: 0o2755}, "")
	write(&tar.Header{Name: "./data/pg.conf", Typeflag: tar.TypeReg, Mode: 0o600, Uid: 999, Gid: 999}, "listen=*")
	write(&tar.Header{Name: "./rooted", Typeflag: tar.TypeReg, Mode: 0o4755}, "#!/bin/sh")
	write(&tar.Header{Name: "./data/rel", Typeflag: tar.TypeSymlink, Linkname: "../data/pg.conf"}, "")
	write(&tar.Header{Name: "./data/out", Typeflag: tar.TypeSymlink, Linkname: "../../../etc/hostname"}, "")
	write(&tar.Header{Name: "./data/abs", Typeflag: tar.TypeSymlink, Linkname: "/etc/hostname"}, "")
	write(&tar.Header{Name: "./data/sda", Typeflag: tar.TypeBlock, Mode: 0o660, Devmajor: 8}, "")
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	var drops tarDrops
	if err := sanitizeTar(&out, &in, &drops); err != nil {
		t.Fatalf("sanitizeTar: %v", err)
	}
	if drops.links != 2 || drops.special != 1 {
		t.Errorf("drops = %d links / %d special, want 2 / 1", drops.links, drops.special)
	}
	for _, want := range []string{"./data/out", "./data/abs", "./data/sda"} {
		if !containsString(drops.names, want) {
			t.Errorf("%s was dropped but not named: %v", want, drops.names)
		}
	}

	kept := map[string]*tar.Header{}
	tr := tar.NewReader(&out)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		kept[h.Name] = h
	}
	for _, gone := range []string{"./data/out", "./data/abs", "./data/sda"} {
		if _, ok := kept[gone]; ok {
			t.Errorf("%s survived the import", gone)
		}
	}
	if _, ok := kept["./data/rel"]; !ok {
		t.Error("a relative in-tree symlink was dropped - a certbot live/ dir is nothing else")
	}
	if h := kept["./rooted"]; h == nil {
		t.Fatal("./rooted was dropped; it should arrive without its setuid bit")
	} else if h.Mode&^0o777 != 0 {
		t.Errorf("./rooted kept mode %o - setuid/setgid/sticky must go", h.Mode)
	}
	if h := kept["./data"]; h == nil || h.Mode&^0o777 != 0 {
		t.Errorf("the setgid directory kept mode %v", h)
	}
	if h := kept["./data/pg.conf"]; h == nil || h.Uid != 999 || h.Gid != 999 || h.Mode != 0o600 {
		t.Errorf("ownership must survive (a DB volume is owned by its engine's uid): %v", h)
	}
}

// TestImportReportsDrops is the wire half of the tally: an import that silently
// arrived short reported "0 failed", so the pump's count has to reach StackResult.
func TestImportReportsDrops(t *testing.T) {
	var raw bytes.Buffer
	tw := tar.NewWriter(&raw)
	for _, h := range []*tar.Header{
		{Name: "./keep", Typeflag: tar.TypeReg, Mode: 0o644},
		{Name: "./escape", Typeflag: tar.TypeSymlink, Linkname: "/etc/shadow"},
		{Name: "./pipe", Typeflag: tar.TypeFifo, Mode: 0o600},
	} {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	var gzipped bytes.Buffer
	zw := gzip.NewWriter(&gzipped)
	if _, err := zw.Write(raw.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	pump, err := newSanitizingGunzipPump(io.Discard)
	if err != nil {
		t.Fatalf("pump: %v", err)
	}
	if _, err := pump.Write(gzipped.Bytes()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := pump.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	res := importResult(true, 1, "abc", "", &pump.drops)
	if res.GetDroppedLinks() != 1 || res.GetDroppedSpecial() != 1 {
		t.Errorf("StackResult = %d links / %d special, want 1 / 1", res.GetDroppedLinks(), res.GetDroppedSpecial())
	}
	if !containsString(res.GetDroppedNames(), "./escape") || !containsString(res.GetDroppedNames(), "./pipe") {
		t.Errorf("the dropped entries must be named: %v", res.GetDroppedNames())
	}
	// An import whose pump never ran reports nothing, which the control plane must
	// not read as a clean run.
	if bare := importResult(false, 0, "", "boom", nil); bare.GetDroppedLinks() != 0 || bare.GetDroppedNames() != nil {
		t.Errorf("a result with no pump must carry no tally: %+v", bare)
	}
}

// TestDroppedNamesAreBounded keeps the tally an RPC field, not a log: it rides a
// response, so both the count of names and each name are capped.
func TestDroppedNamesAreBounded(t *testing.T) {
	var drops tarDrops
	long := strings.Repeat("a", droppedNameMax+50)
	for i := 0; i < droppedNamesMax+3; i++ {
		drops.add(long, i%2 == 0)
	}
	if len(drops.names) != droppedNamesMax {
		t.Errorf("kept %d names, want at most %d", len(drops.names), droppedNamesMax)
	}
	if got := len(drops.names[0]); got > droppedNameMax+1 {
		t.Errorf("a name survived at %d chars, want it truncated to %d", got, droppedNameMax)
	}
	if drops.links+drops.special != int32(droppedNamesMax+3) {
		t.Errorf("the COUNT must not stop at the name cap: %d + %d", drops.links, drops.special)
	}
}

// The control plane can tell "this agent does not report drops" from "it reported
// none" only if the capability is advertised.
func TestCapabilities_advertisesDropReport(t *testing.T) {
	if !containsString(Capabilities, "volume-copy.drop-report") {
		t.Error("Capabilities must advertise \"volume-copy.drop-report\"")
	}
}

// The control plane stops tolerating a missing digest once this is advertised, and
// only advertises the hardened import behind it.
func TestCapabilities_advertisesHardenedCopy(t *testing.T) {
	if !containsString(Capabilities, "volume-copy-hardened") {
		t.Error("Capabilities must advertise \"volume-copy-hardened\"")
	}
}
