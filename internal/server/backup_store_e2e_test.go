package server

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"google.golang.org/grpc/metadata"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

// backup_store_e2e_test.go is the store sibling of backup_e2e_test.go: the same
// dump → artifact → restore round-trip against a REAL Postgres, but landing on
// THIS host's filesystem instead of a bucket, and encrypted.
//
// It proves the three things unit tests cannot:
//
//  1. a store restore actually OVERWRITES (the locked guarantee for every engine),
//  2. the artifact on disk is genuinely unreadable without the identity, and
//     genuinely readable WITH it — the promise the recovery key makes,
//  3. the cross-host relay shape (stream_out → WriteStoreFile → ReadStoreFile →
//     RestoreFrom) restores the same database, which is the path the control
//     plane takes whenever the destination server is not the target's server.

// startE2EPostgres brings up a throwaway Postgres and returns its descriptor plus
// a psql runner. Skips (never fails) when the host cannot host it.
func startE2EPostgres(t *testing.T, ctx context.Context, name string) (*pb.DatabaseDescriptor, func(string) (dockercli.Result, error)) {
	t.Helper()
	_, _ = dockercli.Run(ctx, 10*time.Second, "rm", "-f", name)
	res, err := dockercli.Run(ctx, 60*time.Second,
		"run", "-d", "--name", name,
		"-e", "POSTGRES_PASSWORD=secret", "-e", "POSTGRES_USER=admin", "-e", "POSTGRES_DB=appdb",
		e2ePostgresImage)
	if err != nil || res.Code != 0 {
		t.Skipf("cannot start postgres (%v / %s)", err, res.Stderr)
	}
	t.Cleanup(func() {
		_, _ = dockercli.Run(context.Background(), 15*time.Second, "rm", "-f", name)
	})

	psql := func(sql string) (dockercli.Result, error) {
		return dockercli.Run(ctx, 20*time.Second, "exec", "-e", "PGPASSWORD=secret", name,
			"psql", "-U", "admin", "-d", "appdb", "-tAc", sql)
	}
	for i := 0; i < 30; i++ {
		if r, e := psql("SELECT 1"); e == nil && r.Code == 0 && strings.Contains(r.Stdout, "1") {
			return &pb.DatabaseDescriptor{
				Container: name, DbType: "postgres", DbName: "appdb", User: "admin", Password: "secret",
			}, psql
		}
		time.Sleep(2 * time.Second)
	}
	t.Skip("postgres did not become ready")
	return nil, nil
}

func TestE2E_StoreBackupRestoreOverwrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	d, psql := startE2EPostgres(t, ctx, "deplo-e2e-store-pg")

	svc, root := newStoreService(t)
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	recipient, identity := id.Recipient().String(), id.String()
	key := "deplo/team_e2e/database/pg/e2e.dump.gz.age"

	if _, e := psql("CREATE TABLE t(v text); INSERT INTO t VALUES ('sentinel-A');"); e != nil {
		t.Fatalf("seed: %v", e)
	}

	// Back up to this host's store.
	bs := &fakeBackupStream{}
	if err := svc.Backup(&pb.BackupRequest{
		Kind:         pb.BackupKind_BACKUP_KIND_DATABASE,
		Store:        &pb.StoreTarget{ObjectKey: key},
		AgeRecipient: recipient,
		Database:     d,
	}, bs); err != nil {
		t.Fatalf("Backup rpc: %v", err)
	}
	br := bs.lastResult(t)
	if !br.GetOk() {
		t.Fatalf("backup failed: %s", br.GetError())
	}
	if br.GetSizeBytes() <= 0 || br.GetSha256() == "" {
		t.Errorf("a store backup must report size + digest, got %d / %q", br.GetSizeBytes(), br.GetSha256())
	}

	// The artifact is on disk and it is REALLY an age file. Asserted on the age
	// header rather than "the plaintext string is absent": pg_dump -Fc output is
	// itself compressed, so a plaintext-substring check would pass even against a
	// completely unencrypted dump and prove nothing.
	onDisk := filepath.Join(root, key)
	raw, rerr := os.ReadFile(onDisk)
	if rerr != nil {
		t.Fatalf("artifact not on disk at %s: %v", onDisk, rerr)
	}
	if !bytes.HasPrefix(raw, []byte("age-encryption.org/v1")) {
		t.Fatalf("the artifact on disk is not age-encrypted (starts with %q)", firstBytes(raw, 24))
	}
	// ...and it IS readable with the identity — the recovery key's whole promise.
	// PGDMP is pg_dump's custom-format magic, so this pins that what comes back
	// out is a real restorable dump, not merely bytes that decrypted.
	rc, oerr := openArtifactReader(bytes.NewReader(raw), identity)
	if oerr != nil {
		t.Fatalf("the artifact must decrypt with its identity: %v", oerr)
	}
	plain, _ := io.ReadAll(rc)
	_ = rc.Close()
	if !bytes.HasPrefix(plain, []byte("PGDMP")) {
		t.Errorf("the decrypted artifact is not a pg_dump archive (starts with %q)", firstBytes(plain, 16))
	}

	// Mutate, then restore in place — must drop-and-recreate back to sentinel-A.
	if _, e := psql("UPDATE t SET v='sentinel-B';"); e != nil {
		t.Fatalf("mutate: %v", e)
	}
	rs := &fakeRestoreStream{}
	if err := svc.Restore(&pb.RestoreRequest{
		Kind:        pb.BackupKind_BACKUP_KIND_DATABASE,
		Store:       &pb.StoreTarget{ObjectKey: key},
		AgeIdentity: identity,
		Database:    d,
	}, rs); err != nil {
		t.Fatalf("Restore rpc: %v", err)
	}
	if rr := rs.lastResult(t); !rr.GetOk() {
		t.Fatalf("restore failed: %s", rr.GetError())
	}
	if r, _ := psql("SELECT v FROM t;"); !strings.Contains(r.Stdout, "sentinel-A") || strings.Contains(r.Stdout, "sentinel-B") {
		t.Fatalf("store restore did not overwrite: got %q", r.Stdout)
	}

	// A restore with the WRONG key must fail loudly, not half-restore.
	other, _ := age.GenerateX25519Identity()
	rsBad := &fakeRestoreStream{}
	_ = svc.Restore(&pb.RestoreRequest{
		Kind:        pb.BackupKind_BACKUP_KIND_DATABASE,
		Store:       &pb.StoreTarget{ObjectKey: key},
		AgeIdentity: other.String(),
		Database:    d,
	}, rsBad)
	if rsBad.lastResult(t).GetOk() {
		t.Error("a restore with the wrong recovery key must fail")
	}

	// Retention: delete by exact key, then idempotently again.
	del1, _ := svc.S3Delete(ctx, &pb.S3DeleteRequest{Store: &pb.StoreTarget{ObjectKey: key}})
	if !del1.GetOk() || del1.GetDeleted() != 1 {
		t.Errorf("store delete should remove 1, got ok=%v n=%d err=%s", del1.GetOk(), del1.GetDeleted(), del1.GetError())
	}
	del2, _ := svc.S3Delete(ctx, &pb.S3DeleteRequest{Store: &pb.StoreTarget{ObjectKey: key}})
	if !del2.GetOk() || del2.GetDeleted() != 0 {
		t.Errorf("a second store delete must be idempotent, got ok=%v n=%d", del2.GetOk(), del2.GetDeleted())
	}
}

// The CROSS-HOST shape, with the control plane's relay simulated in-process:
// stream_out produces the artifact, WriteStoreFile lands it "elsewhere",
// ReadStoreFile streams it back, and RestoreFrom replays it into the database.
// Both halves run against the same Service here — what is being proven is the
// FRAMING and the fact that only ciphertext ever crosses the relay, not that two
// hosts exist.
func TestE2E_StoreRelayRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	d, psql := startE2EPostgres(t, ctx, "deplo-e2e-relay-pg")

	svc, root := newStoreService(t)
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	recipient, identity := id.Recipient().String(), id.String()
	key := "deplo/team_e2e/database/pg/relay.dump.gz.age"

	if _, e := psql("CREATE TABLE t(v text); INSERT INTO t VALUES ('relay-A');"); e != nil {
		t.Fatalf("seed: %v", e)
	}

	// 1. Backup with stream_out — the control plane's side of the relay.
	bs := &fakeBackupStream{}
	if err := svc.Backup(&pb.BackupRequest{
		Kind:         pb.BackupKind_BACKUP_KIND_DATABASE,
		StreamOut:    true,
		Store:        &pb.StoreTarget{ObjectKey: key},
		AgeRecipient: recipient,
		Database:     d,
	}, bs); err != nil {
		t.Fatalf("Backup rpc: %v", err)
	}
	br := bs.lastResult(t)
	if !br.GetOk() {
		t.Fatalf("relayed backup failed: %s", br.GetError())
	}
	relayed := bs.dataBytes()
	if len(relayed) == 0 || int64(len(relayed)) != br.GetSizeBytes() {
		t.Fatalf("relayed %d bytes, reported %d", len(relayed), br.GetSizeBytes())
	}
	if bytes.Contains(relayed, []byte("relay-A")) {
		t.Fatal("plaintext crossed the relay")
	}

	// 2. WriteStoreFile on the "destination" host.
	ws := &fakeWriteStoreStream{
		msgs: framedStoreChunks(&pb.StoreTarget{Root: root, ObjectKey: key}, relayed),
	}
	if err := svc.WriteStoreFile(ws); err != nil {
		t.Fatalf("WriteStoreFile: %v", err)
	}
	if !ws.result.GetOk() {
		t.Fatalf("WriteStoreFile failed: %s", ws.result.GetError())
	}
	if ws.result.GetBytesWritten() != br.GetSizeBytes() {
		t.Errorf("destination wrote %d bytes, source produced %d", ws.result.GetBytesWritten(), br.GetSizeBytes())
	}
	if ws.result.GetSha256() != br.GetSha256() {
		t.Errorf("digest mismatch across the relay: %q vs %q", ws.result.GetSha256(), br.GetSha256())
	}

	// 3. ReadStoreFile streams it back out.
	rsf := &fakeReadStoreStream{}
	if err := svc.ReadStoreFile(&pb.ReadStoreFileRequest{
		Store: &pb.StoreTarget{Root: root, ObjectKey: key},
	}, rsf); err != nil {
		t.Fatalf("ReadStoreFile: %v", err)
	}
	if !bytes.Equal(rsf.buf.Bytes(), relayed) {
		t.Fatal("ReadStoreFile did not return the bytes WriteStoreFile stored")
	}

	// 4. RestoreFrom replays them into the database.
	if _, e := psql("UPDATE t SET v='relay-B';"); e != nil {
		t.Fatalf("mutate: %v", e)
	}
	rf := &fakeRestoreFromStream{
		msgs: framedRestoreChunks(&pb.RestoreChunk_Header{
			Kind:        pb.BackupKind_BACKUP_KIND_DATABASE,
			Database:    d,
			AgeIdentity: identity,
		}, rsf.buf.Bytes()),
	}
	if err := svc.RestoreFrom(rf); err != nil {
		t.Fatalf("RestoreFrom: %v", err)
	}
	if res := rf.lastResult(t); !res.GetOk() {
		t.Fatalf("RestoreFrom failed: %s", res.GetError())
	}
	if r, _ := psql("SELECT v FROM t;"); !strings.Contains(r.Stdout, "relay-A") || strings.Contains(r.Stdout, "relay-B") {
		t.Fatalf("relayed restore did not overwrite: got %q", r.Stdout)
	}
}

// firstBytes renders a short prefix for a failure message without dumping a
// multi-MB artifact into the test log.
func firstBytes(b []byte, n int) string {
	if len(b) < n {
		n = len(b)
	}
	return string(b[:n])
}

// ---- fakes for the three streaming store RPCs ----

// dataBytes reassembles the artifact from a stream_out backup's data frames.
func (f *fakeBackupStream) dataBytes() []byte {
	var out []byte
	for _, ev := range f.events {
		out = append(out, ev.GetData()...)
	}
	return out
}

// fakeWriteStoreStream replays a pre-framed client stream into WriteStoreFile and
// captures the terminal StoreResult.
type fakeWriteStoreStream struct {
	msgs   []*pb.StoreChunk
	i      int
	result *pb.StoreResult
}

func (f *fakeWriteStoreStream) Recv() (*pb.StoreChunk, error) {
	if f.i >= len(f.msgs) {
		return nil, io.EOF
	}
	m := f.msgs[f.i]
	f.i++
	return m, nil
}
func (f *fakeWriteStoreStream) SendAndClose(r *pb.StoreResult) error {
	f.result = r
	return nil
}
func (f *fakeWriteStoreStream) Context() context.Context     { return context.Background() }
func (f *fakeWriteStoreStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeWriteStoreStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeWriteStoreStream) SetTrailer(metadata.MD)       {}
func (f *fakeWriteStoreStream) SendMsg(any) error            { return nil }
func (f *fakeWriteStoreStream) RecvMsg(any) error            { return nil }

// fakeReadStoreStream accumulates everything ReadStoreFile sends.
type fakeReadStoreStream struct {
	buf bytes.Buffer
}

func (f *fakeReadStoreStream) Send(c *pb.StoreChunk) error {
	f.buf.Write(c.GetData())
	return nil
}
func (f *fakeReadStoreStream) Context() context.Context     { return context.Background() }
func (f *fakeReadStoreStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeReadStoreStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeReadStoreStream) SetTrailer(metadata.MD)       {}
func (f *fakeReadStoreStream) SendMsg(any) error            { return nil }
func (f *fakeReadStoreStream) RecvMsg(any) error            { return nil }

// fakeRestoreFromStream is the bidi fake: it feeds pre-framed chunks in and
// captures the RestoreEvents that come back out.
type fakeRestoreFromStream struct {
	msgs   []*pb.RestoreChunk
	i      int
	events []*pb.RestoreEvent
}

func (f *fakeRestoreFromStream) Recv() (*pb.RestoreChunk, error) {
	if f.i >= len(f.msgs) {
		return nil, io.EOF
	}
	m := f.msgs[f.i]
	f.i++
	return m, nil
}
func (f *fakeRestoreFromStream) Send(ev *pb.RestoreEvent) error {
	f.events = append(f.events, ev)
	return nil
}
func (f *fakeRestoreFromStream) Context() context.Context     { return context.Background() }
func (f *fakeRestoreFromStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeRestoreFromStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeRestoreFromStream) SetTrailer(metadata.MD)       {}
func (f *fakeRestoreFromStream) SendMsg(any) error            { return nil }
func (f *fakeRestoreFromStream) RecvMsg(any) error            { return nil }

func (f *fakeRestoreFromStream) lastResult(t *testing.T) *pb.RestoreResult {
	t.Helper()
	for i := len(f.events) - 1; i >= 0; i-- {
		if r := f.events[i].GetResult(); r != nil {
			return r
		}
	}
	t.Fatal("no terminal RestoreResult emitted on the stream")
	return nil
}

func framedStoreChunks(target *pb.StoreTarget, data []byte) []*pb.StoreChunk {
	out := []*pb.StoreChunk{{Frame: &pb.StoreChunk_Header_{
		Header: &pb.StoreChunk_Header{Store: target, Overwrite: true},
	}}}
	for off := 0; off < len(data); off += storeChunkBytes {
		end := off + storeChunkBytes
		if end > len(data) {
			end = len(data)
		}
		out = append(out, &pb.StoreChunk{Frame: &pb.StoreChunk_Data{Data: data[off:end]}})
	}
	return out
}

func framedRestoreChunks(h *pb.RestoreChunk_Header, data []byte) []*pb.RestoreChunk {
	out := []*pb.RestoreChunk{{Frame: &pb.RestoreChunk_Header_{Header: h}}}
	for off := 0; off < len(data); off += storeChunkBytes {
		end := off + storeChunkBytes
		if end > len(data) {
			end = len(data)
		}
		out = append(out, &pb.RestoreChunk{Frame: &pb.RestoreChunk_Data{Data: data[off:end]}})
	}
	return out
}
