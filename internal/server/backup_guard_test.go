package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func collectRestore() (*rsEmitter, func() *pb.RestoreResult) {
	var res *pb.RestoreResult
	return &rsEmitter{send: func(ev *pb.RestoreEvent) error {
			if r := ev.GetResult(); r != nil {
				res = r
			}
			return nil
		}}, func() *pb.RestoreResult {
			return res
		}
}

func TestDatabaseBackupRestore_refuseAnotherProjectsContainer(t *testing.T) {
	argv := dockerOnPath(t, `[ "$1" = inspect ] && echo db_other; exit 0`)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	d := &pb.DatabaseDescriptor{
		Container: "deplo-agent-test-db", DbType: "redis", DbName: "0", User: "default", ProjectId: "db_mine",
	}

	var bres *pb.BackupResult
	s.backupDatabase(context.Background(), d, &artifactDestination{stream: func([]byte) error { return nil }},
		&bkEmitter{send: func(ev *pb.BackupEvent) error {
			if r := ev.GetResult(); r != nil {
				bres = r
			}
			return nil
		}})
	if bres == nil || bres.GetOk() || !strings.Contains(bres.GetError(), "does not belong") {
		t.Fatalf("backup: want an ownership refusal, got %+v", bres)
	}
	e, result := collectRestore()
	s.restoreDatabase(context.Background(), d, &artifactSource{stream: strings.NewReader("")}, e)
	if r := result(); r == nil || r.GetOk() || !strings.Contains(r.GetError(), "does not belong") {
		t.Fatalf("restore: want an ownership refusal, got %+v", r)
	}

	calls := argv()
	for _, a := range calls {
		if strings.HasPrefix(a, "exec") {
			t.Fatalf("exec ran in a container of another project: %q", a)
		}
	}
	want := `inspect --type container -f {{index .Config.Labels "deplo.project"}} deplo-agent-test-db`
	if !slices.Contains(calls, want) {
		t.Fatalf("want %q, got %q", want, calls)
	}
}

// A bucket object that is not the artifact Deplo wrote is refused before the project is stopped or wiped.
func TestRestore_s3DigestIsCheckedBeforeAnythingIsWiped(t *testing.T) {
	argv := dockerOnPath(t, "exit 0")
	s, store := newStoreService(t)
	recipient, identity := testKeypair(t)
	const slug = "deplo-agent-test-restore"
	keep := filepath.Join(s.filesRoot(slug), "keep.txt")
	mustWrite(t, keep, "precious")
	wrong := sha256.Sum256([]byte("another artifact"))
	src, err := sourceFromRestore(s, &pb.RestoreRequest{
		S3:             fakeBucket(t, "deplo/x.tar.gz.age", encryptedArtifact(t, recipient, []byte("tampered"))),
		AgeIdentity:    identity,
		ExpectedSha256: hex.EncodeToString(wrong[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	e, result := collectRestore()
	s.restoreArtifact(context.Background(), pb.BackupKind_BACKUP_KIND_PROJECT, nil,
		&pb.ProjectDescriptor{Slug: slug, VolumeNames: []string{"deplo-agent-test-vol"}, IncludeFiles: true}, src, e)
	if r := result(); r == nil || r.GetOk() || !strings.Contains(r.GetError(), "not the one Deplo wrote") {
		t.Fatalf("want a digest refusal, got %+v", r)
	}
	if b, _ := os.ReadFile(keep); string(b) != "precious" {
		t.Error("the files dir was wiped for an artifact that failed its check")
	}
	if calls := argv(); len(calls) > 0 && calls[0] != "" {
		t.Errorf("docker ran before the artifact was proven: %q", calls)
	}
	assertNoSpool(t, store)
}

// A proven artifact is restored from the local copy, which is removed afterwards.
func TestRestore_s3ProvenArtifactRestoresFromTheLocalCopy(t *testing.T) {
	out := filepath.Join(t.TempDir(), "restored")
	dockerOnPath(t, `[ "$1" = exec ] && cat > `+out+`; exit 0`)
	s, store := newStoreService(t)
	recipient, identity := testKeypair(t)
	payload := []byte("pg_dump custom format")
	artifact := encryptedArtifact(t, recipient, payload)
	sum := sha256.Sum256(artifact)
	src, err := sourceFromRestore(s, &pb.RestoreRequest{
		S3:          fakeBucket(t, "deplo/db.dump.gz.age", artifact),
		AgeIdentity: identity, ExpectedSha256: hex.EncodeToString(sum[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	e, result := collectRestore()
	s.restoreArtifact(context.Background(), pb.BackupKind_BACKUP_KIND_DATABASE,
		&pb.DatabaseDescriptor{Container: "deplo-agent-test-db", DbType: "postgres", DbName: "app", User: "u"}, nil, src, e)
	if r := result(); r == nil || !r.GetOk() {
		t.Fatalf("want a successful restore, got %+v", r)
	}
	if b, _ := os.ReadFile(out); string(b) != string(payload) {
		t.Errorf("restored %q, want %q", b, payload)
	}
	assertNoSpool(t, store)
}

func TestSpoolToVerify_refusesToFillTheDisk(t *testing.T) {
	orig := spoolReserveBytes
	spoolReserveBytes = 1 << 62
	t.Cleanup(func() { spoolReserveBytes = orig })
	dir := t.TempDir()
	src := &artifactSource{stream: strings.NewReader("artifact"), expectedSha256: "00"}
	if _, err := src.spoolToVerify(context.Background(), dir); !errors.Is(err, errSpoolNoRoom) {
		t.Fatalf("want errSpoolNoRoom, got %v", err)
	}
	assertNoSpool(t, dir)
}

// FLUSHALL empties redis, so an artifact that cannot even be opened must stop the restore before it.
func TestRestoreRedis_opensTheSourceBeforeFlushing(t *testing.T) {
	argv := dockerOnPath(t, "exit 0")
	s := New(t.TempDir(), t.TempDir(), "/", "")
	e, result := collectRestore()
	s.restoreDatabase(context.Background(), &pb.DatabaseDescriptor{Container: "deplo-agent-test-redis", DbType: "redis"},
		&artifactSource{stream: strings.NewReader("not a backup")}, e)
	if r := result(); r == nil || r.GetOk() {
		t.Fatalf("an unreadable artifact must fail the restore, got %+v", r)
	}
	for _, a := range argv() {
		if strings.Contains(a, "FLUSHALL") || strings.Contains(a, "CONFIG SET") {
			t.Fatalf("redis was changed before the artifact opened: %q", a)
		}
	}
}

func assertNoSpool(t *testing.T, dir string) {
	t.Helper()
	matches, _ := filepath.Glob(filepath.Join(dir, ".deplo-restore-*"))
	if len(matches) != 0 {
		t.Errorf("the local copy was left behind: %v", matches)
	}
}

// An older control plane sends no project_id; its backups keep working.
func TestDatabaseBackup_noProjectIDSkipsTheCheck(t *testing.T) {
	argv := dockerOnPath(t, "exit 1")
	s := New(t.TempDir(), t.TempDir(), "/", "")
	d := &pb.DatabaseDescriptor{Container: "deplo-agent-test-db", DbType: "redis"}
	e, _ := collectRestore()
	s.restoreDatabase(context.Background(), d, &artifactSource{stream: strings.NewReader("")}, e)
	for _, a := range argv() {
		if strings.HasPrefix(a, "inspect") {
			t.Fatalf("no project_id must mean no ownership check, got %q", a)
		}
	}
}
