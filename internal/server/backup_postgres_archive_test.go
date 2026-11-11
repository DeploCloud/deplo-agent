package server

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// A fake docker that answers what Postgres would: the base backup, the waiting change log, and a server out of recovery.
const pgDocker = `case "$*" in
*pg_basebackup*) printf 'BASE-BACKUP-TAR';;
*"ls -1"*) printf 'WAITING';;
*"tar -cf"*) printf 'LOG-TAR';;
*inspect*) printf 'postgres:17-alpine\nPGDATA=/var/lib/postgresql/data\nPOSTGRES_DB=main\n';;
*pg_is_in_recovery*) echo f;;
*"tar -xf"*) cat > /dev/null;;
esac
exit 0`

var pgArchived = &pb.DatabaseDescriptor{Container: "deplo-agent-test-pg", DbType: "postgres", DbName: "main", User: "app", Password: "pgpass"}

func called(argv []string, sub string) int {
	return slices.IndexFunc(argv, func(a string) bool { return strings.Contains(a, sub) })
}

// helperSteps is the last word of every run of the unpacking container, in order: base, swap, log, replay.
func helperSteps(argv []string) string {
	var steps []string
	for _, line := range argv {
		if strings.Contains(line, "--volumes-from") {
			steps = append(steps, line[strings.LastIndex(line, " ")+1:])
		}
	}
	return strings.Join(steps, ",")
}

// relayedBackup runs one Backup that streams its artifact out, and returns the result and the decrypted artifact.
func relayedBackup(t *testing.T, s *Service, archive pb.PostgresArchive) (*pb.BackupResult, []byte) {
	t.Helper()
	recipient, identity := testKeypair(t)
	stream := &fakeBackupStream{}
	if err := s.Backup(&pb.BackupRequest{
		Kind: pb.BackupKind_BACKUP_KIND_DATABASE, Database: pgArchived, Archive: archive,
		StreamOut: true, AgeRecipient: recipient, Store: &pb.StoreTarget{ObjectKey: "t/database/db_1/a"},
	}, stream); err != nil {
		t.Fatal(err)
	}
	var sent bytes.Buffer
	for _, ev := range stream.events {
		sent.Write(ev.GetData())
	}
	res := stream.lastResult(t)
	if sent.Len() == 0 {
		return res, nil
	}
	rc, err := openArtifactReader(&sent, identity)
	if err != nil {
		t.Fatalf("artifact unreadable: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	return res, got
}

func TestPostgresArchive_isAdvertised(t *testing.T) {
	if !slices.Contains(Capabilities, "backup.postgres-archive") {
		t.Fatal("backup.postgres-archive is not advertised")
	}
}

func TestPostgresArchive_aBaseBackupIsAPhysicalCopyNotADump(t *testing.T) {
	argv := dockerOnPath(t, pgDocker)
	s, _ := newStoreService(t)
	res, artifact := relayedBackup(t, s, pb.PostgresArchive_POSTGRES_ARCHIVE_BASE)
	if !res.GetOk() || string(artifact) != "BASE-BACKUP-TAR" {
		t.Fatalf("result %+v, artifact %q", res, artifact)
	}
	if called(argv(), "pg_basebackup -U app -D - -Ft -X fetch") < 0 || called(argv(), "pg_dump") >= 0 {
		t.Fatalf("argv %v", argv())
	}
	if called(argv(), "pgpass") >= 0 {
		t.Fatalf("the password is on a command line: %v", argv())
	}
}

func TestPostgresArchive_theChangeLogIsShippedThenRemovedFromTheMember(t *testing.T) {
	argv := dockerOnPath(t, strings.Replace(pgDocker, "WAITING", `000000010000000000000002\n000000010000000000000003\n00000002.history\n../../etc/passwd\n`, 1))
	s, _ := newStoreService(t)
	res, artifact := relayedBackup(t, s, pb.PostgresArchive_POSTGRES_ARCHIVE_LOG)
	if !res.GetOk() || string(artifact) != "LOG-TAR" {
		t.Fatalf("result %+v, artifact %q", res, artifact)
	}
	names := "000000010000000000000002 000000010000000000000003 00000002.history"
	tarAt, rmAt := called(argv(), "tar -cf - -- \"$@\" sh "+names), called(argv(), "rm -f -- \"$@\" sh "+names)
	if tarAt < 0 || rmAt < tarAt {
		t.Fatalf("want the waiting files archived, then removed: %v", argv())
	}
}

func TestPostgresArchive_nothingWaitingWritesNoArtifact(t *testing.T) {
	argv := dockerOnPath(t, strings.Replace(pgDocker, "WAITING", "", 1))
	s, _ := newStoreService(t)
	res, artifact := relayedBackup(t, s, pb.PostgresArchive_POSTGRES_ARCHIVE_LOG)
	if !res.GetOk() || res.GetObjectKey() != "" || res.GetSizeBytes() != 0 || artifact != nil {
		t.Fatalf("result %+v, artifact %q", res, artifact)
	}
	if called(argv(), "tar -cf") >= 0 || called(argv(), "rm -f") >= 0 {
		t.Fatalf("argv %v", argv())
	}
}

func TestPostgresArchive_aChangeLogThatDidNotLandStaysOnTheMember(t *testing.T) {
	argv := dockerOnPath(t, strings.Replace(pgDocker, "WAITING", `000000010000000000000002\n`, 1))
	s, root := newStoreService(t)
	recipient, _ := testKeypair(t)
	key := "t/database/db_1/base.wal/20260301T120000Z.tar.gz.age"
	// The destination already holds this key, and a store never overwrites.
	mustWrite(t, filepath.Join(root, key), "taken")
	stream := &fakeBackupStream{}
	_ = s.Backup(&pb.BackupRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, Database: pgArchived,
		Archive: pb.PostgresArchive_POSTGRES_ARCHIVE_LOG, AgeRecipient: recipient, Store: &pb.StoreTarget{ObjectKey: key}}, stream)
	if res := stream.lastResult(t); res.GetOk() {
		t.Fatalf("result %+v", res)
	}
	if called(argv(), "rm -f") >= 0 {
		t.Fatalf("the change log was removed though it never landed: %v", argv())
	}
}

func TestPostgresArchive_onlyPostgresHasOne(t *testing.T) {
	dockerOnPath(t, pgDocker)
	s, _ := newStoreService(t)
	recipient, _ := testKeypair(t)
	stream := &fakeBackupStream{}
	_ = s.Backup(&pb.BackupRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, Archive: pb.PostgresArchive_POSTGRES_ARCHIVE_BASE,
		Database:  &pb.DatabaseDescriptor{Container: "deplo-agent-test-my", DbType: "mysql"},
		StreamOut: true, AgeRecipient: recipient, Store: &pb.StoreTarget{ObjectKey: "k"}}, stream)
	if res := stream.lastResult(t); res.GetOk() || !strings.Contains(res.GetError(), "Postgres") {
		t.Fatalf("result %+v", res)
	}
}

// baseTar is what pg_basebackup writes: a tar whose first entry is backup_label.
func baseTar(t *testing.T, files ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range append([]string{"backup_label"}, files...) {
		_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: 1})
		_, _ = tw.Write([]byte("x"))
	}
	_ = tw.Close()
	return buf.Bytes()
}

func TestPostgresArchive_aBaseBackupIsRestoredByReplacingTheDataDirectory(t *testing.T) {
	argv := dockerOnPath(t, pgDocker)
	s, _ := newStoreService(t)
	recipient, identity := testKeypair(t)
	e, result := collectRestore()
	s.restoreDatabase(context.Background(), pgArchived,
		&artifactSource{stream: bytes.NewReader(encryptedArtifact(t, recipient, baseTar(t, "PG_VERSION"))), identity: identity}, e)
	if r := result(); r == nil || !r.GetOk() {
		t.Fatalf("restore failed: %+v", r)
	}
	a := argv()
	stop, swap, start, up := called(a, "stop deplo-agent-test-pg"), called(a, "--volumes-from deplo-agent-test-pg"), called(a, "start deplo-agent-test-pg"), called(a, "pg_is_in_recovery")
	if stop < 0 || stop > swap || swap > start || start > up {
		t.Fatalf("want stop, replace, start, then wait for it: %v", a)
	}
	// Same image, the container's own data directory, and no network for a container that only unpacks.
	if !strings.Contains(a[swap], "-e D=/var/lib/postgresql/data") || !strings.Contains(a[swap], "postgres:17-alpine") || !strings.Contains(a[swap], "--network none") {
		t.Fatalf("helper %q", a[swap])
	}
	if called(a, "pg_restore") >= 0 || helperSteps(a) != "base,swap" {
		t.Fatalf("argv %v", a)
	}
}

func TestPostgresArchive_aDumpIsStillRestoredWithPgRestore(t *testing.T) {
	argv := dockerOnPath(t, pgDocker)
	s, _ := newStoreService(t)
	recipient, identity := testKeypair(t)
	e, result := collectRestore()
	s.restoreDatabase(context.Background(), pgArchived,
		&artifactSource{stream: bytes.NewReader(encryptedArtifact(t, recipient, []byte("PGDMP\x01\x0e\x00rest of a dump"))), identity: identity}, e)
	if r := result(); r == nil || !r.GetOk() {
		t.Fatalf("restore failed: %+v", r)
	}
	if a := argv(); called(a, "pg_restore -U app --clean --if-exists -d main") < 0 || called(a, "stop ") >= 0 {
		t.Fatalf("argv %v", a)
	}
}

func TestPostgresArchive_aFailoverRestoreReplaysTheChangeLogAfterTheBase(t *testing.T) {
	argv := dockerOnPath(t, pgDocker)
	s, root := newStoreService(t)
	recipient, identity := testKeypair(t)
	base := "t/database/db_1/20260301T114500Z-brun_1.base.tar.gz.age"
	put := func(key string, body []byte) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, key)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, key), encryptedArtifact(t, recipient, body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	put(base, baseTar(t))
	put(base+".wal/20260301T115000Z.tar.gz.age", []byte("second"))
	put(base+".wal/20260301T114600Z.tar.gz.age", []byte("first"))
	mustWrite(t, filepath.Join(root, base+".wal/20260301T115100Z.tar.gz.age.partial"), "never finished")

	err := restoreLikeTheRPC(s, context.Background(), &pb.RestoreRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, Database: pgArchived,
		Store: &pb.StoreTarget{ObjectKey: base}, AgeIdentity: identity, LogPrefix: base + ".wal"})
	if err != nil {
		t.Fatal(err)
	}
	a := argv()
	if got := helperSteps(a); got != "base,swap,log,log,replay" {
		t.Fatalf("helper steps %q, argv %v", got, a)
	}
	if called(a, "--volumes-from") > called(a, "start deplo-agent-test-pg") {
		t.Fatalf("the data directory was touched after the start: %v", a)
	}
	// Once Postgres has replayed it, what was put there for the replay is gone.
	if called(a, "RESET restore_command") < 0 {
		t.Fatalf("argv %v", a)
	}
}

func TestPostgresArchive_aChangeLogOutsideTheStoreIsRefusedBeforeAnythingStops(t *testing.T) {
	argv := dockerOnPath(t, pgDocker)
	s, root := newStoreService(t)
	recipient, identity := testKeypair(t)
	base := "t/database/db_1/a.base.tar.gz.age"
	mustWrite(t, filepath.Join(root, base), string(encryptedArtifact(t, recipient, baseTar(t))))
	err := restoreLikeTheRPC(s, context.Background(), &pb.RestoreRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, Database: pgArchived,
		Store: &pb.StoreTarget{ObjectKey: base}, AgeIdentity: identity, LogPrefix: "../../etc"})
	if err == nil {
		t.Fatal("a prefix that climbs out of the store was accepted")
	}
	if called(argv(), "stop ") >= 0 {
		t.Fatalf("the database was stopped: %v", argv())
	}
}

// The helper is a shell script nothing else covers: run it for real against a directory.
func TestPostgresArchive_theDataDirectoryIsReplacedOnlyOnceTheBackupIsUnpacked(t *testing.T) {
	run := func(dir, step string, in []byte) {
		t.Helper()
		cmd := exec.Command("sh", "-c", pgHelperScript, "sh", step)
		cmd.Env = append(os.Environ(), "D="+dir)
		cmd.Stdin = bytes.NewReader(in)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v: %s", step, err, out)
		}
	}
	names := func(dir string) string {
		entries, _ := os.ReadDir(dir)
		var out []string
		for _, e := range entries {
			out = append(out, e.Name())
		}
		return strings.Join(out, ",")
	}
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "base", "old"), "old")
	mustWrite(t, filepath.Join(dir, ".hidden"), "old")
	run(dir, "base", baseTar(t, "PG_VERSION", ".dot"))
	if got := names(dir); got != ".deplo-restore,.hidden,base" {
		t.Fatalf("unpacking touched the data: %s", got)
	}
	run(dir, "drop", nil)
	if got := names(dir); got != ".hidden,base" {
		t.Fatalf("a backup that was dropped left: %s", got)
	}
	run(dir, "base", baseTar(t, "PG_VERSION", ".dot"))
	run(dir, "swap", nil)
	if got := names(dir); got != ".dot,PG_VERSION,backup_label" {
		t.Fatalf("after the swap: %s", got)
	}
}

func TestPostgresArchive_aBackupThatDoesNotUnpackLeavesTheDatabaseAsItWas(t *testing.T) {
	argv := dockerOnPath(t, strings.Replace(pgDocker, `*"tar -xf"*) cat > /dev/null;;`, `*" base") cat > /dev/null; exit 2;;`+"\n"+`*"tar -xf"*) cat > /dev/null;;`, 1))
	s, _ := newStoreService(t)
	recipient, identity := testKeypair(t)
	e, result := collectRestore()
	s.restoreDatabase(context.Background(), pgArchived,
		&artifactSource{stream: bytes.NewReader(encryptedArtifact(t, recipient, baseTar(t, "PG_VERSION"))), identity: identity}, e)
	if r := result(); r == nil || r.GetOk() || !strings.Contains(r.GetError(), "keeps the data it had") {
		t.Fatalf("result: %+v", r)
	}
	a := argv()
	if got := helperSteps(a); got != "base,drop" || called(a, "start deplo-agent-test-pg") < 0 {
		t.Fatalf("helper steps %q, argv %v", got, a)
	}
}
