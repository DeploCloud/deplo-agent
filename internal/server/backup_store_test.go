package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// newStoreService builds a Service whose managed backup store is under a temp
// dir, and returns the resolved root (already initialized, as a check would).
func newStoreService(t *testing.T) (*Service, string) {
	t.Helper()
	base := t.TempDir()
	s := New(filepath.Join(base, "stacks"), t.TempDir(), "/", base)
	root, err := s.resolveStoreRoot("", true)
	if err != nil {
		t.Fatalf("resolveStoreRoot: %v", err)
	}
	return s, root
}

func testKeypair(t *testing.T) (recipient, identity string) {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	return id.Recipient().String(), id.String()
}

// ---------------------------------------------------------------------------
// Root resolution + the sentinel rule
// ---------------------------------------------------------------------------

func TestResolveStoreRoot_managedIsCreatedAndMarked(t *testing.T) {
	s, root := newStoreService(t)
	if want := filepath.Join(s.dataBase, "backups"); root != want {
		t.Errorf("root = %q, want %q", root, want)
	}
	if _, err := os.Stat(filepath.Join(root, storeSentinel)); err != nil {
		t.Errorf("the managed root must be marked: %v", err)
	}
	st, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if perm := st.Mode().Perm(); perm != storeDirPerm {
		t.Errorf("root perm = %v, want %v (a backup is a full copy of the data)", perm, storeDirPerm)
	}
}

func TestResolveStoreRoot_customNeedsSentinel(t *testing.T) {
	s, _ := newStoreService(t)
	custom := t.TempDir()

	// Without the sentinel, a plain write/delete path must refuse: this is what
	// stops a mistyped path from becoming a remote wipe on the next retention run.
	if _, err := s.resolveStoreRoot(custom, false); err == nil {
		t.Fatal("an unmarked custom root must be refused outside a check")
	} else if !strings.Contains(err.Error(), "not initialized") {
		t.Errorf("error should explain the root is not initialized, got %v", err)
	}

	// A check on an EMPTY directory marks it, and then it is usable.
	if _, err := s.resolveStoreRoot(custom, true); err != nil {
		t.Fatalf("an empty custom root must be adoptable: %v", err)
	}
	if _, err := s.resolveStoreRoot(custom, false); err != nil {
		t.Fatalf("a marked custom root must be usable: %v", err)
	}
}

func TestResolveStoreRoot_refusesNonEmptyUnmarkedDir(t *testing.T) {
	s, _ := newStoreService(t)
	custom := t.TempDir()
	if err := os.WriteFile(filepath.Join(custom, "important.db"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.resolveStoreRoot(custom, true); err == nil {
		t.Fatal("a non-empty directory with no sentinel must not be adopted")
	}
}

func TestResolveStoreRoot_refusesRelative(t *testing.T) {
	s, _ := newStoreService(t)
	if _, err := s.resolveStoreRoot("backups/here", true); err == nil {
		t.Error("a relative root must be refused")
	}
}

// --------------------------------------------------------------------------- THE
// regression that justifies living in internal/server: deleting a prefix that does not
// exist must delete NOTHING. safepath.Inside returns the BASE on every failure path, so
// the obvious implementation (join, resolve, RemoveAll) resolves a missing prefix to
// the root and wipes every backup on the server.

func TestStoreDeletePrefix_missingPrefixDeletesNothing(t *testing.T) {
	_, root := newStoreService(t)
	keep := "deplo/team_a/app/prj_keep/20260101T000000Z-brun_1.tar.gz.age"
	if _, _, err := storeWrite(root, keep, strings.NewReader("precious"), false); err != nil {
		t.Fatalf("seed: %v", err)
	}

	n, err := storeDeletePrefix(root, "deplo/team_a/app/prj_gone/")
	if err != nil {
		t.Fatalf("deleting a missing prefix must be idempotent, got %v", err)
	}
	if n != 0 {
		t.Errorf("deleted = %d, want 0", n)
	}
	if _, err := os.Stat(filepath.Join(root, keep)); err != nil {
		t.Fatalf("the surviving artifact was deleted: %v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("the store root itself was deleted: %v", err)
	}
}

func TestStoreDeletePrefix_refusesTheRoot(t *testing.T) {
	_, root := newStoreService(t)
	for _, prefix := range []string{"", "/", ".", "./"} {
		if _, err := storeDeletePrefix(root, prefix); err == nil {
			t.Errorf("prefix %q must be refused, not treated as the whole store", prefix)
		}
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("the store root was deleted: %v", err)
	}
}

func TestStoreDeletePrefix_refusesTraversal(t *testing.T) {
	_, root := newStoreService(t)
	if _, err := storeDeletePrefix(root, "../"); err == nil {
		t.Error("a traversing prefix must be refused")
	}
}

func TestStoreDeletePrefix_removesOneTargetsFolderOnly(t *testing.T) {
	_, root := newStoreService(t)
	mine := "deplo/team_a/app/prj_x/20260101T000000Z-brun_1.tar.gz.age"
	theirs := "deplo/team_b/app/prj_y/20260101T000000Z-brun_2.tar.gz.age"
	for _, k := range []string{mine, theirs} {
		if _, _, err := storeWrite(root, k, strings.NewReader("data"), false); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
	}
	n, err := storeDeletePrefix(root, "deplo/team_a/app/prj_x/")
	if err != nil {
		t.Fatalf("delete prefix: %v", err)
	}
	if n != 1 {
		t.Errorf("deleted = %d, want 1", n)
	}
	if _, err := os.Stat(filepath.Join(root, theirs)); err != nil {
		t.Errorf("another team's artifact was removed: %v", err)
	}
}

func TestStoreDeleteOne_isIdempotent(t *testing.T) {
	_, root := newStoreService(t)
	key := "deplo/t/app/x/a.tar.gz.age"
	if _, _, err := storeWrite(root, key, strings.NewReader("data"), false); err != nil {
		t.Fatal(err)
	}
	if n, err := storeDeleteOne(root, key); err != nil || n != 1 {
		t.Fatalf("first delete: n=%d err=%v", n, err)
	}
	if n, err := storeDeleteOne(root, key); err != nil || n != 0 {
		t.Errorf("second delete must be a no-op, got n=%d err=%v", n, err)
	}
}

// ---------------------------------------------------------------------------
// Containment: an object key never escapes the root
// ---------------------------------------------------------------------------

func TestStoreWrite_refusesTraversalKey(t *testing.T) {
	_, root := newStoreService(t)
	for _, key := range []string{"../escape.age", "deplo/../../escape.age"} {
		if _, _, err := storeWrite(root, key, strings.NewReader("x"), false); err == nil {
			t.Errorf("key %q must be refused", key)
		}
	}
	// An ABSOLUTE key is not an error - normalizeRel strips the leading slash, the same
	// way it does for the file RPCs, but it must stay CONTAINED.
	if _, _, err := storeWrite(root, "/etc/cron.d/evil", strings.NewReader("x"), false); err != nil {
		t.Fatalf("an absolute key should be relativised, not fail: %v", err)
	}
	if _, err := os.Stat("/etc/cron.d/evil"); err == nil {
		t.Fatal("a write escaped the store root")
	}
	if _, err := os.Stat(filepath.Join(root, "etc/cron.d/evil")); err != nil {
		t.Errorf("the relativised key should have landed inside the root: %v", err)
	}
}

func TestStoreWrite_refusesOverwriteByDefault(t *testing.T) {
	_, root := newStoreService(t)
	key := "deplo/t/app/x/a.tar.gz.age"
	if _, _, err := storeWrite(root, key, strings.NewReader("first"), false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := storeWrite(root, key, strings.NewReader("second"), false); err == nil {
		t.Error("object keys carry a run id; a collision means something is wrong and must not clobber a good backup")
	}
	b, _ := os.ReadFile(filepath.Join(root, key))
	if string(b) != "first" {
		t.Errorf("artifact = %q, want the original", b)
	}
}

// ---------------------------------------------------------------------------
// Atomicity: a failed write leaves no artifact at the real key
// ---------------------------------------------------------------------------

// failingReader yields some bytes and then errors, standing in for a dump that
// dies partway (ENOSPC, a killed container, a dropped relay).
type failingReader struct {
	data []byte
	n    int
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.n >= len(f.data) {
		return 0, io.ErrUnexpectedEOF
	}
	n := copy(p, f.data[f.n:])
	f.n += n
	return n, nil
}

func TestStoreWrite_partialWriteLeavesNoArtifact(t *testing.T) {
	_, root := newStoreService(t)
	key := "deplo/t/app/x/a.tar.gz.age"
	if _, _, err := storeWrite(root, key, &failingReader{data: []byte("half a dump")}, false); err == nil {
		t.Fatal("a truncated write must fail")
	}
	if _, err := os.Stat(filepath.Join(root, key)); err == nil {
		t.Fatal("a failed write must not leave an artifact at the real key - retention never cleans one up, and a restore would hand it to the user")
	}
	if _, err := os.Stat(filepath.Join(root, key+storePartialSuffix)); err == nil {
		t.Error("the temp file should have been removed on the error path")
	}
}

// The sweep must not touch a write that is STILL HAPPENING. The root is shared by every
// destination and every team on the host, and a check is fired by something as ordinary
// as opening the destination dropdown.
func TestSweepPartials_leavesAnInFlightWriteAlone(t *testing.T) {
	s, root := newStoreService(t)
	dir := filepath.Join(root, "deplo", "team_b", "app", "x")
	if err := os.MkdirAll(dir, storeDirPerm); err != nil {
		t.Fatal(err)
	}
	inFlight := filepath.Join(dir, "b.tar.gz.age"+storePartialSuffix)
	if err := os.WriteFile(inFlight, []byte("still streaming"), storeFilePerm); err != nil {
		t.Fatal(err)
	}
	// Freshly written, i.e. exactly what an in-progress relay looks like.
	res := s.storeCheck(&pb.StoreTarget{})
	if !res.GetOk() {
		t.Fatalf("check failed: %s", res.GetError())
	}
	if _, err := os.Stat(inFlight); err != nil {
		t.Fatal("the sweep deleted a temp file that was being written right now")
	}

	// Backdate it past the staleness window: now it is debris and must go.
	old := time.Now().Add(-storePartialStaleAfter - time.Minute)
	if err := os.Chtimes(inFlight, old, old); err != nil {
		t.Fatal(err)
	}
	if res := s.storeCheck(&pb.StoreTarget{}); !res.GetOk() {
		t.Fatalf("check failed: %s", res.GetError())
	}
	if _, err := os.Stat(inFlight); err == nil {
		t.Error("a long-abandoned .partial must still be reclaimed")
	}
}

func TestSweepPartials_reclaimsStrandedTempFiles(t *testing.T) {
	s, root := newStoreService(t)
	dir := filepath.Join(root, "deplo", "t", "app", "x")
	if err := os.MkdirAll(dir, storeDirPerm); err != nil {
		t.Fatal(err)
	}
	stranded := filepath.Join(dir, "a.tar.gz.age"+storePartialSuffix)
	if err := os.WriteFile(stranded, []byte("interrupted"), storeFilePerm); err != nil {
		t.Fatal(err)
	}
	// Aged out, so the sweep is allowed to touch it.
	old := time.Now().Add(-storePartialStaleAfter - time.Minute)
	if err := os.Chtimes(stranded, old, old); err != nil {
		t.Fatal(err)
	}
	res := s.storeCheck(&pb.StoreTarget{})
	if !res.GetOk() {
		t.Fatalf("check failed: %s", res.GetError())
	}
	if _, err := os.Stat(stranded); err == nil {
		t.Error("a stranded .partial must be swept by the check")
	}
	if res.GetTotalBytes() <= 0 {
		t.Error("the check should report the filesystem's headroom")
	}
	if res.GetRoot() != root {
		t.Errorf("root = %q, want %q", res.GetRoot(), root)
	}
}

// ---------------------------------------------------------------------------
// Encryption round-trip, and the close-ordering trap
// ---------------------------------------------------------------------------

func TestArtifactWriter_roundTrip(t *testing.T) {
	recipient, identity := testKeypair(t)
	// Big enough to span several of age's 64 KiB STREAM chunks, so a broken
	// final-chunk marker actually shows up.
	payload := bytes.Repeat([]byte("deplo backup payload\n"), 20_000)

	var sink bytes.Buffer
	aw, err := newArtifactWriter(&sink, recipient)
	if err != nil {
		t.Fatalf("newArtifactWriter: %v", err)
	}
	if _, err := aw.Writer().Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if bytes.Contains(sink.Bytes(), []byte("deplo backup payload")) {
		t.Fatal("the artifact must not contain plaintext")
	}

	rc, err := openArtifactReader(bytes.NewReader(sink.Bytes()), identity)
	if err != nil {
		t.Fatalf("openArtifactReader: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: %d bytes back, want %d", len(got), len(payload))
	}
}

// Pins the close-ordering bug. age's STREAM writer only emits its final-chunk marker on
// Close: skip it and the artifact decrypts perfectly until the last chunk and then
// fails - silent corruption discovered at restore time, months later.
// artifactWriter.Close is what prevents it, so assert that NOT calling it really does
// break the artifact.
func TestArtifactWriter_skippingCloseCorruptsTheArtifact(t *testing.T) {
	recipient, identity := testKeypair(t)
	payload := bytes.Repeat([]byte("x"), 200_000)

	var sink bytes.Buffer
	aw, err := newArtifactWriter(&sink, recipient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := aw.Writer().Write(payload); err != nil {
		t.Fatal(err)
	}
	// Deliberately close ONLY the gzip layer, as a naive implementation would.
	if err := aw.gz.Close(); err != nil {
		t.Fatal(err)
	}

	rc, err := openArtifactReader(bytes.NewReader(sink.Bytes()), identity)
	if err != nil {
		return // already unreadable - the point stands
	}
	defer rc.Close()
	if _, err := io.ReadAll(rc); err == nil {
		t.Error("an artifact whose age layer was never closed must not read back cleanly")
	}
}

func TestOpenArtifactReader_wrongIdentityFails(t *testing.T) {
	recipient, _ := testKeypair(t)
	_, otherIdentity := testKeypair(t)

	var sink bytes.Buffer
	aw, err := newArtifactWriter(&sink, recipient)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := aw.Writer().Write([]byte("secret")); err != nil {
		t.Fatal(err)
	}
	if err := aw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := openArtifactReader(bytes.NewReader(sink.Bytes()), otherIdentity); err == nil {
		t.Error("a different recovery key must not decrypt the artifact")
	}
}

func TestNewArtifactWriter_rejectsBadRecipient(t *testing.T) {
	if _, err := newArtifactWriter(io.Discard, "not-an-age-key"); err == nil {
		t.Error("a malformed recipient must be rejected before any bytes are produced")
	}
}

// --------------------------------------------------------------------------- The full
// store pipeline: write an artifact, read it back through the same artifactSource a
// restore uses.
// ---------------------------------------------------------------------------

func TestWriteArtifact_storeRoundTripThroughSource(t *testing.T) {
	s, root := newStoreService(t)
	recipient, identity := testKeypair(t)
	key := "deplo/team_a/database/db_1/20260101T000000Z-brun_1.dump.gz.age"
	payload := bytes.Repeat([]byte("pg_dump output\n"), 5_000)

	dest, err := destinationFromBackup(s, &pb.BackupRequest{
		Kind:         pb.BackupKind_BACKUP_KIND_DATABASE,
		Store:        &pb.StoreTarget{ObjectKey: key},
		AgeRecipient: recipient,
	}, nil)
	if err != nil {
		t.Fatalf("destinationFromBackup: %v", err)
	}

	written, err := s.writeArtifact(context.Background(), dest, func(w io.Writer) error {
		_, werr := w.Write(payload)
		return werr
	})
	if err != nil {
		t.Fatalf("writeArtifact: %v", err)
	}
	n, digest := written.size, written.digest
	if n <= 0 || digest == "" {
		t.Errorf("a store write must report size + digest, got n=%d digest=%q", n, digest)
	}
	onDisk, err := os.Stat(filepath.Join(root, key))
	if err != nil {
		t.Fatalf("artifact not at the expected key: %v", err)
	}
	if onDisk.Size() != n {
		t.Errorf("reported %d bytes, on disk %d", n, onDisk.Size())
	}
	if perm := onDisk.Mode().Perm(); perm != storeFilePerm {
		t.Errorf("artifact perm = %v, want %v", perm, storeFilePerm)
	}

	src := &artifactSource{
		store:    &pb.StoreTarget{Root: root, ObjectKey: key},
		identity: identity,
	}
	rd, closeSrc, err := src.open(context.Background())
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer closeSrc()
	got, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("the restore path did not read back what the backup path wrote")
	}
}

// A relayed backup must hand out CIPHERTEXT: the control plane forwards these
// frames to another host, and plaintext crossing it would defeat the whole point
// of holding the identity control-plane side.
func TestWriteArtifact_streamOutEmitsCiphertext(t *testing.T) {
	s, _ := newStoreService(t)
	recipient, identity := testKeypair(t)
	payload := bytes.Repeat([]byte("relayed dump\n"), 5_000)

	var relayed bytes.Buffer
	dest, err := destinationFromBackup(s, &pb.BackupRequest{
		Kind:         pb.BackupKind_BACKUP_KIND_DATABASE,
		StreamOut:    true,
		Store:        &pb.StoreTarget{ObjectKey: "deplo/t/database/d/a.dump.gz.age"},
		AgeRecipient: recipient,
	}, func(b []byte) error {
		_, werr := relayed.Write(b)
		return werr
	})
	if err != nil {
		t.Fatalf("destinationFromBackup: %v", err)
	}

	written, err := s.writeArtifact(context.Background(), dest, func(w io.Writer) error {
		_, werr := w.Write(payload)
		return werr
	})
	if err != nil {
		t.Fatalf("writeArtifact: %v", err)
	}
	n, digest := written.size, written.digest
	if int64(relayed.Len()) != n {
		t.Errorf("relayed %d bytes, reported %d", relayed.Len(), n)
	}
	if digest == "" {
		t.Error("a relayed artifact must report a digest so the destination's can be checked against it")
	}
	if bytes.Contains(relayed.Bytes(), []byte("relayed dump")) {
		t.Fatal("plaintext crossed the control plane")
	}
	rc, err := openArtifactReader(bytes.NewReader(relayed.Bytes()), identity)
	if err != nil {
		t.Fatalf("relayed artifact unreadable: %v", err)
	}
	defer rc.Close()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, payload) {
		t.Error("the relayed artifact did not round-trip")
	}
}

// ---------------------------------------------------------------------------
// The managed root is the agent's own, so every path may create it
// ---------------------------------------------------------------------------

// A write path must bring the MANAGED root into being on its own.
func TestResolveStoreRoot_managedRootIsCreatedByAWritePath(t *testing.T) {
	base := t.TempDir()
	s := New(filepath.Join(base, "stacks"), t.TempDir(), "/", base)
	managed := filepath.Join(base, "backups")
	if _, err := os.Stat(managed); !os.IsNotExist(err) {
		t.Fatalf("the store must not exist before the first call: %v", err)
	}

	root, err := s.resolveStoreRoot("", false) // false == a backup, not a check
	if err != nil {
		t.Fatalf("a write path must create the managed root: %v", err)
	}
	if root != managed {
		t.Errorf("root = %q, want %q", root, managed)
	}
	if _, err := os.Stat(filepath.Join(root, storeSentinel)); err != nil {
		t.Errorf("the managed root must be marked even when created by a write: %v", err)
	}
}

// A CUSTOM root keeps the old rule: only a check may mark one, because marking
// it IS the act of vetting it. A backup pointed at an unmarked path fails.
func TestResolveStoreRoot_customRootStillNeedsACheckToMarkIt(t *testing.T) {
	base := t.TempDir()
	s := New(filepath.Join(base, "stacks"), t.TempDir(), "/", base)
	custom := t.TempDir() // exists, empty, unmarked

	if _, err := s.resolveStoreRoot(custom, false); err == nil {
		t.Fatal("a write must refuse a custom root the agent has not marked")
	}
	if _, err := s.resolveStoreRoot(custom, true); err != nil {
		t.Fatalf("a check must mark an empty custom root: %v", err)
	}
	if _, err := s.resolveStoreRoot(custom, false); err != nil {
		t.Fatalf("once marked, a write must accept it: %v", err)
	}
}

// ---------------------------------------------------------------------------
// A bucket artifact is encrypted and hashed too
// ---------------------------------------------------------------------------

func TestDestinationFromBackup_s3CarriesTheRecipient(t *testing.T) {
	base := t.TempDir()
	s := New(filepath.Join(base, "stacks"), t.TempDir(), "/", base)
	recipient, _ := testKeypair(t)

	dest, err := destinationFromBackup(s, &pb.BackupRequest{
		Kind:         pb.BackupKind_BACKUP_KIND_PROJECT,
		S3:           &pb.S3Target{Bucket: "b", ObjectKey: "deplo/t/app/a/x.tar.gz.age"},
		AgeRecipient: recipient,
	}, nil)
	if err != nil {
		t.Fatalf("destinationFromBackup: %v", err)
	}
	// Without this the archive - which carries the app's whole decrypted env -
	// lands in the bucket in the clear.
	if dest.recipient != recipient {
		t.Errorf("an S3 destination must encrypt to %q, got %q", recipient, dest.recipient)
	}

	// A destination created before bucket encryption sends no recipient, and must
	// keep working rather than fail closed on an agent that now supports it.
	legacy, err := destinationFromBackup(s, &pb.BackupRequest{
		Kind: pb.BackupKind_BACKUP_KIND_PROJECT,
		S3:   &pb.S3Target{Bucket: "b", ObjectKey: "deplo/t/app/a/x.tar.gz"},
	}, nil)
	if err != nil {
		t.Fatalf("legacy destinationFromBackup: %v", err)
	}
	if legacy.recipient != "" {
		t.Errorf("a legacy S3 destination must stay plaintext, got recipient %q", legacy.recipient)
	}
}

func TestSourceFromRestore_s3CarriesTheIdentity(t *testing.T) {
	base := t.TempDir()
	s := New(filepath.Join(base, "stacks"), t.TempDir(), "/", base)
	_, identity := testKeypair(t)

	src, err := sourceFromRestore(s, &pb.RestoreRequest{
		Kind:        pb.BackupKind_BACKUP_KIND_PROJECT,
		S3:          &pb.S3Target{Bucket: "b", ObjectKey: "deplo/t/app/a/x.tar.gz.age"},
		AgeIdentity: identity,
	})
	if err != nil {
		t.Fatalf("sourceFromRestore: %v", err)
	}
	if src.identity != identity {
		t.Errorf("an S3 restore must decrypt with the identity, got %q", src.identity)
	}
}

// ---------------------------------------------------------------------------
// A restore trusts the control plane, not the artifact
// ---------------------------------------------------------------------------

// The artifact's own compose is what a restore used to execute.
func TestRestoreConfig_aProvenArchiveRestoresItsOwnConfig(t *testing.T) {
	// The whole point of the snapshot: a restore puts back the config the app was
	// running, not today's config wrapped around last month's volumes.
	archived := "services:\n  web:\n    image: app:0\n"
	rr := restoreConfig("blog", &pb.ProjectDescriptor{
		ComposeYaml: "services:\n  web:\n    image: app:2\n",
		EnvSnapshot: map[string]string{"FROM": "today"},
		Mounts:      []*pb.MountFile{{Path: "a.conf", Content: "today"}},
	}, projectSnapshot{
		compose: archived,
		env:     map[string]string{"FROM": "archive"},
		mounts:  []*pb.MountFile{{Path: "a.conf", Content: "archived"}},
	}, true, false)

	if rr.GetComposeYaml() != archived {
		t.Error("a proven archive must restore the config it captured")
	}
	if rr.GetEnv()["FROM"] != "archive" {
		t.Error("a proven archive must restore the env it captured")
	}
	if got := rr.GetMounts()[0].GetContent(); got != "archived" {
		t.Errorf("a proven archive must restore its own mounts: %q", got)
	}
}

// The artifact's own compose is what a restore executes.
func TestRestoreConfig_anUnprovenArchiveNeverWins(t *testing.T) {
	trusted := "services:\n  web:\n    image: app:1\n"
	hostile := "services:\n  web:\n    image: alpine\n    privileged: true\n    volumes: ['/:/host']\n"

	rr := restoreConfig("blog", &pb.ProjectDescriptor{
		ComposeYaml: trusted,
		EnvSnapshot: map[string]string{"FROM": "control-plane"},
		Mounts:      []*pb.MountFile{{Path: "a.conf", Content: "trusted"}},
	}, projectSnapshot{
		compose: hostile,
		env:     map[string]string{"FROM": "archive"},
		mounts:  []*pb.MountFile{{Path: "a.conf", Content: "hostile"}},
	}, false, false)

	if rr.GetComposeYaml() != trusted {
		t.Error("an unverifiable archive must never reach `docker compose up`")
	}
	if rr.GetEnv()["FROM"] != "control-plane" {
		t.Error("an unverifiable archive's env must not win either")
	}
	if got := rr.GetMounts()[0].GetContent(); got != "trusted" {
		t.Errorf("an unverifiable archive's mounts must not win: %q", got)
	}
	if rr.GetSlug() != "blog" {
		t.Errorf("slug = %q", rr.GetSlug())
	}
}

// Either way, the archive is the fallback when the control plane sends nothing -
// which is what keeps a restore working for a config that no longer exists.
func TestRestoreConfig_fallsBackToTheArchive(t *testing.T) {
	archived := "services:\n  web:\n    image: app:0\n"
	for _, proven := range []bool{true, false} {
		rr := restoreConfig("blog", &pb.ProjectDescriptor{}, projectSnapshot{
			compose: archived,
			env:     map[string]string{"FROM": "archive"},
			mounts:  []*pb.MountFile{{Path: "a.conf", Content: "archived"}},
		}, proven, false)
		if rr.GetComposeYaml() != archived {
			t.Errorf("proven=%v: with nothing from the control plane, use the archive", proven)
		}
		if rr.GetEnv()["FROM"] != "archive" {
			t.Errorf("proven=%v: same for the env", proven)
		}
		if got := rr.GetMounts()[0].GetContent(); got != "archived" {
			t.Errorf("proven=%v: same for the mounts: %q", proven, got)
		}
	}
}

// ---------------------------------------------------------------------------
// An artifact has to be the one the control plane wrote
// ---------------------------------------------------------------------------

// The pre-emptive shape: a store artifact is a local file, so a tampered one is
// caught before the caller stops a stack or wipes a single volume for it.
func TestVerifyStoreDigest_catchesATamperedArtifactUpFront(t *testing.T) {
	s, root := newStoreService(t)
	recipient, identity := testKeypair(t)
	key := "deplo/team_a/app/prj_1/20260101T000000Z-brun_1.tar.gz.age"

	dest, err := destinationFromBackup(s, &pb.BackupRequest{
		Kind:         pb.BackupKind_BACKUP_KIND_PROJECT,
		Store:        &pb.StoreTarget{ObjectKey: key},
		AgeRecipient: recipient,
	}, nil)
	if err != nil {
		t.Fatalf("destinationFromBackup: %v", err)
	}
	written, err := s.writeArtifact(context.Background(), dest, func(w io.Writer) error {
		_, werr := w.Write([]byte("the real backup"))
		return werr
	})
	if err != nil {
		t.Fatalf("writeArtifact: %v", err)
	}
	digest := written.digest

	if verr := verifyStoreDigest(root, key, digest); verr != nil {
		t.Errorf("an untouched artifact must verify: %v", verr)
	}
	if verr := verifyStoreDigest(root, key, ""); verr != nil {
		t.Errorf("a run with no recorded digest must still restore: %v", verr)
	}

	// A compromised storage host can forge one: it holds the recipient, which is a
	// PUBLIC key it is handed on every backup. age proves confidentiality, not who
	// wrote the file, so the digest is the only thing standing here.
	forged, err := age.ParseX25519Recipient(recipient)
	if err != nil {
		t.Fatalf("parse recipient: %v", err)
	}
	var buf bytes.Buffer
	enc, err := age.Encrypt(&buf, forged)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	_, _ = enc.Write([]byte("services:\n  x:\n    privileged: true\n"))
	_ = enc.Close()
	if werr := os.WriteFile(filepath.Join(root, key), buf.Bytes(), storeFilePerm); werr != nil {
		t.Fatalf("plant forged artifact: %v", werr)
	}
	if verr := verifyStoreDigest(root, key, digest); verr == nil {
		t.Fatal("a forged artifact must be refused, even though it decrypts fine")
	}
	// It really does decrypt: the refusal is the digest's doing, nothing else.
	if _, oerr := openArtifactReader(bytes.NewReader(buf.Bytes()), identity); oerr == nil {
		t.Log("confirmed: the forged artifact is valid age, only the digest catches it")
	}
}

// The streaming shape: an S3 object or a relayed artifact can only be hashed as
// it goes past, so the verdict lands at the end - which for a project is still
// before the stack configuration is re-applied.
func TestVerifyingReader_settlesOnlyOnFinish(t *testing.T) {
	payload := bytes.Repeat([]byte("artifact"), 1024)
	sum := sha256.Sum256(payload)
	good := hex.EncodeToString(sum[:])

	v := &verifyingReader{r: bytes.NewReader(payload), sum: sha256.New(), expected: good}
	if _, err := io.ReadAll(v); err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := v.finish(); err != nil {
		t.Errorf("an untouched stream must verify: %v", err)
	}

	// gzip and age stop at their own trailer rather than at EOF, so finish() has
	// to drain the rest itself - a reader that only checked on io.EOF would never
	// fire at all.
	partial := &verifyingReader{r: bytes.NewReader(payload), sum: sha256.New(), expected: good}
	if _, err := io.CopyN(io.Discard, partial, 64); err != nil {
		t.Fatalf("partial read: %v", err)
	}
	if err := partial.finish(); err != nil {
		t.Errorf("finish must hash the undrained tail too: %v", err)
	}

	bad := &verifyingReader{r: bytes.NewReader(append(payload, '!')), sum: sha256.New(), expected: good}
	_, _ = io.ReadAll(bad)
	if err := bad.finish(); err == nil {
		t.Error("a stream that does not match its recorded digest must be refused")
	}
}

// verify() is also what promotes a streaming source to "proven", which is what
// lets its own configuration snapshot be restored rather than discarded.
func TestArtifactSource_verifyMarksTheSourceProven(t *testing.T) {
	payload := []byte("artifact")
	sum := sha256.Sum256(payload)
	src := &artifactSource{
		verifier: &verifyingReader{
			r:        bytes.NewReader(payload),
			sum:      sha256.New(),
			expected: hex.EncodeToString(sum[:]),
		},
	}
	if src.integrityProven {
		t.Fatal("nothing is proven before it has been checked")
	}
	if err := src.verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !src.integrityProven {
		t.Error("a stream that matched its digest is proven")
	}

	failing := &artifactSource{
		verifier: &verifyingReader{
			r:        bytes.NewReader([]byte("something else")),
			sum:      sha256.New(),
			expected: hex.EncodeToString(sum[:]),
		},
	}
	if err := failing.verify(); err == nil {
		t.Fatal("a mismatch must error")
	}
	if failing.integrityProven {
		t.Error("a mismatch must never mark the source proven")
	}
}

// ---------------------------------------------------------------------------
// ReadStoreFile - the two shapes an artifact can be read from
// ---------------------------------------------------------------------------

// fakeBucket serves ONE object over plain http, ignoring the signature: what is
// under test is this agent's plumbing, not minio-go's signing. Path-style, so
// the object is at /<bucket>/<key>.
func fakeBucket(t *testing.T, key string, body []byte) *pb.S3Target {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, key) {
			http.NotFound(w, r)
			return
		}
		// A real modtime, not the zero value: ServeContent omits Last-Modified for
		// a zero time and minio-go refuses a response without one.
		modtime := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
		http.ServeContent(w, r, path.Base(key), modtime, bytes.NewReader(body))
	}))
	t.Cleanup(srv.Close)
	return &pb.S3Target{
		Endpoint:  srv.URL,
		Region:    "us-east-1",
		Bucket:    "deplo-test",
		AccessKey: "k",
		SecretKey: "s",
		ObjectKey: key,
		PathStyle: true,
		// httptest listens on 127.0.0.1, which the SSRF guard refuses by default -
		// the same flag a self-hosted bucket on the operator's own network needs.
		AllowPrivateEndpoint: true,
	}
}

// encryptedArtifact builds what a backup actually writes: gzip inside age.
func encryptedArtifact(t *testing.T, recipient string, payload []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	aw, err := newArtifactWriter(&buf, recipient)
	if err != nil {
		t.Fatalf("newArtifactWriter: %v", err)
	}
	if _, err := aw.Writer().Write(payload); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	if err := aw.Close(); err != nil {
		t.Fatalf("close artifact: %v", err)
	}
	return buf.Bytes()
}

// The download case for a BUCKET artifact: decrypted on the way out, and
// deliberately still gzip. Handing over a bare tar would be a different file
// from the one the panel promises.
func TestReadStoreFile_s3DecryptsButDoesNotDecompress(t *testing.T) {
	s, _ := newStoreService(t)
	recipient, identity := testKeypair(t)
	payload := bytes.Repeat([]byte("bucket artifact payload\n"), 5_000)
	artifact := encryptedArtifact(t, recipient, payload)
	sum := sha256.Sum256(artifact)

	stream := &fakeReadStoreStream{}
	if err := s.ReadStoreFile(&pb.ReadStoreFileRequest{
		S3:             fakeBucket(t, "deplo/team_a/app/prj_1/x.tar.gz.age", artifact),
		AgeIdentity:    identity,
		ExpectedSha256: hex.EncodeToString(sum[:]),
	}, stream); err != nil {
		t.Fatalf("ReadStoreFile: %v", err)
	}

	got := stream.buf.Bytes()
	if len(got) < 2 || got[0] != 0x1f || got[1] != 0x8b {
		t.Fatal("what leaves must still be gzip: the user asked for the .tar.gz")
	}
	gz, err := gzip.NewReader(bytes.NewReader(got))
	if err != nil {
		t.Fatalf("gunzip what was streamed: %v", err)
	}
	unpacked, err := io.ReadAll(gz)
	if err != nil {
		t.Fatalf("read gunzipped: %v", err)
	}
	if !bytes.Equal(unpacked, payload) {
		t.Errorf("round trip mismatch: %d bytes, want %d", len(unpacked), len(payload))
	}
}

// No identity is the RELAY shape, and it must stay verbatim: the control plane
// passing a bucket artifact to another host has no business seeing plaintext.
func TestReadStoreFile_s3WithoutIdentityStaysCiphertext(t *testing.T) {
	s, _ := newStoreService(t)
	recipient, _ := testKeypair(t)
	artifact := encryptedArtifact(t, recipient, []byte("secret env inside"))

	stream := &fakeReadStoreStream{}
	if err := s.ReadStoreFile(&pb.ReadStoreFileRequest{
		S3: fakeBucket(t, "deplo/team_a/db/db_1/x.dump.gz.age", artifact),
	}, stream); err != nil {
		t.Fatalf("ReadStoreFile: %v", err)
	}
	if !bytes.Equal(stream.buf.Bytes(), artifact) {
		t.Error("a read with no identity must hand back exactly what was stored")
	}
	if bytes.Contains(stream.buf.Bytes(), []byte("secret env inside")) {
		t.Error("plaintext must never appear in the relay shape")
	}
}

// The asymmetry, pinned in both directions. A bucket object cannot be hashed before it
// is fetched, so its verdict lands at the END - after bytes have already gone.
func TestReadStoreFile_s3DigestFailsOnlyAfterTheBytesAreGone(t *testing.T) {
	s, _ := newStoreService(t)
	recipient, identity := testKeypair(t)
	artifact := encryptedArtifact(t, recipient, bytes.Repeat([]byte("payload\n"), 5_000))
	wrong := sha256.Sum256([]byte("a different artifact"))

	stream := &fakeReadStoreStream{}
	err := s.ReadStoreFile(&pb.ReadStoreFileRequest{
		S3:             fakeBucket(t, "deplo/team_a/app/prj_1/x.tar.gz.age", artifact),
		AgeIdentity:    identity,
		ExpectedSha256: hex.EncodeToString(wrong[:]),
	}, stream)
	if err == nil {
		t.Fatal("an object that does not match its recorded digest must be refused")
	}
	if stream.buf.Len() == 0 {
		t.Error("this is the shape that CANNOT refuse up front; if it now can, say so here")
	}
}

// The other half of the asymmetry: an artifact on this host's disk is hashed
// before a single byte leaves, so it really can refuse.
func TestReadStoreFile_storeDigestRefusesBeforeAnyBytes(t *testing.T) {
	s, root := newStoreService(t)
	recipient, identity := testKeypair(t)
	key := "deplo/team_a/app/prj_1/20260101T000000Z-brun_1.tar.gz.age"
	if err := os.MkdirAll(filepath.Dir(filepath.Join(root, key)), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	artifact := encryptedArtifact(t, recipient, []byte("on this disk"))
	if err := os.WriteFile(filepath.Join(root, key), artifact, storeFilePerm); err != nil {
		t.Fatalf("plant artifact: %v", err)
	}
	wrong := sha256.Sum256([]byte("a different artifact"))

	stream := &fakeReadStoreStream{}
	err := s.ReadStoreFile(&pb.ReadStoreFileRequest{
		Store:          &pb.StoreTarget{ObjectKey: key},
		AgeIdentity:    identity,
		ExpectedSha256: hex.EncodeToString(wrong[:]),
	}, stream)
	if err == nil {
		t.Fatal("a tampered artifact on disk must be refused")
	}
	if stream.buf.Len() != 0 {
		t.Error("the store shape must refuse BEFORE it streams anything")
	}
}

func TestReadStoreFile_namingNoArtifactIsRefused(t *testing.T) {
	s, _ := newStoreService(t)
	if err := s.ReadStoreFile(&pb.ReadStoreFileRequest{}, &fakeReadStoreStream{}); err == nil {
		t.Fatal("a request naming neither a store nor a bucket must be refused")
	}
}

// An UPLOADED artifact contributes data and nothing else.
func TestRestoreConfig_anUntrustedArchiveNeverConfiguresAnything(t *testing.T) {
	hostile := "services:\n  x:\n    image: alpine\n    privileged: true\n    volumes: ['/:/host']\n"

	// Nothing from the control plane, which is exactly when the fallback fired.
	rr := restoreConfig("blog", &pb.ProjectDescriptor{}, projectSnapshot{
		compose: hostile,
		env:     map[string]string{"LD_PRELOAD": "/tmp/evil.so"},
		mounts:  []*pb.MountFile{{Path: "a.conf", Content: "hostile"}},
	}, false, true)

	if rr.GetComposeYaml() != "" {
		t.Error("an uploaded archive must never reach `docker compose up`")
	}
	if len(rr.GetEnv()) != 0 {
		t.Errorf("an uploaded archive must not choose the environment either: %v", rr.GetEnv())
	}
	if len(rr.GetMounts()) != 0 {
		t.Errorf("nor the config files: %v", rr.GetMounts())
	}

	// And it must not be able to displace a control plane that DID send config.
	trusted := "services:\n  web:\n    image: app:1\n"
	rr = restoreConfig("blog", &pb.ProjectDescriptor{
		ComposeYaml: trusted,
		EnvSnapshot: map[string]string{"FROM": "control-plane"},
	}, projectSnapshot{
		compose: hostile,
		env:     map[string]string{"FROM": "archive"},
	}, false, true)
	if rr.GetComposeYaml() != trusted || rr.GetEnv()["FROM"] != "control-plane" {
		t.Error("the control plane's own config must still be what comes back up")
	}
}

// TestWriteArtifact_decryptedSizeIsWhatADownloadDelivers pins the number the download's
// Content-Length is built from.
func TestWriteArtifact_decryptedSizeIsWhatADownloadDelivers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		encrypt bool
		payload []byte
	}{
		// Several chunk boundaries: age frames at 64 KiB, so a payload spanning
		// more than one chunk is where a per-chunk tag would be miscounted.
		{"encrypted, multi-chunk", true, bytes.Repeat([]byte("volume bytes\n"), 40_000)},
		{"encrypted, tiny", true, []byte("x")},
		{"unencrypted legacy bucket", false, bytes.Repeat([]byte("dump\n"), 1_000)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, root := newStoreService(t)
			recipient, identity := testKeypair(t)
			if !tc.encrypt {
				recipient, identity = "", ""
			}
			key := "deplo/team_a/app/prj_1/20260101T000000Z-brun_1.tar.gz"
			dest := &artifactDestination{
				store:     &pb.StoreTarget{Root: root, ObjectKey: key},
				recipient: recipient,
				key:       key,
			}
			written, err := s.writeArtifact(context.Background(), dest, func(w io.Writer) error {
				_, werr := w.Write(tc.payload)
				return werr
			})
			if err != nil {
				t.Fatalf("writeArtifact: %v", err)
			}

			raw, err := os.ReadFile(filepath.Join(root, key))
			if err != nil {
				t.Fatalf("read artifact: %v", err)
			}
			var delivered []byte
			if identity == "" {
				delivered = raw
			} else {
				id, perr := age.ParseX25519Identity(identity)
				if perr != nil {
					t.Fatalf("parse identity: %v", perr)
				}
				dec, derr := age.Decrypt(bytes.NewReader(raw), id)
				if derr != nil {
					t.Fatalf("decrypt: %v", derr)
				}
				delivered, err = io.ReadAll(dec)
				if err != nil {
					t.Fatalf("read decrypted: %v", err)
				}
			}
			if int64(len(delivered)) != written.decryptedSize {
				t.Errorf("a download delivers %d bytes but the run would advertise %d",
					len(delivered), written.decryptedSize)
			}
			if !tc.encrypt && written.decryptedSize != written.size {
				t.Errorf("with no age layer the two sizes must agree: stored %d, decrypted %d",
					written.size, written.decryptedSize)
			}
			if tc.encrypt && written.decryptedSize >= written.size {
				t.Errorf("the age layer only adds bytes: stored %d, decrypted %d",
					written.size, written.decryptedSize)
			}
		})
	}
}
