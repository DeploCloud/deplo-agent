package server

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"filippo.io/age"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/s3client"
)

// backup_store.go is the SECOND destination shape for a backup artifact: a
// directory on this host, instead of an S3 bucket. It exists because demanding a
// bucket before anyone can take a first backup pushes the user to stand up
// infrastructure they do not have, when the VPS they already pay for has a disk.
//
// It deliberately lives in `internal/server` rather than a sibling package of
// s3client, for one reason: the containment guard it needs already exists here.
// `resolveInside` / `normalizeRel` (files.go) reject "..", reject absolute paths,
// and realpath-check the nearest existing ancestor so a planted symlink cannot
// escape. A fresh package would have re-derived that guard, and the obvious
// re-derivation is wrong — `safepath.Inside` returns the BASE on every failure
// path, so `os.RemoveAll(safepath.Inside(root, missingPrefix))` deletes the whole
// root, which is exactly the idempotent already-deleted case a retention sweep
// hits every night.
//
// THREE properties hold the security model together, and none is optional:
//
//  1. The ROOT is not trusted. It arrives off the wire, and the agent runs as
//     root. Empty means the agent's own managed store; anything else must carry a
//     sentinel file the agent itself wrote (see resolveStoreRoot). A typo'd
//     "/var/lib/docker" therefore fails closed instead of becoming a remote
//     `rm -rf` the first time retention prunes.
//  2. The KEY is contained. It is the control plane's
//     deplo/<teamId>/<kind>/<targetId>/… key, resolved through resolveInside. The
//     team segment is what keeps two teams sharing one storage host out of each
//     other's artifacts.
//  3. The ARTIFACT is encrypted, always, and the key to read it never lives here.
//     A backup is written to an age RECIPIENT (a public key); only a restore
//     carries the identity. A compromised storage box yields ciphertext.

const (
	// storeSentinel marks a directory as "the agent put backups here". Written by
	// StoreCheck on an empty or already-marked directory, and REQUIRED on any
	// custom root before a write or a delete. This is the whole difference
	// between a user-supplied path and an arbitrary-path deleter.
	storeSentinel = ".deplo-backups"
	// storePartialSuffix names an artifact still being written. An S3 multipart
	// PUT never exposes a partial object; a file write does, and the control
	// plane's retention cannot clean one up (a failed run owns no object, so no
	// delete is ever issued for it). So writes land here, fsync, and rename —
	// and a leftover is swept by the next check.
	storePartialSuffix = ".partial"
	// storePartialStaleAfter is how long a `.partial` must have been UNTOUCHED
	// before a sweep may remove it. An in-flight write touches its file
	// continuously, the agent caps a dump at 30 minutes, and the control plane's
	// deadline is an hour — so an hour of silence means nobody is writing it.
	storePartialStaleAfter = time.Hour
	// storeChunkBytes is the payload size of one StoreChunk data frame, matching
	// volumecopy.go's chunkBytes: comfortably under the gRPC max message size,
	// big enough that framing overhead is noise on a multi-GB artifact.
	storeChunkBytes = 1 << 20 // 1 MiB
	// storeDirPerm / storeFilePerm keep artifacts readable only by root. A backup
	// is a full copy of the workload's data; it must not be world-readable just
	// because it sits on a filesystem rather than behind bucket credentials.
	storeDirPerm  os.FileMode = 0o700
	storeFilePerm os.FileMode = 0o600
)

// managedStoreRoot is the store the agent owns outright: a sibling of --stack-dir
// under the host data root, so it lands on the same layout the control plane
// already assumes (/data/stacks -> /data/backups). This is the only root a
// non-admin can produce, and the only one the agent will create on demand.
func (s *Service) managedStoreRoot() string {
	return filepath.Join(s.dataBase, "backups")
}

// resolveStoreRoot turns the wire's `root` into an absolute path this agent is
// willing to write to and delete under, or an error explaining why not.
//
// `create` gates the CUSTOM-root path only: an operator-supplied directory is
// marked with the sentinel exactly once, by a check, because that marking is the
// act of vetting it. A backup or a delete must never mark a fresh path it was
// merely pointed at.
//
// The MANAGED root is created on demand by every path, and that asymmetry is the
// point: this agent derives that path itself from --stack-dir, so there is
// nothing to vet and nothing an caller can influence. Gating it behind a check
// made the platform's own default destination — the one seeded for every team so
// that backups work with no configuration at all — fail its first run with "test
// the destination first". Worse, the only thing that ran a check was a mutation
// requiring `manage_backup_destinations`, so a member holding `manage_backups`
// alone could not get out of it: the fix for their backup was a permission they
// were deliberately not given.
func (s *Service) resolveStoreRoot(root string, create bool) (string, error) {
	managed := s.managedStoreRoot()
	if strings.TrimSpace(root) == "" {
		if err := os.MkdirAll(managed, storeDirPerm); err != nil {
			return "", status.Errorf(codes.Internal, "create backup store %q: %v", managed, err)
		}
		if err := writeStoreSentinel(managed); err != nil {
			return "", err
		}
		return managed, nil
	}

	root = filepath.Clean(root)
	if !filepath.IsAbs(root) {
		return "", status.Errorf(codes.InvalidArgument, "backup store path %q must be absolute", root)
	}
	if root == managed {
		return s.resolveStoreRoot("", create)
	}
	st, err := os.Stat(root)
	if err != nil {
		return "", status.Errorf(codes.FailedPrecondition, "backup store path %q does not exist on this server", root)
	}
	if !st.IsDir() {
		return "", status.Errorf(codes.InvalidArgument, "backup store path %q is not a directory", root)
	}
	// The sentinel rule. A custom root is only ever accepted once the agent has
	// marked it, and it only marks a directory that is empty (or already marked).
	// Without this, "delete every object under this prefix" against a mistyped
	// path is a remote wipe of whatever lives there.
	if _, err := os.Stat(filepath.Join(root, storeSentinel)); err != nil {
		if !create {
			return "", status.Errorf(codes.FailedPrecondition,
				"backup store path %q is not initialized for Deplo; test the destination to set it up", root)
		}
		empty, eerr := dirIsEmpty(root)
		if eerr != nil {
			return "", status.Errorf(codes.Internal, "read backup store path %q: %v", root, eerr)
		}
		if !empty {
			return "", status.Errorf(codes.FailedPrecondition,
				"backup store path %q is not empty and holds no Deplo backups; point it at an empty directory", root)
		}
		if err := writeStoreSentinel(root); err != nil {
			return "", err
		}
	}
	return root, nil
}

func writeStoreSentinel(root string) error {
	p := filepath.Join(root, storeSentinel)
	if _, err := os.Stat(p); err == nil {
		return nil
	}
	body := "This directory holds Deplo backup artifacts. Deplo will create, read and\n" +
		"delete files under it. Do not point a Deplo backup destination at a\n" +
		"directory whose contents you want to keep.\n"
	if err := os.WriteFile(p, []byte(body), storeFilePerm); err != nil {
		return status.Errorf(codes.Internal, "mark backup store %q: %v", root, err)
	}
	return nil
}

func dirIsEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()
	names, err := f.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return len(names) == 0, nil
}

// storeKeyPath resolves an object key under an ALREADY-RESOLVED root. Separate
// from resolveStoreRoot so the root rules are applied exactly once per RPC and a
// caller cannot accidentally skip them by joining a key itself.
func storeKeyPath(root, key string) (string, error) {
	if strings.TrimSpace(key) == "" {
		return "", status.Error(codes.InvalidArgument, "a backup object key is required")
	}
	abs, _, err := resolveParentInside(root, key)
	if err != nil {
		return "", err
	}
	return abs, nil
}

// ---------------------------------------------------------------------------
// Store I/O — the four verbs that mirror s3client's surface
// ---------------------------------------------------------------------------

// storeWrite streams `r` to <root>/<key>, atomically: the bytes land in a
// `.partial` sibling, are fsynced, and only then renamed onto the real key.
// Returns the byte count and the hex sha256 — on a filesystem there is no ETag,
// so this pair is the only durable proof of what was written.
func storeWrite(root, key string, r io.Reader, overwrite bool) (int64, string, error) {
	dst, err := storeKeyPath(root, key)
	if err != nil {
		return 0, "", err
	}
	if !overwrite {
		if _, serr := os.Stat(dst); serr == nil {
			return 0, "", status.Errorf(codes.AlreadyExists, "a backup artifact already exists at %q", key)
		}
	}
	if err := os.MkdirAll(filepath.Dir(dst), storeDirPerm); err != nil {
		return 0, "", fmt.Errorf("create backup directory: %w", err)
	}
	tmp := dst + storePartialSuffix
	// O_NOFOLLOW: resolveInside realpath-checks every EXISTING component, but the
	// leaf is joined lexically because it usually does not exist yet — so a symlink
	// planted at exactly this name is the one thing left that could redirect the
	// write. Anyone who can plant it already has the host, so this is depth rather
	// than the barrier; it costs a flag.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|syscall.O_NOFOLLOW, storeFilePerm)
	if err != nil {
		return 0, "", fmt.Errorf("open backup artifact: %w", err)
	}
	// Remove the temp on EVERY error path. A leftover .partial is swept by the
	// next check, but not leaving one in the first place is cheaper.
	committed := false
	defer func() {
		if !committed {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	sum := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, sum), r)
	if err != nil {
		return 0, "", fmt.Errorf("write backup artifact: %w", err)
	}
	// fsync BEFORE the rename: a rename is atomic in the directory entry, but it
	// says nothing about the data blocks. Without this a host that loses power
	// mid-backup can come back with a full-size artifact of zeroes at a key the
	// control plane believes is good, and a restore would hand it to the user.
	if err := f.Sync(); err != nil {
		return 0, "", fmt.Errorf("flush backup artifact: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, "", fmt.Errorf("close backup artifact: %w", err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		return 0, "", fmt.Errorf("commit backup artifact: %w", err)
	}
	committed = true
	return n, hex.EncodeToString(sum.Sum(nil)), nil
}

// storeOpen opens an artifact for streaming read. The caller closes it.
func storeOpen(root, key string) (io.ReadCloser, error) {
	src, err := storeKeyPath(root, key)
	if err != nil {
		return nil, err
	}
	// O_NOFOLLOW for the same reason storeWrite uses it: the leaf is the one
	// component resolveInside cannot realpath-check, and reading through a symlink
	// planted there would stream a file that is not a backup out to whoever asked.
	f, err := os.OpenFile(src, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "no backup artifact at %q on this server", key)
		}
		return nil, fmt.Errorf("open backup artifact: %w", err)
	}
	return f, nil
}

// storeDeleteOne removes one artifact by exact key. Idempotent, like S3 DELETE:
// removing a missing object returns 0, not an error.
func storeDeleteOne(root, key string) (int64, error) {
	p, err := storeKeyPath(root, key)
	if err != nil {
		return 0, err
	}
	if err := os.Remove(p); err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("delete backup artifact: %w", err)
	}
	pruneEmptyDirs(root, filepath.Dir(p))
	return 1, nil
}

// storeDeletePrefix removes every artifact under a key prefix — one target's
// whole folder, for retention and delete-with-artifacts.
//
// The prefix is treated as a DIRECTORY, never as a string match, and a prefix
// that resolves to the root itself is REFUSED. Both matter: the control plane's
// prefixes are always deplo/<teamId>/<kind>/<targetId>/, so a resolved value of
// root means the key was empty or degenerate, and honouring it would wipe every
// team's backups on this host.
func storeDeletePrefix(root, prefix string) (int64, error) {
	norm, err := normalizeRel(prefix)
	if err != nil {
		return 0, err
	}
	if norm == "" {
		return 0, status.Error(codes.InvalidArgument, "refusing to delete the whole backup store")
	}
	dir, err := resolveInside(root, norm)
	if err != nil {
		return 0, err
	}
	if dir == canonicalRoot(root) {
		return 0, status.Error(codes.InvalidArgument, "refusing to delete the whole backup store")
	}
	st, serr := os.Stat(dir)
	if serr != nil {
		if os.IsNotExist(serr) {
			return 0, nil // already gone — idempotent, same as S3
		}
		return 0, fmt.Errorf("read backup prefix: %w", serr)
	}
	if !st.IsDir() {
		// A prefix that names a single file: delete just it.
		if err := os.Remove(dir); err != nil {
			return 0, fmt.Errorf("delete backup artifact: %w", err)
		}
		pruneEmptyDirs(root, filepath.Dir(dir))
		return 1, nil
	}
	var n int64
	err = filepath.WalkDir(dir, func(_ string, d os.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("count backup artifacts: %w", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		return 0, fmt.Errorf("delete backup artifacts: %w", err)
	}
	pruneEmptyDirs(root, filepath.Dir(dir))
	return n, nil
}

// pruneEmptyDirs walks back up from `dir` removing now-empty directories, so a
// deleted target does not leave an empty deplo/<teamId>/<kind>/<targetId>/ tree
// behind for the operator to wonder about. Stops at the root and never touches it.
func pruneEmptyDirs(root, dir string) {
	base := canonicalRoot(root)
	for dir != base && strings.HasPrefix(dir, base+string(os.PathSeparator)) {
		if err := os.Remove(dir); err != nil {
			return // not empty (or not ours) — stop
		}
		dir = filepath.Dir(dir)
	}
}

// sweepPartials removes `.partial` artifacts left by an interrupted write. Runs
// on every check, which is the one moment the operator is already looking at the
// destination and a surprise reclaim is welcome rather than alarming.
//
// It skips anything written RECENTLY, and that guard is not optional. The
// managed root is shared by every destination and every team on the host, and a
// check is triggered by something as ordinary as opening the destination
// dropdown — which fires a live probe of every destination. Without the guard,
// one person opening a picker deletes the temp file of a backup another team has
// been streaming for twenty minutes, and the write dies on its final rename,
// after the whole dump, with "no such file or directory".
//
// An in-flight write advances the file's mtime continuously (io.Copy), so quiet
// for storePartialStaleAfter means nothing is writing it: the agent caps a dump
// at 30 minutes and the control plane's RPC deadline is an hour, so an hour of
// silence is dead by any measure.
func sweepPartials(root string) int {
	n := 0
	cutoff := time.Now().Add(-storePartialStaleAfter)
	_ = filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable subtree: skip, never fail the check
		}
		if d.IsDir() || !strings.HasSuffix(p, storePartialSuffix) {
			return nil
		}
		info, ierr := d.Info()
		// Unreadable stat: leave it. Deleting a file we know nothing about is
		// the wrong side to err on when the alternative is a wasted gigabyte.
		if ierr != nil || info.ModTime().After(cutoff) {
			return nil
		}
		if os.Remove(p) == nil {
			n++
		}
		return nil
	})
	return n
}

// storeFreeBytes reports the filesystem headroom at `root`. Deliberately NOT a
// pre-flight gate: a dump's size is unknown until it exists (the S3 upload is
// called with size -1 for the same reason), so this is information for the
// operator and ENOSPC on the write is the real guard.
func storeFreeBytes(root string) (free, total int64) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		return 0, 0
	}
	bsize := int64(st.Bsize)
	return int64(st.Bavail) * bsize, int64(st.Blocks) * bsize
}

// ---------------------------------------------------------------------------
// age — the encryption layer, applied in the SOURCE pipeline
// ---------------------------------------------------------------------------
//
// Encryption sits next to gzip in the producer, NOT inside the store. That
// placement is the whole reason a relayed backup is safe: the artifact is
// already ciphertext when it leaves this host, so the control plane relaying it
// to another server never holds plaintext, and one integration covers both the
// same-host and the cross-host path.
//
// age's STREAM is 64 KiB ChaCha20-Poly1305 chunks — constant memory, fine for a
// multi-GB artifact, and `age -d -i key.txt` reads the result without Deplo,
// which is what makes an encrypted backup still a backup after a control-plane
// loss. Hand-rolling chunked AEAD instead would be ~150 lines whose classic
// failure mode is a truncation attack nobody notices until a restore.

// artifactWriter is the producer-side chain: callers write plaintext into
// Writer(), it is gzipped, optionally age-encrypted, and lands in the sink.
type artifactWriter struct {
	gz  io.WriteCloser
	age io.WriteCloser // nil when the destination takes plaintext (S3)
}

// newArtifactWriter builds the chain over `sink`. An empty recipient yields a
// plaintext (gzip-only) artifact, which is correct ONLY for S3; every store and
// relay path validates the recipient before calling this.
func newArtifactWriter(sink io.Writer, recipient string) (*artifactWriter, error) {
	a := &artifactWriter{}
	w := sink
	if recipient != "" {
		r, err := age.ParseX25519Recipient(recipient)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid backup encryption key: %v", err)
		}
		enc, err := age.Encrypt(sink, r)
		if err != nil {
			return nil, fmt.Errorf("start encryption: %w", err)
		}
		a.age = enc
		w = enc
	}
	a.gz = gzip.NewWriter(w)
	return a, nil
}

// Writer is what the dump producer writes into.
func (a *artifactWriter) Writer() io.Writer { return a.gz }

// Close finishes the chain in the ONE order that produces a readable artifact:
// gzip's trailer first, then age's final-chunk marker. Skipping the age Close
// yields a file that decrypts perfectly until the last 64 KiB and then fails —
// silent corruption discovered at restore time, months later. The first error
// wins so the caller reports the cause rather than the consequence.
func (a *artifactWriter) Close() error {
	err := a.gz.Close()
	if a.age != nil {
		if cerr := a.age.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// openArtifactReader is the consumer-side chain: age-decrypt (when an identity is
// given), then gunzip. The returned Close closes the gzip reader; the caller
// still closes whatever underlying source it opened.
func openArtifactReader(src io.Reader, identity string) (io.ReadCloser, error) {
	r := src
	if identity != "" {
		id, err := age.ParseX25519Identity(identity)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid backup decryption key: %v", err)
		}
		dec, err := age.Decrypt(src, id)
		if err != nil {
			return nil, fmt.Errorf("decrypt backup (is this the right recovery key?): %w", err)
		}
		r = dec
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("open gzip stream (is this a Deplo backup?): %w", err)
	}
	return gz, nil
}

// ---------------------------------------------------------------------------
// artifactSource / artifactSink — where a backup goes, where a restore reads
// ---------------------------------------------------------------------------

// artifactSource names where a restore reads its artifact, so restoreDatabase /
// restoreProject / restoreRedis / restoreClickhouse stay identical whether the
// bytes come from S3, from this host's disk, or from a RestoreFrom relay.
type artifactSource struct {
	s3    *pb.S3Target
	store *pb.StoreTarget
	// identity decrypts a store artifact (and a relayed one). Empty for S3.
	identity string
	// stream, when set, IS the artifact — the cross-host RestoreFrom case, where
	// there is no destination on this host to open.
	stream io.Reader
	// label describes the source in log lines ("s3://bucket/key", a path, or
	// "the control plane"), so an operator can tell where a restore read from.
	label string
}

func sourceFromRestore(s *Service, req *pb.RestoreRequest) (*artifactSource, error) {
	switch {
	case req.GetStore() != nil:
		// Argument checks before any filesystem work, so a caller that forgot the
		// key is told THAT rather than something incidental about the root.
		if req.GetAgeIdentity() == "" {
			return nil, status.Error(codes.InvalidArgument,
				"restoring from a server store needs its recovery key")
		}
		root, err := s.resolveStoreRoot(req.GetStore().GetRoot(), false)
		if err != nil {
			return nil, err
		}
		return &artifactSource{
			store:    &pb.StoreTarget{Root: root, ObjectKey: req.GetStore().GetObjectKey()},
			identity: req.GetAgeIdentity(),
			label:    filepath.Join(root, req.GetStore().GetObjectKey()),
		}, nil
	case req.GetS3() != nil && req.GetS3().GetObjectKey() != "":
		// The identity rides an S3 restore too. Empty means a LEGACY artifact,
		// written before bucket artifacts were encrypted, and openArtifactReader
		// then skips the age layer — which is why old object keys keep restoring.
		return &artifactSource{
			s3:       req.GetS3(),
			identity: req.GetAgeIdentity(),
			label:    req.GetS3().GetObjectKey(),
		}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "restore request names no artifact to restore from")
	}
}

// open returns the decompressed, decrypted artifact stream plus a close func
// that tears down every layer the source opened.
func (a *artifactSource) open(ctx context.Context) (io.Reader, func(), error) {
	var (
		raw    io.Reader
		closes []func()
	)
	switch {
	case a.stream != nil:
		raw = a.stream
	case a.store != nil:
		f, err := storeOpen(a.store.GetRoot(), a.store.GetObjectKey())
		if err != nil {
			return nil, nil, err
		}
		raw = f
		closes = append(closes, func() { _ = f.Close() })
	default:
		obj, err := s3client.Download(ctx, s3cfg(a.s3), a.s3.GetObjectKey())
		if err != nil {
			return nil, nil, fmt.Errorf("open S3 object: %w", err)
		}
		raw = obj
		closes = append(closes, func() { _ = obj.Close() })
	}
	rc, err := openArtifactReader(raw, a.identity)
	if err != nil {
		for _, c := range closes {
			c()
		}
		return nil, nil, err
	}
	closes = append([]func(){func() { _ = rc.Close() }}, closes...)
	return rc, func() {
		for _, c := range closes {
			c()
		}
	}, nil
}

// artifactDestination names where a finished artifact lands, so backupDatabase
// and backupProject stay identical across all three sinks. Exactly one of
// s3/store/stream is set.
type artifactDestination struct {
	s3    *pb.S3Target
	store *pb.StoreTarget // root already resolved by destinationFromBackup
	// stream sends one data frame, for the cross-host relay (stream_out).
	stream func([]byte) error
	// recipient is the age public key the artifact is encrypted to. Empty ONLY
	// for S3; destinationFromBackup refuses an empty one anywhere else, so a
	// missing key can never degrade into a silent plaintext write.
	recipient string
	key       string
	label     string
}

func destinationFromBackup(s *Service, req *pb.BackupRequest, send func([]byte) error) (*artifactDestination, error) {
	recipient := req.GetAgeRecipient()
	switch {
	case req.GetStreamOut():
		if recipient == "" {
			return nil, status.Error(codes.InvalidArgument,
				"a relayed backup must be encrypted, but no encryption key was sent")
		}
		// The key still travels: the destination agent writes it, and the result
		// echoes it back for the run record.
		key := req.GetStore().GetObjectKey()
		return &artifactDestination{stream: send, recipient: recipient, key: key, label: "the control plane"}, nil
	case req.GetStore() != nil:
		if recipient == "" {
			return nil, status.Error(codes.InvalidArgument,
				"a backup written to a server must be encrypted, but no encryption key was sent")
		}
		root, err := s.resolveStoreRoot(req.GetStore().GetRoot(), false)
		if err != nil {
			return nil, err
		}
		key := req.GetStore().GetObjectKey()
		if key == "" {
			return nil, status.Error(codes.InvalidArgument, "backup request missing object key")
		}
		return &artifactDestination{
			store:     &pb.StoreTarget{Root: root, ObjectKey: key},
			recipient: recipient,
			key:       key,
			label:     storeObjectLabel(root, key),
		}, nil
	case req.GetS3() != nil && req.GetS3().GetObjectKey() != "":
		// A bucket artifact is encrypted too, whenever a recipient is sent. It was
		// not, originally, and the asymmetry was the worst kind: a project archive
		// carries the app's ENTIRE decrypted env (the restore has to write the real
		// .env back), so the one destination shape deplo shipped first was the one
		// that put every secret in a bucket in the clear.
		//
		// An empty recipient is still accepted HERE and only here, because it is
		// what a destination created before this release sends, and refusing it
		// would break every existing schedule. The control plane is what stops a
		// silent downgrade: it refuses to run a backup for an encrypted destination
		// unless this agent advertises "backup-encrypt-s3".
		return &artifactDestination{
			s3:        req.GetS3(),
			recipient: recipient,
			key:       req.GetS3().GetObjectKey(),
			label:     req.GetS3().GetObjectKey(),
		}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "backup request names no destination")
	}
}

// writeArtifact runs `produce` through the gzip (+age) chain into the
// destination, and reports what landed. One function for all three sinks, so the
// compression, the encryption and the close ordering exist in exactly one place.
func (s *Service) writeArtifact(ctx context.Context, dest *artifactDestination, produce func(io.Writer) error) (size int64, digest string, err error) {
	pr, pw := io.Pipe()
	go func() {
		aw, aerr := newArtifactWriter(pw, dest.recipient)
		if aerr != nil {
			pw.CloseWithError(aerr)
			return
		}
		perr := produce(aw.Writer())
		// Finish the chain BEFORE closing the pipe, so the reader sees a complete
		// artifact rather than one missing its gzip trailer or age final chunk.
		if cerr := aw.Close(); perr == nil {
			perr = cerr
		}
		pw.CloseWithError(perr) // nil => clean EOF
	}()
	// Any early return must unblock the producer, or the dump goroutine hangs on
	// a pipe nobody reads for the rest of the RPC's deadline.
	defer func() {
		if err != nil {
			_ = pr.CloseWithError(err)
		}
	}()

	switch {
	case dest.stream != nil:
		sum := sha256.New()
		buf := make([]byte, storeChunkBytes)
		for {
			if cerr := ctx.Err(); cerr != nil {
				return 0, "", cerr
			}
			n, rerr := pr.Read(buf)
			if n > 0 {
				sum.Write(buf[:n])
				size += int64(n)
				// COPY before handing the slice off. grpc-go happens to marshal
				// synchronously inside Send, but `stream` is an interface the caller
				// supplies, and any implementation that RETAINS the slice (a test
				// double, a buffering relay) would see it overwritten by the next
				// Read — silently shipping an artifact stitched out of repeated
				// fragments that still has the right length. One alloc per MiB is
				// noise next to the I/O; a corrupted backup is not.
				frame := make([]byte, n)
				copy(frame, buf[:n])
				if serr := dest.stream(frame); serr != nil {
					return 0, "", serr
				}
			}
			if rerr == io.EOF {
				return size, hex.EncodeToString(sum.Sum(nil)), nil
			}
			if rerr != nil {
				return 0, "", rerr
			}
		}
	case dest.store != nil:
		return storeWrite(dest.store.GetRoot(), dest.store.GetObjectKey(), pr, false)
	default:
		// Hash on the way past. The bucket's own ETag is not usable as an integrity
		// check: it is a multipart-dependent digest of the object as the PROVIDER
		// saw it, so it proves nothing about the bytes deplo produced, and the
		// control plane cannot compare it to anything. A sha256 taken here is the
		// artifact's identity, recorded on the run and re-checked before a restore
		// ever feeds these bytes to `docker compose up`.
		sum := sha256.New()
		n, uerr := s3client.Upload(ctx, s3cfg(dest.s3), dest.key, io.TeeReader(pr, sum))
		if uerr != nil {
			return 0, "", fmt.Errorf("upload to S3: %w", uerr)
		}
		return n, hex.EncodeToString(sum.Sum(nil)), nil
	}
}

// ---------------------------------------------------------------------------
// The store RPCs — cross-host relay primitives
// ---------------------------------------------------------------------------

// ReadStoreFile streams an artifact out of this host's store. Emits `data`
// frames only. The bytes are exactly what was written — still age-encrypted — so
// the control plane relaying them to another agent, or to a user's browser,
// never holds plaintext it did not explicitly ask to decrypt.
func (s *Service) ReadStoreFile(req *pb.ReadStoreFileRequest, stream pb.Agent_ReadStoreFileServer) error {
	t := req.GetStore()
	if t == nil {
		return status.Error(codes.InvalidArgument, "read store request missing target")
	}
	root, err := s.resolveStoreRoot(t.GetRoot(), false)
	if err != nil {
		return err
	}
	f, err := storeOpen(root, t.GetObjectKey())
	if err != nil {
		return err
	}
	defer f.Close()

	// Verbatim by default (a relay must not see plaintext); decrypted when the
	// caller sends an identity, which is the download case. Deliberately NOT
	// gunzipped: what the user wants is the .tar.gz / .dump.gz itself.
	var src io.Reader = f
	if id := req.GetAgeIdentity(); id != "" {
		identity, perr := age.ParseX25519Identity(id)
		if perr != nil {
			return status.Errorf(codes.InvalidArgument, "invalid backup decryption key: %v", perr)
		}
		dec, derr := age.Decrypt(f, identity)
		if derr != nil {
			return fmt.Errorf("decrypt backup (is this the right recovery key?): %w", derr)
		}
		src = dec
	}

	buf := make([]byte, storeChunkBytes)
	for {
		if err := stream.Context().Err(); err != nil {
			return err
		}
		n, rerr := src.Read(buf)
		if n > 0 {
			// Copy before Send, for the same reason writeArtifact does: the frame
			// must not alias a buffer the next Read overwrites.
			frame := make([]byte, n)
			copy(frame, buf[:n])
			if serr := stream.Send(&pb.StoreChunk{
				Frame: &pb.StoreChunk_Data{Data: frame},
			}); serr != nil {
				return serr
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return fmt.Errorf("read backup artifact: %w", rerr)
		}
	}
}

// WriteStoreFile receives an artifact into this host's store. The first message
// must carry the header; every following one carries data. The write is atomic
// (see storeWrite), so a relay that dies mid-transfer leaves a sweepable
// `.partial` rather than a truncated artifact sitting at a key the control plane
// will later hand to a restore.
func (s *Service) WriteStoreFile(stream pb.Agent_WriteStoreFileServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "write store: no header received: %v", err)
	}
	h := first.GetHeader()
	if h == nil || h.GetStore() == nil {
		return status.Error(codes.InvalidArgument, "write store: first message must carry the header")
	}
	root, err := s.resolveStoreRoot(h.GetStore().GetRoot(), false)
	if err != nil {
		return err
	}

	// Bridge the client stream to an io.Reader so storeWrite stays a plain
	// io.Copy — the same shape the S3 upload has, and the reason the atomic
	// write path is not duplicated per transport.
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

	n, sum, werr := storeWrite(root, h.GetStore().GetObjectKey(), pr, h.GetOverwrite())
	if werr != nil {
		_ = pr.CloseWithError(werr)
		return stream.SendAndClose(&pb.StoreResult{Ok: false, Error: werr.Error()})
	}
	return stream.SendAndClose(&pb.StoreResult{Ok: true, BytesWritten: n, Sha256: sum})
}

// RestoreFrom is the cross-host half of Restore: the artifact lives on another
// server's disk, so the control plane streams it in here rather than asking this
// host to fetch it (agents cannot dial each other).
//
// Bidirectional because a restore must both receive bytes and report progress.
// The alternative — stage the artifact to a temp file, then call Restore — would
// need a full artifact's worth of free space on the very host being restored,
// plus cleanup that has to survive an agent restart, plus a window where a
// stranded temp file looks like a real artifact.
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

	// The remaining client messages ARE the artifact. Feeding them through a pipe
	// lets the restore paths stay byte-for-byte the same code they run for a local
	// artifact — the source is just another io.Reader.
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

	src := &artifactSource{stream: pr, identity: h.GetAgeIdentity(), label: "the control plane"}
	switch h.GetKind() {
	case pb.BackupKind_BACKUP_KIND_DATABASE:
		s.restoreDatabase(ctx, h.GetDatabase(), src, e)
	case pb.BackupKind_BACKUP_KIND_PROJECT:
		s.restoreProject(ctx, h.GetProject(), src, e)
	default:
		e.result(false, "unknown restore kind")
	}
	return nil
}

// storeCheck is the STORE half of S3Check: resolve the root (creating the managed
// one, or marking an empty custom one), round-trip a probe file so a read-only
// mount is reported as not-writable rather than passing a stat, sweep stale
// `.partial` artifacts, and report headroom.
func (s *Service) storeCheck(t *pb.StoreTarget) *pb.S3CheckResponse {
	root, err := s.resolveStoreRoot(t.GetRoot(), true)
	if err != nil {
		return &pb.S3CheckResponse{Ok: false, Error: statusMessage(err)}
	}
	probe := filepath.Join(root, ".deplo-store-check")
	if werr := os.WriteFile(probe, []byte("ok"), storeFilePerm); werr != nil {
		return &pb.S3CheckResponse{
			Ok:    false,
			Error: fmt.Sprintf("cannot write to %s: %v", root, werr),
			Root:  root,
		}
	}
	_ = os.Remove(probe)
	sweepPartials(root)
	free, total := storeFreeBytes(root)
	return &pb.S3CheckResponse{Ok: true, FreeBytes: free, TotalBytes: total, Root: root}
}

// statusMessage unwraps a gRPC status into its bare message, so a destination's
// stored error reads as a sentence rather than "rpc error: code = ... desc = ...".
func statusMessage(err error) string {
	if st, ok := status.FromError(err); ok {
		return st.Message()
	}
	return err.Error()
}

// storeObjectLabel is the human path an artifact landed at, for log lines.
func storeObjectLabel(root, key string) string { return path.Join(root, key) }
