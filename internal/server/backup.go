package server

// https://deplo.build/docs/guides/backups-and-restore

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/s3client"
)

// backup.go implements the BACKUPS half of the contract (ADR-0007): dump a database or
// project to S3, restore one in place, and the S3 affordances (S3Check / S3Delete).

const (
	// A dump/restore can be long for a large DB or a volume-heavy project; the
	// per-step docker exec / helper-container timeout.
	backupStepTimeout = 30 * time.Minute
	// The helper image used to tar a project's named volumes. A tiny, ubiquitous
	// image already present on most hosts; pulled on first use otherwise.
	volumeHelperImage = "busybox:1.36"
	// maxSnapshotBytes caps a single snapshot/ entry (compose.yml, env, a mount file) read
	// fully into memory during restore.
	maxSnapshotBytes = 16 << 20 // 16 MiB
	// maxProjectRestoreBytes caps the TOTAL decompressed bytes the agent will read out of
	// a project archive during restore, so a small "zip-bomb"-style object can't fill the
	// host disk.
	maxProjectRestoreBytes = 64 << 30 // 64 GiB
)

// bkEmitter funnels BackupEvents over the stream (mirrors deploy.go's emitter).
// SERIALISED, unlike deploy.go's, because this one has two writers.
type bkEmitter struct {
	mu   sync.Mutex
	send func(*pb.BackupEvent) error
}

func (e *bkEmitter) emit(ev *pb.BackupEvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.send(ev)
}

func (e *bkEmitter) log(level, text string) {
	_ = e.emit(&pb.BackupEvent{Event: &pb.BackupEvent_Log{Log: &pb.LogLine{Level: level, Text: text}}})
}
func (e *bkEmitter) result(ok bool, errMsg, objectKey string, size int64) {
	e.emitResult(ok, errMsg, objectKey, artifactWritten{size: size})
}
func (e *bkEmitter) emitResult(ok bool, errMsg, objectKey string, w artifactWritten) {
	_ = e.emit(&pb.BackupEvent{Event: &pb.BackupEvent_Result{
		Result: &pb.BackupResult{
			Ok:                 ok,
			Error:              errMsg,
			ObjectKey:          objectKey,
			SizeBytes:          w.size,
			Sha256:             w.digest,
			DecryptedSizeBytes: w.decryptedSize,
		},
	}})
}

// data emits one slice of the finished artifact, for a stream_out relay. Called
// from the RPC goroutine while `log` may be called from the producer's.
func (e *bkEmitter) data(b []byte) error {
	return e.emit(&pb.BackupEvent{Event: &pb.BackupEvent_Data{Data: b}})
}

// rsEmitter funnels RestoreEvents over the stream.
type rsEmitter struct {
	send func(*pb.RestoreEvent) error
}

func (e *rsEmitter) log(level, text string) {
	_ = e.send(&pb.RestoreEvent{Event: &pb.RestoreEvent_Log{Log: &pb.LogLine{Level: level, Text: text}}})
}
func (e *rsEmitter) result(ok bool, errMsg string) {
	_ = e.send(&pb.RestoreEvent{Event: &pb.RestoreEvent_Result{
		Result: &pb.RestoreResult{Ok: ok, Error: errMsg},
	}})
}

// s3cfg maps the wire S3Target to the s3client.Config (object_key handled per call).
func s3cfg(t *pb.S3Target) s3client.Config {
	return s3client.Config{
		Endpoint:             t.GetEndpoint(),
		Region:               t.GetRegion(),
		Bucket:               t.GetBucket(),
		AccessKey:            t.GetAccessKey(),
		SecretKey:            t.GetSecretKey(),
		PathStyle:            t.GetPathStyle(),
		AllowPrivateEndpoint: t.GetAllowPrivateEndpoint(),
		ExtraArgs:            t.GetExtraArgs(),
	}
}

// Backup dumps a database or project to S3, streaming progress.
func (s *Service) Backup(req *pb.BackupRequest, stream pb.Agent_BackupServer) error {
	e := &bkEmitter{send: stream.Send}
	ctx := stream.Context()

	dest, err := destinationFromBackup(s, req, e.data)
	if err != nil {
		e.result(false, statusMessage(err), "", 0)
		return nil
	}
	switch req.GetKind() {
	case pb.BackupKind_BACKUP_KIND_DATABASE:
		s.backupDatabase(ctx, req.GetDatabase(), dest, e)
	case pb.BackupKind_BACKUP_KIND_PROJECT:
		s.backupProject(ctx, req.GetProject(), dest, e)
	default:
		e.result(false, "unknown backup kind", "", 0)
	}
	return nil
}

// Restore restores a database or project from an artifact this host can reach (an S3
// object or a local store), in place.
func (s *Service) Restore(req *pb.RestoreRequest, stream pb.Agent_RestoreServer) error {
	e := &rsEmitter{send: stream.Send}
	ctx := stream.Context()

	src, err := sourceFromRestore(s, req)
	if err != nil {
		e.result(false, statusMessage(err))
		return nil
	}
	switch req.GetKind() {
	case pb.BackupKind_BACKUP_KIND_DATABASE:
		s.restoreDatabase(ctx, req.GetDatabase(), src, e)
	case pb.BackupKind_BACKUP_KIND_PROJECT:
		s.restoreProject(ctx, req.GetProject(), src, e)
	default:
		e.result(false, "unknown restore kind")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Database backup / restore - `docker exec` the engine's dump tool, gzip → S3
// ---------------------------------------------------------------------------

// execWithSecretEnv builds a `docker exec [flags...]` argv prefix that forwards a
// password env var INTO the container WITHOUT putting the value on argv: when `pw` is
// non-empty it adds the bare `-e <name>` flag (no `=value`), and returns the
// `<name>=<pw>` entry for the HOST docker-client process env (PipeOut/PipeIn set it via
// cmd.Env).
func execWithSecretEnv(pw, name string, flags ...string) (argv []string, env []string) {
	a := append([]string{"exec"}, flags...)
	if pw != "" {
		a = append(a, "-e", name)
		env = []string{name + "=" + pw}
	}
	return a, env
}

// dumpArgv returns the `docker exec` argv that dumps the database to stdout, and the
// extra HOST-PROCESS env the docker client needs, for an engine. MySQL keeps its own
// names; it never had the mariadb ones.
func mysqlClient(dbType, kind string) string {
	if dbType == "mariadb" {
		if kind == "dump" {
			return "mariadb-dump"
		}
		return "mariadb"
	}
	if kind == "dump" {
		return "mysqldump"
	}
	return "mysql"
}

func dumpArgv(d *pb.DatabaseDescriptor) (argv []string, env []string, err error) {
	c, user, db := d.GetContainer(), d.GetUser(), d.GetDbName()
	pw := d.GetPassword()
	dbType := strings.ToLower(d.GetDbType())
	switch dbType {
	case "postgres":
		// -Fc = custom (compressed/orderable) format; restored with pg_restore
		// --clean --if-exists to drop-and-recreate (overwrite, not append).
		a, env := execWithSecretEnv(pw, "PGPASSWORD")
		a = append(a, c, "pg_dump", "-U", user, "-Fc", db)
		return a, env, nil
	case "mysql", "mariadb":
		// --add-drop-table makes the restore overwrite each table.
		a, env := execWithSecretEnv(pw, "MYSQL_PWD")
		a = append(a, c, mysqlClient(dbType, "dump"), "-u", user, "--add-drop-table", "--databases", db)
		return a, env, nil
	case "mongodb":
		// --archive writes a single restorable stream to stdout. mongodump has no
		// password env var, so -p is on argv (masked in any error by redactArgs).
		a := []string{"exec", c, "mongodump", "--archive", "--db=" + db}
		if user != "" {
			a = append(a, "-u", user, "--authenticationDatabase=admin")
		}
		if pw != "" {
			a = append(a, "-p", pw)
		}
		return a, nil, nil
	case "redis":
		// redis-cli --rdb - streams a valid RDB to stdout. The password rides in
		// REDISCLI_AUTH (env), which redis-cli honours, so it stays off argv.
		a, env := execWithSecretEnv(pw, "REDISCLI_AUTH")
		a = append(a, c, "redis-cli", "--rdb", "-")
		return a, env, nil
	case "clickhouse":
		// Clickhouse is multi-statement (schema + per-table data), not a single
		// stdout pipe - handled by dumpClickhouse, dispatched in backupDatabase.
		return nil, nil, errClickhouseSeparate
	default:
		return nil, nil, fmt.Errorf("unsupported database engine %q", d.GetDbType())
	}
}

// restoreArgv returns the `docker exec -i` argv that reads the (decompressed)
// dump from stdin and restores it, for an engine. Drop-and-recreate is
// guaranteed by the dump format / restore flags (PLAN locked decision).
func restoreArgv(d *pb.DatabaseDescriptor) (argv []string, env []string, err error) {
	c, user, db := d.GetContainer(), d.GetUser(), d.GetDbName()
	pw := d.GetPassword()
	dbType := strings.ToLower(d.GetDbType())
	switch dbType {
	case "postgres":
		// --clean --if-exists drops existing objects first => overwrite. Password
		// in PGPASSWORD (env), off argv (see dumpArgv's SECRET HANDLING note).
		a, env := execWithSecretEnv(pw, "PGPASSWORD", "-i")
		a = append(a, c, "pg_restore", "-U", user, "--clean", "--if-exists", "-d", db)
		return a, env, nil
	case "mysql", "mariadb":
		// The --databases dump carries its own USE/CREATE; the client applies it.
		// The --add-drop-table in the dump overwrites each table. Password off argv.
		a, env := execWithSecretEnv(pw, "MYSQL_PWD", "-i")
		a = append(a, c, mysqlClient(dbType, "shell"), "-u", user)
		return a, env, nil
	case "mongodb":
		// --drop drops each collection before restoring it => overwrite. mongorestore
		// has no password env var, so -p is on argv (masked in errors by redactArgs).
		a := []string{"exec", "-i", c, "mongorestore", "--archive", "--drop"}
		if user != "" {
			a = append(a, "-u", user, "--authenticationDatabase=admin")
		}
		if pw != "" {
			a = append(a, "-p", pw)
		}
		return a, nil, nil
	case "redis":
		// Redis does NOT restore over a single stdin pipe: the dump is an RDB file (redis-cli
		// --rdb), and `redis-cli --pipe` speaks RESP, not RDB - feeding it an RDB fails
		// ("unknown command 'REDIS0014'").
		return nil, nil, errRedisRestoreSeparate
	case "clickhouse":
		// Clickhouse restores the SQL script via `clickhouse-client --multiquery`,
		// handled by restoreClickhouse (dispatched in restoreDatabase).
		return nil, nil, errClickhouseSeparate
	default:
		return nil, nil, fmt.Errorf("unsupported database engine %q", d.GetDbType())
	}
}

func (s *Service) backupDatabase(ctx context.Context, d *pb.DatabaseDescriptor, dest *artifactDestination, e *bkEmitter) {
	if d == nil || d.GetContainer() == "" {
		e.result(false, "database backup request missing descriptor / container", "", 0)
		return
	}
	e.log("info", fmt.Sprintf("Dumping %s database %q from container %q", d.GetDbType(), d.GetDbName(), d.GetContainer()))

	// The dump PRODUCER writes the raw dump bytes into `w` (the head of the gzip → age →
	// destination chain).
	var produce func(w io.Writer) error
	switch strings.ToLower(d.GetDbType()) {
	case "clickhouse":
		produce = func(w io.Writer) error { return s.dumpClickhouse(ctx, d, w) }
	default:
		argv, env, err := dumpArgv(d)
		if err != nil {
			e.result(false, err.Error(), "", 0)
			return
		}
		produce = func(w io.Writer) error {
			code, derr := dockercli.PipeOut(ctx, backupStepTimeout, w, env, argv...)
			if derr != nil {
				return derr
			}
			if code != 0 {
				return fmt.Errorf("dump exited %d", code)
			}
			return nil
		}
	}

	// Pipeline: producer → gzip → (age) → destination.
	written, werr := s.writeArtifact(ctx, dest, produce)
	if werr != nil {
		e.result(false, statusMessage(werr), "", 0)
		return
	}
	e.log("info", fmt.Sprintf("Wrote %s (%d bytes)", dest.label, written.size))
	e.emitResult(true, "", dest.key, written)
}

func (s *Service) restoreDatabase(ctx context.Context, d *pb.DatabaseDescriptor, src *artifactSource, e *rsEmitter) {
	if d == nil || d.GetContainer() == "" {
		e.result(false, "database restore request missing descriptor / container")
		return
	}
	argv, env, err := restoreArgv(d)
	if err != nil {
		// Redis + clickhouse restore via dedicated paths (an RDB can't be piped to
		// a restore tool's stdin; clickhouse replays a multi-statement script);
		// every other unsupported/erroring engine fails.
		switch err {
		case errRedisRestoreSeparate:
			s.restoreRedis(ctx, d, src, e)
		case errClickhouseSeparate:
			s.restoreClickhouse(ctx, d, src, e)
		default:
			e.result(false, err.Error())
		}
		return
	}
	e.log("info", fmt.Sprintf("Restoring %s database %q into container %q from %s", d.GetDbType(), d.GetDbName(), d.GetContainer(), src.label))

	// Chain: destination → (age-decrypt) → gunzip → docker exec -i stdin.
	rd, closeSrc, oerr := src.open(ctx)
	if oerr != nil {
		e.result(false, statusMessage(oerr))
		return
	}
	defer closeSrc()

	code, rerr := dockercli.PipeIn(ctx, backupStepTimeout, rd, env, argv...)
	if rerr != nil {
		e.result(false, "restore: "+rerr.Error())
		return
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("restore tool exited %d", code))
		return
	}
	// A dump is applied AS it streams, so for a streaming source this verdict arrives
	// after the fact. (A store artifact was proven before any of this ran, which is the
	// shape the default destination uses.)
	if verr := src.verify(); verr != nil {
		e.result(false, statusMessage(verr))
		return
	}
	e.log("info", "Restore complete")
	e.result(true, "")
}

// errRedisRestoreSeparate flags that redis must restore via restoreRedis (the
// RDB file-swap dance) rather than the uniform stdin-pipe restore path.
var errRedisRestoreSeparate = fmt.Errorf("redis restore uses the dedicated file-swap path")

// restoreRedis restores a redis RDB dump IN PLACE. This requires the container to be
// restarted by its supervisor; if it isn't (no restart policy), the stream reports the
// failure clearly rather than leaving redis down silently.
func (s *Service) restoreRedis(ctx context.Context, d *pb.DatabaseDescriptor, src *artifactSource, e *rsEmitter) {
	c, pw := d.GetContainer(), d.GetPassword()
	e.log("info", fmt.Sprintf("Restoring redis %q into container %q from %s", d.GetDbName(), c, src.label))

	// redis-cli auth rides in REDISCLI_AUTH (env), forwarded via `-e REDISCLI_AUTH`
	// (name only) so the password never lands on the host docker-client's argv.
	cliArgv, cliEnv := redisCliPrefix(c, pw)
	cli := func(args ...string) []string { return append(cliArgv, args...) }

	// Resolve the RDB path the server loads at startup (dir + dbfilename).
	dir := redisConfig(ctx, c, pw, "dir")
	if dir == "" {
		dir = "/data"
	}
	dbfile := redisConfig(ctx, c, pw, "dbfilename")
	if dbfile == "" {
		dbfile = "dump.rdb"
	}
	rdbPath := dir + "/" + dbfile

	// 1. Disable save so the imminent shutdown does NOT rewrite the RDB.
	if res, err := dockercli.RunEnv(ctx, 15*time.Second, cliEnv, cli("CONFIG", "SET", "save", "")...); err != nil || res.Code != 0 {
		e.result(false, "redis CONFIG SET save: "+combineErr(err, res.Stderr))
		return
	}
	// 2. FLUSHALL so the restore overwrites rather than merges with live data.
	if res, err := dockercli.RunEnv(ctx, 30*time.Second, cliEnv, cli("FLUSHALL")...); err != nil || res.Code != 0 {
		e.result(false, "redis FLUSHALL: "+combineErr(err, res.Stderr))
		return
	}

	// 3. Stream the decompressed RDB into the container's dump file.
	rd, closeSrc, oerr := src.open(ctx)
	if oerr != nil {
		e.result(false, statusMessage(oerr))
		return
	}
	defer closeSrc()
	// `sh -c 'cat > <path>'` writes the piped bytes into the container fs.
	if code, werr := dockercli.PipeIn(ctx, backupStepTimeout, rd, nil,
		"exec", "-i", c, "sh", "-c", "cat > "+shellQuote(rdbPath)); werr != nil {
		e.result(false, "write RDB into container: "+werr.Error())
		return
	} else if code != 0 {
		e.result(false, fmt.Sprintf("write RDB into container exited %d", code))
		return
	}

	// 4. SHUTDOWN NOSAVE: exits WITHOUT rewriting the RDB, leaving our file intact.
	//    The redis-cli connection drops as the server exits - that is expected, not
	//    an error, so we don't gate on its exit code.
	e.log("info", "Reloading redis from the restored snapshot")
	_, _ = dockercli.RunEnv(ctx, 15*time.Second, cliEnv, cli("SHUTDOWN", "NOSAVE")...)

	// 5. Wait for the supervisor to bring redis back AND for it to answer PING.
	if !waitRedisReady(ctx, c, pw, 60*time.Second) {
		e.result(false, "redis did not come back after SHUTDOWN NOSAVE - ensure the container has a restart policy")
		return
	}
	if verr := src.verify(); verr != nil {
		e.result(false, statusMessage(verr))
		return
	}
	e.log("info", "Restore complete")
	e.result(true, "")
}

// redisConfig reads a single CONFIG GET value from a redis container ("" on any failure -
// the caller falls back to the documented default). redisCliPrefix builds the `docker
// exec [-e REDISCLI_AUTH] <container> redis-cli` argv prefix + the host-process env
// carrying the password value, so redis-cli auth never lands on argv (the value rides
// in REDISCLI_AUTH, forwarded by the valueless `-e` flag).
func redisCliPrefix(container, pw string) (argv []string, env []string) {
	a := []string{"exec"}
	if pw != "" {
		a = append(a, "-e", "REDISCLI_AUTH")
		env = []string{"REDISCLI_AUTH=" + pw}
	}
	a = append(a, container, "redis-cli")
	return a, env
}

func redisConfig(ctx context.Context, container, pw, key string) string {
	argv, env := redisCliPrefix(container, pw)
	res, err := dockercli.RunEnv(ctx, 10*time.Second, env, append(argv, "CONFIG", "GET", key)...)
	if err != nil || res.Code != 0 {
		return ""
	}
	// CONFIG GET returns two lines: the key then the value.
	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	if len(lines) >= 2 {
		return strings.TrimSpace(lines[1])
	}
	return ""
}

// waitRedisReady polls PING until the restarted redis answers PONG or the
// deadline lapses.
func waitRedisReady(ctx context.Context, container, pw string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	argv, env := redisCliPrefix(container, pw)
	ping := append(argv, "PING")
	for time.Now().Before(deadline) {
		if res, err := dockercli.RunEnv(ctx, 5*time.Second, env, ping...); err == nil && res.Code == 0 &&
			strings.Contains(strings.ToUpper(res.Stdout), "PONG") {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
	}
	return false
}

// combineErr renders a spawn error or a non-zero command's stderr into one
// message for the restore stream.
func combineErr(err error, stderr string) string {
	if err != nil {
		return err.Error()
	}
	if s := strings.TrimSpace(stderr); s != "" {
		return s
	}
	return "command failed"
}

// shellQuote single-quotes a path for use inside `sh -c`. The path is a redis
// CONFIG value (dir/dbfilename) - control-plane/agent-derived, not user free
// text, but we quote defensively since it is interpolated into a shell string.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// ---------------------------------------------------------------------------
// Project backup / restore - tar volumes + files + snapshot, gzip → S3
// ---------------------------------------------------------------------------

// backupProject tars a project's named + compose-stack volumes (via a throwaway helper
// container that mounts each), the project files dir, and the rendered compose/env
// snapshot, gzip-compresses, and uploads.
func (s *Service) backupProject(ctx context.Context, p *pb.ProjectDescriptor, dest *artifactDestination, e *bkEmitter) {
	if p == nil || p.GetSlug() == "" {
		e.result(false, "project backup request missing descriptor / slug", "", 0)
		return
	}
	if err := validateSlug(p.GetSlug()); err != nil {
		e.result(false, err.Error(), "", 0)
		return
	}
	e.log("info", fmt.Sprintf("Backing up project %q (%d volume(s), files=%v)", p.GetSlug(), len(p.GetVolumeNames()), p.GetIncludeFiles()))

	// The tar lives INSIDE the producer so its trailer is written before the
	// gzip and age layers are finished - the one ordering that yields a readable
	// artifact (see artifactWriter.Close).
	produce := func(w io.Writer) error {
		tw := tar.NewWriter(w)
		err := s.writeProjectArchive(ctx, p, tw, e)
		if cerr := tw.Close(); err == nil {
			err = cerr
		}
		return err
	}

	written, werr := s.writeArtifact(ctx, dest, produce)
	if werr != nil {
		e.result(false, statusMessage(werr), "", 0)
		return
	}
	e.log("info", fmt.Sprintf("Wrote %s (%d bytes)", dest.label, written.size))
	e.emitResult(true, "", dest.key, written)
}

// writeProjectArchive streams the project's volumes + files + snapshot into tw.
func (s *Service) writeProjectArchive(ctx context.Context, p *pb.ProjectDescriptor, tw *tar.Writer, e *bkEmitter) error {
	// 1. Each named/compose-stack volume, tarred from inside a helper container
	//    that mounts it read-only, re-framed under volumes/<name>/ in our tar.
	for _, vol := range p.GetVolumeNames() {
		if vol == "" {
			continue
		}
		e.log("info", "Archiving volume "+vol)
		if err := s.archiveVolume(ctx, vol, tw, e); err != nil {
			return fmt.Errorf("archive volume %q: %w", vol, err)
		}
	}

	// 2. The project files dir, if present + requested.
	if p.GetIncludeFiles() {
		root := s.filesRoot(p.GetSlug())
		if st, err := os.Stat(root); err == nil && st.IsDir() {
			e.log("info", "Archiving project files")
			if err := addDirToTar(tw, root, "files"); err != nil {
				return fmt.Errorf("archive files: %w", err)
			}
		}
	}

	// 3. The compose/env snapshot so restore re-Reroutes the exact config.
	if y := p.GetComposeYaml(); y != "" {
		if err := addBytesToTar(tw, "snapshot/compose.yml", []byte(y)); err != nil {
			return err
		}
	}
	if len(p.GetEnvSnapshot()) > 0 {
		if err := addBytesToTar(tw, "snapshot/env", []byte(renderEnvFile(p.GetEnvSnapshot()))); err != nil {
			return err
		}
	}
	for _, m := range p.GetMounts() {
		rel, err := normalizeRel(m.GetPath())
		if err != nil || rel == "" {
			e.log("warn", "Skipping unsafe mount path in snapshot: "+m.GetPath())
			continue
		}
		if err := addBytesToTar(tw, "snapshot/mounts/"+rel, []byte(m.GetContent())); err != nil {
			return err
		}
	}
	return nil
}

// archiveVolume runs a helper container that mounts the named volume read-only at /v
// and tars its contents to stdout; we read that tar and re-emit every entry under
// volumes/<vol>/ into our own tar, so one restore-time pass can route each entry back
// to the right volume.
func (s *Service) archiveVolume(ctx context.Context, vol string, tw *tar.Writer, e *bkEmitter) error {
	if err := validateVolumeName(vol); err != nil {
		return err
	}
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		// `tar -C /v -cf - .` streams the volume's contents (no leading /v prefix).
		code, err := dockercli.PipeOut(ctx, backupStepTimeout, pw, nil,
			"run", "--rm", "-v", vol+":/v:ro", volumeHelperImage,
			"tar", "-C", "/v", "-cf", "-", ".")
		if err == nil && code != 0 {
			err = fmt.Errorf("volume tar exited %d", code)
		}
		pw.CloseWithError(err)
		done <- err
	}()
	tr := tar.NewReader(pr)
	prefix := "volumes/" + vol
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			// Drain any trailing bytes so the producer's PipeOut write completes and reports its
			// exit, then WARN (not fail) on a non-zero exit - the reader reached a clean
			// trailer, so the archive is complete (busybox's benign "file changed" exit 1 on a
			// live volume is the common cause).
			_, _ = io.Copy(io.Discard, pr)
			if perr := <-done; perr != nil {
				e.log("warn", fmt.Sprintf("volume %q: %v (archive completed; a file likely changed during read)", vol, perr))
			}
			return nil
		}
		if err != nil {
			_ = pr.CloseWithError(err)
			<-done
			return err
		}
		// Re-frame the entry path under volumes/<vol>/, cleaning the "./" the inner
		// tar emits for the root.
		name := strings.TrimPrefix(filepath.ToSlash(hdr.Name), "./")
		hdr.Name = prefix + "/" + name
		if hdr.Typeflag == tar.TypeDir {
			hdr.Name = strings.TrimRight(hdr.Name, "/") + "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			_ = pr.CloseWithError(err)
			<-done
			return err
		}
		if hdr.Typeflag == tar.TypeReg {
			if _, err := io.Copy(tw, tr); err != nil {
				_ = pr.CloseWithError(err)
				<-done
				return err
			}
		}
	}
}

// restoreProject reverses backupProject: stop the stack, wipe + repopulate each volume
// + the files dir, then re-Reroute the snapshot compose/env (which restarts the stack
// on the EXACT backed-up config).
func (s *Service) restoreProject(ctx context.Context, p *pb.ProjectDescriptor, src *artifactSource, e *rsEmitter) {
	if p == nil || p.GetSlug() == "" {
		e.result(false, "project restore request missing descriptor / slug")
		return
	}
	slug := p.GetSlug()
	if err := validateSlug(slug); err != nil {
		e.result(false, err.Error())
		return
	}
	e.log("info", fmt.Sprintf("Restoring project %q from %s", slug, src.label))

	// Stop the stack so volumes aren't written under us while we wipe them.
	e.log("info", "Stopping the stack")
	if _, err := dockercli.Run(ctx, 90*time.Second, s.composeCtl(slug, "stop")...); err != nil {
		e.log("warn", "stack stop: "+err.Error()+" (continuing)")
	}

	rd, closeSrc, oerr := src.open(ctx)
	if oerr != nil {
		e.result(false, statusMessage(oerr))
		return
	}
	defer closeSrc()

	snapshot, err := s.unpackProjectArchive(ctx, slug, p.GetVolumeNames(), rd, e)
	if err != nil {
		e.result(false, err.Error())
		return
	}

	// The last point this can still refuse.
	if verr := src.verify(); verr != nil {
		e.result(false, statusMessage(verr))
		return
	}

	rr := restoreConfig(slug, p, snapshot, src.integrityProven, src.configUntrusted)
	if rr.ComposeYaml == "" {
		e.log("warn", "No compose snapshot in the archive; leaving the stack stopped")
		e.result(true, "")
		return
	}
	e.log("info", "Re-applying the backed-up stack configuration")
	res, rerr := s.Reroute(ctx, rr)
	if rerr != nil {
		e.result(false, "restart stack: "+rerr.Error())
		return
	}
	if !res.GetOk() {
		e.result(false, "restart stack: "+res.GetError())
		return
	}
	e.log("info", "Restore complete; stack restarted")
	e.result(true, "")
}

// restoreConfig picks the stack configuration a restore brings back up, and it is the
// one place in this file where "which of two sources do we trust" is decided - so it is
// its own function, with its own test.
func restoreConfig(
	slug string,
	p *pb.ProjectDescriptor,
	snap projectSnapshot,
	proven bool,
	untrusted bool,
) *pb.RerouteRequest {
	// `first` is whichever source this restore trusts; the other is the fallback.
	// UNTRUSTED removes the fallback entirely, and it has to: "prefer the control plane's"
	// is not a guard when the control plane has none to prefer.
	pick := func(fromArchive, fromControlPlane string) string {
		if proven && fromArchive != "" {
			return fromArchive
		}
		if fromControlPlane != "" {
			return fromControlPlane
		}
		if untrusted {
			return ""
		}
		return fromArchive
	}
	compose := pick(snap.compose, p.GetComposeYaml())

	env := p.GetEnvSnapshot()
	if proven && len(snap.env) > 0 {
		env = snap.env
	} else if len(env) == 0 && !untrusted {
		env = snap.env
	}

	mounts := p.GetMounts()
	if proven && len(snap.mounts) > 0 {
		mounts = snap.mounts
	} else if len(mounts) == 0 && !untrusted {
		mounts = snap.mounts
	}

	return &pb.RerouteRequest{Slug: slug, ComposeYaml: compose, Env: env, Mounts: mounts}
}

// projectSnapshot is the config recovered from the archive's snapshot/ entries.
type projectSnapshot struct {
	compose string
	env     map[string]string
	mounts  []*pb.MountFile
}

// readSnapshotEntry reads a snapshot/ tar entry fully into memory with a hard
// cap, erroring if it exceeds maxSnapshotBytes rather than reading unboundedly
// (an OOM guard: the entry name + size come from an untrusted S3 object).
func readSnapshotEntry(tr io.Reader, name string) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(tr, maxSnapshotBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxSnapshotBytes {
		return nil, fmt.Errorf("snapshot entry %q exceeds the %d-byte limit", name, int64(maxSnapshotBytes))
	}
	return b, nil
}

// budgetReader wraps a reader and fails once cumulative bytes read exceed a budget, so
// an over-large (or maliciously inflating) archive aborts partway instead of filling
// the host disk.
type budgetReader struct {
	r      io.Reader
	n      int64
	budget int64
}

func (b *budgetReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	if b.n > b.budget {
		return n, fmt.Errorf("project archive exceeds the %d-byte restore limit", b.budget)
	}
	return n, err
}

// unpackProjectArchive reads the gzipped tar, routing each entry: volumes/<vol>/* back
// into a freshly-wiped volume (via a helper container), files/* into a freshly-wiped
// files dir, and snapshot/* into the returned projectSnapshot.
func (s *Service) unpackProjectArchive(ctx context.Context, slug string, volumeNames []string, r io.Reader, e *rsEmitter) (projectSnapshot, error) {
	snap := projectSnapshot{env: map[string]string{}}

	// Validate EVERY volume name BEFORE any destructive action (wipe/untar): a name
	// like "/" or "/etc" would bind-mount a host path, so reject the whole restore
	// up front rather than partway through wiping volumes.
	for _, vol := range volumeNames {
		if vol == "" {
			continue
		}
		if err := validateVolumeName(vol); err != nil {
			return snap, err
		}
	}

	// Wipe the target volumes + files dir up front so the restore overwrites.
	for _, vol := range volumeNames {
		if vol == "" {
			continue
		}
		e.log("info", "Wiping volume "+vol)
		if err := wipeVolume(ctx, vol); err != nil {
			return snap, fmt.Errorf("wipe volume %q: %w", vol, err)
		}
	}
	filesRoot := s.filesRoot(slug)
	if err := os.RemoveAll(filesRoot); err != nil {
		return snap, fmt.Errorf("wipe files dir: %w", err)
	}

	// Per-volume tar streams to feed each helper container's `tar -x`. We
	// demultiplex the archive into one writer per volume, plus direct fs writes
	// for files/ and snapshot/.
	vstreams := newVolumeStreams(ctx, volumeNames)
	defer vstreams.closeAll()

	// Cap the total decompressed bytes read from the archive so a small object
	// that inflates hugely can't fill the host disk (volumes + files write here).
	tr := tar.NewReader(&budgetReader{r: r, budget: maxProjectRestoreBytes})
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return snap, fmt.Errorf("read archive: %w", err)
		}
		name := filepath.ToSlash(hdr.Name)
		switch {
		case strings.HasPrefix(name, "volumes/"):
			rest := strings.TrimPrefix(name, "volumes/")
			vol, inner, ok := strings.Cut(rest, "/")
			if !ok || inner == "" {
				continue // the volumes/<vol>/ dir entry itself
			}
			w, ok := vstreams.writerFor(vol)
			if !ok {
				continue // a volume not in our target set (defensive)
			}
			// The entry name + type come from an S3 object, never trusted.
			if hasDotDot(inner) {
				return snap, fmt.Errorf("archive volume entry %q contains a path traversal", inner)
			}
			if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeDir {
				continue // skip symlink/hardlink/char/block/fifo entries
			}
			// Re-emit the entry into this volume's helper-container tar stream,
			// stripping the volumes/<vol>/ framing back to a volume-relative path.
			hdr.Name = inner
			if err := w.WriteHeader(hdr); err != nil {
				return snap, fmt.Errorf("write volume entry: %w", err)
			}
			if hdr.Typeflag == tar.TypeReg {
				if _, err := io.Copy(w, tr); err != nil {
					return snap, fmt.Errorf("write volume data: %w", err)
				}
			}
		case strings.HasPrefix(name, "files/"):
			rel := strings.TrimPrefix(name, "files/")
			if err := extractToDir(filesRoot, rel, hdr, tr); err != nil {
				return snap, fmt.Errorf("extract files: %w", err)
			}
		case name == "snapshot/compose.yml":
			b, err := readSnapshotEntry(tr, name)
			if err != nil {
				return snap, err
			}
			snap.compose = string(b)
		case name == "snapshot/env":
			b, err := readSnapshotEntry(tr, name)
			if err != nil {
				return snap, err
			}
			snap.env = parseEnvFile(string(b))
		case strings.HasPrefix(name, "snapshot/mounts/"):
			b, err := readSnapshotEntry(tr, name)
			if err != nil {
				return snap, err
			}
			snap.mounts = append(snap.mounts, &pb.MountFile{
				Path:    strings.TrimPrefix(name, "snapshot/mounts/"),
				Content: string(b),
			})
		}
	}

	// Finish each volume's helper-container tar so it extracts cleanly.
	if err := vstreams.finish(e); err != nil {
		return snap, err
	}
	return snap, nil
}

// ---------------------------------------------------------------------------
// S3Check / S3Delete
// ---------------------------------------------------------------------------

// S3Check verifies the bucket is reachable + writable for the "Test connection"
// button. Any agent advertising "backup" can serve it (no Docker needed).
func (s *Service) S3Check(ctx context.Context, req *pb.S3CheckRequest) (*pb.S3CheckResponse, error) {
	if req.GetStore() != nil {
		return s.storeCheck(req.GetStore()), nil
	}
	if req.GetS3() == nil {
		return nil, status.Error(codes.InvalidArgument, "check request missing destination")
	}
	if err := s3client.Check(ctx, s3cfg(req.GetS3())); err != nil {
		return &pb.S3CheckResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.S3CheckResponse{Ok: true}, nil
}

// S3Delete deletes a single artifact (or, with prefix=true, a whole target
// folder) - backs retention + delete-with-artifacts. Idempotent.
func (s *Service) S3Delete(ctx context.Context, req *pb.S3DeleteRequest) (*pb.S3DeleteResponse, error) {
	if t := req.GetStore(); t != nil {
		root, err := s.resolveStoreRoot(t.GetRoot(), false)
		if err != nil {
			return &pb.S3DeleteResponse{Ok: false, Error: statusMessage(err)}, nil
		}
		var n int64
		if req.GetPrefix() {
			n, err = storeDeletePrefix(root, t.GetObjectKey())
		} else {
			n, err = storeDeleteOne(root, t.GetObjectKey())
		}
		if err != nil {
			return &pb.S3DeleteResponse{Ok: false, Error: statusMessage(err)}, nil
		}
		return &pb.S3DeleteResponse{Ok: true, Deleted: n}, nil
	}
	if req.GetS3() == nil || req.GetS3().GetObjectKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "delete request missing key/prefix")
	}
	cfg := s3cfg(req.GetS3())
	var (
		n   int64
		err error
	)
	if req.GetPrefix() {
		n, err = s3client.DeletePrefix(ctx, cfg, req.GetS3().GetObjectKey())
	} else {
		n, err = s3client.DeleteOne(ctx, cfg, req.GetS3().GetObjectKey())
	}
	if err != nil {
		return &pb.S3DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.S3DeleteResponse{Ok: true, Deleted: n}, nil
}
