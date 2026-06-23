package server

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/s3client"
)

// backup.go implements the BACKUPS half of the contract (ADR-0007): dump a
// database or project to S3, restore one in place, and the S3 affordances
// (S3Check / S3Delete). Everything routes through the OWNING server's agent
// because the agent has the Docker socket + host fs the dump needs and an S3
// client (minio-go) so the bytes never round-trip through the control plane.
//
// The control plane stays the source of truth: it decrypts the S3 creds + DB
// password, resolves the container name + engine, parses the project's volume
// names out of the rendered compose, and builds the object key — then sends all
// of it here over mTLS. The agent stays dumb about Deplo's store; it just runs
// the right tool and moves bytes.
//
// FORMAT (docs/research/dbs-backups/PLAN.md, gzip variant): the dump tool's
// output is gzip-compressed in-process (Go stdlib) on the way to S3, and
// gunzipped on the way back. Object extension is `.gz` (e.g. `.dump.gz`).

const (
	// A dump/restore can be long for a large DB or a volume-heavy project; the
	// per-step docker exec / helper-container timeout.
	backupStepTimeout = 30 * time.Minute
	// The helper image used to tar a project's named volumes. A tiny, ubiquitous
	// image already present on most hosts; pulled on first use otherwise.
	volumeHelperImage = "busybox:1.36"
)

// bkEmitter funnels BackupEvents over the stream (mirrors deploy.go's emitter).
type bkEmitter struct {
	send func(*pb.BackupEvent) error
}

func (e *bkEmitter) log(level, text string) {
	_ = e.send(&pb.BackupEvent{Event: &pb.BackupEvent_Log{Log: &pb.LogLine{Level: level, Text: text}}})
}
func (e *bkEmitter) result(ok bool, errMsg, objectKey string, size int64) {
	_ = e.send(&pb.BackupEvent{Event: &pb.BackupEvent_Result{
		Result: &pb.BackupResult{Ok: ok, Error: errMsg, ObjectKey: objectKey, SizeBytes: size},
	}})
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
		Endpoint:  t.GetEndpoint(),
		Region:    t.GetRegion(),
		Bucket:    t.GetBucket(),
		AccessKey: t.GetAccessKey(),
		SecretKey: t.GetSecretKey(),
		PathStyle: t.GetPathStyle(),
	}
}

// Backup dumps a database or project to S3, streaming progress. The whole RPC
// runs on the stream's context so a control-plane disconnect cancels the dump
// (unlike Deploy, a half-finished backup is not worth keeping alive — the
// control plane re-runs it).
func (s *Service) Backup(req *pb.BackupRequest, stream pb.Agent_BackupServer) error {
	e := &bkEmitter{send: stream.Send}
	ctx := stream.Context()

	if req.GetS3() == nil || req.GetS3().GetObjectKey() == "" {
		e.result(false, "backup request missing S3 target / object key", "", 0)
		return nil
	}
	switch req.GetKind() {
	case pb.BackupKind_BACKUP_KIND_DATABASE:
		s.backupDatabase(ctx, req, e)
	case pb.BackupKind_BACKUP_KIND_PROJECT:
		s.backupProject(ctx, req, e)
	default:
		e.result(false, "unknown backup kind", "", 0)
	}
	return nil
}

// Restore restores a database or project from an S3 object, in place.
func (s *Service) Restore(req *pb.RestoreRequest, stream pb.Agent_RestoreServer) error {
	e := &rsEmitter{send: stream.Send}
	ctx := stream.Context()

	if req.GetS3() == nil || req.GetS3().GetObjectKey() == "" {
		e.result(false, "restore request missing S3 target / object key")
		return nil
	}
	switch req.GetKind() {
	case pb.BackupKind_BACKUP_KIND_DATABASE:
		s.restoreDatabase(ctx, req, e)
	case pb.BackupKind_BACKUP_KIND_PROJECT:
		s.restoreProject(ctx, req, e)
	default:
		e.result(false, "unknown restore kind")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Database backup / restore — `docker exec` the engine's dump tool, gzip → S3
// ---------------------------------------------------------------------------

// execWithSecretEnv builds a `docker exec [flags...]` argv prefix that forwards a
// password env var INTO the container WITHOUT putting the value on argv: when
// `pw` is non-empty it adds the bare `-e <name>` flag (no `=value`), and returns
// the `<name>=<pw>` entry for the HOST docker-client process env (PipeOut/PipeIn
// set it via cmd.Env). `docker exec -e <name>` (valueless) copies the host
// process's env var into the container, so the in-container tool reads the
// password from its env and the secret never appears on any command line. When
// `pw` is empty no env is forwarded. Extra `flags` (e.g. "-i") are appended after
// `exec`. The caller appends the container + tool argv after the returned prefix.
func execWithSecretEnv(pw, name string, flags ...string) (argv []string, env []string) {
	a := append([]string{"exec"}, flags...)
	if pw != "" {
		a = append(a, "-e", name)
		env = []string{name + "=" + pw}
	}
	return a, env
}

// dumpArgv returns the `docker exec` argv that dumps the database to stdout, and
// the extra HOST-PROCESS env the docker client needs, for an engine. `container`
// is the DB container; the tool runs INSIDE it. Mirrors the format table in the
// PLAN. Returns an error for an unsupported engine.
//
// SECRET HANDLING: the password is kept OFF the host docker-client's argv (which
// is world-readable via `ps`/`/proc/<pid>/cmdline`). For engines whose tool reads
// a password ENV VAR (postgres PGPASSWORD, mysql MYSQL_PWD, redis REDISCLI_AUTH),
// the VALUE is returned in `env` (set on the host docker process via PipeOut's
// cmd.Env) and only the bare `-e NAME` flag goes on argv — `docker exec -e NAME`
// (no value) forwards the host process's env var into the container, so the
// value never touches any argv. mongodump/mongorestore have no password env var,
// so mongo still passes `-p <pw>` on argv (a documented residual); redactArgs in
// dockercli masks it out of any error string regardless.
func dumpArgv(d *pb.DatabaseDescriptor) (argv []string, env []string, err error) {
	c, user, db := d.GetContainer(), d.GetUser(), d.GetDbName()
	pw := d.GetPassword()
	switch strings.ToLower(d.GetDbType()) {
	case "postgres":
		// -Fc = custom (compressed/orderable) format; restored with pg_restore
		// --clean --if-exists to drop-and-recreate (overwrite, not append).
		a, env := execWithSecretEnv(pw, "PGPASSWORD")
		a = append(a, c, "pg_dump", "-U", user, "-Fc", db)
		return a, env, nil
	case "mysql", "mariadb":
		// --add-drop-table makes the restore overwrite each table.
		a, env := execWithSecretEnv(pw, "MYSQL_PWD")
		a = append(a, c, "mysqldump", "-u", user, "--add-drop-table", "--databases", db)
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
		// REDISCLI_AUTH (env), which redis-cli honours — so it stays off argv.
		a, env := execWithSecretEnv(pw, "REDISCLI_AUTH")
		a = append(a, c, "redis-cli", "--rdb", "-")
		return a, env, nil
	case "clickhouse":
		// Clickhouse is multi-statement (schema + per-table data), not a single
		// stdout pipe — handled by dumpClickhouse, dispatched in backupDatabase.
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
	switch strings.ToLower(d.GetDbType()) {
	case "postgres":
		// --clean --if-exists drops existing objects first => overwrite. Password
		// in PGPASSWORD (env), off argv (see dumpArgv's SECRET HANDLING note).
		a, env := execWithSecretEnv(pw, "PGPASSWORD", "-i")
		a = append(a, c, "pg_restore", "-U", user, "--clean", "--if-exists", "-d", db)
		return a, env, nil
	case "mysql", "mariadb":
		// The --databases dump carries its own USE/CREATE; mysql applies it. The
		// --add-drop-table in the dump overwrites each table. Password off argv.
		a, env := execWithSecretEnv(pw, "MYSQL_PWD", "-i")
		a = append(a, c, "mysql", "-u", user)
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
		// Redis does NOT restore over a single stdin pipe: the dump is an RDB file
		// (redis-cli --rdb), and `redis-cli --pipe` speaks RESP, not RDB — feeding
		// it an RDB fails ("unknown command 'REDIS0014'"). A correct RDB restore is
		// a multi-step dance (disable save → flush → write /data/dump.rdb → SHUTDOWN
		// NOSAVE → reload), so it has its own path (restoreRedis), not this argv.
		return nil, nil, errRedisRestoreSeparate
	case "clickhouse":
		// Clickhouse restores the SQL script via `clickhouse-client --multiquery`,
		// handled by restoreClickhouse (dispatched in restoreDatabase).
		return nil, nil, errClickhouseSeparate
	default:
		return nil, nil, fmt.Errorf("unsupported database engine %q", d.GetDbType())
	}
}

func (s *Service) backupDatabase(ctx context.Context, req *pb.BackupRequest, e *bkEmitter) {
	d := req.GetDatabase()
	if d == nil || d.GetContainer() == "" {
		e.result(false, "database backup request missing descriptor / container", "", 0)
		return
	}
	key := req.GetS3().GetObjectKey()
	e.log("info", fmt.Sprintf("Dumping %s database %q from container %q", d.GetDbType(), d.GetDbName(), d.GetContainer()))

	// The dump PRODUCER writes the raw dump bytes into `w` (which is the gzip
	// writer feeding the S3 upload). Most engines are a single `docker exec`
	// piped to stdout; clickhouse is a multi-statement SQL script the agent
	// assembles per table. Both shapes funnel through the same gzip→S3 pipeline.
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

	// Pipeline: producer → gzip → S3 multipart PUT. A pipe couples the producer
	// to the uploader so there is no temp file; the upload reads as the dump
	// writes. The producer runs in a goroutine writing the pipe; the uploader
	// reads on this goroutine.
	pr, pw := io.Pipe()
	gz := gzip.NewWriter(pw)
	go func() {
		// Run the producer, then close gzip BEFORE the pipe so the reader sees a
		// clean EOF (gzip trailer flushed) — or the producer's error, which aborts
		// the upload with that cause.
		derr := produce(gz)
		cerr := gz.Close()
		if derr != nil {
			pw.CloseWithError(derr)
			return
		}
		pw.CloseWithError(cerr) // nil on success => clean EOF
	}()

	size, uerr := s3client.Upload(ctx, s3cfg(req.GetS3()), key, pr)
	if uerr != nil {
		// Drain/cancel the producer so the dump goroutine isn't left blocked.
		_ = pr.CloseWithError(uerr)
		e.result(false, "upload to S3: "+uerr.Error(), "", 0)
		return
	}
	e.log("info", fmt.Sprintf("Uploaded %s (%d bytes)", key, size))
	e.result(true, "", key, size)
}

func (s *Service) restoreDatabase(ctx context.Context, req *pb.RestoreRequest, e *rsEmitter) {
	d := req.GetDatabase()
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
			s.restoreRedis(ctx, req, e)
		case errClickhouseSeparate:
			s.restoreClickhouse(ctx, req, e)
		default:
			e.result(false, err.Error())
		}
		return
	}
	key := req.GetS3().GetObjectKey()
	e.log("info", fmt.Sprintf("Restoring %s database %q into container %q from %s", d.GetDbType(), d.GetDbName(), d.GetContainer(), key))

	obj, derr := s3client.Download(ctx, s3cfg(req.GetS3()), key)
	if derr != nil {
		e.result(false, "open S3 object: "+derr.Error())
		return
	}
	defer obj.Close()

	gz, gerr := gzip.NewReader(obj)
	if gerr != nil {
		e.result(false, "open gzip stream (is the object a Deplo backup?): "+gerr.Error())
		return
	}
	defer gz.Close()

	// Pipeline: S3 object → gunzip → docker exec -i stdin (the restore tool).
	code, rerr := dockercli.PipeIn(ctx, backupStepTimeout, gz, env, argv...)
	if rerr != nil {
		e.result(false, "restore: "+rerr.Error())
		return
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("restore tool exited %d", code))
		return
	}
	e.log("info", "Restore complete")
	e.result(true, "")
}

// errRedisRestoreSeparate flags that redis must restore via restoreRedis (the
// RDB file-swap dance) rather than the uniform stdin-pipe restore path.
var errRedisRestoreSeparate = fmt.Errorf("redis restore uses the dedicated file-swap path")

// restoreRedis restores a redis RDB dump IN PLACE. Redis only loads its RDB at
// startup and a graceful shutdown would SAVE the live (about-to-be-replaced)
// dataset over our file — so the sequence is: disable save rules (so shutdown
// won't overwrite), FLUSHALL (overwrite, not merge), stream the decompressed RDB
// to <dir>/<dbfilename>, then SHUTDOWN NOSAVE and wait for the supervisor
// (Docker restart policy / compose) to bring the container back, which loads the
// restored RDB. Verified end-to-end against redis:alpine.
//
// This requires the container to be restarted by its supervisor; if it isn't
// (no restart policy), the stream reports the failure clearly rather than
// leaving redis down silently.
func (s *Service) restoreRedis(ctx context.Context, req *pb.RestoreRequest, e *rsEmitter) {
	d := req.GetDatabase()
	c, pw := d.GetContainer(), d.GetPassword()
	key := req.GetS3().GetObjectKey()
	e.log("info", fmt.Sprintf("Restoring redis %q into container %q from %s", d.GetDbName(), c, key))

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
	obj, derr := s3client.Download(ctx, s3cfg(req.GetS3()), key)
	if derr != nil {
		e.result(false, "open S3 object: "+derr.Error())
		return
	}
	defer obj.Close()
	gz, gerr := gzip.NewReader(obj)
	if gerr != nil {
		e.result(false, "open gzip stream (is the object a Deplo backup?): "+gerr.Error())
		return
	}
	defer gz.Close()
	// `sh -c 'cat > <path>'` writes the piped bytes into the container fs.
	if code, werr := dockercli.PipeIn(ctx, backupStepTimeout, gz, nil,
		"exec", "-i", c, "sh", "-c", "cat > "+shellQuote(rdbPath)); werr != nil {
		e.result(false, "write RDB into container: "+werr.Error())
		return
	} else if code != 0 {
		e.result(false, fmt.Sprintf("write RDB into container exited %d", code))
		return
	}

	// 4. SHUTDOWN NOSAVE: exits WITHOUT rewriting the RDB, leaving our file intact.
	//    The redis-cli connection drops as the server exits — that is expected, not
	//    an error, so we don't gate on its exit code.
	e.log("info", "Reloading redis from the restored snapshot")
	_, _ = dockercli.RunEnv(ctx, 15*time.Second, cliEnv, cli("SHUTDOWN", "NOSAVE")...)

	// 5. Wait for the supervisor to bring redis back AND for it to answer PING.
	if !waitRedisReady(ctx, c, pw, 60*time.Second) {
		e.result(false, "redis did not come back after SHUTDOWN NOSAVE — ensure the container has a restart policy")
		return
	}
	e.log("info", "Restore complete")
	e.result(true, "")
}

// redisConfig reads a single CONFIG GET value from a redis container ("" on any
// failure — the caller falls back to the documented default).
// redisCliPrefix builds the `docker exec [-e REDISCLI_AUTH] <container> redis-cli`
// argv prefix + the host-process env carrying the password value, so redis-cli
// auth never lands on argv (the value rides in REDISCLI_AUTH, forwarded by the
// valueless `-e` flag). Used by every redis control call in the restore dance.
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
// CONFIG value (dir/dbfilename) — control-plane/agent-derived, not user free
// text — but we quote defensively since it is interpolated into a shell string.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// ---------------------------------------------------------------------------
// Project backup / restore — tar volumes + files + snapshot, gzip → S3
// ---------------------------------------------------------------------------

// backupProject tars a project's named + compose-stack volumes (via a throwaway
// helper container that mounts each), the project files dir, and the rendered
// compose/env snapshot, gzip-compresses, and uploads. Host bind mounts are NOT
// in volume_names (the control plane excluded them), so they are never touched.
//
// The archive layout (a single gzipped tar) is:
//
//	volumes/<volumeName>/...   — each named volume's contents
//	files/...                  — the project files dir (<stack-dir>/files/<slug>)
//	snapshot/compose.yml       — the rendered compose at backup time
//	snapshot/env               — the decrypted env snapshot (KEY=VALUE lines)
//	snapshot/mounts/<path>     — compose mount files (template config)
//
// Restore reverses it: wipe + repopulate the volumes + files, then re-Reroute
// the snapshot so the stack restarts on the EXACT backed-up config.
func (s *Service) backupProject(ctx context.Context, req *pb.BackupRequest, e *bkEmitter) {
	p := req.GetProject()
	if p == nil || p.GetSlug() == "" {
		e.result(false, "project backup request missing descriptor / slug", "", 0)
		return
	}
	if err := validateSlug(p.GetSlug()); err != nil {
		e.result(false, err.Error(), "", 0)
		return
	}
	key := req.GetS3().GetObjectKey()
	e.log("info", fmt.Sprintf("Backing up project %q (%d volume(s), files=%v)", p.GetSlug(), len(p.GetVolumeNames()), p.GetIncludeFiles()))

	pr, pw := io.Pipe()
	go func() {
		gz := gzip.NewWriter(pw)
		tw := tar.NewWriter(gz)
		err := s.writeProjectArchive(ctx, p, tw, e)
		// Finish the tar + gzip BEFORE closing the pipe so the trailer is written.
		if cerr := tw.Close(); err == nil {
			err = cerr
		}
		if cerr := gz.Close(); err == nil {
			err = cerr
		}
		pw.CloseWithError(err)
	}()

	size, uerr := s3client.Upload(ctx, s3cfg(req.GetS3()), key, pr)
	if uerr != nil {
		_ = pr.CloseWithError(uerr)
		e.result(false, "upload to S3: "+uerr.Error(), "", 0)
		return
	}
	e.log("info", fmt.Sprintf("Uploaded %s (%d bytes)", key, size))
	e.result(true, "", key, size)
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

// archiveVolume runs a helper container that mounts the named volume read-only
// at /v and tars its contents to stdout; we read that tar and re-emit every
// entry under volumes/<vol>/ into our own tar, so one restore-time pass can
// route each entry back to the right volume.
//
// Producer exit handling balances two failure modes. A tar.Reader returns io.EOF
// at the in-band trailer, NOT when the pipe closes, so a non-zero helper exit can
// hide behind a "clean" EOF. But busybox `tar -cf -` legitimately exits 1 for a
// benign "file changed as we read it" on a LIVE volume while STILL emitting a
// complete, valid archive — failing the whole backup on that would make any
// live-volume backup impossible. So: a reader-side error mid-stream (truncation)
// is fatal, but a non-zero producer exit AFTER a clean EOF (complete archive) is
// surfaced as a WARNING, not a failure — the archive is whole.
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
			// Drain any trailing bytes so the producer's PipeOut write completes and
			// reports its exit, then WARN (not fail) on a non-zero exit — the reader
			// reached a clean trailer, so the archive is complete (busybox's benign
			// "file changed" exit 1 on a live volume is the common cause).
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

// restoreProject reverses backupProject: stop the stack, wipe + repopulate each
// volume + the files dir, then re-Reroute the snapshot compose/env (which
// restarts the stack on the EXACT backed-up config). A restart failure (e.g. a
// snapshot image that no longer exists) is reported clearly rather than leaving
// the stack silently down.
func (s *Service) restoreProject(ctx context.Context, req *pb.RestoreRequest, e *rsEmitter) {
	p := req.GetProject()
	if p == nil || p.GetSlug() == "" {
		e.result(false, "project restore request missing descriptor / slug")
		return
	}
	slug := p.GetSlug()
	if err := validateSlug(slug); err != nil {
		e.result(false, err.Error())
		return
	}
	key := req.GetS3().GetObjectKey()
	e.log("info", fmt.Sprintf("Restoring project %q from %s", slug, key))

	// Stop the stack so volumes aren't written under us while we wipe them.
	e.log("info", "Stopping the stack")
	if _, err := dockercli.Run(ctx, 90*time.Second, "compose", "-p", "deplo-"+slug, "-f", s.stackPath(slug), "stop"); err != nil {
		e.log("warn", "stack stop: "+err.Error()+" (continuing)")
	}

	obj, derr := s3client.Download(ctx, s3cfg(req.GetS3()), key)
	if derr != nil {
		e.result(false, "open S3 object: "+derr.Error())
		return
	}
	defer obj.Close()
	gz, gerr := gzip.NewReader(obj)
	if gerr != nil {
		e.result(false, "open gzip stream (is the object a Deplo backup?): "+gerr.Error())
		return
	}
	defer gz.Close()

	snapshot, err := s.unpackProjectArchive(ctx, slug, p.GetVolumeNames(), gz, e)
	if err != nil {
		e.result(false, err.Error())
		return
	}

	// Re-Reroute the snapshot so the stack restarts on the exact backed-up config.
	// Prefer the snapshot captured in the archive; fall back to the descriptor's
	// (the control plane sends both for resilience).
	composeYaml := snapshot.compose
	if composeYaml == "" {
		composeYaml = p.GetComposeYaml()
	}
	if composeYaml == "" {
		e.log("warn", "No compose snapshot in the archive; leaving the stack stopped")
		e.result(true, "")
		return
	}
	e.log("info", "Re-applying the backed-up stack configuration")
	rr := &pb.RerouteRequest{
		Slug:        slug,
		ComposeYaml: composeYaml,
		Env:         snapshot.env,
		Mounts:      snapshot.mounts,
	}
	if len(rr.Env) == 0 {
		rr.Env = p.GetEnvSnapshot()
	}
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

// projectSnapshot is the config recovered from the archive's snapshot/ entries.
type projectSnapshot struct {
	compose string
	env     map[string]string
	mounts  []*pb.MountFile
}

// unpackProjectArchive reads the gzipped tar, routing each entry: volumes/<vol>/*
// back into a freshly-wiped volume (via a helper container), files/* into a
// freshly-wiped files dir, and snapshot/* into the returned projectSnapshot. The
// volumes are wiped first (only the ones present in volumeNames) so the restore
// overwrites rather than merges.
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

	tr := tar.NewReader(r)
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
			// The entry name + type come from an S3 object — never trusted. The
			// helper's `tar -x` would honour a `..` path or a symlink, which could
			// escape the volume mount inside the container; reject traversal and
			// restore only dirs + regular files (skip symlinks/hardlinks/devices),
			// mirroring extractToDir's guard for the files/ arm.
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
			b, err := io.ReadAll(tr)
			if err != nil {
				return snap, err
			}
			snap.compose = string(b)
		case name == "snapshot/env":
			b, err := io.ReadAll(tr)
			if err != nil {
				return snap, err
			}
			snap.env = parseEnvFile(string(b))
		case strings.HasPrefix(name, "snapshot/mounts/"):
			b, err := io.ReadAll(tr)
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
	if req.GetS3() == nil {
		return nil, status.Error(codes.InvalidArgument, "s3 check request missing target")
	}
	if err := s3client.Check(ctx, s3cfg(req.GetS3())); err != nil {
		return &pb.S3CheckResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.S3CheckResponse{Ok: true}, nil
}

// S3Delete deletes a single object (or, with prefix=true, a whole target folder)
// — backs retention + delete-with-artifacts. Idempotent.
func (s *Service) S3Delete(ctx context.Context, req *pb.S3DeleteRequest) (*pb.S3DeleteResponse, error) {
	if req.GetS3() == nil || req.GetS3().GetObjectKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "s3 delete request missing key/prefix")
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
