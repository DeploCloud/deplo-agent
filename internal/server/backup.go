package server

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

const (
	backupStepTimeout      = 30 * time.Minute
	volumeHelperImage      = "busybox:1.36"
	maxSnapshotBytes       = 16 << 20
	maxProjectRestoreBytes = 64 << 30
)

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

func (e *bkEmitter) data(b []byte) error {
	return e.emit(&pb.BackupEvent{Event: &pb.BackupEvent_Data{Data: b}})
}

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

// Restore restores a database or project from an artifact this host can reach (an S3 object or a local store), in place.
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

func execWithSecretEnv(pw, name string, flags ...string) (argv []string, env []string) {
	a := append([]string{"exec"}, flags...)
	if pw != "" {
		a = append(a, "-e", name)
		env = []string{name + "=" + pw}
	}
	return a, env
}

func mongoShell(tool string, withPassword bool) string {
	script := "exec " + tool + ` ${1:+--db="$1"} ${2:+-u "$2" --authenticationDatabase=admin}`
	if withPassword {
		script += ` -p "$MONGO_PW"`
	}
	return script
}

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
		a, env := execWithSecretEnv(pw, "PGPASSWORD")
		a = append(a, c, "pg_dump", "-U", user, "-Fc", db)
		return a, env, nil
	case "mysql", "mariadb":
		a, env := execWithSecretEnv(pw, "MYSQL_PWD")
		a = append(a, c, mysqlClient(dbType, "dump"), "-u", user, "--add-drop-table", "--databases", db)
		return a, env, nil
	case "mongodb":
		a, env := execWithSecretEnv(pw, "MONGO_PW")
		a = append(a, c, "sh", "-c", mongoShell("mongodump --archive", pw != ""), "sh", db, user)
		return a, env, nil
	case "redis":
		a, env := execWithSecretEnv(pw, "REDISCLI_AUTH")
		a = append(a, c, "redis-cli", "--rdb", "-")
		return a, env, nil
	case "clickhouse":
		return nil, nil, errClickhouseSeparate
	default:
		return nil, nil, fmt.Errorf("unsupported database engine %q", d.GetDbType())
	}
}

func restoreArgv(d *pb.DatabaseDescriptor) (argv []string, env []string, err error) {
	c, user, db := d.GetContainer(), d.GetUser(), d.GetDbName()
	pw := d.GetPassword()
	dbType := strings.ToLower(d.GetDbType())
	switch dbType {
	case "postgres":
		a, env := execWithSecretEnv(pw, "PGPASSWORD", "-i")
		a = append(a, c, "pg_restore", "-U", user, "--clean", "--if-exists", "-d", db)
		return a, env, nil
	case "mysql", "mariadb":
		a, env := execWithSecretEnv(pw, "MYSQL_PWD", "-i")
		a = append(a, c, mysqlClient(dbType, "shell"), "-u", user)
		return a, env, nil
	case "mongodb":
		a, env := execWithSecretEnv(pw, "MONGO_PW", "-i")
		a = append(a, c, "sh", "-c", mongoShell("mongorestore --archive --drop", pw != ""), "sh", "", user)
		return a, env, nil
	case "redis":
		return nil, nil, errRedisRestoreSeparate
	case "clickhouse":
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
	if verr := src.verify(); verr != nil {
		e.result(false, statusMessage(verr))
		return
	}
	e.log("info", "Restore complete")
	e.result(true, "")
}

var errRedisRestoreSeparate = fmt.Errorf("redis restore uses the dedicated file-swap path")

func (s *Service) restoreRedis(ctx context.Context, d *pb.DatabaseDescriptor, src *artifactSource, e *rsEmitter) {
	c, pw := d.GetContainer(), d.GetPassword()
	e.log("info", fmt.Sprintf("Restoring redis %q into container %q from %s", d.GetDbName(), c, src.label))

	cliArgv, cliEnv := redisCliPrefix(c, pw)
	cli := func(args ...string) []string { return append(cliArgv, args...) }

	dir := redisConfig(ctx, c, pw, "dir")
	if dir == "" {
		dir = "/data"
	}
	dbfile := redisConfig(ctx, c, pw, "dbfilename")
	if dbfile == "" {
		dbfile = "dump.rdb"
	}
	rdbPath := dir + "/" + dbfile

	if res, err := dockercli.RunEnv(ctx, 15*time.Second, cliEnv, cli("CONFIG", "SET", "save", "")...); err != nil || res.Code != 0 {
		e.result(false, "redis CONFIG SET save: "+combineErr(err, res.Stderr))
		return
	}
	if res, err := dockercli.RunEnv(ctx, 30*time.Second, cliEnv, cli("FLUSHALL")...); err != nil || res.Code != 0 {
		e.result(false, "redis FLUSHALL: "+combineErr(err, res.Stderr))
		return
	}

	rd, closeSrc, oerr := src.open(ctx)
	if oerr != nil {
		e.result(false, statusMessage(oerr))
		return
	}
	defer closeSrc()
	if code, werr := dockercli.PipeIn(ctx, backupStepTimeout, rd, nil,
		"exec", "-i", c, "sh", "-c", "cat > "+shellQuote(rdbPath)); werr != nil {
		e.result(false, "write RDB into container: "+werr.Error())
		return
	} else if code != 0 {
		e.result(false, fmt.Sprintf("write RDB into container exited %d", code))
		return
	}

	e.log("info", "Reloading redis from the restored snapshot")
	_, _ = dockercli.RunEnv(ctx, 15*time.Second, cliEnv, cli("SHUTDOWN", "NOSAVE")...)

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
	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	if len(lines) >= 2 {
		return strings.TrimSpace(lines[1])
	}
	return ""
}

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

func combineErr(err error, stderr string) string {
	if err != nil {
		return err.Error()
	}
	if s := strings.TrimSpace(stderr); s != "" {
		return s
	}
	return "command failed"
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

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

func (s *Service) writeProjectArchive(ctx context.Context, p *pb.ProjectDescriptor, tw *tar.Writer, e *bkEmitter) error {
	for _, vol := range p.GetVolumeNames() {
		if vol == "" {
			continue
		}
		e.log("info", "Archiving volume "+vol)
		if err := s.archiveVolume(ctx, vol, tw, e); err != nil {
			return fmt.Errorf("archive volume %q: %w", vol, err)
		}
	}

	if p.GetIncludeFiles() {
		root := s.filesRoot(p.GetSlug())
		if st, err := os.Stat(root); err == nil && st.IsDir() {
			e.log("info", "Archiving project files")
			if err := addDirToTar(tw, root, "files"); err != nil {
				return fmt.Errorf("archive files: %w", err)
			}
		}
	}

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

func (s *Service) archiveVolume(ctx context.Context, vol string, tw *tar.Writer, e *bkEmitter) error {
	if err := validateVolumeName(vol); err != nil {
		return err
	}
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		code, err := dockercli.PipeOut(ctx, backupStepTimeout, pw, nil,
			volumeHelperRun(ctx, "-v", vol+":/v:ro", volumeHelperImage,
				"tar", "-C", "/v", "-cf", "-", ".")...)
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

func restoreConfig(
	slug string,
	p *pb.ProjectDescriptor,
	snap projectSnapshot,
	proven bool,
	untrusted bool,
) *pb.RerouteRequest {
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
	compose := retargetStackNetwork(pick(snap.compose, p.GetComposeYaml()), p.GetNetwork())

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

	return &pb.RerouteRequest{
		Slug: slug, ComposeYaml: compose, Env: env, Mounts: mounts,
		Network: p.GetNetwork(),
	}
}

type projectSnapshot struct {
	compose string
	env     map[string]string
	mounts  []*pb.MountFile
}

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

func (s *Service) unpackProjectArchive(ctx context.Context, slug string, volumeNames []string, r io.Reader, e *rsEmitter) (projectSnapshot, error) {
	snap := projectSnapshot{env: map[string]string{}}

	for _, vol := range volumeNames {
		if vol == "" {
			continue
		}
		if err := validateVolumeName(vol); err != nil {
			return snap, err
		}
	}

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

	vstreams := newVolumeStreams(ctx, volumeNames)
	defer vstreams.closeAll()

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
				continue
			}
			w, ok := vstreams.writerFor(vol)
			if !ok {
				continue
			}
			if hasDotDot(inner) {
				return snap, fmt.Errorf("archive volume entry %q contains a path traversal", inner)
			}
			if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeDir {
				continue
			}
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

	if err := vstreams.finish(e); err != nil {
		return snap, err
	}
	return snap, nil
}

// S3Check verifies the bucket is reachable + writable for the "Test connection" button.
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

// S3Delete deletes a single artifact (or, with prefix=true, a whole target folder) - backs retention + delete-with-artifacts.
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

func retargetStackNetwork(composeYaml, network string) string {
	if composeYaml == "" || network == "" {
		return composeYaml
	}
	lines := strings.Split(composeYaml, "\n")
	inNetworks := false
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			inNetworks = trimmed == "networks:"
			continue
		}
		if !inNetworks || !strings.HasPrefix(trimmed, "name:") {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(trimmed, "name:"))
		value = strings.Trim(value, `"'`)
		if !dockercli.IsTenantNetwork(value) || value == network {
			continue
		}
		indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
		lines[i] = indent + "name: " + network
	}
	return strings.Join(lines, "\n")
}
