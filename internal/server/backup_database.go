package server

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

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
	if err := assertDatabaseOwned(ctx, d); err != nil {
		e.result(false, statusMessage(err), "", 0)
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
	if err := assertDatabaseOwned(ctx, d); err != nil {
		e.result(false, statusMessage(err))
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

	// Opened first: a missing or undecryptable artifact must fail before FLUSHALL empties redis.
	rd, closeSrc, oerr := src.open(ctx)
	if oerr != nil {
		e.result(false, statusMessage(oerr))
		return
	}
	defer closeSrc()
	if res, err := dockercli.RunEnv(ctx, 15*time.Second, cliEnv, cli("CONFIG", "SET", "save", "")...); err != nil || res.Code != 0 {
		e.result(false, "redis CONFIG SET save: "+combineErr(err, res.Stderr))
		return
	}
	if res, err := dockercli.RunEnv(ctx, 30*time.Second, cliEnv, cli("FLUSHALL")...); err != nil || res.Code != 0 {
		e.result(false, "redis FLUSHALL: "+combineErr(err, res.Stderr))
		return
	}
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
