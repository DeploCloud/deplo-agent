package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/s3client"
	"google.golang.org/protobuf/proto"
)

// Postgres on a cluster: a physical base backup, the change log archived after it, and the restore that replays
// both. The control plane's render makes Postgres archive into <PGDATA>/deplo-wal; that name is the contract.

const (
	pgDefaultDataDir = "/var/lib/postgresql/data"
	// ponytail: 256 segments (4 GB) per artifact; a database writing more than that a minute falls behind.
	pgLogBatch      = 256
	pgRecoveryWait  = 30 * time.Minute
	pgInSpool       = `cd "${PGDATA:-` + pgDefaultDataDir + `}/deplo-wal" 2>/dev/null || exit 0; `
	pgRestoreLogDir = "deplo-wal-restore"
	pgHelperScript  = `set -e; o=$(stat -c %u:%g "$D"); r="$D/` + pgRestoreLogDir + `"; case "$1" in ` +
		`base) find "$D" -mindepth 1 -delete; tar -xf - -C "$D"; rm -rf "$D/deplo-wal"; chown -R "$o" "$D";; ` +
		`log) mkdir -p "$r"; tar -xf - -C "$r"; chown -R "$o" "$r";; ` +
		`replay) touch "$D/recovery.signal"; echo "restore_command = 'cp $r/%f %p'" >> "$D/postgresql.auto.conf"; ` +
		`chown "$o" "$D/recovery.signal" "$D/postgresql.auto.conf";; esac`
)

// What Postgres archives: a segment, a partial one, a backup or timeline history file. Never a path or a flag.
var pgLogName = regexp.MustCompile(`^[0-9A-F]{8}[0-9A-F.a-z]*$`)

func isPostgres(d *pb.DatabaseDescriptor) bool { return strings.ToLower(d.GetDbType()) == "postgres" }

// backupPostgresArchive writes a base backup or the waiting change log through the same artifact pipeline a dump takes.
func (s *Service) backupPostgresArchive(ctx context.Context, mode pb.PostgresArchive, d *pb.DatabaseDescriptor, dest *artifactDestination, e *bkEmitter) {
	c := d.GetContainer()
	if c == "" || !isPostgres(d) {
		e.result(false, "only a Postgres database has a base backup and a change log", "", 0)
		return
	}
	if err := assertDatabaseOwned(ctx, d); err != nil {
		e.result(false, statusMessage(err), "", 0)
		return
	}
	var argv, env, waiting []string
	if mode == pb.PostgresArchive_POSTGRES_ARCHIVE_BASE {
		argv, env = execWithSecretEnv(d.GetPassword(), "PGPASSWORD")
		// A fast checkpoint: the default spreads it over minutes, and this runs every few.
		argv = append(argv, c, "pg_basebackup", "-U", d.GetUser(), "-D", "-", "-Ft", "-X", "fetch", "-c", "fast")
	} else {
		res, err := dockercli.Run(ctx, time.Minute, "exec", c, "sh", "-c", pgInSpool+"ls -1")
		if err != nil || res.Code != 0 {
			e.result(false, "list the change log: "+combineErr(err, res.Stderr), "", 0)
			return
		}
		for _, name := range strings.Fields(res.Stdout) {
			if pgLogName.MatchString(name) && len(waiting) < pgLogBatch {
				waiting = append(waiting, name)
			}
		}
		if len(waiting) == 0 {
			e.result(true, "", "", 0)
			return
		}
		argv = append([]string{"exec", c, "sh", "-c", pgInSpool + `exec tar -cf - -- "$@"`, "sh"}, waiting...)
	}
	written, err := s.writeArtifact(ctx, dest, func(w io.Writer) error {
		code, derr := dockercli.PipeOut(ctx, backupStepTimeout, w, env, argv...)
		if derr == nil && code != 0 {
			derr = fmt.Errorf("exited %d", code)
		}
		return derr
	})
	if err != nil {
		e.result(false, statusMessage(err), "", 0)
		return
	}
	if len(waiting) > 0 {
		// Only now: what never reached the destination is sent again with the next one.
		rm := append([]string{"exec", c, "sh", "-c", pgInSpool + `rm -f -- "$@"`, "sh"}, waiting...)
		if res, rerr := dockercli.Run(ctx, time.Minute, rm...); rerr != nil || res.Code != 0 {
			e.log("warn", "The change log was archived but not cleared here: "+combineErr(rerr, res.Stderr))
		}
	}
	e.log("info", fmt.Sprintf("Wrote %s (%d bytes)", dest.label, written.size))
	e.emitResult(true, "", dest.key, written)
}

// isBaseBackup tells a base backup from a dump: pg_basebackup's tar always opens with backup_label.
func isBaseBackup(r *bufio.Reader) bool {
	head, _ := r.Peek(len("backup_label") + 1)
	return bytes.Equal(head, []byte("backup_label\x00"))
}

// logSources lists the change-log artifacts under the request's prefix, oldest first, at the base backup's destination.
func (a *artifactSource) logSources(ctx context.Context) ([]*artifactSource, error) {
	if a.logPrefix == "" {
		return nil, nil
	}
	var out []*artifactSource
	switch {
	case a.store != nil:
		dir, err := resolveInside(a.store.GetRoot(), a.logPrefix)
		if err != nil {
			return nil, err
		}
		if dir == canonicalRoot(a.store.GetRoot()) {
			return nil, fmt.Errorf("the change log's folder is the whole backup store")
		}
		entries, err := os.ReadDir(dir)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("list the change log: %w", err)
		}
		for _, f := range entries {
			if f.Type().IsRegular() && !strings.HasSuffix(f.Name(), storePartialSuffix) {
				key := path.Join(a.logPrefix, f.Name())
				out = append(out, &artifactSource{store: &pb.StoreTarget{Root: a.store.GetRoot(), ObjectKey: key}, identity: a.identity, label: key})
			}
		}
	case a.s3 != nil:
		keys, err := s3client.List(ctx, s3cfg(a.s3), strings.TrimRight(a.logPrefix, "/")+"/")
		if err != nil {
			return nil, fmt.Errorf("list the change log: %w", err)
		}
		for _, key := range keys {
			t := proto.Clone(a.s3).(*pb.S3Target)
			t.ObjectKey = key
			out = append(out, &artifactSource{s3: t, identity: a.identity, label: key})
		}
	default:
		return nil, fmt.Errorf("a relayed backup carries no change log")
	}
	sort.Slice(out, func(i, j int) bool { return out[i].label < out[j].label })
	return out, nil
}

// restorePostgresBase replaces the data directory with the base backup, then lets Postgres replay the change log.
// ponytail: a change-log artifact carries no digest of its own; record one per artifact if a destination is not trusted.
func (s *Service) restorePostgresBase(ctx context.Context, d *pb.DatabaseDescriptor, base io.Reader, src *artifactSource, e *rsEmitter) {
	c := d.GetContainer()
	logs, err := src.logSources(ctx)
	if err != nil {
		e.result(false, statusMessage(err))
		return
	}
	res, ierr := dockercli.Run(ctx, 30*time.Second, "inspect", "-f", `{{.Config.Image}}{{"\n"}}{{range .Config.Env}}{{println .}}{{end}}`, c)
	lines := strings.Split(strings.TrimSpace(res.Stdout), "\n")
	if ierr != nil || res.Code != 0 || lines[0] == "" {
		e.result(false, "read the database's container: "+combineErr(ierr, res.Stderr))
		return
	}
	dataDir := pgDefaultDataDir
	for _, l := range lines[1:] {
		if v, ok := strings.CutPrefix(l, "PGDATA="); ok && v != "" {
			dataDir = v
		}
	}
	// The database's own image unpacks it: it has tar and the user the files belong to. Nothing else is given to it.
	helper := func(step string, in io.Reader) error {
		args := []string{"run", "--rm", "--log-driver=none", "--network", "none", "--volumes-from", c, "-e", "D=" + dataDir}
		if in != nil {
			args = append(args, "-i")
		}
		args = append(args, "--entrypoint", "sh", lines[0], "-c", pgHelperScript, "sh", step)
		code, herr := dockercli.PipeIn(ctx, backupStepTimeout, in, nil, args...)
		if herr == nil && code != 0 {
			herr = fmt.Errorf("exited %d", code)
		}
		return herr
	}
	e.log("info", fmt.Sprintf("Restoring postgres database %q into container %q from the base backup %s", d.GetDbName(), c, src.label))
	if r, serr := dockercli.Run(ctx, 2*time.Minute, "stop", c); serr != nil || r.Code != 0 {
		e.result(false, "stop the database: "+combineErr(serr, r.Stderr))
		return
	}
	if err := helper("base", base); err != nil {
		e.result(false, "unpack the base backup: "+err.Error())
		return
	}
	if verr := src.verify(); verr != nil {
		e.result(false, statusMessage(verr))
		return
	}
	for _, l := range logs {
		rd, closeLog, oerr := l.open(ctx)
		if oerr == nil {
			oerr = helper("log", rd)
			closeLog()
		}
		if oerr != nil {
			// Postgres replays what did arrive and stops at the first gap: an older point, never a broken one.
			e.log("warn", fmt.Sprintf("The change log %s could not be read, so the restore ends before it: %v", l.label, oerr))
			break
		}
	}
	if len(logs) > 0 {
		if err := helper("replay", nil); err != nil {
			e.result(false, "set up the change log replay: "+err.Error())
			return
		}
	}
	if r, serr := dockercli.Run(ctx, 2*time.Minute, "start", c); serr != nil || r.Code != 0 {
		e.result(false, "start the database: "+combineErr(serr, r.Stderr))
		return
	}
	psql, env := execWithSecretEnv(d.GetPassword(), "PGPASSWORD")
	psql = append(psql, c, "psql", "-U", d.GetUser(), "-d", d.GetDbName(), "-Atc")
	deadline := time.Now().Add(pgRecoveryWait)
	for {
		r, perr := dockercli.RunEnv(ctx, 15*time.Second, env, append(psql, "select pg_is_in_recovery()")...)
		if perr == nil && r.Code == 0 && strings.TrimSpace(r.Stdout) == "f" {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			e.result(false, "postgres did not finish replaying its change log")
			return
		}
		time.Sleep(pgRecoveryPoll)
	}
	if len(logs) > 0 {
		_, _ = dockercli.RunEnv(ctx, 15*time.Second, env, append(psql, "ALTER SYSTEM RESET restore_command")...)
		_, _ = dockercli.Run(ctx, time.Minute, "exec", c, "rm", "-rf", path.Join(dataDir, pgRestoreLogDir))
	}
	e.log("info", "Restore complete")
	e.result(true, "")
}

var pgRecoveryPoll = 2 * time.Second
