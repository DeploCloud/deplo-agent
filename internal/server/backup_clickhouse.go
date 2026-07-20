package server

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/s3client"
)

// backup_clickhouse.go implements clickhouse backup/restore, which — unlike the
// single-`docker exec`-pipe engines — needs multi-statement orchestration: a
// clickhouse database is a SET of tables, each with its own DDL + data, and
// there is no single command that streams a restorable whole-database dump.
//
// DUMP: assemble a SQL SCRIPT into the backup stream — for every (non-view,
// non-temporary) table in the database, emit `DROP TABLE IF EXISTS` +
// `CREATE TABLE IF NOT EXISTS <ddl>` (from system.tables.create_table_query) +
// the rows as `INSERT … VALUES` (FORMAT SQLInsert, with the real qualified table
// name). The DROP+CREATE makes the restore drop-and-recreate (overwrite, not
// merge), matching every other engine's locked guarantee.
//
// RESTORE: stream the script back through `clickhouse-client --multiquery`,
// which replays the DROP/CREATE/INSERT statements in order. Verified end-to-end
// against clickhouse-server:24-alpine.

// errClickhouseSeparate flags that clickhouse uses the dedicated dump/restore
// path (dumpClickhouse / restoreClickhouse) rather than the single-pipe argv.
var errClickhouseSeparate = fmt.Errorf("clickhouse uses the dedicated multi-statement path")

const clickhouseQueryTimeout = 5 * time.Minute

// chClient builds the `docker exec <c> clickhouse-client [--user U] …` prefix.
// The password rides in CLICKHOUSE_PASSWORD on the host docker-client process
// env (forwarded via the valueless `-e`), never on argv — same discipline as the
// other engines (see execWithSecretEnv).
func chClientPrefix(d *pb.DatabaseDescriptor, stdin bool) (argv []string, env []string) {
	a := []string{"exec"}
	if stdin {
		a = append(a, "-i")
	}
	if pw := d.GetPassword(); pw != "" {
		a = append(a, "-e", "CLICKHOUSE_PASSWORD")
		env = []string{"CLICKHOUSE_PASSWORD=" + pw}
	}
	a = append(a, d.GetContainer(), "clickhouse-client")
	if u := d.GetUser(); u != "" {
		a = append(a, "--user", u)
	}
	// No --password on argv: clickhouse-client picks up the password from the
	// CLICKHOUSE_PASSWORD env var we forwarded above (when set), so the secret
	// never lands on the host docker-client's command line.
	return a, env
}

// chQuery runs a single clickhouse-client `--query` and returns trimmed stdout.
func (s *Service) chQuery(ctx context.Context, d *pb.DatabaseDescriptor, query string) (string, error) {
	argv, env := chClientPrefix(d, false)
	argv = append(argv, "--query", query)
	res, err := dockercli.RunEnv(ctx, clickhouseQueryTimeout, env, argv...)
	if err != nil {
		return "", err
	}
	if res.Code != 0 {
		return "", fmt.Errorf("clickhouse query failed: %s", strings.TrimSpace(res.Stderr))
	}
	return res.Stdout, nil
}

// dumpClickhouse writes the database's full restorable SQL script to `w`. It runs
// on the same context as the Backup RPC; an error aborts the upload with its
// cause (the pipe propagates it to the uploader).
func (s *Service) dumpClickhouse(ctx context.Context, d *pb.DatabaseDescriptor, w io.Writer) error {
	db := d.GetDbName()
	if db == "" {
		return fmt.Errorf("clickhouse backup requires a database name")
	}
	// Enumerate the real tables (exclude views + temporary tables — a view's data
	// is derived, and its definition would be restored by its source table's DDL
	// only if we dumped views too; keeping to base tables is the safe, portable set).
	out, err := s.chQuery(ctx, d, fmt.Sprintf(
		"SELECT name FROM system.tables WHERE database='%s' AND engine NOT LIKE '%%View%%' AND NOT is_temporary ORDER BY name",
		chEscape(db)))
	if err != nil {
		return err
	}
	tables := nonEmptyLines(out)

	bw := bufio.NewWriter(w)
	fmt.Fprintf(bw, "-- Deplo clickhouse backup of database %q\n", db)
	for _, tbl := range tables {
		// DROP first so the restore overwrites rather than merges.
		fmt.Fprintf(bw, "DROP TABLE IF EXISTS `%s`.`%s`;\n", chQuoteIdent(db), chQuoteIdent(tbl))

		// Schema: the stored CREATE query, made idempotent. create_table_query is
		// `CREATE TABLE <db>.<t> (...)`; rewrite the leading verb to IF NOT EXISTS.
		ddl, err := s.chQuery(ctx, d, fmt.Sprintf(
			"SELECT create_table_query FROM system.tables WHERE database='%s' AND name='%s'",
			chEscape(db), chEscape(tbl)))
		if err != nil {
			return fmt.Errorf("read schema for %q: %w", tbl, err)
		}
		ddl = strings.TrimSpace(ddl)
		ddl = strings.Replace(ddl, "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
		fmt.Fprintf(bw, "%s;\n", ddl)

		// Data: rows as INSERT … VALUES with the real qualified table name. Stream
		// it (a table can be large) directly into the script via PipeOut.
		if err := bw.Flush(); err != nil {
			return err
		}
		argv, env := chClientPrefix(d, false)
		qdb, qtbl := chQuoteIdent(db), chQuoteIdent(tbl)
		argv = append(argv, "--query", fmt.Sprintf(
			"SELECT * FROM `%s`.`%s` SETTINGS output_format_sql_insert_table_name='`%s`.`%s`', "+
				"output_format_sql_insert_include_column_names=1, output_format_sql_insert_max_batch_size=1000 FORMAT SQLInsert",
			qdb, qtbl, qdb, qtbl))
		code, derr := dockercli.PipeOut(ctx, backupStepTimeout, w, env, argv...)
		if derr != nil {
			return fmt.Errorf("dump data for %q: %w", tbl, derr)
		}
		if code != 0 {
			return fmt.Errorf("dump data for %q exited %d", tbl, code)
		}
		if _, err := io.WriteString(w, "\n"); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// restoreClickhouse streams the backed-up SQL script back through
// `clickhouse-client --multiquery`, which replays the DROP/CREATE/INSERT
// statements (drop-and-recreate overwrite).
func (s *Service) restoreClickhouse(ctx context.Context, req *pb.RestoreRequest, e *rsEmitter) {
	d := req.GetDatabase()
	key := req.GetS3().GetObjectKey()
	e.log("info", fmt.Sprintf("Restoring clickhouse database %q into container %q from %s", d.GetDbName(), d.GetContainer(), key))

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

	argv, env := chClientPrefix(d, true)
	argv = append(argv, "--multiquery")
	code, rerr := dockercli.PipeIn(ctx, backupStepTimeout, gz, env, argv...)
	if rerr != nil {
		e.result(false, "restore: "+rerr.Error())
		return
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("clickhouse-client exited %d", code))
		return
	}
	e.log("info", "Restore complete")
	e.result(true, "")
}

// chEscape escapes a single-quoted clickhouse string literal (the db/table names
// are control-plane-derived identifiers, but they are interpolated into a query,
// so escape defensively).
func chEscape(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `'`, `\'`)
}

// chQuoteIdent escapes a name for a backtick-quoted clickhouse identifier
// (`name`), doubling any backtick it contains (clickhouse's escape for a literal
// backtick inside a quoted identifier). The db/table names are control-plane-
// derived, but they are interpolated into backtick-quoted positions in the DDL
// and queries, so escape defensively — a name carrying a backtick must not be
// able to break out of the identifier and inject SQL.
func chQuoteIdent(s string) string {
	return strings.ReplaceAll(s, "`", "``")
}

// nonEmptyLines splits stdout into trimmed, non-empty lines.
func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if t := strings.TrimSpace(l); t != "" {
			out = append(out, t)
		}
	}
	return out
}
