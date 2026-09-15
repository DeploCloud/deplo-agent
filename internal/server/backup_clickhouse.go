package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
)

var errClickhouseSeparate = fmt.Errorf("clickhouse uses the dedicated multi-statement path")

const clickhouseQueryTimeout = 5 * time.Minute

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
	return a, env
}

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

func (s *Service) dumpClickhouse(ctx context.Context, d *pb.DatabaseDescriptor, w io.Writer) error {
	db := d.GetDbName()
	if db == "" {
		return fmt.Errorf("clickhouse backup requires a database name")
	}
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
		fmt.Fprintf(bw, "DROP TABLE IF EXISTS `%s`.`%s`;\n", chQuoteIdent(db), chQuoteIdent(tbl))

		ddl, err := s.chQuery(ctx, d, fmt.Sprintf(
			"SELECT create_table_query FROM system.tables WHERE database='%s' AND name='%s'",
			chEscape(db), chEscape(tbl)))
		if err != nil {
			return fmt.Errorf("read schema for %q: %w", tbl, err)
		}
		ddl = strings.TrimSpace(ddl)
		ddl = strings.Replace(ddl, "CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
		fmt.Fprintf(bw, "%s;\n", ddl)

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

func (s *Service) restoreClickhouse(ctx context.Context, d *pb.DatabaseDescriptor, src *artifactSource, e *rsEmitter) {
	e.log("info", fmt.Sprintf("Restoring clickhouse database %q into container %q from %s", d.GetDbName(), d.GetContainer(), src.label))

	rd, closeSrc, oerr := src.open(ctx)
	if oerr != nil {
		e.result(false, statusMessage(oerr))
		return
	}
	defer closeSrc()

	argv, env := chClientPrefix(d, true)
	argv = append(argv, "--multiquery")
	code, rerr := dockercli.PipeIn(ctx, backupStepTimeout, rd, env, argv...)
	if rerr != nil {
		e.result(false, "restore: "+rerr.Error())
		return
	}
	if code != 0 {
		e.result(false, fmt.Sprintf("clickhouse-client exited %d", code))
		return
	}
	if verr := src.verify(); verr != nil {
		e.result(false, statusMessage(verr))
		return
	}
	e.log("info", "Restore complete")
	e.result(true, "")
}

func chEscape(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `'`, `\'`)
}

func chQuoteIdent(s string) string {
	return strings.ReplaceAll(s, "`", "``")
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if t := strings.TrimSpace(l); t != "" {
			out = append(out, t)
		}
	}
	return out
}
