package server

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc/metadata"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// ---- dumpArgv / restoreArgv: the per-engine command + overwrite contract ----

// The dump argv must name the right tool per engine, dump to stdout (no -f /
// file), and carry the OVERWRITE-guaranteeing flags the restore relies on. The
// password must NEVER appear anywhere on argv (which is world-readable on the
// host via ps/proc): for env-capable engines it rides in the returned env (the
// `-e NAME` flag only forwards it), and even the inline `NAME=value` form must
// not appear on argv.
func TestDumpArgv_perEngine(t *testing.T) {
	cases := []struct {
		dbType     string
		wantTool   string
		wantTokens []string // every token must be present, in order-independent check
		pwEnvKey   string   // the env KEY the password must ride in (empty => on argv via a flag, e.g. mongo -p)
	}{
		{"postgres", "pg_dump", []string{"-Fc", "mydb", "-U", "admin"}, "PGPASSWORD"},
		{"mysql", "mysqldump", []string{"--add-drop-table", "--databases", "mydb"}, "MYSQL_PWD"},
		{"mariadb", "mysqldump", []string{"--add-drop-table", "--databases", "mydb"}, "MYSQL_PWD"},
		{"mongodb", "mongodump", []string{"--archive", "--db=mydb"}, ""},
		{"redis", "redis-cli", []string{"--rdb", "-"}, "REDISCLI_AUTH"},
	}
	for _, tc := range cases {
		t.Run(tc.dbType, func(t *testing.T) {
			d := &pb.DatabaseDescriptor{
				Container: "deplo-db-x", DbType: tc.dbType, DbName: "mydb",
				User: "admin", Password: "s3cret",
			}
			argv, env, err := dumpArgv(d)
			if err != nil {
				t.Fatalf("dumpArgv(%s): %v", tc.dbType, err)
			}
			joined := strings.Join(argv, " ")
			if argv[0] != "exec" {
				t.Errorf("dump argv must start with exec, got %v", argv)
			}
			if !containsToken(argv, tc.wantTool) {
				t.Errorf("dump argv for %s missing tool %q: %v", tc.dbType, tc.wantTool, argv)
			}
			if !containsToken(argv, "deplo-db-x") {
				t.Errorf("dump argv must exec the container, got %v", argv)
			}
			for _, tok := range tc.wantTokens {
				if !containsToken(argv, tok) {
					t.Errorf("dump argv for %s missing %q: %v", tc.dbType, tok, argv)
				}
			}
			if tc.pwEnvKey != "" {
				// Env-capable engine (postgres/mysql/redis): the cleartext password
				// must NEVER appear on argv (the ps/proc-readable host command line);
				// the value rides in env and only `-e NAME` (name-only) goes on argv.
				if strings.Contains(joined, "s3cret") {
					t.Errorf("%s leaked the password onto argv: %v", tc.dbType, argv)
				}
				if !containsToken(argv, tc.pwEnvKey) {
					t.Errorf("%s must forward the env var via `-e %s`, got %v", tc.dbType, tc.pwEnvKey, argv)
				}
				wantEnv := tc.pwEnvKey + "=s3cret"
				if !containsToken(env, wantEnv) {
					t.Errorf("%s password must ride in env %q, got env=%v", tc.dbType, wantEnv, env)
				}
			} else if tc.dbType == "mongodb" {
				// DOCUMENTED RESIDUAL: mongodump/mongorestore have no password env
				// var, so the password stays on argv as `-p <pw>`. This is a known,
				// bounded exposure (host-local; masked out of any error string by
				// dockercli.redactArgs). If mongo ever gains an env/stdin password
				// path, move it off argv and into the env-capable branch above.
				if !containsToken(argv, "-p") || !containsToken(argv, "s3cret") {
					t.Errorf("mongodb is expected to pass -p <pw> on argv (documented residual), got %v", argv)
				}
				if len(env) != 0 {
					t.Errorf("mongodb has no password env var; env should be empty, got %v", env)
				}
			}
		})
	}
}

func TestDumpArgv_unsupportedEngine(t *testing.T) {
	_, _, err := dumpArgv(&pb.DatabaseDescriptor{Container: "c", DbType: "cassandra", DbName: "d"})
	if err == nil {
		t.Fatal("expected an error for an unsupported engine")
	}
	// clickhouse uses the dedicated multi-statement path (dumpClickhouse), so the
	// single-pipe argv builder signals that with errClickhouseSeparate rather than
	// returning a usable argv.
	if _, _, err := dumpArgv(&pb.DatabaseDescriptor{Container: "c", DbType: "clickhouse", DbName: "d"}); err != errClickhouseSeparate {
		t.Errorf("clickhouse dumpArgv should signal the dedicated path, got %v", err)
	}
	if _, _, err := restoreArgv(&pb.DatabaseDescriptor{Container: "c", DbType: "clickhouse", DbName: "d"}); err != errClickhouseSeparate {
		t.Errorf("clickhouse restoreArgv should signal the dedicated path, got %v", err)
	}
}

// The restore argv must use -i (stdin), the right tool, and the drop-and-recreate
// flags that make a restore OVERWRITE rather than append (the locked decision).
func TestRestoreArgv_overwriteFlags(t *testing.T) {
	cases := []struct {
		dbType   string
		wantTool string
		wantDrop string // a token that proves drop-and-recreate
	}{
		{"postgres", "pg_restore", "--clean"},
		{"mysql", "mysql", ""}, // overwrite comes from the dump's --add-drop-table
		{"mongodb", "mongorestore", "--drop"},
	}
	for _, tc := range cases {
		t.Run(tc.dbType, func(t *testing.T) {
			argv, _, err := restoreArgv(&pb.DatabaseDescriptor{
				Container: "deplo-db-x", DbType: tc.dbType, DbName: "mydb", User: "admin",
			})
			if err != nil {
				t.Fatalf("restoreArgv(%s): %v", tc.dbType, err)
			}
			if !containsToken(argv, "exec") || !containsToken(argv, "-i") {
				t.Errorf("restore argv must `exec -i` (stdin), got %v", argv)
			}
			if !containsToken(argv, tc.wantTool) {
				t.Errorf("restore argv for %s missing tool %q: %v", tc.dbType, tc.wantTool, argv)
			}
			if tc.wantDrop != "" && !containsToken(argv, tc.wantDrop) {
				t.Errorf("restore argv for %s missing overwrite flag %q: %v", tc.dbType, tc.wantDrop, argv)
			}
		})
	}
}

// postgres restore --if-exists must accompany --clean (so dropping a not-yet-
// present object on a fresh DB is not a fatal error).
func TestRestoreArgv_postgresIfExists(t *testing.T) {
	argv, _, _ := restoreArgv(&pb.DatabaseDescriptor{Container: "c", DbType: "postgres", DbName: "d", User: "u"})
	if !containsToken(argv, "--clean") || !containsToken(argv, "--if-exists") {
		t.Errorf("postgres restore must pass --clean --if-exists, got %v", argv)
	}
}

// Redis restore must NOT go through the uniform stdin-pipe argv path: an RDB
// dump can't be fed to a restore tool's stdin (redis-cli --pipe speaks RESP, not
// RDB). restoreArgv signals this with errRedisRestoreSeparate so restoreDatabase
// dispatches to the dedicated file-swap path. Guards against a regression that
// re-introduces the broken `redis-cli --pipe` restore.
func TestRestoreArgv_redisUsesSeparatePath(t *testing.T) {
	_, _, err := restoreArgv(&pb.DatabaseDescriptor{Container: "c", DbType: "redis", DbName: "d"})
	if err != errRedisRestoreSeparate {
		t.Fatalf("redis restore must signal the dedicated path, got %v", err)
	}
}

// The redis DUMP, by contrast, IS a single stdout pipe (redis-cli --rdb -),
// verified to emit a valid RDB. Keep that contract.
func TestDumpArgv_redisToStdout(t *testing.T) {
	argv, _, err := dumpArgv(&pb.DatabaseDescriptor{Container: "c", DbType: "redis", DbName: "d"})
	if err != nil {
		t.Fatalf("redis dump: %v", err)
	}
	if !containsToken(argv, "--rdb") || argv[len(argv)-1] != "-" {
		t.Errorf("redis dump must write the RDB to stdout (--rdb -), got %v", argv)
	}
}

// shellQuote must single-quote a path and escape embedded single quotes so the
// `sh -c 'cat > <path>'` in restoreRedis can't be broken out of.
func TestShellQuote(t *testing.T) {
	if got := shellQuote("/data/dump.rdb"); got != "'/data/dump.rdb'" {
		t.Errorf("shellQuote: %q", got)
	}
	// A quote in the path is escaped, not left to terminate the quoting.
	if got := shellQuote("/da'ta"); got != `'/da'\''ta'` {
		t.Errorf("shellQuote escape: %q", got)
	}
}

// The restore argv must also keep the password off argv (env-capable engines).
func TestRestoreArgv_passwordOffArgv(t *testing.T) {
	for _, eng := range []string{"postgres", "mysql", "mariadb"} {
		argv, env, err := restoreArgv(&pb.DatabaseDescriptor{
			Container: "c", DbType: eng, DbName: "d", User: "u", Password: "s3cret",
		})
		if err != nil {
			t.Fatalf("restoreArgv(%s): %v", eng, err)
		}
		if strings.Contains(strings.Join(argv, " "), "s3cret") {
			t.Errorf("%s restore leaked the password onto argv: %v", eng, argv)
		}
		if len(env) == 0 || !strings.Contains(strings.Join(env, " "), "s3cret") {
			t.Errorf("%s restore must carry the password in env, got %v", eng, env)
		}
	}
}

// chClientPrefix forwards CLICKHOUSE_PASSWORD via env, not argv, and includes
// --user when set.
func TestChClientPrefix_passwordOffArgv(t *testing.T) {
	d := &pb.DatabaseDescriptor{Container: "c", DbType: "clickhouse", User: "app", Password: "s3cret"}
	argv, env := chClientPrefix(d, false)
	if strings.Contains(strings.Join(argv, " "), "s3cret") {
		t.Errorf("clickhouse prefix leaked the password onto argv: %v", argv)
	}
	if !containsToken(argv, "CLICKHOUSE_PASSWORD") || !containsToken(env, "CLICKHOUSE_PASSWORD=s3cret") {
		t.Errorf("clickhouse prefix must forward CLICKHOUSE_PASSWORD via env, argv=%v env=%v", argv, env)
	}
	if !containsToken(argv, "--user") || !containsToken(argv, "app") {
		t.Errorf("clickhouse prefix must pass --user, got %v", argv)
	}
	// stdin=true adds -i (used by the restore --multiquery pipe).
	argvIn, _ := chClientPrefix(d, true)
	if !containsToken(argvIn, "-i") {
		t.Errorf("stdin prefix must include -i, got %v", argvIn)
	}
}

func TestChEscapeAndLines(t *testing.T) {
	if got := chEscape(`a'b\c`); got != `a\'b\\c` {
		t.Errorf("chEscape: %q", got)
	}
	got := nonEmptyLines("a\n\n  b  \n\nc\n")
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("nonEmptyLines: %v", got)
	}
}

// redisCliPrefix forwards REDISCLI_AUTH via env, not argv.
func TestRedisCliPrefix_passwordOffArgv(t *testing.T) {
	argv, env := redisCliPrefix("c", "s3cret")
	if strings.Contains(strings.Join(argv, " "), "s3cret") {
		t.Errorf("redis prefix leaked the password onto argv: %v", argv)
	}
	if !containsToken(argv, "REDISCLI_AUTH") || !containsToken(env, "REDISCLI_AUTH=s3cret") {
		t.Errorf("redis prefix must forward REDISCLI_AUTH via env, argv=%v env=%v", argv, env)
	}
	// No password => no env forwarded, no -e flag.
	argv2, env2 := redisCliPrefix("c", "")
	if len(env2) != 0 || containsToken(argv2, "-e") {
		t.Errorf("no-password redis prefix must not forward env, argv=%v env=%v", argv2, env2)
	}
}

// validateVolumeName rejects path-shaped names so a wire-supplied "/" / "/etc"
// can't become a host bind mount in `-v <name>:/v`.
func TestValidateVolumeName(t *testing.T) {
	ok := []string{"deplo-myapp-data", "vol_1", "a", "deplo-x.y-z"}
	for _, v := range ok {
		if err := validateVolumeName(v); err != nil {
			t.Errorf("expected %q to be accepted, got %v", v, err)
		}
	}
	bad := []string{"/", "/etc", "../escape", ".hidden", "a/b", "", "vol;rm", "$(x)", "a b", "..", "name/../x"}
	for _, v := range bad {
		if err := validateVolumeName(v); err == nil {
			t.Errorf("expected %q to be REJECTED (path/unsafe), but it was accepted", v)
		}
	}
}

// ---- env-file round-trip: renderEnvFile (deploy.go) ⇄ parseEnvFile ----

func TestEnvFileRoundTrip(t *testing.T) {
	in := map[string]string{"FOO": "bar", "EMPTY": "", "WITH_EQ": "a=b=c"}
	got := parseEnvFile(renderEnvFile(in))
	for k, v := range in {
		if got[k] != v {
			t.Errorf("round-trip %q: want %q got %q", k, v, got[k])
		}
	}
	if len(got) != len(in) {
		t.Errorf("round-trip changed key count: want %d got %d (%v)", len(in), len(got), got)
	}
}

func TestParseEnvFile_skipsJunk(t *testing.T) {
	got := parseEnvFile("FOO=1\n\nno_equals_here\nBAR=2\r\n")
	if got["FOO"] != "1" || got["BAR"] != "2" {
		t.Errorf("expected FOO=1 BAR=2, got %v", got)
	}
	if len(got) != 2 {
		t.Errorf("a line with no '=' must be ignored, got %v", got)
	}
}

// ---- tar helpers: round-trip a dir, and reject a traversal on extract ----

func TestTarDirRoundTrip(t *testing.T) {
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "b.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := addDirToTar(tw, src, "files"); err != nil {
		t.Fatalf("addDirToTar: %v", err)
	}
	tw.Close()

	dst := t.TempDir()
	tr := tar.NewReader(&buf)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rel := strings.TrimPrefix(filepath.ToSlash(hdr.Name), "files/")
		if err := extractToDir(dst, rel, hdr, tr); err != nil {
			t.Fatalf("extractToDir %q: %v", hdr.Name, err)
		}
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "a.txt")); string(b) != "alpha" {
		t.Errorf("a.txt round-trip mismatch: %q", b)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "sub", "b.txt")); string(b) != "beta" {
		t.Errorf("sub/b.txt round-trip mismatch: %q", b)
	}
}

// extractToDir must reject an entry whose path escapes the target root (the entry
// name came from an S3 object — never trusted).
func TestExtractToDir_rejectsTraversal(t *testing.T) {
	dst := t.TempDir()
	hdr := &tar.Header{Name: "x", Typeflag: tar.TypeReg, Size: 3}
	if err := extractToDir(dst, "../escape.txt", hdr, strings.NewReader("bad")); err == nil {
		t.Fatal("expected a traversal rejection for ../escape.txt")
	}
	// The file must NOT have been written outside the root.
	if _, err := os.Stat(filepath.Join(filepath.Dir(dst), "escape.txt")); err == nil {
		t.Fatal("traversal wrote a file outside the target root")
	}
}

// addBytesToTar then read it back — the snapshot path relies on this.
func TestAddBytesToTar(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := addBytesToTar(tw, "snapshot/compose.yml", []byte("services: {}\n")); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	tr := tar.NewReader(&buf)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Name != "snapshot/compose.yml" {
		t.Errorf("name: %q", hdr.Name)
	}
	b, _ := io.ReadAll(tr)
	if string(b) != "services: {}\n" {
		t.Errorf("content: %q", b)
	}
}

// ---- RPC-level validation: a missing key / unknown kind emits a clean failure
// result on the stream rather than a panic or a fake success ----

func TestBackup_missingKeyResultsInFailure(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	st := &fakeBackupStream{}
	if err := s.Backup(&pb.BackupRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE}, st); err != nil {
		t.Fatalf("Backup rpc error: %v", err)
	}
	res := st.lastResult(t)
	if res.GetOk() {
		t.Error("a backup with no S3 object key must fail, not succeed")
	}
	if !strings.Contains(res.GetError(), "object key") {
		t.Errorf("error should mention the missing key, got %q", res.GetError())
	}
}

func TestBackup_unknownKind(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	st := &fakeBackupStream{}
	req := &pb.BackupRequest{
		Kind: pb.BackupKind_BACKUP_KIND_UNSPECIFIED,
		S3:   &pb.S3Target{Bucket: "b", ObjectKey: "deplo/t/database/x/ts.dump.gz"},
	}
	if err := s.Backup(req, st); err != nil {
		t.Fatalf("Backup rpc error: %v", err)
	}
	if st.lastResult(t).GetOk() {
		t.Error("an unknown backup kind must fail")
	}
}

func TestRestore_missingKey(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	st := &fakeRestoreStream{}
	if err := s.Restore(&pb.RestoreRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE}, st); err != nil {
		t.Fatalf("Restore rpc error: %v", err)
	}
	if st.lastResult(t).GetOk() {
		t.Error("a restore with no S3 object key must fail")
	}
}

// S3Check / S3Delete reject a missing target with InvalidArgument (a programming
// error, surfaced as a gRPC error not a fake ok).
func TestS3Check_missingTarget(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if _, err := s.S3Check(context.Background(), &pb.S3CheckRequest{}); err == nil {
		t.Error("S3Check with no target must error")
	}
}

func TestS3Delete_missingKey(t *testing.T) {
	s := New(t.TempDir(), t.TempDir(), "/", "")
	if _, err := s.S3Delete(context.Background(), &pb.S3DeleteRequest{S3: &pb.S3Target{Bucket: "b"}}); err == nil {
		t.Error("S3Delete with no key must error")
	}
}

// "backup" must be advertised in Hello so the control plane's capability
// preflight passes against this agent.
func TestCapabilities_advertisesBackup(t *testing.T) {
	found := false
	for _, c := range Capabilities {
		if c == "backup" {
			found = true
		}
	}
	if !found {
		t.Error("Capabilities must advertise \"backup\"")
	}
}

// ---- test helpers ----

func containsToken(argv []string, tok string) bool {
	for _, a := range argv {
		if a == tok {
			return true
		}
	}
	return false
}

// fakeBackupStream / fakeRestoreStream satisfy grpc.ServerStreamingServer[T] for
// the validation tests: they capture every Send and hand back the terminal
// result. Only Send + Context are exercised; the rest satisfy the embedded
// ServerStream interface.
type fakeBackupStream struct {
	events []*pb.BackupEvent
}

func (f *fakeBackupStream) Send(ev *pb.BackupEvent) error {
	f.events = append(f.events, ev)
	return nil
}
func (f *fakeBackupStream) Context() context.Context     { return context.Background() }
func (f *fakeBackupStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeBackupStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeBackupStream) SetTrailer(metadata.MD)       {}
func (f *fakeBackupStream) SendMsg(any) error            { return nil }
func (f *fakeBackupStream) RecvMsg(any) error            { return nil }

func (f *fakeBackupStream) lastResult(t *testing.T) *pb.BackupResult {
	t.Helper()
	for i := len(f.events) - 1; i >= 0; i-- {
		if r := f.events[i].GetResult(); r != nil {
			return r
		}
	}
	t.Fatal("no terminal BackupResult emitted on the stream")
	return nil
}

type fakeRestoreStream struct {
	events []*pb.RestoreEvent
}

func (f *fakeRestoreStream) Send(ev *pb.RestoreEvent) error {
	f.events = append(f.events, ev)
	return nil
}
func (f *fakeRestoreStream) Context() context.Context     { return context.Background() }
func (f *fakeRestoreStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeRestoreStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeRestoreStream) SetTrailer(metadata.MD)       {}
func (f *fakeRestoreStream) SendMsg(any) error            { return nil }
func (f *fakeRestoreStream) RecvMsg(any) error            { return nil }

func (f *fakeRestoreStream) lastResult(t *testing.T) *pb.RestoreResult {
	t.Helper()
	for i := len(f.events) - 1; i >= 0; i-- {
		if r := f.events[i].GetResult(); r != nil {
			return r
		}
	}
	t.Fatal("no terminal RestoreResult emitted on the stream")
	return nil
}
