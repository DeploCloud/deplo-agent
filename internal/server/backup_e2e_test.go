package server

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/dockercli"
	"github.com/DeploCloud/deplo-agent/internal/s3client"
)

const (
	e2eMinioImage      = "minio/minio:latest"
	e2ePostgresImage   = "postgres:16-alpine"
	e2eRedisImage      = "redis:alpine"
	e2eMongoImage      = "mongo:7"
	e2eClickhouseImage = "clickhouse/clickhouse-server:24-alpine"
	e2eBucket          = "deplo-test"
	e2eAccessKey       = "minioadmin"
	e2eSecretKey       = "minioadmin"
)

func startMinio(t *testing.T, ctx context.Context) (s3client.Config, func()) {
	t.Helper()
	name := "deplo-e2e-minio"
	_, _ = dockercli.Run(ctx, 10*time.Second, "rm", "-f", name)
	res, err := dockercli.Run(ctx, 60*time.Second,
		"run", "-d", "--name", name,
		"-p", "19000:9000",
		"-e", "MINIO_ROOT_USER="+e2eAccessKey,
		"-e", "MINIO_ROOT_PASSWORD="+e2eSecretKey,
		e2eMinioImage, "server", "/data")
	if err != nil || res.Code != 0 {
		t.Skipf("cannot start MinIO (%v / %s)", err, res.Stderr)
	}
	cleanup := func() { _, _ = dockercli.Run(context.Background(), 15*time.Second, "rm", "-f", name) }

	cfg := s3client.Config{
		Endpoint:  "http://127.0.0.1:19000",
		Region:    "us-east-1",
		Bucket:    e2eBucket,
		AccessKey: e2eAccessKey,
		SecretKey: e2eSecretKey,
		PathStyle: true,
	}
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		cl, cerr := s3client.New(cfg)
		if cerr == nil {
			if err := cl.MakeBucket(ctx, e2eBucket, minio.MakeBucketOptions{Region: cfg.Region}); err == nil {
				return cfg, cleanup
			} else if strings.Contains(err.Error(), "already own") || strings.Contains(err.Error(), "exists") {
				return cfg, cleanup
			}
		}
		time.Sleep(2 * time.Second)
	}
	cleanup()
	t.Skip("MinIO did not become ready in time")
	return cfg, cleanup
}

func TestE2E_PostgresBackupRestoreOverwrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	cfg, cleanupMinio := startMinio(t, ctx)
	defer cleanupMinio()

	pgName := "deplo-e2e-pg"
	_, _ = dockercli.Run(ctx, 10*time.Second, "rm", "-f", pgName)
	res, err := dockercli.Run(ctx, 60*time.Second,
		"run", "-d", "--name", pgName,
		"-e", "POSTGRES_PASSWORD=secret", "-e", "POSTGRES_USER=admin", "-e", "POSTGRES_DB=appdb",
		e2ePostgresImage)
	if err != nil || res.Code != 0 {
		t.Skipf("cannot start postgres (%v / %s)", err, res.Stderr)
	}
	defer dockercli.Run(context.Background(), 15*time.Second, "rm", "-f", pgName)

	psql := func(sql string) (dockercli.Result, error) {
		return dockercli.Run(ctx, 20*time.Second, "exec", "-e", "PGPASSWORD=secret", pgName,
			"psql", "-U", "admin", "-d", "appdb", "-tAc", sql)
	}
	ready := false
	for i := 0; i < 30; i++ {
		if r, e := psql("SELECT 1"); e == nil && r.Code == 0 && strings.Contains(r.Stdout, "1") {
			ready = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !ready {
		t.Skip("postgres did not become ready")
	}

	if _, e := psql("CREATE TABLE t(v text); INSERT INTO t VALUES ('sentinel-A');"); e != nil {
		t.Fatalf("seed: %v", e)
	}

	d := &pb.DatabaseDescriptor{Container: pgName, DbType: "postgres", DbName: "appdb", User: "admin", Password: "secret"}
	key := "deplo/team/database/pg/" + "e2e.dump.gz"
	target := &pb.S3Target{
		Endpoint: cfg.Endpoint, Region: cfg.Region, Bucket: cfg.Bucket,
		AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey, ObjectKey: key, PathStyle: true,
	}

	svc := New(t.TempDir(), t.TempDir(), "/", "")

	bs := &fakeBackupStream{}
	if err := svc.Backup(&pb.BackupRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, S3: target, Database: d}, bs); err != nil {
		t.Fatalf("Backup rpc: %v", err)
	}
	br := bs.lastResult(t)
	if !br.GetOk() {
		t.Fatalf("backup failed: %s", br.GetError())
	}
	if br.GetSizeBytes() <= 0 {
		t.Errorf("backup reported zero size")
	}
	t.Logf("backed up %d bytes to %s", br.GetSizeBytes(), br.GetObjectKey())

	if _, e := psql("UPDATE t SET v='sentinel-B';"); e != nil {
		t.Fatalf("mutate: %v", e)
	}
	if r, _ := psql("SELECT v FROM t;"); !strings.Contains(r.Stdout, "sentinel-B") {
		t.Fatalf("pre-restore sanity: expected sentinel-B, got %q", r.Stdout)
	}

	rs := &fakeRestoreStream{}
	if err := svc.Restore(&pb.RestoreRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, S3: target, Database: d}, rs); err != nil {
		t.Fatalf("Restore rpc: %v", err)
	}
	rr := rs.lastResult(t)
	if !rr.GetOk() {
		t.Fatalf("restore failed: %s", rr.GetError())
	}
	r, _ := psql("SELECT v FROM t;")
	if !strings.Contains(r.Stdout, "sentinel-A") || strings.Contains(r.Stdout, "sentinel-B") {
		t.Fatalf("restore did not overwrite: expected sentinel-A only, got %q", r.Stdout)
	}
	t.Log("postgres backup→mutate→restore OVERWRITE verified")

	del1, _ := svc.S3Delete(ctx, &pb.S3DeleteRequest{S3: target})
	if !del1.GetOk() || del1.GetDeleted() != 1 {
		t.Errorf("S3Delete should remove 1, got ok=%v n=%d err=%s", del1.GetOk(), del1.GetDeleted(), del1.GetError())
	}
	del2, _ := svc.S3Delete(ctx, &pb.S3DeleteRequest{S3: target})
	if !del2.GetOk() || del2.GetDeleted() != 0 {
		t.Errorf("second S3Delete should be idempotent (0), got ok=%v n=%d", del2.GetOk(), del2.GetDeleted())
	}

	chk, _ := svc.S3Check(ctx, &pb.S3CheckRequest{S3: target})
	if !chk.GetOk() {
		t.Errorf("S3Check on a real bucket should pass, got %s", chk.GetError())
	}
	bad := &pb.S3Target{
		Endpoint: target.Endpoint, Region: target.Region, Bucket: "no-such-bucket-xyz",
		AccessKey: target.AccessKey, SecretKey: target.SecretKey, PathStyle: true,
	}
	chkBad, _ := svc.S3Check(ctx, &pb.S3CheckRequest{S3: bad})
	if chkBad.GetOk() {
		t.Errorf("S3Check on a missing bucket should fail")
	}
}

func TestE2E_RedisBackupRestoreOverwrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	cfg, cleanupMinio := startMinio(t, ctx)
	defer cleanupMinio()

	name := "deplo-e2e-redis"
	_, _ = dockercli.Run(ctx, 10*time.Second, "rm", "-f", name)
	res, err := dockercli.Run(ctx, 60*time.Second,
		"run", "-d", "--name", name, "--restart", "unless-stopped", e2eRedisImage)
	if err != nil || res.Code != 0 {
		t.Skipf("cannot start redis (%v / %s)", err, res.Stderr)
	}
	defer dockercli.Run(context.Background(), 15*time.Second, "rm", "-f", name)

	rcli := func(args ...string) (dockercli.Result, error) {
		return dockercli.Run(ctx, 15*time.Second, append([]string{"exec", name, "redis-cli"}, args...)...)
	}
	if !waitRedisReady(ctx, name, "", 40*time.Second) {
		t.Skip("redis did not become ready")
	}
	if _, e := rcli("SET", "sentinel", "A"); e != nil {
		t.Fatalf("seed: %v", e)
	}

	d := &pb.DatabaseDescriptor{Container: name, DbType: "redis", DbName: "0"}
	target := &pb.S3Target{
		Endpoint: cfg.Endpoint, Region: cfg.Region, Bucket: cfg.Bucket,
		AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey,
		ObjectKey: "deplo/team/database/redis/e2e.rdb.gz", PathStyle: true,
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	bs := &fakeBackupStream{}
	if err := svc.Backup(&pb.BackupRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, S3: target, Database: d}, bs); err != nil {
		t.Fatalf("Backup rpc: %v", err)
	}
	if br := bs.lastResult(t); !br.GetOk() {
		t.Fatalf("redis backup failed: %s", br.GetError())
	}

	if _, e := rcli("SET", "sentinel", "B"); e != nil {
		t.Fatalf("mutate: %v", e)
	}
	if _, e := rcli("SET", "added-after-backup", "1"); e != nil {
		t.Fatalf("mutate2: %v", e)
	}

	rs := &fakeRestoreStream{}
	if err := svc.Restore(&pb.RestoreRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, S3: target, Database: d}, rs); err != nil {
		t.Fatalf("Restore rpc: %v", err)
	}
	if rr := rs.lastResult(t); !rr.GetOk() {
		t.Fatalf("redis restore failed: %s", rr.GetError())
	}

	if r, _ := rcli("GET", "sentinel"); !strings.Contains(r.Stdout, "A") || strings.Contains(r.Stdout, "B") {
		t.Fatalf("redis restore did not overwrite sentinel: %q", r.Stdout)
	}
	if r, _ := rcli("EXISTS", "added-after-backup"); !strings.Contains(strings.TrimSpace(r.Stdout), "0") {
		t.Fatalf("redis restore should have dropped the post-backup key, EXISTS=%q", r.Stdout)
	}
	t.Log("redis backup→mutate→restore OVERWRITE verified")
}

func TestE2E_MongoBackupRestoreOverwrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	cfg, cleanupMinio := startMinio(t, ctx)
	defer cleanupMinio()

	name := "deplo-e2e-mongo"
	_, _ = dockercli.Run(ctx, 10*time.Second, "rm", "-f", name)
	res, err := dockercli.Run(ctx, 90*time.Second,
		"run", "-d", "--name", name,
		"-e", "MONGO_INITDB_ROOT_USERNAME=app", "-e", "MONGO_INITDB_ROOT_PASSWORD=secret",
		e2eMongoImage)
	if err != nil || res.Code != 0 {
		t.Skipf("cannot start mongo (%v / %s)", err, res.Stderr)
	}
	defer dockercli.Run(context.Background(), 15*time.Second, "rm", "-f", name)

	mongo := func(js string) (dockercli.Result, error) {
		return dockercli.Run(ctx, 25*time.Second, "exec", name,
			"mongosh", "-u", "app", "-p", "secret", "--authenticationDatabase", "admin",
			"--quiet", "appdb", "--eval", js)
	}
	ready := false
	for i := 0; i < 40; i++ {
		if r, e := mongo("db.runCommand({ping:1}).ok"); e == nil && r.Code == 0 && strings.Contains(r.Stdout, "1") {
			ready = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !ready {
		t.Skip("mongo did not become ready")
	}

	if r, e := mongo(`db.t.insertOne({k:"sentinel", v:"A"})`); e != nil || r.Code != 0 {
		t.Fatalf("seed: %v / %s", e, r.Stderr)
	}

	d := &pb.DatabaseDescriptor{
		Container: name, DbType: "mongodb", DbName: "appdb", User: "app", Password: "secret",
	}
	target := &pb.S3Target{
		Endpoint: cfg.Endpoint, Region: cfg.Region, Bucket: cfg.Bucket,
		AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey,
		ObjectKey: "deplo/team/database/mongo/e2e.archive.gz", PathStyle: true,
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	bs := &fakeBackupStream{}
	if err := svc.Backup(&pb.BackupRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, S3: target, Database: d}, bs); err != nil {
		t.Fatalf("Backup rpc: %v", err)
	}
	if br := bs.lastResult(t); !br.GetOk() {
		t.Fatalf("mongo backup failed: %s", br.GetError())
	} else {
		t.Logf("backed up %d bytes", br.GetSizeBytes())
	}

	if r, e := mongo(`db.t.updateOne({k:"sentinel"},{$set:{v:"B"}}); db.t.insertOne({k:"added",v:"x"})`); e != nil || r.Code != 0 {
		t.Fatalf("mutate: %v / %s", e, r.Stderr)
	}

	rs := &fakeRestoreStream{}
	if err := svc.Restore(&pb.RestoreRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, S3: target, Database: d}, rs); err != nil {
		t.Fatalf("Restore rpc: %v", err)
	}
	if rr := rs.lastResult(t); !rr.GetOk() {
		t.Fatalf("mongo restore failed: %s", rr.GetError())
	}

	r, _ := mongo(`print(db.t.findOne({k:"sentinel"}).v + "/" + db.t.countDocuments({k:"added"}))`)
	if !strings.Contains(r.Stdout, "A/0") {
		t.Fatalf("mongo restore did not overwrite (want sentinel=A and 0 added docs), got %q / stderr %q", r.Stdout, r.Stderr)
	}
	t.Log("mongodb backup→mutate→restore OVERWRITE verified")
}

func TestE2E_ClickhouseBackupRestoreOverwrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	cfg, cleanupMinio := startMinio(t, ctx)
	defer cleanupMinio()

	name := "deplo-e2e-ch"
	_, _ = dockercli.Run(ctx, 10*time.Second, "rm", "-f", name)
	res, err := dockercli.Run(ctx, 90*time.Second,
		"run", "-d", "--name", name,
		"-e", "CLICKHOUSE_USER=app", "-e", "CLICKHOUSE_PASSWORD=secret", "-e", "CLICKHOUSE_DB=appdb",
		e2eClickhouseImage)
	if err != nil || res.Code != 0 {
		t.Skipf("cannot start clickhouse (%v / %s)", err, res.Stderr)
	}
	defer dockercli.Run(context.Background(), 15*time.Second, "rm", "-f", name)

	ch := func(q string) (dockercli.Result, error) {
		return dockercli.Run(ctx, 25*time.Second, "exec", name,
			"clickhouse-client", "--user", "app", "--password", "secret", "-q", q)
	}
	ready := false
	for i := 0; i < 45; i++ {
		if r, e := ch("SELECT 1 FROM system.databases WHERE name='appdb'"); e == nil && r.Code == 0 && strings.Contains(r.Stdout, "1") {
			ready = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !ready {
		t.Skip("clickhouse did not become ready")
	}

	if r, e := ch("CREATE TABLE appdb.t (id UInt32, name String) ENGINE=MergeTree ORDER BY id"); e != nil || r.Code != 0 {
		t.Fatalf("create table: %v / %s", e, r.Stderr)
	}
	if r, e := ch("INSERT INTO appdb.t VALUES (1,'sentinel-A')(2,'two')"); e != nil || r.Code != 0 {
		t.Fatalf("seed: %v / %s", e, r.Stderr)
	}

	d := &pb.DatabaseDescriptor{
		Container: name, DbType: "clickhouse", DbName: "appdb", User: "app", Password: "secret",
	}
	target := &pb.S3Target{
		Endpoint: cfg.Endpoint, Region: cfg.Region, Bucket: cfg.Bucket,
		AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey,
		ObjectKey: "deplo/team/database/ch/e2e.sql.gz", PathStyle: true,
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	bs := &fakeBackupStream{}
	if err := svc.Backup(&pb.BackupRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, S3: target, Database: d}, bs); err != nil {
		t.Fatalf("Backup rpc: %v", err)
	}
	if br := bs.lastResult(t); !br.GetOk() {
		t.Fatalf("clickhouse backup failed: %s", br.GetError())
	} else {
		t.Logf("backed up %d bytes", br.GetSizeBytes())
	}

	if r, e := ch("INSERT INTO appdb.t VALUES (3,'added-after-backup')"); e != nil || r.Code != 0 {
		t.Fatalf("mutate-insert: %v / %s", e, r.Stderr)
	}
	if r, e := ch("ALTER TABLE appdb.t UPDATE name='sentinel-B' WHERE id=1 SETTINGS mutations_sync=1"); e != nil || r.Code != 0 {
		t.Fatalf("mutate-update: %v / %s", e, r.Stderr)
	}

	rs := &fakeRestoreStream{}
	if err := svc.Restore(&pb.RestoreRequest{Kind: pb.BackupKind_BACKUP_KIND_DATABASE, S3: target, Database: d}, rs); err != nil {
		t.Fatalf("Restore rpc: %v", err)
	}
	if rr := rs.lastResult(t); !rr.GetOk() {
		t.Fatalf("clickhouse restore failed: %s", rr.GetError())
	}

	r, _ := ch("SELECT name FROM appdb.t WHERE id=1")
	r3, _ := ch("SELECT count() FROM appdb.t WHERE id=3")
	if !strings.Contains(r.Stdout, "sentinel-A") || strings.Contains(r.Stdout, "sentinel-B") {
		t.Fatalf("clickhouse restore did not overwrite id=1: %q", r.Stdout)
	}
	if !strings.Contains(strings.TrimSpace(r3.Stdout), "0") {
		t.Fatalf("clickhouse restore should have dropped the post-backup row, count=%q", r3.Stdout)
	}
	t.Log("clickhouse backup→mutate→restore OVERWRITE verified")
}

// TestE2E_VolumeArchiveRoundTrip exercises the project-backup volume machinery directly (archiveVolume → tar → gzip; gunzip → volumeStreams demux → extract) against a REAL docker named volume, proving the round-trip restores the bytes AND that the archiveVolume exit-code fix doesn't break the happy path.
func TestE2E_VolumeArchiveRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if !dockercli.Available(ctx) {
		t.Skip("docker not available")
	}
	svc := New(t.TempDir(), t.TempDir(), "/", "")

	vol := "deplo-e2e-vol"
	_, _ = dockercli.Run(ctx, 10*time.Second, "volume", "rm", "-f", vol)
	if res, err := dockercli.Run(ctx, 20*time.Second, "volume", "create", vol); err != nil || res.Code != 0 {
		t.Skipf("cannot create volume (%v / %s)", err, res.Stderr)
	}
	defer dockercli.Run(context.Background(), 15*time.Second, "volume", "rm", "-f", vol)

	if res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", vol+":/v", volumeHelperImage,
		"sh", "-c", "echo sentinel-data > /v/file.txt && mkdir -p /v/sub && echo nested > /v/sub/n.txt"); err != nil || res.Code != 0 {
		t.Fatalf("seed volume: %v / %s", err, res.Stderr)
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := svc.archiveVolume(ctx, vol, tw, &bkEmitter{send: func(*pb.BackupEvent) error { return nil }}); err != nil {
		t.Fatalf("archiveVolume: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Fatal("archive is empty")
	}

	if res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", vol+":/v", volumeHelperImage,
		"sh", "-c", "echo MUTATED > /v/file.txt && echo added > /v/added.txt"); err != nil || res.Code != 0 {
		t.Fatalf("mutate volume: %v / %s", err, res.Stderr)
	}

	rs := &fakeRestoreStream{}
	e := &rsEmitter{send: rs.Send}
	gzr, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.unpackProjectArchive(ctx, "irrelevant-slug", []string{vol}, gzr, e); err != nil {
		t.Fatalf("unpackProjectArchive: %v", err)
	}

	res, err := dockercli.Run(ctx, 30*time.Second, "run", "--rm", "-v", vol+":/v", volumeHelperImage,
		"sh", "-c", "cat /v/file.txt; echo ---; cat /v/sub/n.txt; echo ---; ls /v/added.txt 2>/dev/null || echo GONE")
	if err != nil || res.Code != 0 {
		t.Fatalf("inspect restored volume: %v / %s", err, res.Stderr)
	}
	if !strings.Contains(res.Stdout, "sentinel-data") {
		t.Errorf("restore did not bring back file.txt: %q", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "nested") {
		t.Errorf("restore did not bring back sub/n.txt: %q", res.Stdout)
	}
	if !strings.Contains(res.Stdout, "GONE") {
		t.Errorf("restore should have wiped the post-backup added.txt: %q", res.Stdout)
	}
	t.Log("volume archive→mutate→restore OVERWRITE verified")
}
