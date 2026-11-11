package server

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/DeploCloud/deplo-agent/gen"
	"github.com/DeploCloud/deplo-agent/internal/clusterlink/lease"
)

// The control plane writes the restore as the agent's own RestoreRequest in JSON; "later" is a field this agent does not know.
const dbRestoreJSON = `{"kind":"BACKUP_KIND_DATABASE","later":1,
	"s3":{"endpoint":"s3.example.com","bucket":"backups","accessKey":"AKIA","secretKey":"s3cr3t","objectKey":"t1/db_1/brun_1.dump.gz.age"},
	"database":{"container":"db-main","dbType":"postgres","dbName":"main","user":"app","password":"pgpass","projectId":"db_1"},
	"ageIdentity":"AGE-SECRET-KEY-1TEST","expectedSha256":"abc123"}`

func dbStartOrder(epoch uint64) lease.Order {
	return lease.Order{Cluster: "clu_1", Member: "srv_a", Epoch: epoch,
		Placement: []lease.Entry{{Kind: "database", ID: "db_1", Member: "srv_a", Slug: "db-main", SingleWriter: true}},
		Start: &lease.Start{DeploymentID: "rev_1", Slug: "db-main", ProjectID: "db_1", ComposeYAML: "services: {db-main: {image: postgres:17}}",
			Network: "deplo-env-e1", ComposeUpArgs: []string{"--force-recreate"},
			Restore: &lease.Restore{BackupID: "brun_1", TakenAt: "2026-03-01T11:45:00Z", Request: json.RawMessage(dbRestoreJSON)}}}
}

// failoverStubs answers the deploy, the restore and the stop without Docker, and records the order they ran in.
type failoverStubs struct {
	steps      []string
	deploys    []*pb.DeployRequest
	restores   []*pb.RestoreRequest
	deployErr  string
	restoreErr []error
}

func stubFailover(t *testing.T) *failoverStubs {
	t.Helper()
	st := &failoverStubs{}
	oldDeploy, oldRestore, oldStop, oldWait := runFailoverDeploy, runFailoverRestore, stopFailoverStack, failoverRestoreWait
	t.Cleanup(func() {
		runFailoverDeploy, runFailoverRestore, stopFailoverStack, failoverRestoreWait = oldDeploy, oldRestore, oldStop, oldWait
	})
	failoverRestoreWait = 0
	runFailoverDeploy = func(_ *Service, _ context.Context, req *pb.DeployRequest, e *emitter) {
		st.steps = append(st.steps, "deploy")
		st.deploys = append(st.deploys, req)
		e.result(st.deployErr == "", st.deployErr, "")
	}
	runFailoverRestore = func(_ *Service, _ context.Context, req *pb.RestoreRequest) error {
		st.steps = append(st.steps, "restore")
		st.restores = append(st.restores, req)
		if len(st.restoreErr) == 0 {
			return nil
		}
		err := st.restoreErr[0]
		st.restoreErr = st.restoreErr[1:]
		return err
	}
	stopFailoverStack = func(_ *Service, _ context.Context, slug string) {
		st.steps = append(st.steps, "stop "+slug)
	}
	return st
}

func TestFailover_aDatabaseIsBroughtUpThenRestoredFromTheBundlesBackup(t *testing.T) {
	st := stubFailover(t)
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	r := serve(t, f.g, dbStartOrder(2))
	if !r.OK || !r.Started || !r.Restored {
		t.Fatalf("reply %+v", r)
	}
	if got := strings.Join(st.steps, ","); got != "deploy,restore" {
		t.Fatalf("steps %q: the backup is restored into the database once it runs", got)
	}
	// A database is a stack with no image of its own to pull by name.
	if d := st.deploys[0]; d.GetSourceKind() != pb.SourceKind_SOURCE_KIND_COMPOSE || d.GetSlug() != "db-main" || d.GetNetwork() != "deplo-env-e1" {
		t.Fatalf("deploy request %+v", d)
	}
	req := st.restores[0]
	if req.GetKind() != pb.BackupKind_BACKUP_KIND_DATABASE || req.GetS3().GetSecretKey() != "s3cr3t" || req.GetS3().GetObjectKey() != "t1/db_1/brun_1.dump.gz.age" ||
		req.GetDatabase().GetPassword() != "pgpass" || req.GetDatabase().GetContainer() != "db-main" || req.GetDatabase().GetProjectId() != "db_1" ||
		req.GetAgeIdentity() != "AGE-SECRET-KEY-1TEST" || req.GetExpectedSha256() != "abc123" {
		t.Fatalf("restore request %+v", req)
	}
}

func TestFailover_whatTheRestoreCarriesIsNeverWrittenToThisMembersDisk(t *testing.T) {
	stubFailover(t)
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	if r := serve(t, f.g, dbStartOrder(2)); !r.Restored {
		t.Fatalf("reply %+v", r)
	}
	raw, err := os.ReadFile(f.file)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"s3cr3t", "pgpass", "AGE-SECRET-KEY", "AKIA"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("the lease file holds %q", secret)
		}
	}
}

func TestFailover_aStackWithAnImageOfItsOwnStillDeploysAsBefore(t *testing.T) {
	st := stubFailover(t)
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	if r := serve(t, f.g, startOrder(2)); !r.Started || r.Restored {
		t.Fatalf("reply %+v", r)
	}
	if st.deploys[0].GetSourceKind() != pb.SourceKind_SOURCE_KIND_IMAGE || len(st.restores) != 0 {
		t.Fatalf("deploy %+v, restores %d", st.deploys[0], len(st.restores))
	}
}

func TestFailover_aDatabaseWhoseRestoreFailsIsStoppedAndNotCountedAsStarted(t *testing.T) {
	st := stubFailover(t)
	for range failoverRestoreTries {
		st.restoreErr = append(st.restoreErr, errors.New("the backup does not match what Deplo wrote"))
	}
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	r := serve(t, f.g, dbStartOrder(2))
	if r.Started || r.Restored || r.OK || !strings.Contains(r.Error, "does not match") {
		t.Fatalf("reply %+v", r)
	}
	// An empty database must not stay up under its name.
	if got := strings.Join(st.steps, ","); got != "deploy"+strings.Repeat(",restore", failoverRestoreTries)+",stop db-main" {
		t.Fatalf("steps %q", got)
	}
}

func TestFailover_aRestoreIsTriedAgainWhileTheEngineStillBoots(t *testing.T) {
	st := stubFailover(t)
	st.restoreErr = []error{errors.New("the database system is starting up")}
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	if r := serve(t, f.g, dbStartOrder(2)); !r.Started || !r.Restored {
		t.Fatalf("reply %+v", r)
	}
	if got := strings.Join(st.steps, ","); got != "deploy,restore,restore" {
		t.Fatalf("steps %q", got)
	}
}

func TestFailover_nothingStartsWhenTheRestoreCannotBeReadOrTheStackFails(t *testing.T) {
	st := stubFailover(t)
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	bad := dbStartOrder(2)
	bad.Start.Restore.Request = json.RawMessage(`{"kind":`)
	if r := serve(t, f.g, bad); r.Started || r.Error == "" {
		t.Fatalf("an unreadable restore was started: %+v", r)
	}
	unknown := dbStartOrder(3)
	unknown.Start.Restore.Request = json.RawMessage(`{"kind":"BACKUP_KIND_UNSPECIFIED"}`)
	if r := serve(t, f.g, unknown); r.Started || r.Error == "" {
		t.Fatalf("a restore of no known kind was accepted: %+v", r)
	}
	if len(st.steps) != 0 {
		t.Fatalf("something ran: %v", st.steps)
	}
	st.deployErr = "no room on this disk"
	if r := serve(t, f.g, dbStartOrder(4)); r.Started || r.Restored || r.Error != "no room on this disk" {
		t.Fatalf("reply %+v", r)
	}
	if got := strings.Join(st.steps, ","); got != "deploy" {
		t.Fatalf("a restore ran into a stack that never came up: %q", got)
	}
}

func TestFailover_helloNamesTheRestoreCapability(t *testing.T) {
	for _, c := range Capabilities {
		if c == "cluster.failover.restore" {
			return
		}
	}
	t.Fatal("Hello does not name cluster.failover.restore")
}

func TestFailover_aStartWithARestoreMayTakeAsLongAsABackupStep(t *testing.T) {
	var left time.Duration
	f := newGuard(t)
	f.g.start = func(ctx context.Context, _ *lease.Start) error {
		if d, ok := ctx.Deadline(); ok {
			left = time.Until(d)
		}
		return nil
	}
	serve(t, f.g, dbStartOrder(1))
	if left <= lease.StartTimeout || left > lease.RestoreTimeout {
		t.Fatalf("the restore ran with %s left, want more than %s and at most %s", left, lease.StartTimeout, lease.RestoreTimeout)
	}
}

const volumeRestoreJSON = `{"kind":"BACKUP_KIND_PROJECT",
	"s3":{"endpoint":"s3.example.com","bucket":"backups","accessKey":"AKIA","secretKey":"s3cr3t","objectKey":"t1/prj_1/brun_2.tar.gz.age"},
	"project":{"slug":"web","volumeNames":["deplo-web-uploads"],"includeFiles":true,"network":"deplo-team-t1"},
	"ageIdentity":"AGE-SECRET-KEY-1TEST","expectedSha256":"abc123"}`

func volumeStartOrder(epoch uint64) lease.Order {
	o := startOrder(epoch)
	o.Placement[0].SingleWriter = true
	o.Start.Restore = &lease.Restore{BackupID: "brun_2", TakenAt: "2026-03-01T14:15:00Z", Request: json.RawMessage(volumeRestoreJSON)}
	return o
}

func TestFailover_anAppsVolumesAreRestoredBeforeItsStackStarts(t *testing.T) {
	st := stubFailover(t)
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	r := serve(t, f.g, volumeStartOrder(2))
	if !r.OK || !r.Started || !r.Restored {
		t.Fatalf("reply %+v", r)
	}
	// The App must never run on an empty Volume, so its data is there first.
	if got := strings.Join(st.steps, ","); got != "restore,deploy" {
		t.Fatalf("steps %q", got)
	}
	req := st.restores[0]
	if req.GetKind() != pb.BackupKind_BACKUP_KIND_PROJECT || req.GetProject().GetSlug() != "web" ||
		strings.Join(req.GetProject().GetVolumeNames(), ",") != "deplo-web-uploads" || !req.GetProject().GetIncludeFiles() {
		t.Fatalf("restore request %+v", req)
	}
	if d := st.deploys[0]; d.GetSlug() != "web" || d.GetImageRef() != "deplo/web:abc" {
		t.Fatalf("deploy request %+v", d)
	}
}

func TestFailover_anAppWhoseVolumeRestoreFailsNeverStarts(t *testing.T) {
	st := stubFailover(t)
	st.restoreErr = []error{errors.New("the backup does not match what Deplo wrote")}
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	r := serve(t, f.g, volumeStartOrder(2))
	if r.Started || r.Restored || !strings.Contains(r.Error, "does not match") {
		t.Fatalf("reply %+v", r)
	}
	if got := strings.Join(st.steps, ","); got != "restore" {
		t.Fatalf("steps %q: nothing starts on a Volume that was not restored", got)
	}
}

func TestFailover_anAppThatDoesNotComeUpOnItsRestoredVolumeIsStopped(t *testing.T) {
	st := stubFailover(t)
	st.deployErr = "the stack did not become ready"
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	if r := serve(t, f.g, volumeStartOrder(2)); r.Started || r.Error == "" {
		t.Fatalf("reply %+v", r)
	}
	// The leader tries another member next: no half-started copy stays behind on this one.
	if got := strings.Join(st.steps, ","); got != "restore,deploy,stop web" {
		t.Fatalf("steps %q", got)
	}
}

// The restore rides in the bundle, and the bundle is for one stack: a restore aimed at another one starts nothing.
func TestFailover_aRestoreAimedAtAnotherStackIsRefused(t *testing.T) {
	st := stubFailover(t)
	f := newGuard(t)
	f.g.start = (&Service{}).startFromBundle
	db := dbStartOrder(3)
	db.Start.Restore.Request = json.RawMessage(strings.Replace(dbRestoreJSON, `"projectId":"db_1"`, `"projectId":"db_other"`, 1))
	app := startOrder(3)
	app.Start.Restore = &lease.Restore{BackupID: "brun_2", Request: json.RawMessage(`{"kind":"BACKUP_KIND_PROJECT","project":{"slug":"another-app","volumeNames":["deplo-another-app-data"]}}`)}
	for name, o := range map[string]lease.Order{"a database": db, "an App": app} {
		if r := serve(t, f.g, o); r.Started || r.OK || !strings.Contains(r.Error, "another stack") {
			t.Errorf("%s: answered %+v", name, r)
		}
	}
	if len(st.steps) != 0 {
		t.Fatalf("a refused restore still ran %v", st.steps)
	}
}
