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
	// ponytail: an App's volumes (ticket #204) are not restored here yet, so such a start is refused, never run empty.
	volumes := dbStartOrder(3)
	volumes.Start.Restore.Request = json.RawMessage(`{"kind":"BACKUP_KIND_PROJECT"}`)
	if r := serve(t, f.g, volumes); r.Started || r.Error == "" {
		t.Fatalf("a volume restore was accepted: %+v", r)
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
