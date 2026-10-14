package server

import (
	"context"
	"slices"
	"strings"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

func collectRestore() (*rsEmitter, func() *pb.RestoreResult) {
	var res *pb.RestoreResult
	return &rsEmitter{send: func(ev *pb.RestoreEvent) error {
			if r := ev.GetResult(); r != nil {
				res = r
			}
			return nil
		}}, func() *pb.RestoreResult {
			return res
		}
}

func TestDatabaseBackupRestore_refuseAnotherProjectsContainer(t *testing.T) {
	argv := dockerOnPath(t, `[ "$1" = inspect ] && echo db_other; exit 0`)
	s := New(t.TempDir(), t.TempDir(), "/", "")
	d := &pb.DatabaseDescriptor{
		Container: "deplo-agent-test-db", DbType: "redis", DbName: "0", User: "default", ProjectId: "db_mine",
	}

	var bres *pb.BackupResult
	s.backupDatabase(context.Background(), d, &artifactDestination{stream: func([]byte) error { return nil }},
		&bkEmitter{send: func(ev *pb.BackupEvent) error {
			if r := ev.GetResult(); r != nil {
				bres = r
			}
			return nil
		}})
	if bres == nil || bres.GetOk() || !strings.Contains(bres.GetError(), "does not belong") {
		t.Fatalf("backup: want an ownership refusal, got %+v", bres)
	}
	e, result := collectRestore()
	s.restoreDatabase(context.Background(), d, &artifactSource{stream: strings.NewReader("")}, e)
	if r := result(); r == nil || r.GetOk() || !strings.Contains(r.GetError(), "does not belong") {
		t.Fatalf("restore: want an ownership refusal, got %+v", r)
	}

	calls := argv()
	for _, a := range calls {
		if strings.HasPrefix(a, "exec") {
			t.Fatalf("exec ran in a container of another project: %q", a)
		}
	}
	want := `inspect --type container -f {{index .Config.Labels "deplo.project"}} deplo-agent-test-db`
	if !slices.Contains(calls, want) {
		t.Fatalf("want %q, got %q", want, calls)
	}
}

// An older control plane sends no project_id; its backups keep working.
func TestDatabaseBackup_noProjectIDSkipsTheCheck(t *testing.T) {
	argv := dockerOnPath(t, "exit 1")
	s := New(t.TempDir(), t.TempDir(), "/", "")
	d := &pb.DatabaseDescriptor{Container: "deplo-agent-test-db", DbType: "redis"}
	e, _ := collectRestore()
	s.restoreDatabase(context.Background(), d, &artifactSource{stream: strings.NewReader("")}, e)
	for _, a := range argv() {
		if strings.HasPrefix(a, "inspect") {
			t.Fatalf("no project_id must mean no ownership check, got %q", a)
		}
	}
}
