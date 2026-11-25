package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/DeploCloud/deplo-agent/gen"
)

// c43: memos was stopped on eu-2 by a stale leader, and its empty volume there was kept as a 398 B backup.
func TestBackupProject_aVolumeWithNoFileIsNoBackup(t *testing.T) {
	for _, c := range []struct {
		name string
		file bool
	}{{"empty", false}, {"with a file", true}} {
		t.Run(c.name, func(t *testing.T) {
			data := t.TempDir()
			if c.file {
				if err := os.WriteFile(filepath.Join(data, "memos.db"), []byte("rows"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			dockerOnPath(t, `case "$*" in *"tar -C /v -cf - ."*) tar -cf - -C `+data+` . ;; esac`)
			var res *pb.BackupResult
			e := &bkEmitter{send: func(ev *pb.BackupEvent) error {
				if r := ev.GetResult(); r != nil {
					res = r
				}
				return nil
			}}
			p := &pb.ProjectDescriptor{Slug: "agent-test-web", VolumeNames: []string{"deplo-agent-test-web-data"}}
			(&Service{dataBase: t.TempDir()}).backupProject(context.Background(), p, &artifactDestination{stream: func([]byte) error { return nil }}, e)
			if res == nil || res.GetOk() != c.file {
				t.Fatalf("ok = %v, want %v (%s)", res.GetOk(), c.file, res.GetError())
			}
			if !c.file && res.GetError() != "the App's data holds no file on this server, so its archive is not a backup" {
				t.Fatalf("error = %q", res.GetError())
			}
		})
	}
}
